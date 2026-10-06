using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace SharedMemory;

// Minimal LiteSpeedLink memory channel over the kept SharedMemory ring (CircularBuffer = MMF + EventWaitHandle).
// Topology as RpcBuffer: "{name}_Host" ring written by every client, "{name}_c{id}" response ring per client.
// Differences: 12-byte frame, slot table + pooled IValueTaskSource (no Task/TCS/ConcurrentDictionary per call),
// one sweep timer for timeouts, no stats/legacy API, refcounted rings and a joined reader thread for Dispose.

delegate TOut MemReader<TOut>(object? state, ReadOnlySpan<byte> response);
delegate void MemItemHandler(object? state, ReadOnlySpan<byte> item);
delegate ValueTask MemHandler(MemRequest request);

// Request = unary; Stream = request whose body starts with an int32 credit mode (see MemClient.OpenStream);
// Credit/Cancel go client -> host for an open stream and are ignored once it ended.
enum Kind : byte { None, Request, Response, Error, Item, Close, Stream, Credit, Cancel }

[StructLayout(LayoutKind.Sequential, Pack = 1)]
struct Frame
{
    public const int Size = 12;
    public int Total;     // whole message length; packets carry Min(chunk, remaining)
    public uint Id;       // client-local call id (slot | generation)
    public ushort Client; // 0 = self-wake
    public Kind Kind;
    public byte Reserved;
}

/// <summary>A ring that may be written by several threads; unmapped only when its last writer leaves.</summary>
sealed class Peer(CircularBuffer ring)
{
    public readonly CircularBuffer Ring = ring;
    // In-process writers are serialized: NodeAvailable is AutoReset and wakes one waiter, others could sleep until timeout.
    public readonly Lock Gate = new();
    int _refs = 1; // owner reference, dropped by the owner on close

    public bool TryAcquire()
    {
        int r;
        do
        {
            r = Volatile.Read(ref _refs);
            if (r == 0) return false;
        }
        while (Interlocked.CompareExchange(ref _refs, r + 1, r) != r);
        return true;
    }

    public void Release()
    {
        if (Interlocked.Decrement(ref _refs) == 0) Ring.Dispose();
    }
}

abstract unsafe class MemEndpoint
{
    const int WriteTimeoutMs = 1000, ReadTimeoutMs = 1000;

    readonly CircularBuffer _in;
    readonly Lock _inGate = new();
    readonly Thread _reader;
    readonly Dictionary<ulong, (byte[] Buffer, int Received)> _partial = []; // reader thread only
    volatile bool _stopping;

    protected MemEndpoint(CircularBuffer input, string threadName)
    {
        _in = input;
        _reader = new Thread(ReadLoop) { IsBackground = true, Name = threadName };
    }

    protected void StartReader() => _reader.Start();

    /// <summary>Reader thread. <paramref name="payload"/> is only valid during the call; <paramref name="owned"/> (pooled) is handed over.</summary>
    protected abstract void OnMessage(in Frame frame, ReadOnlySpan<byte> payload, byte[]? owned);

    internal static int LiveReaders;

    void ReadLoop()
    {
        Interlocked.Increment(ref LiveReaders);
        Func<IntPtr, int> read = ReadPacket;
        while (!_stopping) _in.Read(read, ReadTimeoutMs);

        foreach (var (buffer, _) in _partial.Values) ArrayPool<byte>.Shared.Return(buffer);
        _partial.Clear();
        Interlocked.Decrement(ref LiveReaders);
    }

    int ReadPacket(IntPtr ptr)
    {
        byte* p = (byte*)ptr;
        var f = Unsafe.ReadUnaligned<Frame>(p);
        int chunk = _in.NodeBufferSize - Frame.Size;

        if (f.Kind == Kind.None) return Frame.Size; // aborted write
        if (f.Kind == Kind.Close && f.Client == 0)
        {
            _stopping = true;
            return Frame.Size;
        }

        if (f.Total <= chunk)
        {
            OnMessage(f, new ReadOnlySpan<byte>(p + Frame.Size, f.Total), null);
            return Frame.Size + f.Total;
        }

        // Multi-packet: packets of one message are in order, other messages may interleave (several writers).
        // ponytail: a writer dying mid-message leaks its partial buffer until Dispose.
        ulong key = (ulong)f.Client << 32 | f.Id;
        ref var asm = ref CollectionsMarshal.GetValueRefOrAddDefault(_partial, key, out bool exists);
        if (!exists) asm = (ArrayPool<byte>.Shared.Rent(f.Total), 0);

        int len = Math.Min(chunk, f.Total - asm.Received);
        new ReadOnlySpan<byte>(p + Frame.Size, len).CopyTo(asm.Buffer.AsSpan(asm.Received));
        asm.Received += len;

        if (asm.Received == f.Total)
        {
            var buffer = asm.Buffer;
            _partial.Remove(key);
            OnMessage(f, buffer.AsSpan(0, f.Total), buffer);
        }

        return Frame.Size + len;
    }

    /// <summary>Two-phase stop: flag, self-wake through the own ring, join. After this nothing reads the ring.</summary>
    protected void StopReader()
    {
        _stopping = true;
        try { Send(_in, _inGate, Kind.Close, 0, 0, 0, static (_, _) => { }); }
        catch (TimeoutException) { } // the reader re-checks the flag on its read timeout
        if (Thread.CurrentThread != _reader) _reader.Join();
    }

    [ThreadStatic] static NodeWriter? t_writer;

    internal static void Send<TState>(Peer peer, Kind kind, ushort client, uint id, TState state, Action<IBufferWriter<byte>, TState> write)
        => Send(peer.Ring, peer.Gate, kind, client, id, state, write);

    /// <summary>Serializes straight into the node; spills to a thread-static buffer only beyond one node.</summary>
    internal static void Send<TState>(CircularBuffer ring, Lock gate, Kind kind, ushort client, uint id, TState state, Action<IBufferWriter<byte>, TState> write)
    {
        if (ring.ShuttingDown) throw new InvalidOperationException("Peer closed.");

        lock (gate) SendLocked(ring, kind, client, id, state, write);
    }

    static void SendLocked<TState>(CircularBuffer ring, Kind kind, ushort client, uint id, TState state, Action<IBufferWriter<byte>, TState> write)
    {
        var w = t_writer ??= new NodeWriter();
        var first = new First<TState>(w, new Frame { Kind = kind, Client = client, Id = id }, ring.NodeBufferSize - Frame.Size, state, write);

        if (ring.Write(ref first, static (IntPtr ptr, ref First<TState> c) => c.Run((byte*)ptr), WriteTimeoutMs) == 0)
            throw new TimeoutException("Shared-memory ring full.");
        if (first.Error is { } error) ExceptionDispatchInfo.Throw(error);
        if (!w.Spilled) return;

        var rest = new Rest(w, first.Header, first.Chunk);
        for (rest.Offset = first.Chunk; rest.Offset < first.Header.Total; rest.Offset += first.Chunk)
            if (ring.Write(ref rest, static (IntPtr ptr, ref Rest r) => r.Run((byte*)ptr), WriteTimeoutMs) == 0)
                throw new TimeoutException("Shared-memory ring full.");
    }

    struct First<TState>(NodeWriter w, Frame header, int chunk, TState state, Action<IBufferWriter<byte>, TState> write)
    {
        public Frame Header = header;
        public readonly int Chunk = chunk;
        public Exception? Error;

        public int Run(byte* p)
        {
            w.Reset(p + Frame.Size, Chunk);
            try
            {
                write(w, state);
            }
            catch (Exception ex)
            {
                // The node is already reserved: publish it as Kind.None, which the reader skips.
                Error = ex;
                Unsafe.WriteUnaligned(p, default(Frame));
                return Frame.Size;
            }

            Header.Total = w.Written;
            int first = Math.Min(Header.Total, Chunk);
            if (w.Spilled) w.SpillData[..first].CopyTo(new Span<byte>(p + Frame.Size, first));
            Unsafe.WriteUnaligned(p, Header);
            return Frame.Size + first;
        }
    }

    struct Rest(NodeWriter w, Frame header, int chunk)
    {
        public int Offset;

        public int Run(byte* p)
        {
            int len = Math.Min(chunk, header.Total - Offset);
            Unsafe.WriteUnaligned(p, header);
            w.SpillData.Slice(Offset, len).CopyTo(new Span<byte>(p + Frame.Size, len));
            return Frame.Size + len;
        }
    }

    sealed class NodeWriter : IBufferWriter<byte>
    {
        byte* _dst;
        int _cap, _written;
        ArrayBufferWriter<byte>? _spill;

        public bool Spilled { get; private set; }
        public int Written => Spilled ? _spill!.WrittenCount : _written;
        public ReadOnlySpan<byte> SpillData => _spill!.WrittenSpan;

        public void Reset(byte* dst, int cap)
        {
            _dst = dst;
            _cap = cap;
            _written = 0;
            _spill?.ResetWrittenCount();
            Spilled = false;
        }

        public void Advance(int count)
        {
            if (Spilled) _spill!.Advance(count);
            else _written += count;
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            if (!Spilled && _cap - _written >= Math.Max(sizeHint, 1)) return new Span<byte>(_dst + _written, _cap - _written);
            Spill();
            return _spill!.GetSpan(sizeHint);
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            Spill();
            return _spill!.GetMemory(sizeHint);
        }

        void Spill()
        {
            if (Spilled) return;
            _spill ??= new ArrayBufferWriter<byte>(Math.Max(_cap * 2, 256));
            new ReadOnlySpan<byte>(_dst, _written).CopyTo(_spill.GetSpan(_written));
            _spill.Advance(_written);
            Spilled = true;
        }
    }
}

sealed class MemClient : MemEndpoint, IDisposable
{
    const int SlotBits = 12, Slots = 1 << SlotBits, Mask = Slots - 1, SweepMs = 100;

    readonly Peer _host;
    readonly CircularBuffer _own;
    readonly ushort _id;
    readonly Pending?[] _slots = new Pending?[Slots];
    readonly int[] _free = new int[Slots];
    readonly Lock _gate = new(); // ponytail: one lock for slots/free list/sweep; CAS + generation if it ever contends
    readonly Timer _sweep;
    int _freeCount = Slots, _disposed;
    uint _gen;

    MemClient(CircularBuffer host, CircularBuffer own, ushort id) : base(own, "MemClient#" + id)
    {
        _host = new Peer(host);
        _own = own;
        _id = id;
        for (int i = 0; i < Slots; i++) _free[i] = Slots - 1 - i;
        _sweep = new Timer(static s => ((MemClient)s!).Sweep(), this, SweepMs, SweepMs);
        StartReader();
    }

    public static MemClient Connect(string name)
    {
        var host = new CircularBuffer(name + "_Host");
        try
        {
            ushort id = checked((ushort)host.NextSequence()); // ponytail: 65535 connections per host lifetime
            return new MemClient(host, new CircularBuffer(name + "_c" + id, host.NodeCount, host.NodeBufferSize), id);
        }
        catch
        {
            host.Dispose();
            throw;
        }
    }

    internal int InFlight
    {
        get { lock (_gate) return Slots - _freeCount; }
    }

    /// <summary>False once disposed or once the host went away: the caller reconnects.</summary>
    public bool IsAlive
    {
        get
        {
            if (_disposed != 0 || !_host.TryAcquire()) return false;
            try { return !_host.Ring.ShuttingDown; }
            finally { _host.Release(); }
        }
    }

    public TOut Call<TState, TOut>(TState state, Action<IBufferWriter<byte>, TState> write, MemReader<TOut> read, object? readState = null, int timeoutMs = 30_000, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var p = Begin(read, readState, null, null, timeoutMs, cancellationToken);
        Post(p, Kind.Request, 0, state, write);
        return p.Wait();
    }

    public ValueTask<TOut> CallAsync<TState, TOut>(TState state, Action<IBufferWriter<byte>, TState> write, MemReader<TOut> read, object? readState = null, int timeoutMs = 30_000, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested) return ValueTask.FromCanceled<TOut>(cancellationToken);
        var p = Begin(read, readState, null, null, timeoutMs, cancellationToken);
        Post(p, Kind.Request, 0, state, write);
        return new ValueTask<TOut>(p, p.Version);
    }

    /// <summary>Items arrive on the reader thread (span valid during the callback). Modes as in <see cref="OpenStream"/>.</summary>
    public TOut Stream<TState, TOut>(TState state, Action<IBufferWriter<byte>, TState> write, MemItemHandler onItem, object? itemState, MemReader<TOut> read, object? readState = null, int credits = 0, int idleTimeoutMs = 30_000)
    {
        var p = Begin(read, readState, onItem, itemState, idleTimeoutMs, default);
        Post(p, Kind.Stream, credits, state, write);
        return p.Wait();
    }

    public ValueTask<TOut> StreamAsync<TState, TOut>(TState state, Action<IBufferWriter<byte>, TState> write, MemItemHandler onItem, object? itemState, MemReader<TOut> read, object? readState = null, int credits = 0, int idleTimeoutMs = 30_000, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested) return ValueTask.FromCanceled<TOut>(cancellationToken);
        return OpenStream(state, write, onItem, itemState, read, readState, credits, idleTimeoutMs, cancellationToken).Completion;
    }

    /// <summary>
    /// <paramref name="credits"/> (fixed at compile time per operation): 0 = server-sized, the host pushes until its final response;
    /// N &gt; 0 = window, the host sends at most N items ahead of <see cref="MemStream{TOut}.Grant"/>; -N = client-sized, the host stops after N items.
    /// <paramref name="idleTimeoutMs"/> is a watchdog re-armed by every item; <see cref="Timeout.Infinite"/> for open streams that may stay silent.
    /// Any local end (cancel, watchdog, item callback fault) sends Cancel so the host producer stops too.
    /// </summary>
    public MemStream<TOut> OpenStream<TState, TOut>(TState state, Action<IBufferWriter<byte>, TState> write, MemItemHandler onItem, object? itemState, MemReader<TOut> read, object? readState = null, int credits = 0, int idleTimeoutMs = 30_000, CancellationToken cancellationToken = default)
    {
        var p = Begin(read, readState, onItem, itemState, idleTimeoutMs, cancellationToken);
        var stream = new MemStream<TOut>(this, p); // id and version captured before the request can complete
        Post(p, Kind.Stream, credits, state, write);
        return stream;
    }

    Pending<TOut> Begin<TOut>(MemReader<TOut> read, object? readState, MemItemHandler? onItem, object? itemState, int timeoutMs, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        var p = Pending<TOut>.Rent();
        long span = timeoutMs == Timeout.Infinite ? 0 : timeoutMs * Stopwatch.Frequency / 1000;
        p.Init(this, read, readState, onItem, itemState, span == 0 ? 0 : Stopwatch.GetTimestamp() + span);
        p.Idle = onItem is null ? 0 : span; // streams: the timeout is an idle watchdog re-armed by each item

        lock (_gate)
        {
            if (_freeCount == 0) throw new InvalidOperationException("Too many calls in flight.");
            int slot = _free[--_freeCount];
            p.Id = ++_gen << SlotBits | (uint)slot;
            _slots[slot] = p;
        }

        if (cancellationToken.CanBeCanceled)
            p.Register(cancellationToken.UnsafeRegister(static s =>
            {
                var pending = (Pending)s!;
                pending.Owner.Abort(pending, pending.Id, new OperationCanceledException()); // Recycle waits for this callback
            }, p));

        return p;
    }

    void Post<TState>(Pending p, Kind kind, int credits, TState state, Action<IBufferWriter<byte>, TState> write)
    {
        uint id = p.Id; // p may complete and recycle as soon as the request is in the ring
        if (!_host.TryAcquire())
        {
            Abort(p, id, new ObjectDisposedException(nameof(MemClient)));
            return;
        }

        try
        {
            if (kind == Kind.Request)
            {
                Send(_host, Kind.Request, _id, id, state, write);
                return;
            }

            Send(_host, Kind.Stream, _id, id, (credits, state, write), static (w, s) =>
            {
                BinaryPrimitives.WriteInt32LittleEndian(w.GetSpan(4), s.credits);
                w.Advance(4);
                s.write(w, s.state);
            });

            bool live;
            lock (_gate) live = ReferenceEquals(_slots[id & Mask], p) && p.Id == id;
            if (!live) Control(Kind.Cancel, id, 0); // ended before the request was out: its Cancel reached the host first
        }
        catch (Exception ex)
        {
            Abort(p, id, ex);
        }
        finally
        {
            _host.Release();
        }
    }

    internal void Abort(Pending p, uint id, Exception error)
    {
        bool stream;
        lock (_gate)
        {
            if (p.Id != id || !Release(p)) return;
            stream = p.OnItem is not null;
        }

        p.Fail(error);
        if (stream) Control(Kind.Cancel, id, 0); // the host producer stops too
    }

    /// <summary>Best-effort Credit/Cancel; a dead host has nothing left to stop.</summary>
    internal void Control(Kind kind, uint id, int value)
    {
        if (!_host.TryAcquire()) return;
        try
        {
            Send(_host, kind, _id, id, value, static (w, v) =>
            {
                if (v == 0) return; // Cancel: header only
                BinaryPrimitives.WriteInt32LittleEndian(w.GetSpan(4), v);
                w.Advance(4);
            });
        }
        catch { }
        finally
        {
            _host.Release();
        }
    }

    bool Release(Pending p) // under _gate
    {
        int slot = (int)(p.Id & Mask);
        if (!ReferenceEquals(_slots[slot], p)) return false;
        _slots[slot] = null;
        _free[_freeCount++] = slot;
        return true;
    }

    protected override void OnMessage(in Frame frame, ReadOnlySpan<byte> payload, byte[]? owned)
    {
        try
        {
            Pending? p;
            MemItemHandler? onItem = null;
            object? itemState = null;
            lock (_gate)
            {
                p = _slots[frame.Id & Mask];
                if (p is null || p.Id != frame.Id) return; // late reply of a timed-out/cancelled call
                if (frame.Kind == Kind.Item)
                {
                    (onItem, itemState) = (p.OnItem, p.ItemState);
                    if (p.Idle != 0) p.Deadline = Stopwatch.GetTimestamp() + p.Idle;
                }
                else Release(p);
            }

            if (frame.Kind != Kind.Item)
                p.Complete(frame.Kind == Kind.Error, payload);
            else
            {
                // A concurrent cancel may complete p meanwhile: the snapshot keeps the callback valid, the item is just unused.
                try { onItem!(itemState, payload); }
                catch (Exception ex) { Abort(p, frame.Id, ex); }
            }
        }
        finally
        {
            if (owned is not null) ArrayPool<byte>.Shared.Return(owned);
        }
    }

    // ponytail: timeout resolution = SweepMs and an O(Slots) scan per tick; a timer wheel if that ever matters.
    void Sweep()
    {
        if (!_host.TryAcquire()) return;
        try
        {
            bool hostGone = _host.Ring.ShuttingDown;
            long now = Stopwatch.GetTimestamp();
            List<uint>? cancel = null; // allocates only when a stream times out
            lock (_gate)
            {
                for (int s = 0; s < Slots; s++)
                {
                    if (_slots[s] is not { } p || !hostGone && (p.Deadline == 0 || now < p.Deadline)) continue;
                    Release(p);
                    if (p.OnItem is not null && !hostGone) (cancel ??= []).Add(p.Id);
                    p.Fail(hostGone ? new InvalidOperationException("Host closed.") : new TimeoutException());
                }
            }

            if (cancel is not null)
                foreach (uint id in cancel) Control(Kind.Cancel, id, 0);
        }
        finally
        {
            _host.Release();
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        _sweep.Dispose();

        if (_host.TryAcquire())
        {
            try
            {
                if (!_host.Ring.ShuttingDown) Send(_host, Kind.Close, _id, 0, 0, static (_, _) => { });
            }
            catch { } // best effort: a dead host has nothing to release
            finally
            {
                _host.Release();
            }
        }

        _host.Release(); // unmapped once in-flight writers leave
        StopReader();

        lock (_gate)
        {
            for (int s = 0; s < Slots; s++)
            {
                if (_slots[s] is not { } p) continue;
                Release(p);
                p.Fail(new ObjectDisposedException(nameof(MemClient)));
            }
        }

        _own.Dispose();
    }
}

/// <summary>Handle of an open stream: completion, credit grants (window mode) and cancellation.</summary>
readonly struct MemStream<TOut>
{
    readonly MemClient _client;
    readonly Pending<TOut> _pending;
    readonly uint _id;
    readonly short _version;

    internal MemStream(MemClient client, Pending<TOut> pending)
        => (_client, _pending, _id, _version) = (client, pending, pending.Id, pending.Version);

    public ValueTask<TOut> Completion => new(_pending, _version);

    /// <summary>Lets the host send <paramref name="credits"/> more items; call as the consumer drains, not on arrival.</summary>
    public void Grant(int credits) => _client.Control(Kind.Credit, _id, credits);

    public void Cancel() => _client.Abort(_pending, _id, new OperationCanceledException());
}

/// <summary>
/// Pull side of a stream for ONE async consumer, passed as item state with <see cref="Append"/>. The reader thread appends
/// each item as <c>[int32 len][payload]</c>; the consumer waits once and takes everything buffered (two swapped pooled
/// buffers, one reusable waiter). The pending call signals the end (reply, error, watchdog, cancel), so no Channel, no
/// completion Task and no allocation per item or per wait. Rent/Dispose pool the instance with its warm buffers.
/// </summary>
// Reuse is safe only after an end on the reader thread (reply): items of a call are dispatched on that same thread,
// so none can be in flight. Ends from other threads (cancel, watchdog, dispose) may race a snapshotted Add: dropped.
// ponytail: unbounded like the Channel it replaces; window credits are the backpressure knob.
sealed class MemPull : IValueTaskSource, IDisposable
{
    public static readonly MemItemHandler Append = static (s, item) => ((MemPull)s!).Add(item, framed: true);

    /// <summary>For items that already are <c>[int32 len][payload]</c> records (batched lots): copied as they are.</summary>
    public static readonly MemItemHandler AppendRecords = static (s, item) => ((MemPull)s!).Add(item, framed: false);

    // ponytail: retained memory = pooled instances x 2 buffers of up to KeepBytes; trim the pool if that ever matters.
    const int KeepBytes = 256 * 1024;
    static readonly Stack<MemPull> s_pool = new();

    readonly Lock _gate = new();
    ManualResetValueTaskSourceCore<bool> _signal = new() { RunContinuationsAsynchronously = true };
    byte[] _back = [], _front = [];
    int _backLen;
    bool _ended, _clean, _waiting, _disposed;

    public static MemPull Rent()
    {
        lock (s_pool)
            if (s_pool.TryPop(out var pull)) return pull;
        return new();
    }

    void Add(ReadOnlySpan<byte> item, bool framed)
    {
        if (!framed && item.IsEmpty) return; // an empty Take means "ended"
        lock (_gate)
        {
            if (_disposed) return;
            int head = framed ? sizeof(int) : 0;
            int need = _backLen + head + item.Length;
            if (need > _back.Length) Grow(ref _back, _backLen, need);
            if (framed) BinaryPrimitives.WriteInt32LittleEndian(_back.AsSpan(_backLen), item.Length);
            item.CopyTo(_back.AsSpan(_backLen + head));
            _backLen = need;
            if (!_waiting) return;
            _waiting = false;
        }
        _signal.SetResult(true);
    }

    /// <param name="clean">Ended on the reader thread: no stale item can follow, the instance may be reused.</param>
    internal void End(bool clean)
    {
        lock (_gate)
        {
            _ended = true;
            _clean = clean;
            if (!_waiting) return;
            _waiting = false;
        }
        _signal.SetResult(true);
    }

    /// <summary>Completes when items are buffered or the stream ended; then <see cref="Take"/>.</summary>
    public ValueTask WaitAsync()
    {
        lock (_gate)
        {
            if (_backLen > 0 || _ended) return default;
            _signal.Reset();
            _waiting = true;
            return new(this, _signal.Version);
        }
    }

    /// <summary>Everything buffered as <c>[int32 len][payload]</c> records, valid until the next call; empty = ended.</summary>
    public ReadOnlyMemory<byte> Take()
    {
        lock (_gate)
        {
            (_front, _back) = (_back, _front);
            int len = _backLen;
            _backLen = 0;
            return _front.AsMemory(0, len);
        }
    }

    static void Grow(ref byte[] buffer, int used, int need)
    {
        var next = ArrayPool<byte>.Shared.Rent(Math.Max(need, 4096));
        buffer.AsSpan(0, used).CopyTo(next);
        if (buffer.Length > 0) ArrayPool<byte>.Shared.Return(buffer);
        buffer = next;
    }

    public void Dispose()
    {
        bool reuse;
        lock (_gate)
        {
            if (_disposed) return;
            reuse = _clean;
            _disposed = !reuse;
            _ended = _clean = _waiting = false;
            _backLen = 0;
        }

        if (reuse)
        {
            Trim(ref _back);
            Trim(ref _front);
            lock (s_pool) s_pool.Push(this);
            return;
        }

        if (_back.Length > 0) ArrayPool<byte>.Shared.Return(_back);
        if (_front.Length > 0) ArrayPool<byte>.Shared.Return(_front);
        _back = _front = [];
    }

    static void Trim(ref byte[] buffer)
    {
        if (buffer.Length <= KeepBytes) return;
        ArrayPool<byte>.Shared.Return(buffer);
        buffer = [];
    }

    void IValueTaskSource.GetResult(short token) => _signal.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _signal.GetStatus(token);
    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _signal.OnCompleted(continuation, state, token, flags);
}

/// <summary>
/// Pooled <see cref="IAsyncEnumerator{T}"/> over a <see cref="MemPull"/>: items are decoded on the consumer thread from
/// <c>[int32 len][item]</c> records, MoveNextAsync completes synchronously while records are buffered and otherwise
/// through this instance (no async state machine, no Channel). Rented per enumeration, recycled by DisposeAsync.
/// Ends with the final response (<see cref="Result"/>) or rethrows the stream fault; disposing early cancels the host producer.
/// </summary>
sealed class MemPullEnumerator<TItem, TEnd> : IAsyncEnumerator<TItem>, IValueTaskSource<bool>
{
    static readonly Stack<MemPullEnumerator<TItem, TEnd>> s_pool = new();

    readonly Action _onWake;
    ManualResetValueTaskSourceCore<bool> _core; // wakes already arrive on the pool (MemPull runs them asynchronously)
    MemPull _pull = null!;
    MemStream<TEnd> _stream;
    MemReader<TItem> _read = null!;
    object? _readState;
    CancellationTokenSource? _linked;
    ReadOnlyMemory<byte> _lot;
    ValueTask _wait;
    bool _done;

    MemPullEnumerator() => _onWake = OnWake;

    public TItem Current { get; private set; } = default!;

    /// <summary>Final response, once MoveNextAsync returned false.</summary>
    public TEnd Result { get; private set; } = default!;

    /// <param name="records">Item frames already are <c>[int32 len][item]</c> lots (production batching); false = one item per frame.</param>
    /// <param name="enumeratorToken">The <c>GetAsyncEnumerator</c> token, linked with <paramref name="cancellationToken"/> only when both can cancel.</param>
    public static MemPullEnumerator<TItem, TEnd> Open<TState>(MemClient client, TState state, Action<IBufferWriter<byte>, TState> write, MemReader<TItem> read, MemReader<TEnd> readEnd, object? readState, bool records, int credits = 0, int idleTimeoutMs = 30_000, CancellationToken cancellationToken = default, CancellationToken enumeratorToken = default)
    {
        MemPullEnumerator<TItem, TEnd>? e;
        lock (s_pool) s_pool.TryPop(out e);
        e ??= new();
        e._pull = MemPull.Rent();
        e._read = read;
        e._readState = readState;

        var token = cancellationToken;
        if (enumeratorToken.CanBeCanceled && enumeratorToken != token)
        {
            if (!token.CanBeCanceled) token = enumeratorToken;
            else token = (e._linked = CancellationTokenSource.CreateLinkedTokenSource(token, enumeratorToken)).Token;
        }

        try
        {
            e._stream = client.OpenStream(state, write, records ? MemPull.AppendRecords : MemPull.Append, e._pull, readEnd, readState, credits, idleTimeoutMs, token);
        }
        catch
        {
            e._done = true; // nothing was opened: nothing to cancel
            e.Recycle();
            throw;
        }
        return e;
    }

    public ValueTask<bool> MoveNextAsync()
    {
        _core.Reset();
        return Step(woke: false) is { } more ? new(more) : new(this, _core.Version);
    }

    /// <summary>true = item, false = ended, null = waiting (OnWake resumes).</summary>
    bool? Step(bool woke)
    {
        while (true)
        {
            if (woke)
            {
                woke = false;
                _wait.GetAwaiter().GetResult();
                _lot = _pull.Take();
                if (_lot.IsEmpty)
                {
                    _done = true;
                    Result = _stream.Completion.GetAwaiter().GetResult(); // set before the pull ended; rethrows its fault
                    return false;
                }
            }

            if (!_lot.IsEmpty)
            {
                var s = _lot.Span;
                int len = BinaryPrimitives.ReadInt32LittleEndian(s);
                Current = _read(_readState, s.Slice(sizeof(int), len));
                _lot = _lot[(sizeof(int) + len)..];
                return true;
            }

            if (_done) return false;

            _wait = _pull.WaitAsync();
            if (!_wait.IsCompleted)
            {
                _wait.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(_onWake);
                return null;
            }
            woke = true;
        }
    }

    void OnWake()
    {
        bool? more;
        try
        {
            more = Step(woke: true);
        }
        catch (Exception ex)
        {
            _core.SetException(ex);
            return;
        }
        if (more is { } m) _core.SetResult(m);
    }

    public ValueTask DisposeAsync()
    {
        if (!_done)
        {
            _done = true;
            _stream.Cancel(); // abandoned early: the host producer stops
            var end = _stream.Completion;
            if (end.IsCompleted)
                try { end.GetAwaiter().GetResult(); } catch { } // recycles the pending
            // else a reply racing the cancel completes it later: that pending is left to the GC
        }
        Recycle();
        return default;
    }

    void Recycle()
    {
        _pull.Dispose(); // pooled only if the reply ended it
        _linked?.Dispose();
        _linked = null;
        _pull = null!;
        _stream = default;
        _read = null!;
        _readState = null;
        _lot = default;
        _wait = default;
        _done = false;
        Current = default!;
        Result = default!;
        lock (s_pool) s_pool.Push(this);
    }

    bool IValueTaskSource<bool>.GetResult(short token) => _core.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) => _core.GetStatus(token);
    void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);
}

abstract class Pending
{
    public uint Id;
    public long Deadline;
    public long Idle; // stream watchdog in Stopwatch ticks, 0 = none
    public MemClient Owner = null!;
    public MemItemHandler? OnItem;
    public object? ItemState;

    public abstract void Complete(bool error, ReadOnlySpan<byte> payload);
    public abstract void Fail(Exception error);
}

/// <summary>Pooled per TOut; returned to the pool by GetResult unless a cancellation callback may still hold it.</summary>
sealed class Pending<TOut> : Pending, IValueTaskSource<TOut>
{
    static readonly Stack<Pending<TOut>> s_pool = new();

    ManualResetValueTaskSourceCore<TOut> _core = new() { RunContinuationsAsynchronously = true };
    MemReader<TOut> _read = null!;
    object? _readState;
    CancellationTokenRegistration _ctr;
    bool _registered;
    int _syncWaiter;

    public short Version => _core.Version;

    public static Pending<TOut> Rent()
    {
        lock (s_pool)
            if (s_pool.TryPop(out var p)) return p;
        return new();
    }

    public void Init(MemClient owner, MemReader<TOut> read, object? readState, MemItemHandler? onItem, object? itemState, long deadline)
    {
        Owner = owner;
        _read = read;
        _readState = readState;
        OnItem = onItem;
        ItemState = itemState;
        Deadline = deadline;
    }

    public void Register(CancellationTokenRegistration ctr)
    {
        _ctr = ctr;
        _registered = true;
    }

    public override void Complete(bool error, ReadOnlySpan<byte> payload)
    {
        if (error)
        {
            Fail(new InvalidOperationException(Encoding.UTF8.GetString(payload)));
            return;
        }

        TOut value;
        try
        {
            value = _read(_readState, payload);
        }
        catch (Exception ex)
        {
            Fail(ex);
            return;
        }

        var pull = ItemState as MemPull; // read before an async consumer can recycle `this`
        _core.SetResult(value);
        Signal();
        pull?.End(clean: true); // Complete runs on the reader thread
    }

    public override void Fail(Exception error)
    {
        var pull = ItemState as MemPull;
        _core.SetException(error);
        Signal();
        pull?.End(clean: false); // may race an item already snapshotted by the reader thread
    }

    // Dekker pair with Wait: SetResult publishes through an interlocked op on the core, Wait through _syncWaiter.
    // An async consumer may already have recycled `this` here: a spurious pulse is harmless (Wait re-checks the status).
    void Signal()
    {
        if (Volatile.Read(ref _syncWaiter) != 0)
            lock (this) Monitor.PulseAll(this);
    }

    bool IsPending => _core.GetStatus(_core.Version) == ValueTaskSourceStatus.Pending;

    public TOut Wait()
    {
        var spin = new SpinWait();
        while (IsPending && !spin.NextSpinWillYield) spin.SpinOnce();

        if (IsPending)
        {
            Interlocked.Exchange(ref _syncWaiter, 1);
            lock (this)
                while (IsPending) Monitor.Wait(this);
        }

        return GetResult(_core.Version);
    }

    public TOut GetResult(short token)
    {
        try
        {
            return _core.GetResult(token);
        }
        finally
        {
            Recycle();
        }
    }

    void Recycle()
    {
        if (_registered && !_ctr.Unregister()) return; // callback ran or is running: let the GC take this instance

        _core.Reset();
        _read = null!;
        _readState = ItemState = null;
        OnItem = null;
        _ctr = default;
        _registered = false;
        _syncWaiter = 0;
        Owner = null!;
        lock (s_pool) s_pool.Push(this);
    }

    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);
}

sealed class MemHost : MemEndpoint, IDisposable
{
    readonly string _name;
    readonly CircularBuffer _ring;
    readonly ConcurrentDictionary<ushort, Peer> _peers = new();
    readonly Dictionary<ulong, MemRequest> _streams = []; // live streams by (client, id); Finish removes before recycling
    readonly Lock _streamsGate = new();
    int _disposed;

    internal readonly MemHandler Handler;

    MemHost(string name, CircularBuffer ring, MemHandler handler) : base(ring, "MemHost")
    {
        _name = name;
        _ring = ring;
        Handler = handler;
        StartReader();
    }

    public static MemHost Start(string name, MemHandler handler, int nodeSize = 50_000, int nodeCount = 10)
        => new(name, new CircularBuffer(name + "_Host", nodeCount, nodeSize), handler);

    protected override void OnMessage(in Frame frame, ReadOnlySpan<byte> payload, byte[]? owned)
    {
        ulong key = (ulong)frame.Client << 32 | frame.Id;
        switch (frame.Kind)
        {
            case Kind.Close:
                CancelStreams(frame.Client); // its producers stop instead of writing to a dead ring
                if (_peers.TryRemove(frame.Client, out var gone)) gone.Release();
                break;

            case Kind.Credit or Kind.Cancel:
                lock (_streamsGate) // the request cannot be recycled while registered
                    if (_streams.TryGetValue(key, out var s))
                    {
                        if (frame.Kind == Kind.Cancel) s.CancelStream();
                        else if (payload.Length >= 4) s.AddCredits(BinaryPrimitives.ReadInt32LittleEndian(payload));
                    }
                break; // unknown key: the stream already ended

            case Kind.Request or Kind.Stream when payload.Length >= (frame.Kind == Kind.Stream ? 4 : 0) && GetPeer(frame.Client) is { } peer && peer.TryAcquire():
            {
                var buffer = owned ?? ArrayPool<byte>.Shared.Rent(Math.Max(1, payload.Length));
                if (owned is null) payload.CopyTo(buffer);
                var request = MemRequest.Rent(this, peer, frame.Client, frame.Id, buffer, payload.Length);
                if (frame.Kind == Kind.Stream)
                {
                    request.BeginStream(key, BinaryPrimitives.ReadInt32LittleEndian(payload));
                    lock (_streamsGate) _streams[key] = request;
                }

                ThreadPool.UnsafeQueueUserWorkItem(request, preferLocal: false);
                return;
            }
        }

        if (owned is not null) ArrayPool<byte>.Shared.Return(owned);
    }

    void CancelStreams(ushort? client)
    {
        lock (_streamsGate)
            foreach (var (key, s) in _streams)
                if (client is null || (ushort)(key >> 32) == client) s.CancelStream();
    }

    internal void EndStream(ulong key)
    {
        lock (_streamsGate) _streams.Remove(key);
    }

    Peer? GetPeer(ushort client)
    {
        if (_peers.TryGetValue(client, out var peer)) return peer;

        try
        {
            return _peers[client] = new Peer(new CircularBuffer(_name + "_c" + client)); // only the reader thread adds
        }
        catch (IOException)
        {
            return null; // client already gone
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        StopReader();
        CancelStreams(null);
        foreach (var key in _peers.Keys)
            if (_peers.TryRemove(key, out var peer)) peer.Release(); // in-flight handlers keep their ring mapped

        _ring.Dispose(); // owner: raises Shutdown, clients fail their calls on the next sweep
    }
}

/// <summary>Pooled request: payload is pooled and valid until the handler (sync or async) completes.</summary>
sealed class MemRequest : IThreadPoolWorkItem
{
    static readonly Stack<MemRequest> s_pool = new();

    MemHost _host = null!;
    Peer _peer = null!;
    byte[] _buffer = null!;
    int _offset, _length, _replied;
    ushort _client;
    uint _id;

    // Stream state (unary requests leave it untouched).
    ulong _key;
    bool _stream;
    int _mode, _takeLeft, _cancelled;
    SemaphoreSlim? _window;
    CancellationTokenSource? _cts;

    public ReadOnlySpan<byte> Payload => _buffer.AsSpan(_offset, _length);
    public ReadOnlyMemory<byte> PayloadMemory => _buffer.AsMemory(_offset, _length);

    /// <summary>Set when the client cancels, its watchdog fires, it disconnects or the host stops.</summary>
    public bool IsCancelled => Volatile.Read(ref _cancelled) != 0;

    /// <summary>Created on first use only (async producers that await something else).</summary>
    public CancellationToken Aborted
    {
        get
        {
            if (!_stream) return default;
            if (_cts is null)
            {
                var cts = new CancellationTokenSource();
                if (Interlocked.CompareExchange(ref _cts, cts, null) is not null) cts.Dispose();
                else if (IsCancelled) cts.Cancel(); // cancelled before the token existed
            }
            return _cts!.Token;
        }
    }

    internal static MemRequest Rent(MemHost host, Peer peer, ushort client, uint id, byte[] buffer, int length)
    {
        MemRequest? r;
        lock (s_pool) s_pool.TryPop(out r);
        r ??= new();
        r._host = host;
        r._peer = peer;
        r._client = client;
        r._id = id;
        r._buffer = buffer;
        r._offset = 0;
        r._length = length;
        return r;
    }

    internal void BeginStream(ulong key, int credits)
    {
        _stream = true;
        _key = key;
        _mode = credits;
        _offset = 4;
        _length -= 4;
        if (credits < 0) _takeLeft = -credits;
        else if (credits > 0) _window = new SemaphoreSlim(credits); // ponytail: one allocation per windowed stream
    }

    internal void AddCredits(int credits)
    {
        if (_window is { } w && credits > 0) w.Release(credits);
    }

    internal void CancelStream() // under the host streams gate
    {
        if (Interlocked.Exchange(ref _cancelled, 1) != 0) return;
        if (Volatile.Read(ref _cts) is { } cts) _ = cts.CancelAsync(); // callbacks off the gate
    }

    public void Reply<TState>(TState state, Action<IBufferWriter<byte>, TState> write)
    {
        Claim();
        try
        {
            MemEndpoint.Send(_peer, Kind.Response, _client, _id, state, write);
        }
        catch (Exception ex) when (ex is not TimeoutException)
        {
            Volatile.Write(ref _replied, 0); // nothing was published (serializer threw / peer closed): the caller may still Fail
            throw;
        }
    }

    /// <summary>
    /// Sends one item; false = stop producing (cancelled, client-sized limit reached). In window mode it blocks
    /// the calling thread for credit: async producers should use <see cref="ItemAsync"/>.
    /// </summary>
    public bool Item<TState>(TState state, Action<IBufferWriter<byte>, TState> write)
    {
        if (_window is { } w && !w.Wait(0))
        {
            try { w.Wait(Aborted); }
            catch (OperationCanceledException) { return false; }
        }

        return SendItem(state, write);
    }

    public ValueTask<bool> ItemAsync<TState>(TState state, Action<IBufferWriter<byte>, TState> write)
    {
        if (_window is { } w && !w.Wait(0)) return WaitCredit(w, state, write); // allocates only when the consumer lags
        return new(SendItem(state, write));
    }

    async ValueTask<bool> WaitCredit<TState>(SemaphoreSlim w, TState state, Action<IBufferWriter<byte>, TState> write)
    {
        try { await w.WaitAsync(Aborted).ConfigureAwait(false); }
        catch (OperationCanceledException) { return false; }
        return SendItem(state, write);
    }

    bool SendItem<TState>(TState state, Action<IBufferWriter<byte>, TState> write)
    {
        if (Volatile.Read(ref _replied) != 0) throw new InvalidOperationException("Already replied.");
        if (IsCancelled) return false;
        if (_mode < 0 && --_takeLeft < 0) return false; // ponytail: one producer per stream, so no interlocked
        MemEndpoint.Send(_peer, Kind.Item, _client, _id, state, write);
        return _mode >= 0 || _takeLeft > 0;
    }

    public void Fail(string message)
    {
        Claim();
        SendError(message);
    }

    void Claim()
    {
        if (Interlocked.Exchange(ref _replied, 1) != 0) throw new InvalidOperationException("Already replied.");
    }

    void SendError(string message)
        => MemEndpoint.Send(_peer, Kind.Error, _client, _id, message, static (w, m) => Encoding.UTF8.GetBytes(m.AsSpan(), w));

    void IThreadPoolWorkItem.Execute()
    {
        ValueTask pending;
        try
        {
            pending = _host.Handler(this);
        }
        catch (Exception ex)
        {
            Finish(ex);
            return;
        }

        if (pending.IsCompletedSuccessfully) Finish(null);
        else _ = AwaitHandler(pending); // allocates only when the handler really goes async
    }

    async Task AwaitHandler(ValueTask pending)
    {
        Exception? error = null;
        try
        {
            await pending.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            error = ex;
        }

        Finish(error);
    }

    void Finish(Exception? error)
    {
        if (_stream) _host.EndStream(_key); // before recycling: no Credit/Cancel can touch the next tenant

        if (Interlocked.Exchange(ref _replied, 1) == 0 && !IsCancelled)
        {
            try { SendError(error?.Message ?? "Handler completed without a reply."); }
            catch { } // client gone
        }

        _peer.Release();
        ArrayPool<byte>.Shared.Return(_buffer);
        _host = null!;
        _peer = null!;
        _buffer = null!;
        _replied = 0;
        if (_stream)
        {
            _stream = false;
            _mode = _takeLeft = _cancelled = 0;
            _window?.Dispose();
            _window = null;
            _cts?.Dispose();
            _cts = null;
        }
        lock (s_pool) s_pool.Push(this);
    }
}
