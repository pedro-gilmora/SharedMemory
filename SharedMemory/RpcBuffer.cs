// SharedMemory (File: SharedMemory\RpcBuffer.cs)
// Copyright (c) 2020 Justin Stenning
// http://spazzarama.com
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

namespace SharedMemory
{
    using System;
    // Only supported in .NET 4.5+ and .NET Standard 2.0

    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Runtime.InteropServices;
    using System.Threading;
    using System.Threading.Tasks;

    internal enum InstanceType
    {
        Master,
        Slave
    }

    /// <summary>
    /// The available RPC protocols
    /// </summary>
#if SG_CONTEXT
    internal
#else
    public
#endif
    enum RpcProtocol
    {
        /// <summary>
        /// Version 1 - messages are split into packets that fit the buffer capacity and include a protocol header in each packet
        /// </summary>
        V1 = 1
    }

    /// <summary>
    /// Extension methods for adorning RPC tasks
    /// </summary>

    #if SG_CONTEXT
        internal
    #else
        public
    #endif
        static class ResponseTaskHelper
    {
        static readonly RpcResponse CancelledRpcResponse = new RpcResponse(false, null);
        static readonly Task<RpcResponse> CancelledRpcResponseTask = Task.FromResult(CancelledRpcResponse);

        /// <summary>
        /// Adds timeout and manual cancellation capabilities to an existing Task
        /// </summary>
        /// <param name="task"></param>
        /// <param name="millisecondsTimeout"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static Task<RpcResponse> TimeoutOrCancel(this Task<RpcResponse> task, int millisecondsTimeout, CancellationToken cancellationToken = default)
        {
            if (task.IsCompleted) return task;
            if (millisecondsTimeout == 0) return CancelledRpcResponseTask;
            // Infinite (streams): cancellation throws, so callers can tell a cancelled stream from a failed one.
            if (millisecondsTimeout == Timeout.Infinite) return task.WaitAsync(cancellationToken);

            return Await(task, millisecondsTimeout, cancellationToken);

            // Native WaitAsync: one timer-backed promise instead of CTS + WaitAsync chain per call. Timeout/cancel yield a failed response.
            static async Task<RpcResponse> Await(Task<RpcResponse> task, int millisecondsTimeout, CancellationToken cancellationToken)
            {
                try
                {
                    return await task.WaitAsync(TimeSpan.FromMilliseconds(millisecondsTimeout), cancellationToken).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    return CancelledRpcResponse;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return CancelledRpcResponse;
                }
            }
        }

        /// <summary>
        /// Holds the task for a cancellation token, as well as the token registration. The registration is disposed when this instance is disposed.
        /// </summary>
        public sealed class RpcResponseCancellationTokenTaskSource : IDisposable
        {
            /// <summary>
            /// The cancellation token registration, if any. This is <c>null</c> if the registration was not necessary.
            /// </summary>
            private readonly IDisposable? _registration;

            /// <summary>
            /// Creates a task for the specified cancellation token, registering with the token if necessary.
            /// </summary>
            /// <param name="cancellationToken">The cancellation token to observe.</param>
            public RpcResponseCancellationTokenTaskSource(CancellationToken cancellationToken)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    Task = CancelledRpcResponseTask;
                    return;
                }
                var tcs = new TaskCompletionSource<RpcResponse>();
                _registration = cancellationToken.Register(() => tcs.TrySetResult(CancelledRpcResponse), useSynchronizationContext: false);
                Task = tcs.Task;
            }

            /// <summary>
            /// Gets the task for the source cancellation token.
            /// </summary>
            public Task<RpcResponse> Task { get; private set; }

            /// <summary>
            /// Disposes the cancellation token registration, if any. Note that this may cause <see cref="Task"/> to never complete.
            /// </summary>
            public void Dispose()
            {
                _registration?.Dispose();
            }
        }
    }

    /// <summary>
    /// The RPC message type
    /// </summary>
    #if SG_CONTEXT
        internal
    #else
        public
    #endif
        enum MessageType : byte
    {
        /// <summary>
        /// A request message
        /// </summary>
        RpcRequest = 1,
        /// <summary>
        /// A response message
        /// </summary>
        RpcResponse = 2,
        /// <summary>
        /// An error message
        /// </summary>
        ErrorInRpc = 3,
        /// <summary>
        /// An item of a stream; <c>ResponseId</c> is the MsgId of the request that opened it
        /// </summary>
        StreamItem = 4,
        /// <summary>
        /// Multi-client only: the client leaves; the host releases its response ring
        /// </summary>
        Close = 5,
    }

    /// <summary>
    /// The V1 protocol header
    /// </summary>
    #if SG_CONTEXT
        internal
    #else
        public
    #endif
        record struct RpcProtocolHeaderV1
    {
        /// <summary>
        /// Message Type
        /// </summary>
        public MessageType MsgType;
        /// <summary>
        /// Message Id
        /// </summary>
        public ulong MsgId;
        /// <summary>
        /// Total message size
        /// </summary>
        public int PayloadSize;
        /// <summary>
        /// The current packet number
        /// </summary>
        public ushort CurrentPacket;
        /// <summary>
        /// The total number of packets in the message
        /// </summary>
        public ushort TotalPackets;
        /// <summary>
        /// If a response, the Id of the remote message this is a response to
        /// </summary>
        public ulong ResponseId;
    }

    /// <summary>
    /// Represents a request to be sent on the channel
    /// </summary>
    #if SG_CONTEXT
    internal
#else
    public
#endif
    class RpcRequest
    {
        internal RpcRequest() { }
        /// <summary>
        /// The message Id
        /// </summary>
        public ulong MsgId { get; set; }
        /// <summary>
        /// The message type
        /// </summary>
        public MessageType MsgType { get; set; }
        /// <summary>
        /// The message payload (if any)
        /// </summary>
        public byte[] Data { get; set; } = null!;
        /// <summary>
        /// <see cref="Data"/> is rented from <see cref="System.Buffers.ArrayPool{T}.Shared"/> and may be longer than <see cref="Length"/>
        /// </summary>
        internal bool Pooled;
        internal int Length;
        /// <summary>
        /// A wait event that is signaled when a response is ready
        /// </summary>
        // Lazy: incoming requests and direct calls (PendingCall) never need it.
        public TaskCompletionSource<RpcResponse> ResponseReady =>
            _responseReady ?? Interlocked.CompareExchange(ref _responseReady, new TaskCompletionSource<RpcResponse>(TaskCreationOptions.RunContinuationsAsynchronously), null) ?? _responseReady;
        TaskCompletionSource<RpcResponse>? _responseReady;
        /// <summary>
        /// Receives the stream items addressed to this request (invoked on the reader thread, in order)
        /// </summary>
        internal StreamItemHandler? OnStreamItem { get; set; }
        /// <summary>
        /// Was the request successful
        /// </summary>
        public bool IsSuccess { get; internal set; }
        /// <summary>
        /// When the request was created
        /// </summary>
        public DateTime Created { get; } = DateTime.Now;
    }

    /// <summary>
    /// Represents the result of a remote request.
    /// </summary>
    /// <remarks>
    /// Constructs an RpcResponse
    /// </remarks>
    /// <param name="success">was it a success</param>
    /// <param name="data">the message data (if any)</param>
    /// <summary>
    /// Receives a stream item; the span is only valid during the call (it may point into shared memory).
    /// </summary>
    #if SG_CONTEXT
    internal
#else
    public
#endif
    delegate void StreamItemHandler(ReadOnlySpan<byte> item);

    /// <summary>
    /// Reads the final response; the span is only valid during the call (it may point into shared memory).
    /// Invoked on the reader thread: keep it to parsing/deserialization.
    /// </summary>
#if SG_CONTEXT
    internal
#else
    public
#endif
    delegate TOut ResponseReader<TState, TOut>(TState state, ReadOnlySpan<byte> response);

    /// <summary>
    /// Direct host handler: <paramref name="request"/> is pooled and only valid until the handler returns.
    /// The handler replies itself with <see cref="RpcBuffer.Reply{TState}"/>.
    /// </summary>
#if SG_CONTEXT
    internal
#else
    public
#endif
    delegate void RpcRequestHandler(ulong msgId, ReadOnlyMemory<byte> request);

    /// <summary>
    /// Async direct host handler: <paramref name="request"/> is pooled and only valid until the returned task completes.
    /// The handler replies itself with <see cref="RpcBuffer.Reply{TState}"/>.
    /// </summary>
#if SG_CONTEXT
    internal
#else
    public
#endif
    delegate Task RpcAsyncRequestHandler(ulong msgId, ReadOnlyMemory<byte> request);

    /// <summary>Outgoing request whose response is read straight from the node (no <see cref="RpcResponse"/>/array).</summary>
    abstract class PendingCall : RpcRequest
    {
        internal abstract void Complete(bool success, ReadOnlySpan<byte> response);
    }

    sealed class PendingCall<TState, TOut>(TState state, ResponseReader<TState, TOut> read) : PendingCall
    {
        readonly TaskCompletionSource<TOut> _done = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<TOut> Task => _done.Task;

        internal override void Complete(bool success, ReadOnlySpan<byte> response)
        {
            if (!success)
            {
                _done.TrySetException(new InvalidOperationException("Remote request handler failed."));
                return;
            }

            try { _done.TrySetResult(read(state, response)); }
            catch (Exception ex) { _done.TrySetException(ex); }
        }
    }

#if SG_CONTEXT
    internal
#else
    public
#endif
    class RpcResponse(bool success, byte[]? data)
    {

        /// <summary>
        /// If the request was successful
        /// </summary>
        public bool Success { get; } = success;

        /// <summary>
        /// The returned result (if applicable)
        /// </summary>
        public byte[]? Data { get; } = data;
    }

    /// <summary>
    /// Represents the channel statistics of an <see cref="RpcBuffer"/> instance
    /// </summary>
    #if SG_CONTEXT
    internal
#else
    public
#endif
    class RpcStatistics
    {
        /// <summary>
        /// The protocol overhead per packet
        /// </summary>
        public int ProtocolOverheadPerPacket { get; internal set; }

        /// <summary>
        /// Bytes read from channel (excluding protocol overhead)
        /// </summary>
        public ulong BytesRead { get; private set; }
        /// <summary>
        /// Number of packets read from channel
        /// </summary>
        public ulong PacketsRead { get; private set; }
        /// <summary>
        /// The largest packet read from channel (excluding protocol overhead)
        /// </summary>
        public int ReadingMaxPacketSize { get; private set; }
        /// <summary>
        /// The size of last packet read from channel (excluding protocol overhead)
        /// </summary>
        public int ReadingLastPacketSize { get; private set; } = -1;
        /// <summary>
        /// The size of last message read from channel (excluding protocol overhead)
        /// </summary>
        public int ReadingLastMessageSize { get; private set; } = -1;

        /// <summary>
        /// The total number of messages received
        /// </summary>
        public ulong MessagesReceived { get { return RequestsReceived + ResponsesReceived + ErrorsReceived; } }

        /// <summary>
        /// The number of request messages received
        /// </summary>
        public ulong RequestsReceived { get; private set; }

        /// <summary>
        /// The number of response messages received
        /// </summary>
        public ulong ResponsesReceived { get; private set; }

        /// <summary>
        /// The number of error message received
        /// </summary>
        public ulong ErrorsReceived { get; private set; }

        /// <summary>
        /// The number of bytes written to channel (excluding protocol overhead)
        /// </summary>
        public ulong BytesWritten { get; private set; }
        /// <summary>
        /// The number of packets written to channel
        /// </summary>
        public ulong PacketsWritten { get; private set; }
        /// <summary>
        /// The largest packet written to channel (excluding protocol overhead)
        /// </summary>
        public int WritingMaxPacketSize { get; private set; }

        /// <summary>
        /// The size of last packet written to channel (excluding protocol overhead)
        /// </summary>
        public int WritingLastPacketSize { get; private set; } = -1;

        /// <summary>
        /// The size of last message written to channel (excluding protocol overhead)
        /// </summary>
        public int WritingLastMessageSize { get; private set; } = -1;

        /// <summary>
        /// Number of response messages received that were discarded (provided a non-existent message Id)
        /// </summary>
        public ulong DiscardedResponses { get; private set; }
        /// <summary>
        /// The response message Id that was last discarded
        /// </summary>
        public ulong LastDiscardedResponseId { get; private set; }

        /// <summary>
        /// The total number of messages sent
        /// </summary>
        public ulong MessagesSent { get { return RequestsSent + ResponsesSent + ErrorsSent; } }

        /// <summary>
        /// The number of request messages sent
        /// </summary>
        public ulong RequestsSent { get; private set; }

        /// <summary>
        /// The number of response messages sent
        /// </summary>
        public ulong ResponsesSent { get; private set; }

        /// <summary>
        /// The number of error messages sent
        /// </summary>
        public ulong ErrorsSent { get; private set; }

        /// <summary>
        /// Number of timeouts
        /// </summary>
        public ulong Timeouts { get; private set; }
        /// <summary>
        /// DateTime of last timeout
        /// </summary>
        public DateTime LastTimeout { get; private set; }
        DateTime StartWaitWriteTimestamp { get; set; }
        DateTime EndWaitWriteTimestamp { get; set; }
        /// <summary>
        /// Maximum Ticks waited for available write slot
        /// </summary>
        public long MaxWaitWriteTicks { get; private set; } = -1;
        DateTime StartWaitReadTimestamp { get; set; }
        DateTime EndWaitReadTimestamp { get; set; }
        /// <summary>
        /// Maximum Ticks waiting for read slot (cannot exceed 1sec)
        /// </summary>
        public long MaxWaitReadTicks { get; private set; } = -1;

        internal void StartWaitRead()
        {
            StartWaitReadTimestamp = DateTime.Now;
        }

        internal void ReadPacket(int bytes)
        {
            EndWaitReadTimestamp = DateTime.Now;

            var ticks = EndWaitReadTimestamp.Ticks - StartWaitReadTimestamp.Ticks;
            if (ticks > MaxWaitReadTicks)
            {
                MaxWaitReadTicks = ticks;
            }

            PacketsRead++;
            BytesRead += (ulong)bytes;
            ReadingLastPacketSize = bytes;
            if (bytes > ReadingMaxPacketSize)
            {
                ReadingMaxPacketSize = bytes;
            }
        }

        internal void StartWaitWrite()
        {
            StartWaitWriteTimestamp = DateTime.Now;
        }

        internal void WritePacket(int bytes)
        {
            EndWaitWriteTimestamp = DateTime.Now;

            var ticks = EndWaitWriteTimestamp.Ticks - StartWaitWriteTimestamp.Ticks;
            if (ticks > MaxWaitWriteTicks)
            {
                MaxWaitWriteTicks = ticks;
            }

            PacketsWritten++;
            BytesWritten += (ulong)bytes;
            WritingLastPacketSize = bytes;
            if (bytes > WritingMaxPacketSize)
            {
                WritingMaxPacketSize = bytes;
            }
        }

        internal void MessageReceived(MessageType msgType, int size)
        {
            ReadingLastMessageSize = size;
            switch (msgType)
            {
                case MessageType.RpcRequest:
                    RequestsReceived++;
                    break;
                case MessageType.RpcResponse:
                    ResponsesReceived++;
                    break;
                case MessageType.ErrorInRpc:
                    ErrorsReceived++;
                    break;
            }
        }

        internal void MessageSent(MessageType msgType, int size)
        {
            WritingLastMessageSize = size;
            switch (msgType)
            {
                case MessageType.RpcRequest:
                    RequestsSent++;
                    break;
                case MessageType.RpcResponse:
                    ResponsesSent++;
                    break;
                case MessageType.ErrorInRpc:
                    ErrorsSent++;
                    break;
            }
        }

        internal void Timeout()
        {
            Timeouts++;
            LastTimeout = DateTime.Now;
        }

        internal void DiscardResponse(ulong msgId)
        {
            DiscardedResponses++;
            LastDiscardedResponseId = msgId;
        }

        /// <summary>
        /// Reset all statistics
        /// </summary>
        public void Reset()
        {
            Timeouts = 0;
            LastTimeout = DateTime.MinValue;
            PacketsRead = 0;
            BytesRead = 0;
            MaxWaitReadTicks = -1;
            ReadingMaxPacketSize = 0;
            ReadingLastMessageSize = -1;
            ReadingLastPacketSize = -1;
            RequestsReceived = 0;
            ResponsesReceived = 0;
            ErrorsReceived = 0;
            PacketsWritten = 0;
            BytesWritten = 0;
            MaxWaitWriteTicks = -1;
            WritingMaxPacketSize = 0;
            WritingLastMessageSize = -1;
            WritingLastPacketSize = -1;
            RequestsSent = 0;
            ResponsesSent = 0;
            ErrorsSent = 0;
            DiscardedResponses = 0;
            LastDiscardedResponseId = 0;
        }
    }

    /// <summary>
    /// A simple RPC implementation designed for a single master/slave pair
    /// </summary>

#if !NETSTANDARD
        [System.Runtime.Versioning.SupportedOSPlatform("windows")]
#endif
#if !SG_CONTEXT
    public 
#else
    internal
#endif
         class RpcBuffer : IDisposable
    {
        private Mutex masterMutex;
        private long _disposed = 0;

        /// <summary>
        /// Whether the RpcBuffer has been disposed
        /// </summary>
        protected bool Disposed
        {
            get
            {
                return Interlocked.Read(ref _disposed) != 0;
            }
        }

        /// <summary>
        /// Dispose has completed
        /// </summary>
        public bool DisposeFinished
        {
            get
            {
                return Interlocked.Read(ref _disposed) == 2;
            }
        }

        private readonly InstanceType instanceType;
        private readonly RpcProtocol protocolVersion;
        private readonly int protocolLength;
        private readonly int bufferCapacity;
        private readonly int bufferNodeCount;
        private readonly int msgBufferLength; // The amount of room left in the node after protocol header

        /// <summary>
        /// The buffer used to send messages to remote channel endpoint
        /// </summary>
        protected CircularBuffer WriteBuffer { get; private set; }
        /// <summary>
        /// The buffer used to receive message from the remote channel endpoint
        /// </summary>
        protected CircularBuffer ReadBuffer { get; private set; }

        /// <summary>
        /// Channel endpoint statistics
        /// </summary>
        public RpcStatistics Statistics { get; private set; }

        const int defaultTimeoutMs = 30000;
        object lock_sendQ = new object();

        /// <summary>
        /// Outgoing requests waiting for responses
        /// </summary>
        protected ConcurrentDictionary<ulong, RpcRequest> Requests { get; } = new ConcurrentDictionary<ulong, RpcRequest>();
        /// <summary>
        /// Incoming requests waiting for more packets
        /// </summary>
        protected ConcurrentDictionary<ulong, RpcRequest> IncomingRequests { get; } = new ConcurrentDictionary<ulong, RpcRequest>();

        Action<ulong, byte[]>? RemoteCallHandler = null;
        Func<ulong, byte[], Task>? AsyncRemoteCallHandler = null;
        Func<ulong, byte[], byte[]>? RemoteCallHandlerWithResult = null;
        Func<ulong, byte[], Task<byte[]>>? AsyncRemoteCallHandlerWithResult = null;
        RpcRequestHandler? RequestHandler = null;
        RpcAsyncRequestHandler? AsyncRequestHandler = null;

        // Multi-client: one request ring "{name}_Host" (created by the host, written by every client) and one response
        // ring "{name}_c{id}" per client (created by the client). The client id lives in the high bits of every MsgId
        // it issues, so the host routes responses/stream items without any extra header bytes.
        // ponytail: 24-bit client id / 40-bit per-client counter; a client sending >2^40 messages would spill into the id.
        const int ClientShift = 40;
        const string HostSuffix = "_Host";
        readonly string? _hostName;
        readonly Dictionary<int, CircularBuffer>? _clients; // host only, guarded by lock_sendQ
        readonly ulong _clientBits; // client only: id << ClientShift
        int _closeSent;

        static string ClientRing(string name, int id) => name + "_c" + id;

        /// <summary>
        /// Creates a multi-client host: a single reader thread serves every <see cref="Connect"/>ed client.
        /// </summary>
        public static RpcBuffer Host(string name, Func<ulong, byte[], byte[]> handler, int bufferCapacity = 50000, int bufferNodeCount = 10)
            => new(name, handler, bufferCapacity, bufferNodeCount);

        /// <inheritdoc cref="Host(string, Func{ulong, byte[], byte[]}, int, int)"/>
        public static RpcBuffer Host(string name, Func<ulong, byte[], Task<byte[]>> handler, int bufferCapacity = 50000, int bufferNodeCount = 10)
            => new(name, handler, bufferCapacity, bufferNodeCount);

        /// <summary>
        /// Multi-client host with a direct handler: the request is pooled (no per-request array) and the handler replies
        /// itself with <see cref="Reply{TState}"/>, writing straight into the client's node. A throwing handler yields <see cref="MessageType.ErrorInRpc"/>.
        /// </summary>
        public static RpcBuffer Host(string name, RpcRequestHandler handler, int bufferCapacity = 50000, int bufferNodeCount = 10)
            => new(name, handler, bufferCapacity, bufferNodeCount);

        /// <inheritdoc cref="Host(string, RpcRequestHandler, int, int)"/>
        public static RpcBuffer Host(string name, RpcAsyncRequestHandler handler, int bufferCapacity = 50000, int bufferNodeCount = 10)
            => new(name, handler, bufferCapacity, bufferNodeCount);

        /// <summary>
        /// Connects to a <see cref="Host(string, Func{ulong, byte[], byte[]}, int, int)"/>. Throws <see cref="System.IO.FileNotFoundException"/> if no host exists.
        /// Disposing the client tells the host to release it.
        /// </summary>
        public static RpcBuffer Connect(string name) => new(name, null, 0, 0);

#pragma warning disable CS8618
        private RpcBuffer(string name, Delegate? hostHandler, int bufferCapacity, int bufferNodeCount)
#pragma warning restore CS8618
        {
            Statistics = new RpcStatistics();
            protocolVersion = RpcProtocol.V1;
            protocolLength = FastStructure<RpcProtocolHeaderV1>.Size;
            Statistics.ProtocolOverheadPerPacket = protocolLength;

            if (hostHandler is not null)
            {
                if (bufferCapacity is < 256 or > 1024 * 1024) throw new ArgumentOutOfRangeException(nameof(bufferCapacity), "must be between 256 bytes and 1MB");

                instanceType = InstanceType.Master;
                _hostName = name;
                _clients = [];
                RemoteCallHandlerWithResult = hostHandler as Func<ulong, byte[], byte[]>;
                AsyncRemoteCallHandlerWithResult = hostHandler as Func<ulong, byte[], Task<byte[]>>;
                RequestHandler = hostHandler as RpcRequestHandler;
                AsyncRequestHandler = hostHandler as RpcAsyncRequestHandler;
                ReadBuffer = new CircularBuffer(name + HostSuffix, bufferNodeCount, bufferCapacity);
                WriteBuffer = null!;
            }
            else
            {
                instanceType = InstanceType.Slave;
                WriteBuffer = new CircularBuffer(name + HostSuffix);
                try
                {
                    int id = WriteBuffer.NextSequence();
                    _clientBits = (ulong)id << ClientShift;
                    ReadBuffer = new CircularBuffer(ClientRing(name, id), WriteBuffer.NodeCount, WriteBuffer.NodeBufferSize);
                }
                catch
                {
                    WriteBuffer.Dispose();
                    throw;
                }
            }

            this.bufferCapacity = ReadBuffer.NodeBufferSize;
            this.bufferNodeCount = ReadBuffer.NodeCount;
            msgBufferLength = this.bufferCapacity - protocolLength;

            StartReader();
        }

        /// <summary>Ring the next write goes to: the peer's for pairs and clients, the addressed client's for a host. Call under lock_sendQ.</summary>
        CircularBuffer? Target(ulong responseId)
        {
            if (_clients is null) return WriteBuffer;

            int id = (int)(responseId >> ClientShift);
            if (!_clients.TryGetValue(id, out var ring))
            {
                try { _clients[id] = ring = new CircularBuffer(ClientRing(_hostName!, id)); }
                catch (System.IO.IOException) { return null; } // client already gone (or a host-initiated request: no target)
            }

            if (!ring.ShuttingDown) return ring;

            DropClient(id);
            return null;
        }

        void DropClient(int id)
        {
            if (_clients!.Remove(id, out var ring)) ring.Dispose();
        }

        void SendClose()
        {
            if (_clientBits == 0 || Interlocked.Exchange(ref _closeSent, 1) == 1) return;

            try { WriteProtocolV1(MessageType.Close, NextMsgId(), ReadOnlyMemory<byte>.Empty, 0, 100); }
            catch { } // best effort: a dead host has nothing to release
        }

        void StartReader()
        {
            // Hilo dedicado: el bucle lector bloquea; en el pool sufre inanicion con llamadas sync concurrentes.
            _ = Task.Factory.StartNew(() =>
            {
                switch (protocolVersion)
                {
                    case RpcProtocol.V1:
                        ReadThreadV1();
                        break;
                }
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        /// <summary>
        /// Construct a new RpcBuffer
        /// </summary>
        /// <param name="name">The channel name. This is the name to be shared between the master/slave pair. Each pair must have a unique value.</param>
        /// <param name="remoteCallHandler">Action to handle requests with no response.</param>
        /// <param name="bufferCapacity">Master only: Maximum buffer capacity. Messages will be split into packets that fit this capacity (including a packet header of 64-bytes). The slave will use the same size as defined by the master</param>
        /// <param name="protocolVersion">ProtocolVersion.V1 = 64-byte header for each packet</param>
        /// <param name="bufferNodeCount">Master only: The number of nodes in the underlying circular buffers, each with a size of <paramref name="bufferCapacity"/></param>
        public RpcBuffer(string name, Action<ulong, byte[]> remoteCallHandler, int bufferCapacity = 50000, RpcProtocol protocolVersion = RpcProtocol.V1, int bufferNodeCount = 10) :
            this(name, bufferCapacity, protocolVersion, bufferNodeCount)
        {
            RemoteCallHandler = remoteCallHandler;
        }

        /// <summary>
        /// Construct a new RpcBuffer
        /// </summary>
        /// <param name="name">The unique channel name. This is the name to be shared between the master/slave pair. Each pair must have a unique value.</param>
        /// <param name="asyncRemoteCallHandler">Asynchronous action to handle requests with no response.</param>
        /// <param name="bufferCapacity">Master only: Maximum buffer capacity. Messages will be split into packets that fit this capacity (including a packet header of 64-bytes). The slave will use the same size as defined by the master</param>
        /// <param name="protocolVersion">ProtocolVersion.V1 = 64-byte header for each packet</param>
        /// <param name="bufferNodeCount">Master only: The number of nodes in the underlying circular buffers, each with a size of <paramref name="bufferCapacity"/></param>
        public RpcBuffer(string name, Func<ulong, byte[], Task> asyncRemoteCallHandler, int bufferCapacity = 50000, RpcProtocol protocolVersion = RpcProtocol.V1, int bufferNodeCount = 10) :
            this(name, bufferCapacity, protocolVersion, bufferNodeCount)
        {
            AsyncRemoteCallHandler = asyncRemoteCallHandler;
        }

        /// <summary>
        /// Construct a new RpcBuffer
        /// </summary>
        /// <param name="name">The unique channel name. This is the name to be shared between the master/slave pair. Each pair must have a unique value.</param>
        /// <param name="remoteCallHandlerWithResult">Function to handle requests with a response.</param>
        /// <param name="bufferCapacity">Master only: Maximum buffer capacity. Messages will be split into packets that fit this capacity (including a packet header of 64-bytes). The slave will use the same size as defined by the master</param>
        /// <param name="protocolVersion">ProtocolVersion.V1 = 64-byte header for each packet</param>
        /// <param name="bufferNodeCount">Master only: The number of nodes in the underlying circular buffers, each with a size of <paramref name="bufferCapacity"/></param>
        public RpcBuffer(string name, Func<ulong, byte[], byte[]> remoteCallHandlerWithResult, int bufferCapacity = 50000, RpcProtocol protocolVersion = RpcProtocol.V1, int bufferNodeCount = 10) :
            this(name, bufferCapacity, protocolVersion, bufferNodeCount)
        {
            RemoteCallHandlerWithResult = remoteCallHandlerWithResult;
        }

        /// <summary>
        /// Construct a new RpcBuffer
        /// </summary>
        /// <param name="name">The unique channel name. This is the name to be shared between the master/slave pair. Each pair must have a unique value.</param>
        /// <param name="asyncRemoteCallHandlerWithResult">Function to asynchronously handle requests with a response.</param>
        /// <param name="bufferCapacity">Master only: Maximum buffer capacity. Messages will be split into packets that fit this capacity (including a packet header of 64-bytes). The slave will use the same size as defined by the master</param>
        /// <param name="protocolVersion">ProtocolVersion.V1 = 64-byte header for each packet</param>
        /// <param name="bufferNodeCount">Master only: The number of nodes in the underlying circular buffers, each with a size of <paramref name="bufferCapacity"/></param>
        public RpcBuffer(string name, Func<ulong, byte[], Task<byte[]>> asyncRemoteCallHandlerWithResult, int bufferCapacity = 50000, RpcProtocol protocolVersion = RpcProtocol.V1, int bufferNodeCount = 10) :
            this(name, bufferCapacity, protocolVersion, bufferNodeCount)
        {
            AsyncRemoteCallHandlerWithResult = asyncRemoteCallHandlerWithResult;
        }

        /// <summary>
        /// Construct a new RpcBuffer
        /// </summary>
        /// <param name="name">The unique channel name. This is the name to be shared between the master/slave pair. Each pair must have a unique value.</param>
        /// <param name="bufferCapacity">Master only: Maximum buffer capacity. Messages will be split into packets that fit this capacity (including a packet header of 64-bytes). The slave will use the same size as defined by the master</param>
        /// <param name="protocolVersion">ProtocolVersion.V1 = 64-byte header for each packet</param>
        /// <param name="bufferNodeCount">Master only: The number of nodes in the underlying circular buffers, each with a size of <paramref name="bufferCapacity"/></param>
#pragma warning disable CS8618 // Un campo que no acepta valores NULL debe contener un valor distinto de NULL al salir del constructor. Considere la posibilidad de agregar el modificador "required" o declararlo como un valor que acepta valores NULL.
        public RpcBuffer(string name, int bufferCapacity = 50000, RpcProtocol protocolVersion = RpcProtocol.V1, int bufferNodeCount = 10)
#pragma warning restore CS8618 // Un campo que no acepta valores NULL debe contener un valor distinto de NULL al salir del constructor. Considere la posibilidad de agregar el modificador "required" o declararlo como un valor que acepta valores NULL.
        {
            if (bufferCapacity < 256) // min 256 bytes
            {
                throw new ArgumentOutOfRangeException(nameof(bufferCapacity), "cannot be less than 256 bytes");
            }

            if (bufferCapacity > 1024 * 1024) // max 1MB
            {
                throw new ArgumentOutOfRangeException(nameof(bufferCapacity), "cannot be larger than 1MB");
            }

            Statistics = new RpcStatistics();

            masterMutex = new Mutex(true, name + "SharedMemory_MasterMutex", out bool createdNew);

            if (createdNew && masterMutex.WaitOne(500))
            {
                instanceType = InstanceType.Master;
            }
            else
            {
                instanceType = InstanceType.Slave;
                if (masterMutex != null)
                {
                    masterMutex.Close();
                    masterMutex.Dispose();
                    masterMutex = null!;
                }
            }

            switch (protocolVersion)
            {
                case RpcProtocol.V1:
                    this.protocolVersion = protocolVersion;
                    protocolLength = FastStructure<RpcProtocolHeaderV1>.Size;
                    Statistics.ProtocolOverheadPerPacket = protocolLength;
                    break;
            }

            this.bufferCapacity = bufferCapacity;
            this.bufferNodeCount = bufferNodeCount;
            if (instanceType == InstanceType.Master)
            {
                WriteBuffer = new CircularBuffer(name + "_Slave_SharedMemory_MMF", bufferNodeCount, this.bufferCapacity);
                ReadBuffer = new CircularBuffer(name + "_Master_SharedMemory_MMF", bufferNodeCount, this.bufferCapacity);
            }
            else
            {
                ReadBuffer = new CircularBuffer(name + "_Slave_SharedMemory_MMF");
                WriteBuffer = new CircularBuffer(name + "_Master_SharedMemory_MMF");
                this.bufferCapacity = ReadBuffer.NodeBufferSize;
                this.bufferNodeCount = ReadBuffer.NodeCount;
            }

            msgBufferLength = Convert.ToInt32(this.bufferCapacity) - protocolLength;

            StartReader();
        }

        private readonly object mutex = new ();
        ulong messageId = 1;

        /// <summary>
        /// Constructs a new request message, giving it a new unique MsgId
        /// </summary>
        /// <returns></returns>
        protected RpcRequest CreateMessageRequest()
        {
            return new RpcRequest { MsgId = NextMsgId() };
        }

        ulong NextMsgId()
        {
            lock (mutex) return _clientBits | messageId++;
        }

        /// <summary>
        /// Send a remote request on the channel, blocking until a result is returned
        /// </summary>
        /// <param name="args">Arguments (if any) as a byte array to be sent to the remote endpoint</param>
        /// <param name="timeoutMs">Timeout in milliseconds (defaults to 30sec)</param>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>The returned response</returns>
        /// <exception cref="ObjectDisposedException">Thrown if this object has been disposed</exception>
        /// <exception cref="InvalidOperationException">Thrown if the underlying buffers have been closed by the channel owner</exception>
        public RpcResponse RemoteRequest(byte[]? args = null, int timeoutMs = defaultTimeoutMs, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposedOrShutdown();

            var request = CreateMessageRequest();
            var sendMessage = SendMessage(request, args, timeoutMs, cancellationToken);
            var rpcResponse = sendMessage.GetAwaiter().GetResult();

            return rpcResponse;
        }

        /// <summary>
        /// Send a remote request on the channel, blocking until a result is returned
        /// </summary>
        /// <param name="args">Arguments (if any) as a byte array to be sent to the remote endpoint</param>
        /// <param name="timeoutMs">Timeout in milliseconds (defaults to 30sec)</param>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>The returned response</returns>
        /// <exception cref="ObjectDisposedException">Thrown if this object has been disposed</exception>
        /// <exception cref="InvalidOperationException">Thrown if the underlying buffers have been closed by the channel owner</exception>
        public RpcResponse Send(ReadOnlyMemory<byte> bytes, int timeoutMs = defaultTimeoutMs, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposedOrShutdown();

            var request = CreateMessageRequest();

            return SendMessage(request, bytes, timeoutMs, cancellationToken)
                .GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// Send a remote request on the channel, blocking until a result is returned
        /// </summary>
        /// <param name="args">Arguments (if any) as a byte array to be sent to the remote endpoint</param>
        /// <param name="timeoutMs">Timeout in milliseconds (defaults to 30sec)</param>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>The returned response</returns>
        /// <exception cref="ObjectDisposedException">Thrown if this object has been disposed</exception>
        /// <exception cref="InvalidOperationException">Thrown if the underlying buffers have been closed by the channel owner</exception>
        public Task<RpcResponse> SendAsync(ReadOnlyMemory<byte> bytes, int timeoutMs = defaultTimeoutMs, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposedOrShutdown();

            var request = CreateMessageRequest();

            return SendMessage(request, bytes, timeoutMs, cancellationToken);
        }

        /// <summary>
        /// Send a remote request on the channel, blocking until a result is returned
        /// </summary>
        /// <param name="args">Arguments (if any) as a byte array to be sent to the remote endpoint</param>
        /// <param name="timeoutMs">Timeout in milliseconds (defaults to 30sec)</param>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>The returned response</returns>
        /// <exception cref="ObjectDisposedException">Thrown if this object has been disposed</exception>
        /// <exception cref="InvalidOperationException">Thrown if the underlying buffers have been closed by the channel owner</exception>
        public async Task<RpcResponse> SendAsync(byte[]? args = null, int timeoutMs = defaultTimeoutMs, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposedOrShutdown();

            var request = CreateMessageRequest();

            return await SendMessage(request, args, timeoutMs, cancellationToken);
        }

        /// <summary>
        /// Send a remote request on the channel (awaitable)
        /// </summary>
        /// <param name="args">Arguments (if any) as a byte array to be sent to the remote endpoint</param>
        /// <param name="timeoutMs">Timeout in milliseconds (defaults to 30sec)</param>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns></returns>
        /// <exception cref="ObjectDisposedException">Thrown if this object has been disposed</exception>
        /// <exception cref="InvalidOperationException">Thrown if the underlying buffers have been closed by the channel owner</exception>
        public Task<RpcResponse> RemoteRequestAsync(byte[]? args = null, int timeoutMs = defaultTimeoutMs, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposedOrShutdown();

            var request = CreateMessageRequest();

            return SendMessage(request, args, timeoutMs, cancellationToken);
        }

        /// <summary>
        /// Sends a request whose remote handler streams items back with <see cref="SendStreamItem"/>. Items are delivered
        /// to <paramref name="onItem"/> in order on the reader thread; the returned task completes with the final response.
        /// No timeout: streams may be long, cancel with <paramref name="cancellationToken"/>.
        /// </summary>
        public Task<RpcResponse> RemoteStreamAsync(byte[]? args, StreamItemHandler onItem, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposedOrShutdown();

            var request = CreateMessageRequest();
            request.OnStreamItem = onItem;

            return SendMessage(request, args, Timeout.Infinite, cancellationToken);
        }

        /// <summary>
        /// Sends a stream item addressed to the incoming request <paramref name="requestMsgId"/> (the msgId given to the handler).
        /// Only the peer that sent that request accepts it.
        /// </summary>
        // ponytail: like every V1 write, a node that stays full for >1s is dropped; fine while the client reader only enqueues.
        /// <remarks>The item is written by <paramref name="write"/> straight into the shared-memory node (no intermediate array).</remarks>
        public bool SendStreamItem<TState>(ulong requestMsgId, TState state, Action<System.Buffers.IBufferWriter<byte>, TState> write)
        {
            ThrowIfDisposedOrShutdown();

            ulong msgId = NextMsgId();

            if (!WriteProtocolV1(MessageType.StreamItem, msgId, requestMsgId, state, write, out int size)) return false;

            Statistics.MessageSent(MessageType.StreamItem, size);
            return true;
        }

        async Task<RpcResponse> SendMessage(RpcRequest request, ReadOnlyMemory<byte> payload, int timeout = defaultTimeoutMs, CancellationToken cancellationToken = default)
        {
            return await SendMessage(MessageType.RpcRequest, request, payload, timeout: timeout, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Sends a message to the remote endpoint
        /// </summary>
        /// <param name="msgType"></param>
        /// <param name="request"></param>
        /// <param name="payload"></param>
        /// <param name="responseMsgId"></param>
        /// <param name="timeout"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="ObjectDisposedException">Thrown if this object has been disposed</exception>
        /// <exception cref="InvalidOperationException">Thrown if the underlying buffers have been closed by the channel owner</exception>
        protected virtual Task<RpcResponse> SendMessage(MessageType msgType, RpcRequest request, ReadOnlyMemory<byte> payload, ulong responseMsgId = 0, int timeout = defaultTimeoutMs, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposedOrShutdown();

            var msgId = request.MsgId;

            if (msgType == MessageType.RpcRequest)
            {
                Requests[request.MsgId] = request;
            }

            bool success ;
            switch (protocolVersion) //Check version
            {
                case RpcProtocol.V1:
                    success = WriteProtocolV1(msgType, msgId, payload, responseMsgId, timeout);
                    break;
                default:
                    // Invalid protocol
                    return Task.FromResult(new RpcResponse(false, null));
            }

            if (success)
            {
                Statistics.MessageSent(msgType, payload.Length);
            }

            if (success && msgType == MessageType.RpcRequest)
            {
                return request?.ResponseReady.Task.TimeoutOrCancel(timeout, cancellationToken) ?? Task.FromResult(new RpcResponse(true, null));
            }
            else
            {
                RpcResponse rpcResponse = new(success, null);

                if (request != null)
                {
                    request.IsSuccess = success;
                    request.ResponseReady.SetResult(rpcResponse);
                }

                return Task.FromResult(rpcResponse);
            }
        }

        bool WriteProtocolV1(MessageType msgType, ulong msgId, ReadOnlyMemory<byte> msgMem, ulong responseMsgId, int timeout)
        {
            if (Disposed)
            {
                return false;
            }

            var msg = msgMem.Span;

            // Send the request packets
            lock (lock_sendQ)
            {
                var target = Target(responseMsgId);
                if (target is null || target.ShuttingDown) return false;

                // Split message into correct packet size
                int i = 0;
                int left = msg.Length;

                byte[] pMsg;
                int iLen = msg.Length;
                ushort totalPackets = (iLen == 0) ? (ushort)1 : Convert.ToUInt16(Math.Ceiling((double)iLen / (double)msgBufferLength));
                ushort currentPacket = 1;

                while (true)
                {
                    if (target.ShuttingDown)
                    {
                        return false;
                    }

                    pMsg = new byte[left > msgBufferLength ? msgBufferLength + protocolLength : left + protocolLength];

                    // Writing protocol header
                    var header = new RpcProtocolHeaderV1
                    {
                        MsgType = msgType,
                        MsgId = msgId,
                        CurrentPacket = currentPacket,
                        TotalPackets = totalPackets,
                        PayloadSize = msg.Length,
                        ResponseId = responseMsgId
                    };

                    header.ToBytes().CopyTo(pMsg);

                    if (left > msgBufferLength)
                    {
                        // Writing payload
                        if (msg.Length > 0)
                            msg.Slice(i, msgBufferLength).CopyTo(pMsg.AsSpan(protocolLength, msgBufferLength));

                        left -= msgBufferLength;
                        i += msgBufferLength;
                    }
                    else
                    {
                        // Writing last packet of payload
                        if (msg.Length > 0)
                        {
                            msg.Slice(i, left).CopyTo(pMsg.AsSpan(protocolLength, left));
                        }

                        left = 0;
                    }

                    Statistics.StartWaitWrite();
                    var bytes = target.Write((ptr) =>
                    {
                        ptr.WriteBytes(pMsg, 0, pMsg.Length);
                        return pMsg.Length;
                    }, 1000);

                    Statistics.WritePacket(bytes - protocolLength);

                    if (left <= 0)
                    {
                        break;
                    }
                    currentPacket++;
                }
            }

            return true;
        }

        /// <summary>
        /// Sends a request whose payload is written by <paramref name="write"/> directly into the shared-memory node
        /// (no intermediate buffer). Payloads larger than one packet spill to a reusable buffer and are split as usual.
        /// </summary>
        public RpcResponse Send<TState>(TState state, Action<System.Buffers.IBufferWriter<byte>, TState> write, int timeoutMs = defaultTimeoutMs, CancellationToken cancellationToken = default)
            => SendAsync(state, write, timeoutMs, cancellationToken).GetAwaiter().GetResult();

        /// <inheritdoc cref="Send{TState}(TState, Action{System.Buffers.IBufferWriter{byte}, TState}, int, CancellationToken)"/>
        public Task<RpcResponse> SendAsync<TState>(TState state, Action<System.Buffers.IBufferWriter<byte>, TState> write, int timeoutMs = defaultTimeoutMs, CancellationToken cancellationToken = default)
            => SendCore(state, write, null, timeoutMs, cancellationToken);

        /// <summary>
        /// Like <see cref="RemoteStreamAsync(byte[], StreamItemHandler, CancellationToken)"/> but the request is written by
        /// <paramref name="write"/> straight into the shared-memory node (no intermediate array).
        /// </summary>
        public Task<RpcResponse> RemoteStreamAsync<TState>(TState state, Action<System.Buffers.IBufferWriter<byte>, TState> write, StreamItemHandler onItem, CancellationToken cancellationToken = default)
            => SendCore(state, write, onItem, Timeout.Infinite, cancellationToken);

        Task<RpcResponse> SendCore<TState>(TState state, Action<System.Buffers.IBufferWriter<byte>, TState> write, StreamItemHandler? onItem, int timeoutMs, CancellationToken cancellationToken)
        {
            ThrowIfDisposedOrShutdown();

            var request = CreateMessageRequest();
            request.OnStreamItem = onItem;
            Requests[request.MsgId] = request;

            bool success;
            int size;
            try
            {
                success = WriteProtocolV1(MessageType.RpcRequest, request.MsgId, 0, state, write, out size);
            }
            catch
            {
                Requests.TryRemove(request.MsgId, out _);
                throw;
            }

            if (!success)
            {
                Requests.TryRemove(request.MsgId, out _);
                return Task.FromResult(new RpcResponse(false, null));
            }

            Statistics.MessageSent(MessageType.RpcRequest, size);

            return request.ResponseReady.Task.TimeoutOrCancel(timeoutMs, cancellationToken);
        }

        NodeWriter? _nodeWriter;

        /// <summary>
        /// Replies to the incoming request <paramref name="requestMsgId"/> (direct handlers): <paramref name="write"/> writes the
        /// response straight into the client's node. Returns false if the client is gone.
        /// </summary>
        public bool Reply<TState>(ulong requestMsgId, TState state, Action<System.Buffers.IBufferWriter<byte>, TState> write)
        {
            if (!WriteProtocolV1(MessageType.RpcResponse, NextMsgId(), requestMsgId, state, write, out int size)) return false;

            Statistics.MessageSent(MessageType.RpcResponse, size);
            return true;
        }

        /// <summary>
        /// Sends a request written by <paramref name="write"/> straight into the node and parses the response with
        /// <paramref name="read"/> straight from the node (reader thread): no <see cref="RpcResponse"/> nor response array.
        /// Faults with <see cref="TimeoutException"/>, <see cref="OperationCanceledException"/> or the reader's exception.
        /// </summary>
        public Task<TOut> Call<TState, TReadState, TOut>(TState state, Action<System.Buffers.IBufferWriter<byte>, TState> write, TReadState readState, ResponseReader<TReadState, TOut> read, int timeoutMs = defaultTimeoutMs, CancellationToken cancellationToken = default)
            => CallCore(state, write, null, readState, read, timeoutMs, cancellationToken);

        /// <summary>
        /// Like <see cref="Call{TState, TReadState, TOut}"/> for a streaming handler: items reach <paramref name="onItem"/> in order
        /// on the reader thread, the final response completes the task. No timeout: cancel with <paramref name="cancellationToken"/>.
        /// </summary>
        public Task<TOut> CallStream<TState, TReadState, TOut>(TState state, Action<System.Buffers.IBufferWriter<byte>, TState> write, StreamItemHandler onItem, TReadState readState, ResponseReader<TReadState, TOut> read, CancellationToken cancellationToken = default)
            => CallCore(state, write, onItem, readState, read, Timeout.Infinite, cancellationToken);

        Task<TOut> CallCore<TState, TReadState, TOut>(TState state, Action<System.Buffers.IBufferWriter<byte>, TState> write, StreamItemHandler? onItem, TReadState readState, ResponseReader<TReadState, TOut> read, int timeoutMs, CancellationToken cancellationToken)
        {
            ThrowIfDisposedOrShutdown();

            var call = new PendingCall<TReadState, TOut>(readState, read) { MsgId = NextMsgId(), OnStreamItem = onItem, Pooled = true };
            Requests[call.MsgId] = call;

            bool success;
            int size;
            try
            {
                success = WriteProtocolV1(MessageType.RpcRequest, call.MsgId, 0, state, write, out size);
            }
            catch
            {
                Requests.TryRemove(call.MsgId, out _);
                throw;
            }

            if (!success)
            {
                Requests.TryRemove(call.MsgId, out _);
                return Task.FromException<TOut>(new System.IO.IOException("Channel closed: the request was not sent."));
            }

            Statistics.MessageSent(MessageType.RpcRequest, size);

            return AwaitCall(call, timeoutMs, cancellationToken);
        }

        async Task<TOut> AwaitCall<TReadState, TOut>(PendingCall<TReadState, TOut> call, int timeoutMs, CancellationToken cancellationToken)
        {
            try
            {
                return await call.Task.WaitAsync(timeoutMs == Timeout.Infinite ? Timeout.InfiniteTimeSpan : TimeSpan.FromMilliseconds(timeoutMs), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is TimeoutException or OperationCanceledException)
            {
                Requests.TryRemove(call.MsgId, out _); // a late response is discarded by the reader
                throw;
            }
        }

        unsafe bool WriteProtocolV1<TState>(MessageType msgType, ulong msgId, ulong responseId, TState state, Action<System.Buffers.IBufferWriter<byte>, TState> write, out int size)
        {
            size = 0;

            if (Disposed) return false;

            lock (lock_sendQ)
            {
                var target = Target(responseId);
                if (target is null || target.ShuttingDown) return false;

                var w = _nodeWriter ??= new NodeWriter();
                int hdr = protocolLength, chunk = msgBufferLength;

                Statistics.StartWaitWrite();
                var ctx = new FirstPacket<TState>(w, hdr, chunk, msgType, msgId, responseId, state, write);
                var bytes = target.Write(ref ctx, static (IntPtr ptr, ref FirstPacket<TState> c) =>
                {
                    byte* p = (byte*)ptr;
                    var w = c.Writer;
                    int hdr = c.Hdr, chunk = c.Chunk;
                    w.Reset(p + hdr, chunk);

                    try
                    {
                        c.Write(w, c.State);
                    }
                    catch (Exception ex)
                    {
                        // The node is already reserved: publish an orphan response (ResponseId 0) that the reader discards.
                        c.Error = ex;
                        WriteHeader(p, new RpcProtocolHeaderV1 { MsgType = MessageType.RpcResponse, CurrentPacket = 1, TotalPackets = 1 });
                        return hdr;
                    }

                    int len = w.Written, first = Math.Min(len, chunk);
                    WriteHeader(p, new RpcProtocolHeaderV1
                    {
                        MsgType = c.MsgType,
                        MsgId = c.MsgId,
                        ResponseId = c.ResponseId,
                        CurrentPacket = 1,
                        TotalPackets = len == 0 ? (ushort)1 : checked((ushort)((len + chunk - 1) / chunk)),
                        PayloadSize = len
                    });

                    if (w.Spilled) w.SpillData[..first].CopyTo(new Span<byte>(p + hdr, first));

                    return hdr + first;
                }, 1000);

                if (ctx.Error is { } error) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(error);
                if (bytes == 0) return false;

                Statistics.WritePacket(bytes - hdr);
                int total_size = size = w.Written;

                if (!w.Spilled || total_size <= chunk) return true;

                return WriteRemainingPackets(target, msgType, msgId, responseId, w, hdr, chunk, total_size);
            }
        }

        // Separate method: its closure would otherwise be allocated on every WriteProtocolV1 call.
        unsafe bool WriteRemainingPackets(CircularBuffer target, MessageType msgType, ulong msgId, ulong responseId, NodeWriter w, int hdr, int chunk, int total_size)
        {
            {
                int bytes;

                // Remaining packets of a spilled payload (copy path, only for payloads > one packet).
                ushort total = checked((ushort)((total_size + chunk - 1) / chunk));
                for (ushort packet = 2; packet <= total; packet++)
                {
                    if (target.ShuttingDown) return false;

                    int offset = (packet - 1) * chunk, len = Math.Min(chunk, total_size - offset);
                    ushort current = packet;

                    bytes = target.Write(ptr =>
                    {
                        byte* p = (byte*)ptr;
                        WriteHeader(p, new RpcProtocolHeaderV1 { MsgType = msgType, MsgId = msgId, ResponseId = responseId, CurrentPacket = current, TotalPackets = total, PayloadSize = total_size });
                        w.SpillData.Slice(offset, len).CopyTo(new Span<byte>(p + hdr, len));
                        return hdr + len;
                    }, 1000);

                    if (bytes == 0) return false;

                    Statistics.WritePacket(bytes - hdr);
                }

                return true;
            }
        }

        static unsafe void WriteHeader(byte* p, RpcProtocolHeaderV1 header) => MemoryMarshal.Write(new Span<byte>(p, FastStructure<RpcProtocolHeaderV1>.Size), in header);

        struct FirstPacket<TState>(NodeWriter writer, int hdr, int chunk, MessageType msgType, ulong msgId, ulong responseId, TState state, Action<System.Buffers.IBufferWriter<byte>, TState> write)
        {
            public readonly NodeWriter Writer = writer;
            public readonly int Hdr = hdr, Chunk = chunk;
            public readonly MessageType MsgType = msgType;
            public readonly ulong MsgId = msgId, ResponseId = responseId;
            public readonly TState State = state;
            public readonly Action<System.Buffers.IBufferWriter<byte>, TState> Write = write;
            public Exception? Error;
        }

        /// <summary>Writes into a shared-memory node; spills to a reusable heap buffer once the node is full.</summary>
        // ponytail: the spill buffer keeps its peak size for the RpcBuffer lifetime; pool it if big payloads become common.
        sealed unsafe class NodeWriter : System.Buffers.IBufferWriter<byte>
        {
            byte* _dst;
            int _cap, _written;
            System.Buffers.ArrayBufferWriter<byte>? _spill;

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

                _spill ??= new System.Buffers.ArrayBufferWriter<byte>(Math.Max(_cap * 2, 256));
                new ReadOnlySpan<byte>(_dst, _written).CopyTo(_spill.GetSpan(_written));
                _spill.Advance(_written);
                Spilled = true;
            }
        }

        private Func<IntPtr, int>? _readPacket;
        private bool m_ReadThreadIsReading = false;
        private readonly object m_ReadThreadIsReadingLock = new();

        void ReadThreadV1()
        {
            try
            {
                // Work with Local Variable to prevent NPE after dispose  
                CircularBuffer l_TempReadBuffer = ReadBuffer;

                while (true)
                {
                    // Check If Reading must be stopped
                    if (_needDisposeManagedResources)
                    {
                        DisposeManagedResources();
                        return;
                    }

                    // Claim the reading marker atomically with the disposed check: dispose unmaps the view
                    // only while the marker is clear, so the buffer must not be touched outside of it
                    lock (m_ReadThreadIsReadingLock)
                    {
                        if (Interlocked.Read(ref _disposed) != 0)
                            return;

                        m_ReadThreadIsReading = true;
                    }

                    try
                    {
                        if (l_TempReadBuffer.ShuttingDown)
                            return;

                        Statistics.StartWaitRead();

                        l_TempReadBuffer.Read(_readPacket ??= ptr =>
                        {
                            int readLength = 0;
                            var header = RpcProtocolHeaderV1.FromPointer(ptr);
                            ptr += protocolLength;
                            readLength += protocolLength;


                            RpcRequest request;
                            switch (header.MsgType)
                            {
                                case MessageType.RpcResponse:
                                case MessageType.ErrorInRpc:
                                    if (!Requests.TryGetValue(header.ResponseId, out request!))
                                    {
                                        // The response received does not have a  matching message that was sent
                                        Statistics.DiscardResponse(header.ResponseId);
                                        return protocolLength;
                                    }

                                    if (header.TotalPackets == 1 && request is PendingCall direct)
                                    {
                                        // Fast path: the response is parsed straight from the node, no array/RpcResponse
                                        int len = header.PayloadSize;
                                        Requests.TryRemove(header.ResponseId, out _);
                                        Statistics.MessageReceived(header.MsgType, len);
                                        unsafe { direct.Complete(header.MsgType == MessageType.RpcResponse, new ReadOnlySpan<byte>((void*)ptr, len)); }
                                        Statistics.ReadPacket(len);
                                        return protocolLength + len;
                                    }
                                    break;
                                case MessageType.StreamItem when !Requests.ContainsKey(header.ResponseId):
                                    // Item of a stream opened by another peer
                                    Statistics.DiscardResponse(header.ResponseId);
                                    return protocolLength;
                                case MessageType.StreamItem when header.TotalPackets == 1 && Requests.TryGetValue(header.ResponseId, out var streamOwner):
                                    {
                                        // Fast path: single-packet item goes straight to its owner, no RpcRequest/TCS per item
                                        int size = header.PayloadSize;
                                        Statistics.MessageReceived(header.MsgType, size);
                                        unsafe { streamOwner.OnStreamItem?.Invoke(new ReadOnlySpan<byte>((void*)ptr, size)); }
                                        return protocolLength + size;
                                    }
                                case MessageType.Close when _clients is not null:
                                    lock (lock_sendQ) DropClient((int)(header.MsgId >> ClientShift));
                                    return protocolLength;
                                default:
                                    // Direct handlers and stream items only read the payload while processing it: rent it.
                                    bool pooled = header.MsgType == MessageType.StreamItem || RequestHandler is not null || AsyncRequestHandler is not null;
                                    request = header.TotalPackets == 1
                                        ? new RpcRequest { MsgId = header.MsgId, Pooled = pooled }
                                        : IncomingRequests.GetOrAdd(header.MsgId, static (id, pooled) => new RpcRequest { MsgId = id, Pooled = pooled }, pooled);
                                    break;
                            }

                            int packetSize = header.PayloadSize < msgBufferLength ? header.PayloadSize :
                                (header.CurrentPacket < header.TotalPackets ? msgBufferLength : header.PayloadSize % msgBufferLength);

                            if (header.PayloadSize > 0)
                            {
                                request.Data ??= request.Pooled ? System.Buffers.ArrayPool<byte>.Shared.Rent(header.PayloadSize) : new byte[header.PayloadSize];
                                request.Length = header.PayloadSize;

                                int index = msgBufferLength * (header.CurrentPacket - 1);
                                request.Data.ReadBytes(ptr, index, packetSize);
                                readLength += packetSize;
                            }

                            if (header.CurrentPacket == header.TotalPackets)
                            {
                                switch (header.MsgType)
                                {
                                    case MessageType.RpcResponse:
                                    case MessageType.ErrorInRpc:
                                        Requests.TryRemove(request.MsgId, out _);
                                        break;

                                    default:
                                        IncomingRequests.TryRemove(request.MsgId, out _);
                                        break;
                                }

                                // Full message is ready

                                Statistics.MessageReceived(header.MsgType, request.Length);


                                switch (header.MsgType)
                                {
                                    case MessageType.RpcResponse or MessageType.ErrorInRpc when request is PendingCall call:
                                        call.Complete(header.MsgType == MessageType.RpcResponse, request.Data.AsSpan(0, request.Length));
                                        ReturnPooled(request);
                                        break;
                                    case MessageType.RpcResponse:
                                        request.IsSuccess = true;
                                        request.ResponseReady.SetResult(new RpcResponse(request.IsSuccess, request.Data));
                                        break;
                                    case MessageType.ErrorInRpc:
                                        request.IsSuccess = false;
                                        request.ResponseReady.SetResult(new RpcResponse(request.IsSuccess, request.Data));
                                        break;
                                    case MessageType.StreamItem:
                                        if (Requests.TryGetValue(header.ResponseId, out var owner))
                                            owner.OnStreamItem?.Invoke(request.Data.AsSpan(0, request.Length));
                                        ReturnPooled(request);
                                        break;
                                    case MessageType.RpcRequest:
                                        DispatchRequest(request);
                                        break;
                                }
                            }

                            Statistics.ReadPacket(packetSize);

                            return protocolLength + packetSize;
                        }, 500);
                    }
                    finally
                    {
                        lock (m_ReadThreadIsReadingLock)
                        {
                            m_ReadThreadIsReading = false;
                        }
                    }
                }
            }
            finally
            {
                // Make sure that Dispose ManageResource has been progressed
                if (_needDisposeManagedResources)
                {
                    DisposeManagedResources();
                }
            }
        }

        // Separate method: capturing `request` inline allocated a closure per packet read, even for stream items.
        // Same pool hop as Task.Run, minus the Task, closure and ExecutionContext capture.
        void DispatchRequest(RpcRequest request) =>
            ThreadPool.UnsafeQueueUserWorkItem(static s => _ = s.Self.ProcessCallHandler(s.Request), (Self: this, Request: request), preferLocal: false);

        private int _processCount = 0;
        private object _processLock = new object();

        static void ReturnPooled(RpcRequest request)
        {
            if (!request.Pooled || request.Data is not { } data) return;

            request.Data = null!;
            System.Buffers.ArrayPool<byte>.Shared.Return(data);
        }

        // Replies go straight into the node: no RpcRequest/TCS, per-packet array or closure per response.
        void SendReply(MessageType msgType, ulong responseId, byte[]? payload)
        {
            if (WriteProtocolV1(msgType, NextMsgId(), responseId, payload, static (w, data) =>
                {
                    if (data is not null) System.Buffers.BuffersExtensions.Write(w, (ReadOnlySpan<byte>)data);
                }, out int size))
                Statistics.MessageSent(msgType, size);
        }

        // Fire-and-forget safe: never faults, so callers can just discard the task.
        async Task ProcessCallHandler(RpcRequest request)
        {
            // Mark as processing
            lock (_processLock)
            {
                _processCount++;
            }

            try
            {
                if (RequestHandler is { } direct)
                {
                    direct(request.MsgId, new ReadOnlyMemory<byte>(request.Data, 0, request.Length));
                }
                else if (AsyncRequestHandler is { } directAsync)
                {
                    await directAsync(request.MsgId, new ReadOnlyMemory<byte>(request.Data, 0, request.Length)).ConfigureAwait(false);
                }
                else if (RemoteCallHandler != null)
                {
                    RemoteCallHandler(request.MsgId, request.Data);
                    SendReply(MessageType.RpcResponse, request.MsgId, null);
                }
                else if (AsyncRemoteCallHandler != null)
                {
                    await AsyncRemoteCallHandler(request.MsgId, request.Data).ConfigureAwait(false);
                    SendReply(MessageType.RpcResponse, request.MsgId, null);
                }
                else if (RemoteCallHandlerWithResult != null)
                {
                    SendReply(MessageType.RpcResponse, request.MsgId, RemoteCallHandlerWithResult(request.MsgId, request.Data));
                }
                else if (AsyncRemoteCallHandlerWithResult != null)
                {
                    var result = await AsyncRemoteCallHandlerWithResult(request.MsgId, request.Data)
                        .ConfigureAwait(false);
                    SendReply(MessageType.RpcResponse, request.MsgId, result);
                }
            }
            catch
            {
                try { SendReply(MessageType.ErrorInRpc, request.MsgId, null); }
                catch { /* disposed/closed channel: nobody left to notify */ }
            }
            finally
            {
                ReturnPooled(request);

                lock (_processLock)
                {
                    _processCount--;
                }

                // Make sure that ManagedResources are disposed if needed
                if (_needDisposeManagedResources)
                {
                    DisposeManagedResources();
                }
            }
        }

        #region IDisposable

        /// <summary>
        /// Checks that the object has not been disposed, and that the underlying buffers are not shutting down.
        /// </summary>
        /// <exception cref="ObjectDisposedException">Thrown if this object has been disposed</exception>
        /// <exception cref="InvalidOperationException">Thrown if the underlying buffers have been closed by the channel owner</exception>
        protected void ThrowIfDisposedOrShutdown()
        {
            if (Disposed)
            {
                throw new ObjectDisposedException("RpcBuffer");
            }

            if (ReadBuffer.ShuttingDown || WriteBuffer is { ShuttingDown: true })
            {
                throw new InvalidOperationException("Channel owner has closed buffers");
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// IDisposable pattern - dispose of managed/unmanaged resources
        /// </summary>
        /// <param name="disposeManagedResources">true to dispose of managed resources as well as unmanaged.</param>
        protected virtual void Dispose(bool disposeManagedResources)
        {
            if (Disposed)
            {
                return;
            }

            if (disposeManagedResources)
            {
                SendClose();

                // The unmap may be deferred while the reader is inside Read: raise the flag now so the peer sees it at once
                if (ReadBuffer is { IsOwnerOfSharedMemory: true } r) r.MarkShutdown();
                if (WriteBuffer is { IsOwnerOfSharedMemory: true } w) w.MarkShutdown();

                DisposeManagedResources();
            }
        }

        private bool _needDisposeManagedResources = false;

        private void DisposeManagedResources()
        {
            lock (_processLock)
            {
                lock (m_ReadThreadIsReadingLock)
                {
                    // Check if dispose is possible otherwise
                    // mark for dispose later
                    if (_processCount > 0)
                    {
                        _needDisposeManagedResources = true;
                        return;
                    }

                    if (m_ReadThreadIsReading)
                    {
                        _needDisposeManagedResources = true;
                        return;
                    }

                    // Disconnect handle to prevent processing
                    RemoteCallHandler = null;
                    AsyncRemoteCallHandler = null;
                    RemoteCallHandlerWithResult = null;
                    AsyncRemoteCallHandlerWithResult = null;
                    RequestHandler = null;
                    AsyncRequestHandler = null;
                    _needDisposeManagedResources = false;

                    // Mark as Disposed under the reader lock: the reader checks it before claiming the buffer.
                    // Only one Thread processes the dispose
                    if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
                        return;
                }
            }

            if (WriteBuffer != null)
            {
                WriteBuffer.Dispose();
                WriteBuffer = null!;
            }

            if (_clients != null)
            {
                lock (lock_sendQ)
                {
                    foreach (var ring in _clients.Values) ring.Dispose();
                    _clients.Clear();
                }
            }

            if (ReadBuffer != null)
            {
                ReadBuffer.Dispose();
                ReadBuffer = null!;
            }

            if (masterMutex != null)
            {
                masterMutex.Close();
                masterMutex.Dispose();
                masterMutex = null!;
            }

            // Mark as DisposeFinished
            Interlocked.Exchange(ref _disposed, 2);
        }

        #endregion
    }
}
