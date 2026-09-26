using System;
using System.IO;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace SharedMemory
{
    public class BufferReader(byte[] buffer, Encoding? encoding = null)
    {
        protected ReadOnlyMemory<byte> _buffer = MemoryExtensions.AsMemory(buffer);
        Decoder? _decoder;
        char[]? _charBuffer = null;
        private const int MaxCharBytesSize = 128;
        int _pos = 0;
        readonly Encoding _encoding = encoding ?? Encoding.UTF8;
        readonly int _bufferLen = buffer.Length;
        readonly int _maxCharsSize = (encoding ?? Encoding.UTF8).GetMaxCharCount(MaxCharBytesSize);

        public bool ReadBoolean() => ReadByte() != 0;

        public byte ReadByte() => _buffer.Span[_pos++];

        public double ReadDouble() => Int64BitsToDouble(ReadInt64());

        public decimal ReadDecimal()
        {
            try
            {
                return ToDecimal(default, _buffer.Span[_pos..(_pos += 16)]);
            }
            catch (ArgumentException e)
            {
                throw new IOException("ReadDecimal cannot leak out", e);
            }
        }

        public char ReadChar() => MemoryMarshal.Read<char>(_buffer.Span[_pos..(_pos += 2)]);

        public ushort ReadUInt16() => BinaryPrimitives.ReadUInt16LittleEndian(_buffer.Span[_pos..(_pos += 2)]);

        public uint ReadUInt32() => BinaryPrimitives.ReadUInt32LittleEndian(_buffer.Span[_pos..(_pos += 4)]);

        public ulong ReadUInt64() => BinaryPrimitives.ReadUInt64LittleEndian(_buffer.Span[_pos..(_pos += 8)]);

        public short ReadInt16() => BinaryPrimitives.ReadInt16LittleEndian(_buffer.Span[_pos..(_pos += 2)]);

        public int ReadInt32() => BinaryPrimitives.ReadInt32LittleEndian(_buffer.Span[_pos..(_pos += 4)]);

        public long ReadInt64() => BinaryPrimitives.ReadInt64LittleEndian(_buffer.Span[_pos..(_pos += 8)]);

        public Guid ReadGuid() => MemoryMarshal.Read<Guid>(_buffer.Span[_pos..(_pos += 16)]);

        public unsafe string ReadString()
        {
            // Length of the string in bytes, not chars
            int stringLength = Read7BitEncodedInt();

            if (stringLength < 0) throw new IOException("Invalid string length", stringLength);

            if (stringLength == 0) return string.Empty;

            ReadOnlySpan<byte> charBytes = stackalloc byte[MaxCharBytesSize];

            int currPos = 0;

            StringBuilder? sb = null;
            do
            {
                int readLength = Math.Min(MaxCharBytesSize, stringLength - currPos);
                int n;
                // Read(Span<byte>) inlined
                {
                    var slice = new Span<byte>((byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(charBytes)), readLength);
                    n = Math.Min(_bufferLen - _pos, slice.Length);
                    if (n <= 0)
                        n = 0;

                    _buffer.Span.Slice(_pos, n).CopyTo(slice);

                    _pos += n;
                }

                if (n == 0)
                {
                    throw new IOException("End of buffer");
                }

                if (currPos == 0 && n == stringLength)
                {
                    _encoding.GetString((byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(charBytes)), n);
                }

                _decoder ??= _encoding.GetDecoder();
                _charBuffer ??= new char[_maxCharsSize];

                int charsRead = GetChars(_decoder, charBytes[..n], _charBuffer, flush: false);

                // Since we could be reading from an untrusted data source, limit the initial size of the
                // StringBuilder instance we're about to get or create. It'll expand automatically as needed.

                sb ??= StringBuilderCache.Acquire(Math.Min(stringLength, StringBuilderCache.MaxBuilderSize)); // Actual string length in chars may be smaller.
                sb.Append(_charBuffer, 0, charsRead);
                currPos += n;
            } while (currPos < stringLength);

            return StringBuilderCache.GetStringAndRelease(sb);
        }

        static unsafe int GetChars(Decoder decoder, ReadOnlySpan<byte> bytes, Span<char> chars, bool flush)
        {
            fixed (byte* nonNullPinnableReference = &bytes.GetNonNullPinnableReference())
            {
                fixed (char* nonNullPinnableReference2 = &chars.GetNonNullPinnableReference())
                {
                    return decoder.GetChars(nonNullPinnableReference, bytes.Length, nonNullPinnableReference2, chars.Length, flush);
                }
            }
        }

        public bool? TryReadBoolean() => ReadBoolean() ? ReadBoolean() : null;

        public byte? TryReadByte() => ReadBoolean() ? ReadByte() : null;

        public double? TryReadDouble() => ReadBoolean() ? ReadDouble() : null;

        public decimal? TryReadDecimal() => ReadBoolean() ? ReadDecimal() : null;

        public char? TryReadChar() => ReadBoolean() ? ReadChar() : null;

        public ushort? TryReadUInt16() => ReadBoolean() ? ReadUInt16() : null;

        public uint? TryReadUInt32() => ReadBoolean() ? ReadUInt32() : null;

        public ulong? TryReadUInt64() => ReadBoolean() ? ReadUInt64() : null;

        public short? TryReadInt16() => ReadBoolean() ? ReadInt16() : null;

        public int? TryReadInt32() => ReadBoolean() ? ReadInt32() : null;

        public long? TryReadInt64() => ReadBoolean() ? ReadInt64() : null;

        public Guid? TryReadGuid() => ReadBoolean() ? ReadGuid() : null;

        public string? TryReadString() => ReadBoolean() ? ReadString() : null;

        public int Read7BitEncodedInt()
        {
            // Unlike writing, we can't delegate to the 64-bit read on
            // 64-bit platforms. The reason for this is that we want to
            // stop consuming bytes if we encounter an integer overflow.

            uint result = 0;
            byte byteReadJustNow;

            // Read the integer 7 bits at a time. The high bit
            // of the byte when on means to continue reading more bytes.
            //
            // There are two failure cases: we've read more than 5 bytes,
            // or the fifth byte is about to cause integer overflow.
            // This means that we can read the first 4 bytes without
            // worrying about integer overflow.

            const int MaxBytesWithoutOverflow = 4;
            for (int shift = 0; shift < MaxBytesWithoutOverflow * 7; shift += 7)
            {
                // ReadByte handles end of stream cases for us.
                byteReadJustNow = ReadByte();
                result |= (byteReadJustNow & 0x7Fu) << shift;

                if (byteReadJustNow <= 0x7Fu)
                {
                    return (int)result; // early exit
                }
            }

            // Read the 5th byte. Since we already read 28 bits,
            // the value of this byte must fit within 4 bits (32 - 28),
            // and it must not have the high bit set.

            byteReadJustNow = ReadByte();
            if (byteReadJustNow > 0b_1111u)
            {
                throw new FormatException("Bad 7-bit int format");
            }

            result |= (uint)byteReadJustNow << (MaxBytesWithoutOverflow * 7);
            return (int)result;
        }

        [Intrinsic]
        static double Int64BitsToDouble(long value) => BitCast<long, double>(value);
        public static TTo BitCast<TFrom, TTo>(TFrom source)
        {
            if (Unsafe.SizeOf<TFrom>() != Unsafe.SizeOf<TTo>() || !typeof(TFrom).IsValueType || !typeof(TTo).IsValueType)
            {
                throw new NotSupportedException();
            }
            return Unsafe.ReadUnaligned<TTo>(ref Unsafe.As<TFrom, byte>(ref source));
        }

        [UnsafeAccessor(UnsafeAccessorKind.StaticMethod, Name = "ToDecimal")]
        static extern decimal ToDecimal(decimal _, ReadOnlySpan<byte> span);

        public ReadOnlySpan<byte> AsSpan()
        {
            return _buffer.Span[_pos..];
        }
        public void Reset() => _pos = 0;
        static class StringBuilderCache
        {
            // The value 360 was chosen in discussion with performance experts as a compromise between using
            // as little memory per thread as possible and still covering a large part of short-lived
            // StringBuilder creations on the startup path of VS designers.
            internal const int MaxBuilderSize = 360;
            private const int DefaultCapacity = 16; // == StringBuilder.DefaultCapacity

            [ThreadStatic]
            private static StringBuilder? t_cachedInstance;

            /// <summary>Get a StringBuilder for the specified capacity.</summary>
            /// <remarks>If a StringBuilder of an appropriate size is cached, it will be returned and the cache emptied.</remarks>
            public static StringBuilder Acquire(int capacity = DefaultCapacity)
            {
                if (capacity <= MaxBuilderSize)
                {
                    StringBuilder? sb = t_cachedInstance;
                    if (sb != null)
                    {
                        // Avoid stringbuilder block fragmentation by getting a new StringBuilder
                        // when the requested size is larger than the current capacity
                        if (capacity <= sb.Capacity)
                        {
                            t_cachedInstance = null;
                            sb.Clear();
                            return sb;
                        }
                    }
                }

                return new StringBuilder(capacity);
            }

            /// <summary>Place the specified builder in the cache if it is not too big.</summary>
            public static void Release(StringBuilder sb)
            {
                if (sb.Capacity <= MaxBuilderSize)
                {
                    t_cachedInstance = sb;
                }
            }

            /// <summary>ToString() the stringbuilder, Release it to the cache, and return the resulting string.</summary>
            public static string GetStringAndRelease(StringBuilder sb)
            {
                string result = sb.ToString();
                Release(sb);
                return result;
            }
        }
    }

    public sealed class BufferBuilder() : IBufferWriter<byte>
    {
        [UnsafeAccessor(UnsafeAccessorKind.StaticMethod, Name = "GetBytes")]
        static extern void GetDecimalBytes(decimal _, in decimal d, Span<byte> buffer);

        [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_IsUTF8CodePage")]
        static extern bool IsUTF8CodePage(Encoding _);

        // Copy of Array.MaxLength.
        // Used by projects targeting .NET Framework.
        private const int ArrayMaxLength = 0x7FFFFFC7;

        private const int DefaultInitialBufferSize = 256;
        private readonly Encoding _encoding = Encoding.UTF8;
        private byte[] _buffer = [];
        private int _index = 0;


        /// <summary>
        /// Creates an instance of an <see cref="BufferBuilder{byte}"/>, in which data can be written to,
        /// with the default initial capacity.
        /// </summary>
        public BufferBuilder(Encoding encoding) : this()
        {
            _index = 0;
            _encoding = encoding;
        }

        /// <summary>
        /// Creates an instance of an <see cref="ArrayBufferWriter{byte}"/>, in which data can be written to,
        /// with an initial capacity specified.
        /// </summary>
        /// <param name="initialCapacity">The minimum capacity with which to initialize the underlying buffer.</param>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="initialCapacity"/> is not positive (i.e. less than or equal to 0).
        /// </exception>
        public BufferBuilder(int initialCapacity) : this()
        {
            if (initialCapacity <= 0)
                throw new ArgumentException(null, nameof(initialCapacity));

            _buffer = new byte[initialCapacity];
            _index = 0;
        }

        /// <summary>
        /// Returns the data written to the underlying buffer so far, as a <see cref="ReadOnlyMemory{byte}"/>.
        /// </summary>
        public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _index);

        /// <summary>
        /// Returns the data written to the underlying buffer so far, as a <see cref="ReadOnlySpan{byte}"/>.
        /// </summary>
        public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _index);

        /// <summary>
        /// Returns the amount of data written to the underlying buffer so far.
        /// </summary>
        public int WrittenCount => _index;

        /// <summary>
        /// Returns the total amount of space within the underlying buffer.
        /// </summary>
        public int Capacity => _buffer.Length;

        /// <summary>
        /// Returns the amount of space available that can still be written into without forcing the underlying buffer to grow.
        /// </summary>
        public int FreeCapacity => _buffer.Length - _index;

        /// <summary>
        /// Clears the data written to the underlying buffer.
        /// </summary>
        /// <remarks>
        /// <para>
        /// You must reset or clear the <see cref="ArrayBufferWriter{byte}"/> before trying to re-use it.
        /// </para>
        /// <para>
        /// The <see cref="ResetWrittenCount"/> method is faster since it only sets to zero the writer's index
        /// while the <see cref="Clear"/> method additionally zeroes the content of the underlying buffer.
        /// </para>
        /// </remarks>
        /// <seealso cref="ResetWrittenCount"/>
        public void Clear()
        {
            Debug.Assert(_buffer.Length >= _index);
            _buffer.AsSpan(0, _index).Clear();
            _index = 0;
        }

        /// <summary>
        /// Resets the data written to the underlying buffer without zeroing its content.
        /// </summary>
        /// <remarks>
        /// <para>
        /// You must reset or clear the <see cref="ArrayBufferWriter{byte}"/> before trying to re-use it.
        /// </para>
        /// <para>
        /// If you reset the writer using the <see cref="ResetWrittenCount"/> method, the underlying buffer will not be cleared.
        /// </para>
        /// </remarks>
        /// <seealso cref="Clear"/>
        public void ResetWrittenCount() => _index = 0;

        /// <summary>
        /// Notifies <see cref="IBufferWriter{byte}"/> that <paramref name="count"/> amount of data was written to the output <see cref="Span{byte}"/>/<see cref="Memory{byte}"/>
        /// </summary>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="count"/> is negative.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when attempting to advance past the end of the underlying buffer.
        /// </exception>
        /// <remarks>
        /// You must request a new buffer after calling Advance to continue writing more data and cannot write to a previously acquired buffer.
        /// </remarks>
        public void Advance(int count)
        {
            if (count < 0)
                throw new ArgumentException(null, nameof(count));

            if (_index > _buffer.Length - count)
                ThrowInvalidOperationException_AdvancedTooFar(_buffer.Length);

            _index += count;
        }

        /// <summary>
        /// Returns a <see cref="Memory{byte}"/> to write to that is at least the requested length (specified by <paramref name="sizeHint"/>).
        /// If no <paramref name="sizeHint"/> is provided (or it's equal to <code>0</code>), some non-empty buffer is returned.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="sizeHint"/> is negative.
        /// </exception>
        /// <remarks>
        /// <para>
        /// This will never return an empty <see cref="Memory{byte}"/>.
        /// </para>
        /// <para>
        /// There is no guarantee that successive calls will return the same buffer or the same-sized buffer.
        /// </para>
        /// <para>
        /// You must request a new buffer after calling Advance to continue writing more data and cannot write to a previously acquired buffer.
        /// </para>
        /// <para>
        /// If you reset the writer using the <see cref="ResetWrittenCount"/> method, this method may return a non-cleared <see cref="Memory{byte}"/>.
        /// </para>
        /// <para>
        /// If you clear the writer using the <see cref="Clear"/> method, this method will return a <see cref="Memory{byte}"/> with its content zeroed.
        /// </para>
        /// </remarks>
        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            CheckAndResizeBuffer(sizeHint);
            Debug.Assert(_buffer.Length > _index);
            return _buffer.AsMemory(_index);
        }

        /// <summary>
        /// Returns a <see cref="Span{byte}"/> to write to that is at least the requested length (specified by <paramref name="sizeHint"/>).
        /// If no <paramref name="sizeHint"/> is provided (or it's equal to <code>0</code>), some non-empty buffer is returned.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="sizeHint"/> is negative.
        /// </exception>
        /// <remarks>
        /// <para>
        /// This will never return an empty <see cref="Span{byte}"/>.
        /// </para>
        /// <para>
        /// There is no guarantee that successive calls will return the same buffer or the same-sized buffer.
        /// </para>
        /// <para>
        /// You must request a new buffer after calling Advance to continue writing more data and cannot write to a previously acquired buffer.
        /// </para>
        /// <para>
        /// If you reset the writer using the <see cref="ResetWrittenCount"/> method, this method may return a non-cleared <see cref="Span{byte}"/>.
        /// </para>
        /// <para>
        /// If you clear the writer using the <see cref="Clear"/> method, this method will return a <see cref="Span{byte}"/> with its content zeroed.
        /// </para>
        /// </remarks>
        public Span<byte> GetSpan(int sizeHint = 0)
        {
            CheckAndResizeBuffer(sizeHint);
            Debug.Assert(_buffer.Length > _index);
            return _buffer.AsSpan(_index);
        }

        private void CheckAndResizeBuffer(int sizeHint)
        {
            if (sizeHint < 0)
                throw new ArgumentException(nameof(sizeHint));

            if (sizeHint == 0)
            {
                sizeHint = 1;
            }

            if (sizeHint > FreeCapacity)
            {
                int currentLength = _buffer.Length;

                // Attempt to grow by the larger of the sizeHint and double the current size.
                int growBy = Math.Max(sizeHint, currentLength);

                if (currentLength == 0)
                {
                    growBy = Math.Max(growBy, DefaultInitialBufferSize);
                }

                int newSize = currentLength + growBy;

                if ((uint)newSize > int.MaxValue)
                {
                    // Attempt to grow to ArrayMaxLength.
                    uint needed = (uint)(currentLength - FreeCapacity + sizeHint);
                    Debug.Assert(needed > currentLength);

                    if (needed > ArrayMaxLength)
                    {
                        ThrowOutOfMemoryException(needed);
                    }

                    newSize = ArrayMaxLength;
                }

                Array.Resize(ref _buffer, newSize);
            }

            Debug.Assert(FreeCapacity > 0 && FreeCapacity >= sizeHint);
        }

        static bool UseFastUtf8(Encoding encoding) => IsUTF8CodePage(encoding) && encoding.EncoderFallback.MaxCharCount <= 1;

        public void Write(bool value)
        {
            Write((byte)(value ? 1 : 0));
        }

        public void Write(byte value) => Write([value]);

        void Write(ReadOnlySpan<byte> value)
        {
            Span<byte> span = GetSpan();
            if (value.Length <= span.Length)
            {
                value.CopyTo(span);
                Advance(value.Length);
            }
            else
            {
                WriteMultiSegment(value, span);
            }
        }

        private void WriteMultiSegment(in ReadOnlySpan<byte> source, Span<byte> destination)
        {
            ReadOnlySpan<byte> readOnlySpan = source;
            while (true)
            {
                var num = Math.Min(destination.Length, readOnlySpan.Length);
                readOnlySpan[..num].CopyTo(destination);
                Advance(num);

                if ((readOnlySpan = readOnlySpan[num..]).Length <= 0) break; 

                destination = GetSpan(readOnlySpan.Length);
            }
        }

        public void Write(decimal value)
        {
            Span<byte> span = stackalloc byte[16];
            GetDecimalBytes(default, value, span);
            Write(span);
        }

        public void Write(char value)
        {
            Write(_encoding.GetBytes([value]));
        }

        public void Write(ushort value)
        {
            Span<byte> span = stackalloc byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(span, value);
            Write(span);
        }

        public void Write(uint value)
        {
            Span<byte> span = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(span, value);
            Write(span);
        }

        public void Write(ulong value)
        {
            Span<byte> span = stackalloc byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(span, value);
            Write(span);
        }

        public void Write(short value)
        {
            Span<byte> span = stackalloc byte[2];
            BinaryPrimitives.WriteInt16LittleEndian(span, value);
            Write(span);
        }

        public void Write(int value)
        {
            Span<byte> span = stackalloc byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(span, value);
            Write(span);
        }

        public void Write(double value)
        {
            Span<byte> span = stackalloc byte[8];
            if (!BitConverter.IsLittleEndian) { }
            MemoryMarshal.Write(span,
#if !NETCOREAPP2_1_OR_GREATER
                ref 
#else
                in
#endif
                value);
            Write(span);
        }

        public void Write(long value)
        {
            Span<byte> span = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(span, value);
            Write(span);
        }

        public void Write(Guid value)
        {
            Write(value.ToByteArray());
        }

        public void Write(string value)
        {
            if (value is null) throw new ArgumentNullException("value");

            if (UseFastUtf8(_encoding))
            {
                if (value.Length <= 42)
                {
                    Span<byte> span = stackalloc byte[128];
                    int bytes = GetEncodingBytes(_encoding, value.AsSpan(), span[1..]);
                    span[0] = (byte)bytes;
                    Write(span[..(bytes + 1)]);
                    return;
                }
                if (value.Length <= 21845)
                {
                    byte[] array = ArrayPool<byte>.Shared.Rent(value.Length * 3);
                    int bytes2 = GetEncodingBytes(_encoding, value.AsSpan(), array);
                    Write7BitEncodedInt(bytes2);
                    Write(array.AsSpan(0, bytes2));
                    ArrayPool<byte>.Shared.Return(array);
                    return;
                }
            }
            int byteCount = _encoding.GetByteCount(value);
            Write7BitEncodedInt(byteCount);
            WriteCharsCommonWithoutLengthPrefix(value);
        }

        static unsafe int GetEncodingBytes(Encoding encoding, ReadOnlySpan<char> chars, Span<byte> bytes)
        {
            fixed (char* nonNullPinnableReference = &chars.GetNonNullPinnableReference())
            fixed (byte* nonNullPinnableReference2 = &bytes.GetNonNullPinnableReference())
                return encoding.GetBytes(nonNullPinnableReference, chars.Length, nonNullPinnableReference2, bytes.Length);
        }

        public void TryWrite(bool? value)
        {
            Write(value.HasValue);
            if (value.HasValue) Write(value.Value);
        }

        public void TryWrite(byte? value)
        {
            Write(value.HasValue);
            if (value.HasValue) Write(value.Value);
        }

        public void TryWrite(double? value)
        {
            Write(value.HasValue);
            if (value.HasValue) Write(value.Value);
        }

        public void TryWrite(decimal? value)
        {
            Write(value.HasValue);
            if (value.HasValue) Write(value.Value);
        }

        public void TryWrite(char? value)
        {
            Write(value.HasValue);
            if (value.HasValue) Write(value.Value);
        }

        public void TryWrite(ushort? value)
        {
            Write(value.HasValue);
            if (value.HasValue) Write(value.Value);
        }

        public void TryWrite(uint? value)
        {
            Write(value.HasValue);
            if (value.HasValue) Write(value.Value);
        }

        public void TryWrite(ulong? value)
        {
            Write(value.HasValue);
            if (value.HasValue) Write(value.Value);
        }

        public void TryWrite(short? value)
        {
            Write(value.HasValue);
            if (value.HasValue) Write(value.Value);
        }

        public void TryWrite(int? value)
        {
            Write(value.HasValue);
            if (value.HasValue) Write(value.Value);
        }

        public void TryWrite(long? value)
        {
            Write(value.HasValue);
            if (value.HasValue) Write(value.Value);
        }

        public void TryWrite(Guid? value)
        {
            Write(value.HasValue);
            if (value.HasValue) Write(value.Value);
        }

        public void TryWrite(string? value)
        {
            var hasValue = value != null;
            Write(hasValue);
            if (hasValue) Write(value!);
        }

        public void Write7BitEncodedInt(int value)
        {
            uint num;
            for (num = (uint)value; num > 127; num >>= 7)
            {
                Write((byte)(num | 0xFFFFFF80u));
            }
            Write((byte)num);
        }

        private void WriteCharsCommonWithoutLengthPrefix(ReadOnlySpan<char> chars)
        {
            byte[] array;
            if (chars.Length <= 65536)
            {
                int maxByteCount = _encoding.GetMaxByteCount(chars.Length);
                if (maxByteCount <= 65536)
                {
                    array = ArrayPool<byte>.Shared.Rent(maxByteCount);
                    int bytes = GetEncodingBytes(_encoding, chars, array);
                    Write(array.AsSpan(0, bytes));
                    ArrayPool<byte>.Shared.Return(array);
                    return;
                }
            }
            array = ArrayPool<byte>.Shared.Rent(65536);
            Encoder encoder = _encoding.GetEncoder();
            bool completed;
            do
            {
                Convert(encoder, chars, array, flush: true, out var charsUsed, out var bytesUsed, out completed);
                if (bytesUsed != 0)
                {
                    Write(array.AsSpan(0, bytesUsed));
                }
                chars = chars[charsUsed..];
            }
            while (!completed);
            ArrayPool<byte>.Shared.Return(array);
        }

        static unsafe void Convert(Encoder encoder, ReadOnlySpan<char> chars, Span<byte> bytes, bool flush, out int charsUsed, out int bytesUsed, out bool completed)
        {
            fixed (char* nonNullPinnableReference = &chars.GetNonNullPinnableReference())
            fixed (byte* nonNullPinnableReference2 = &bytes.GetNonNullPinnableReference())
                encoder.Convert(nonNullPinnableReference, chars.Length, nonNullPinnableReference2, bytes.Length, flush, out charsUsed, out bytesUsed, out completed);
        }

        public BufferReader GetReader() => new(_buffer, _encoding);

        public byte[] ToArray()
        {
            return WrittenSpan.ToArray();
        }

        private static void ThrowInvalidOperationException_AdvancedTooFar(int capacity)
        {
            throw new InvalidOperationException($"Buffer writer advanced too far from capacity ({capacity}");
        }

        private static void ThrowOutOfMemoryException(uint capacity)
        {
            throw new OutOfMemoryException($"Buffer maximum size of {capacity} wasexceeded");
        }


    }

    internal static class BufferExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe static ref T GetNonNullPinnableReference<T>(this Span<T> span)
        {
            if (span.Length == 0)
            {
                return ref Unsafe.AsRef<T>((void*)1);
            }
            return ref Unsafe.AsRef(in MemoryMarshal.GetReference(span));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe static ref T GetNonNullPinnableReference<T>(this ReadOnlySpan<T> span)
        {
            if (span.Length == 0)
            {
                return ref Unsafe.AsRef<T>((void*)1);
            }
            return ref Unsafe.AsRef(in MemoryMarshal.GetReference(span));
        }

        public static BufferReader CreateBufferReader(this byte[] ms, Encoding? encoding = null) => new(ms, encoding ?? Encoding.UTF8);
    }

    //    extension(BinaryReader reader)
    //    {
    //        public Guid ReadGuid() => new(reader.ReadBytes(16));

    //        public bool? TryReadBoolean() => reader.ReadBoolean() ? reader.ReadBoolean() : null;

    //        public byte? TryReadByte() => reader.ReadBoolean() ? reader.ReadByte() : null;

    //        public double? TryReadDouble() => reader.ReadBoolean() ? reader.ReadDouble() : null;

    //        public decimal? TryReadDecimal() => reader.ReadBoolean() ? reader.ReadDecimal() : null;

    //        public char? TryReadChar() => reader.ReadBoolean() ? reader.ReadChar() : null;

    //        public ushort? TryReadUInt16() => reader.ReadBoolean() ? reader.ReadUInt16() : null;

    //        public uint? TryReadUInt32() => reader.ReadBoolean() ? reader.ReadUInt32() : null;

    //        public ulong? TryReadUInt64() => reader.ReadBoolean() ? reader.ReadUInt64() : null;

    //        public short? TryReadInt16() => reader.ReadBoolean() ? reader.ReadInt16() : null;

    //        public int? TryReadInt32() => reader.ReadBoolean() ? reader.ReadInt32() : null;

    //        public long? TryReadInt64() => reader.ReadBoolean() ? reader.ReadInt64() : null;

    //        public Guid? TryReadGuid() => reader.ReadBoolean() ? reader.ReadGuid() : null;

    //        public string? TryReadString()
    //        {
    //            if (reader.ReadBoolean())
    //            {
    //                return reader.ReadString();
    //            }
    //            else
    //            {
    //                return null;
    //            }
    //        }
    //    }

    //    extension(BinaryWriter writer)
    //    {
    //        public static BinaryWriter CreateDynamicBuffer(Encoding? encoding = null) => new(new MemoryStream(), encoding ??= Encoding.UTF8);

    //        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_encoding")]
    //        static extern ref Encoding GetEncoding(BinaryWriter _);


    //        public void Write(Guid value)
    //        {
    //            writer.Write(value.ToByteArray());
    //        }

    //        public void TryWrite(bool? value)
    //        {
    //            writer.Write(value.HasValue);
    //            if (value.HasValue) writer.Write(value.Value);
    //        }

    //        public void TryWrite(byte? value)
    //        {
    //            writer.Write(value.HasValue);
    //            if (value.HasValue) writer.Write(value.Value);
    //        }

    //        public void TryWrite(double? value)
    //        {
    //            writer.Write(value.HasValue);
    //            if (value.HasValue) writer.Write(value.Value);
    //        }

    //        public void TryWrite(decimal? value)
    //        {
    //            writer.Write(value.HasValue);
    //            if (value.HasValue) writer.Write(value.Value);
    //        }

    //        public void TryWrite(char? value)
    //        {
    //            writer.Write(value.HasValue);
    //            if (value.HasValue) writer.Write(value.Value);
    //        }

    //        public void TryWrite(ushort? value)
    //        {
    //            writer.Write(value.HasValue);
    //            if (value.HasValue) writer.Write(value.Value);
    //        }

    //        public void TryWrite(uint? value)
    //        {
    //            writer.Write(value.HasValue);
    //            if (value.HasValue) writer.Write(value.Value);
    //        }

    //        public void TryWrite(ulong? value)
    //        {
    //            writer.Write(value.HasValue);
    //            if (value.HasValue) writer.Write(value.Value);
    //        }

    //        public void TryWrite(short? value)
    //        {
    //            writer.Write(value.HasValue);
    //            if (value.HasValue) writer.Write(value.Value);
    //        }

    //        public void TryWrite(int? value)
    //        {
    //            writer.Write(value.HasValue);
    //            if (value.HasValue) writer.Write(value.Value);
    //        }

    //        public void TryWrite(long? value)
    //        {
    //            writer.Write(value.HasValue);
    //            if (value.HasValue) writer.Write(value.Value);
    //        }

    //        public void TryWrite(Guid? value)
    //        {
    //            writer.Write(value.HasValue);
    //            if (value.HasValue) writer.Write(value.Value);
    //        }

    //        public void TryWrite(string? value)
    //        {
    //            var hasValue = value != null;
    //            writer.Write(hasValue);
    //            if (hasValue) writer.Write(value!);
    //        }

    //        public BinaryReader GetMemoryReader()
    //        {
    //            writer.Flush();
    //            writer.BaseStream.Position = 0;
    //            return new((MemoryStream)writer.BaseStream, GetEncoding(writer));
    //        }

    //        public byte[] ToArray()
    //        {
    //            return ((MemoryStream)writer.BaseStream).ToArray();
    //        }
    //    }
    //}
}



namespace System
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Constructor | AttributeTargets.Method | AttributeTargets.Field | AttributeTargets.Interface, Inherited = false)]
    internal sealed class IntrinsicAttribute : Attribute;

#if !NETCOREAPP2_1_OR_GREATER
    namespace Runtime.CompilerServices
    {
        public enum UnsafeAccessorKind
        {
            Constructor,
            Method,
            StaticMethod,
            Field,
            StaticField
        }
        [AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
        public sealed class UnsafeAccessorAttribute(UnsafeAccessorKind kind) : Attribute
        {
            public UnsafeAccessorKind Kind { get; } = kind;

            public string? Name { get; set; }
        }
    }
#endif
}