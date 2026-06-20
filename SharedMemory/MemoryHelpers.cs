using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace SharedMemory
{
    internal static class MemoryHelpers
    {
        readonly static uint elementSize = sizeof(byte);
        public static unsafe void ReadBytes(in this Span<byte> buffer, IntPtr source, int index, int count)
        {
            if (count < 0)
                throw new ArgumentOutOfRangeException("count");
            if (index < 0)
                throw new ArgumentOutOfRangeException("index");
            if (buffer.Length - index < count)
                throw new ArgumentException("Invalid offset into array specified by index and count");

            void* ptr = source.ToPointer();
            byte* p = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(buffer));
            var target = new Span<byte>(p + (index * elementSize), (int)elementSize * count);
            new Span<byte>(ptr, (int)elementSize * count).CopyTo(target);
        }

        public static unsafe void ReadArray<T>(in this Span<T> buffer, IntPtr source, int index, int count)
    where T : struct
        {
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (index < 0)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (buffer.Length - index < count)
                throw new ArgumentException("Invalid offset into array specified by index and count");

            // Wrap the unmanaged source as a Span<T>
            var sourceSpan = new ReadOnlySpan<T>((void*)source, count);

            // Copy element‑wise
            sourceSpan.CopyTo(buffer.Slice(index, count));
        }


        extension<T>(T item) where T : struct
        {
            public byte[] ToBytes()
            {
                byte[] result = new byte[FastStructure<T>.Size];
                MemoryMarshal.Write(new Span<byte>(result),
#if NETSTANDARD
                    ref 
#else
                    in
#endif
                    item);

                return result;
            }

            public static int MemorySize => FastStructure<T>.Size;

            public static T FromBytes(ReadOnlySpan<byte> buffer, int startIndex = 0)
            {
                if (startIndex > buffer.Length || startIndex < 0)
                    throw new ArgumentOutOfRangeException("startIndex");
                if (startIndex + FastStructure<T>.Size > buffer.Length)
                    throw new ArgumentException("Insufficient buffer data");

                return MemoryMarshal.Read<T>(buffer[startIndex..]);
            }

            public unsafe static T FromPointer(IntPtr pointer)
            {
                var span = new ReadOnlySpan<byte>((void*)pointer, FastStructure<T>.Size);
                return MemoryMarshal.Read<T>(span);
            }

            public unsafe void WritePointer(IntPtr pointer)
            {
                var span = new Span<byte>((void*)pointer, FastStructure<T>.Size);
                item.ToBytes().CopyTo(span);
            }
        }

        public static unsafe void WriteArray<T>(this IntPtr destination, T[] buffer, int index, int count)
            where T : struct
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));
            if (buffer.Length - index < count) throw new ArgumentException("Invalid offset");

            buffer.AsSpan(index, count).CopyTo(new Span<T>((void*)destination, count));
        }

        public static unsafe void WriteBytes(this IntPtr destination, Span<byte> buffer, int index, int count)
        {
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (index < 0)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (buffer.Length - index < count)
                throw new ArgumentException("Invalid offset into array specified by index and count");

            // Wrap the unmanaged destination as a Span<byte>
            var destSpan = new Span<byte>((void*)destination, count);

            // Copy element‑wise
            buffer.Slice(index, count).CopyTo(destSpan);
        }

        public static unsafe void WriteArray<T>(this T[] buffer, IntPtr destination, int index, int count)
     where T : struct
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (index < 0)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (buffer.Length - index < count)
                throw new ArgumentException("Invalid offset into array specified by index and count");

            // Slice the managed array
            var sourceSpan = new ReadOnlySpan<T>(buffer, index, count);

            // Wrap the unmanaged destination as a Span<T>
            var destSpan = new Span<T>((void*)destination, count);

            // Copy element‑wise
            sourceSpan.CopyTo(destSpan);
        }
    }
}