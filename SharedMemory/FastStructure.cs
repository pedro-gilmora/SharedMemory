// SharedMemory (File: SharedMemory\FastStructure.cs)
// Copyright (c) 2014 Justin Stenning
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
//
// The SharedMemory library is inspired by the following Code Project article:
//   "Fast IPC Communication Using Shared Memory and InterlockedCompareExchange"
//   http://www.codeproject.com/Articles/14740/Fast-IPC-Communication-Using-Shared-Memory-and-Int
using System;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;

namespace SharedMemory
{
    /// <summary>
    /// Emits optimized IL for the reading and writing of structures to/from memory.
    /// <para>For a 32-byte structure with 1 million iterations:</para>
    /// <para>The <see cref="FastStructure{T}.PtrToStructure"/> method performs approx. 20x faster than
    /// <see cref="System.Runtime.InteropServices.Marshal.PtrToStructure(IntPtr, Type)"/> (8ms vs 160ms), and about 1.6x slower than the non-generic equivalent (8ms vs 5ms)</para>
    /// <para>The <see cref="FastStructure{T}.StructureToPtr"/> method performs approx. 8x faster than 
    /// <see cref="System.Runtime.InteropServices.Marshal.StructureToPtr(object, IntPtr, bool)"/> (4ms vs 34ms). </para>
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public static class FastStructure<T>
        where T : struct
    {
        /// <summary>
        /// Cached size of T as determined by <see cref="System.Runtime.InteropServices.Marshal.SizeOf(Type)"/>.
        /// </summary>
        public static readonly int Size = Unsafe.SizeOf<T>();
        

        /// <summary>
        /// Performs once of type compatibility check.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown if the type T is incompatible</exception>
        static FastStructure()
        {
            // Performs compatibility checks upon T
            CheckTypeCompatibility(typeof(T));
        }

        private static void CheckTypeCompatibility(Type t, System.Collections.Generic.HashSet<Type>? checkedItems = null)
        {
            checkedItems ??=
                [
                    typeof(char),
                    typeof(byte),
                    typeof(sbyte),
                    typeof(bool),
                    typeof(double),
                    typeof(float),
                    typeof(decimal),
                    typeof(int),
                    typeof(short),
                    typeof(long),
                    typeof(uint),
                    typeof(ushort),
                    typeof(ulong),
                    typeof(IntPtr),
                    typeof(void*),
                ];

            if (!checkedItems.Add(t))
                return;

            FieldInfo[] fi = t.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic);
            foreach (FieldInfo info in fi)
            {
                if (!info.FieldType.IsPrimitive && !info.FieldType.IsValueType && !info.FieldType.IsPointer)
                {
                    throw new ArgumentException(string.Format("Non-value types are not supported: field {0} is of type {1} in structure {2}", info.Name, info.FieldType.Name, info.DeclaringType.Name));
                }

                CheckTypeCompatibility(info.FieldType, checkedItems);
            }
        }
    }
}