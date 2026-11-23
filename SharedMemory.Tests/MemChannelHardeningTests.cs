using System;
using System.Buffers.Binary;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SharedMemory;

namespace SharedMemoryTests
{
    /// <summary>
    /// Whatever another process writes into a shared ring (layout, node offsets, frames) is untrusted: it must not move reads or writes
    /// outside the mapping, and it must not take the reader down.
    /// </summary>
    [TestClass]
    [SupportedOSPlatform("windows")]
    public class MemChannelHardeningTests
    {
        static readonly long BufferStart = Marshal.SizeOf<SharedHeader>(); // SharedBuffer.HeaderOffset is 0

        static string Unique() => "lsl-hardening-" + Guid.NewGuid().ToString("N");

        static void Poke<T>(string name, long offset, T value) where T : struct
        {
            using var mmf = MemoryMappedFile.OpenExisting(name);
            using var view = mmf.CreateViewAccessor();
            view.Write(offset, ref value);
        }

        [TestMethod]
        public void ARingLayoutLargerThanTheSectionIsRejected()
        {
            string name = Unique();
            using var owner = new CircularBuffer(name, 2, 64);

            Poke(name, BufferStart + (long)Marshal.OffsetOf<CircularBuffer.NodeHeader>(nameof(CircularBuffer.NodeHeader.NodeCount)), 1_000_000);

            Assert.ThrowsException<InvalidDataException>(() => new CircularBuffer(name));
        }

        [TestMethod]
        public void NodeOffsetsInSharedMemoryAreIgnored()
        {
            string name = Unique();
            using var owner = new CircularBuffer(name, 2, 64);

            // A peer points node 0's data far outside the mapping; reads and writes still land in node 0's own slot.
            Poke(name, BufferStart + Marshal.SizeOf<CircularBuffer.NodeHeader>() + (long)Marshal.OffsetOf<CircularBuffer.Node>(nameof(CircularBuffer.Node.Offset)), long.MaxValue / 2);

            using var opened = new CircularBuffer(name);
            byte[] data = [1, 2, 3, 4], back = new byte[4];
            Assert.AreEqual(4, opened.Write(data));
            Assert.AreEqual(4, owner.Read(back));
            CollectionAssert.AreEqual(data, back);
        }

        static byte[] Packet(int total, uint id, ushort client, int payload = 0)
        {
            var b = new byte[12 + payload];
            BinaryPrimitives.WriteInt32LittleEndian(b, total);
            BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(4), id);
            BinaryPrimitives.WriteUInt16LittleEndian(b.AsSpan(8), client);
            b[10] = 1; // Kind.Request
            return b;
        }

        [TestMethod]
        public void MalformedFramesDoNotTakeTheHostDown()
        {
            const int nodeSize = 256, chunk = nodeSize - 12;
            string name = Unique();
            using var host = MemHost.Start(name, static r =>
            {
                r.Reply(r.Payload.Length, static (w, n) => { BinaryPrimitives.WriteInt32LittleEndian(w.GetSpan(4), n); w.Advance(4); });
                return default;
            }, nodeSize, 8);

            using (var rogue = new CircularBuffer(name + "_Host"))
            {
                rogue.Write(Packet(-1, 1, 900));                           // negative length
                rogue.Write(Packet(int.MaxValue - 64, 2, 901, chunk));    // ~2 GB claimed
                for (ushort c = 0; c < 8; c++)                             // 8 x 16 MB partials from forged writers: beyond the budget
                    rogue.Write(Packet(16 * 1024 * 1024, 3, (ushort)(1000 + c), chunk));
                rogue.Write(Packet(1000, 4, 950, chunk));                  // continuation that changes its length
                rogue.Write(Packet(600, 4, 950, chunk));
            }

            using var client = MemClient.Connect(name);
            int length = client.Call(7, static (w, n) => { w.GetSpan(n); w.Advance(n); }, static (_, response) => BinaryPrimitives.ReadInt32LittleEndian(response), timeoutMs: 5000);
            Assert.AreEqual(7, length);
        }
    }
}
