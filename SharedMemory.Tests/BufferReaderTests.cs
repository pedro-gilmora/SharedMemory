using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;

namespace SharedMemory.Tests
{
    [TestClass]
    public class BufferReaderTests
    {
        [TestMethod]
        public void ReadString_RoundTrips()
        {
            string longMultiByte = new('€', 300); // 900 UTF-8 bytes: crosses the old 128-byte chunking
            var writer = new BufferBuilder();
            writer.Write("hello");
            writer.Write(string.Empty);
            writer.Write(longMultiByte);
            writer.TryWrite((string?)null);
            writer.TryWrite("ñandú");
            writer.Write(42);

            var reader = new BufferReader(writer.WrittenMemory.ToArray());

            Assert.AreEqual("hello", reader.ReadString());
            Assert.AreEqual(string.Empty, reader.ReadString());
            Assert.AreEqual(longMultiByte, reader.ReadString());
            Assert.IsNull(reader.TryReadString());
            Assert.AreEqual("ñandú", reader.TryReadString());
            Assert.AreEqual(42, reader.ReadInt32());
        }

        [TestMethod]
        public void ReadString_Truncated_Throws()
        {
            var writer = new BufferBuilder();
            writer.Write("hello");
            byte[] data = writer.WrittenMemory.ToArray()[..^1];

            Assert.ThrowsException<IOException>(() => new BufferReader(data).ReadString());
        }
    }
}
