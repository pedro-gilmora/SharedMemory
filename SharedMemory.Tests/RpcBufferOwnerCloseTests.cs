using Microsoft.VisualStudio.TestTools.UnitTesting;
using SharedMemory;
using System;
using System.Diagnostics;
using System.Threading;

namespace SharedMemory.Tests
{
    /// <summary>
    /// Si el cliente es master (dueño) del RpcBuffer, su Dispose marca Shutdown=1 en el MMF compartido y el lector
    /// del slave sale solo, sin Bye ni lobby.
    /// </summary>
    [TestClass]
    public class RpcBufferOwnerCloseTests
    {
        [TestMethod]
        public void ClientOwnedChannelEndsServerReaderOnDispose()
        {
            const int clients = 20;
            int threads0 = Threads();
            var servers = new RpcBuffer[clients];

            for (int i = 0; i < clients; i++)
            {
                string name = $"Owner-{Guid.CreateVersion7():N}";
                using var client = new RpcBuffer(name); // master: crea los MMF
                servers[i] = new RpcBuffer(name, (_, payload) => [(byte)(payload[0] + 1)]); // slave: el servidor se une
                Assert.AreEqual((byte)(i + 1), client.RemoteRequest([(byte)i], 2000).Data![0]);
            }

            Assert.IsTrue(SpinWait.SpinUntil(() => Threads() - threads0 < clients, 5000), "el lector slave sale al ver Shutdown=1 del master");

            foreach (var s in servers)
                Assert.ThrowsException<InvalidOperationException>(() => s.RemoteRequest([0], 200), "el slave ve el canal cerrado por su dueño");

            foreach (var s in servers) s.Dispose();
        }

        static int Threads()
        {
            using var p = Process.GetCurrentProcess();
            return p.Threads.Count;
        }
    }
}
