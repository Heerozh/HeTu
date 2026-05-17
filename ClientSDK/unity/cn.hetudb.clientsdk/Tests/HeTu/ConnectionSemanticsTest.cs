using System;
using System.Collections.Generic;
using HeTu;
using NUnit.Framework;

namespace Tests.HeTu
{
    [TestFixture]
    public class ConnectionSemanticsTest
    {
        [Test]
        public void CallSystem_DuringHandshake_FailsInsteadOfQueueing()
        {
            var client = new TestClient();
            var canceled = false;

            client.ForceReadyForConnect();
            client.CallSystem("login", Array.Empty<object>(), (_, cancel) =>
            {
                canceled = cancel;
            });

            Assert.True(canceled);
            Assert.AreEqual(0, client.SentCount);
        }

        private sealed class TestClient : HeTuClientBase
        {
            public TestClient() =>
                SetupPipeline(new List<MessageProcessLayer> { new JsonbLayer() });

            public int SentCount { get; private set; }

            public void ForceReadyForConnect() => State = ConnectionState.ReadyForConnect;

            public void CallSystem(string systemName, object[] args,
                Action<JsonObject, bool> onResponse) =>
                CallSystemSync(systemName, args, onResponse);

            protected override void ConnectCore(string url, Action onConnected,
                Action<byte[]> onMessage, Action<string> onClose, Action<string> onError)
            {
            }

            protected override void CloseCore()
            {
            }

            protected override void SendCore(byte[] data)
            {
                SentCount++;
            }
        }
    }
}
