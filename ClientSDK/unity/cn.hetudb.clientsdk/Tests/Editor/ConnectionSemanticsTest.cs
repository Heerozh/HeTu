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

            Logger.Instance.SetLogger(_ => { }, _ => { }, _ => { });
            client.ForceReadyForConnect();
            client.CallSystem("login", Array.Empty<object>(), (_, outcome, _2) =>
            {
                canceled = outcome == CallOutcome.Canceled;
            });

            Assert.True(canceled);
            Assert.AreEqual(0, client.SentCount);
        }

        // 被顶号时服务端先发 close 4001 "kicked" 再断开：close 码要透给使用方（OnClosed 里读
        // LastCloseCode），好提示"账号已在别处登录"。码与服务端 CLOSE_KICKED 对齐
        [Test]
        public void ServerCloseCode_IsExposedToOnClosed()
        {
            var client = new TestClient();
            Logger.Instance.SetLogger(_ => { }, _ => { }, _ => { });
            string closedReason = null;
            var codeSeenInOnClosed = 0;
            client.OnClosed += reason =>
            {
                closedReason = reason;
                codeSeenInOnClosed = client.LastCloseCode;
            };

            client.Connect();
            client.RaiseClosed(4001, "kicked");

            Assert.AreEqual(4001, HeTuCloseCode.Kicked);
            Assert.AreEqual(HeTuCloseCode.Kicked, codeSeenInOnClosed);
            Assert.AreEqual(HeTuCloseCode.Kicked, client.LastCloseCode);
            Assert.AreEqual("kicked", closedReason);

            // 重新连接时清零，不把上一条连接的 close 码带过来
            client.Connect();
            Assert.AreEqual(0, client.LastCloseCode);
        }

        private sealed class TestClient : HeTuClientBase
        {
            private Action<int, string> _onClose;

            public TestClient() =>
                SetupPipeline(new List<MessageProcessLayer> { new JsonbLayer() });

            public int SentCount { get; private set; }

            public void ForceReadyForConnect() => State = ConnectionState.ReadyForConnect;

            public void Connect() => ConnectSync("ws://test");

            public void RaiseClosed(int code, string reason) => _onClose(code, reason);

            public void CallSystem(string systemName, object[] args,
                Action<JsonObject, CallOutcome, string> onResponse) =>
                CallSystemSync(systemName, args, onResponse);

            protected override void ConnectCore(string url, Action onConnected,
                Action<byte[]> onMessage, Action<int, string> onClose,
                Action<string> onError) =>
                _onClose = onClose;

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
