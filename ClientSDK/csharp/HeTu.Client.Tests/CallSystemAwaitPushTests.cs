using System;
using System.Collections.Generic;
using HeTu;
using MessagePack;
using NUnit.Framework;

namespace HeTu.Client.Tests
{
    // 等推送的调用（rpcs，设计稿 docs/superpowers/specs/2026-10-10-rpcs-sync-design.md）：
    // 发 ["rpcs", id, system, ...]，rsp 照常按顺序到；["sync", id] 到了（这次调用引起的订阅
    // 推送都已先到）才算完成。rej / err 立即结束；断线时已收到 rsp 的按成功完成。
    public class CallSystemAwaitPushTests
    {
        private static readonly Action<JsonObject, CallOutcome, string> Ignore =
            (_, _, _) => { };

        [SetUp]
        public void SilenceLogger() =>
            Logger.Instance.SetLogger(_ => { }, _ => { }, _ => { });

        [Test]
        public void AwaitPush_SendsRpcsWithIncrementingIds()
        {
            var client = PushTestClient.Connected();
            client.Call("buy", new object[] { 5 }, Ignore, true);
            client.Call("sell", Array.Empty<object>(), Ignore, true);
            client.Call("move", new object[] { 1 }, Ignore);

            Assert.That(client.Sent, Has.Count.EqualTo(3));
            Assert.That(client.Sent[0], Is.EqualTo(new object[] { "rpcs", 1, "buy", 5 }));
            Assert.That(client.Sent[1], Is.EqualTo(new object[] { "rpcs", 2, "sell" }));
            Assert.That(client.Sent[2], Is.EqualTo(new object[] { "rpc", "move", 1 }));
        }

        [Test]
        public void AwaitPush_CompletesOnlyAfterSync()
        {
            var client = PushTestClient.Connected();
            var done = new List<(JsonObject Response, CallOutcome Outcome)>();
            client.Call("buy", Array.Empty<object>(), (r, oc, _) => done.Add((r, oc)), true);

            client.Receive(new object[]
                { "rsp", new Dictionary<string, object> { ["gold"] = 5 } });
            Assert.That(done, Is.Empty, "rsp 到了还要等 sync");

            client.Receive(new object[] { "sync", 1 });
            Assert.That(done, Has.Count.EqualTo(1));
            Assert.That(done[0].Outcome, Is.EqualTo(CallOutcome.Completed));
            Assert.That(done[0].Response.ToDict<string, int>()["gold"], Is.EqualTo(5));
        }

        [Test]
        public void AwaitPush_SyncBeforeRsp_CompletesWhenRspArrives()
        {
            // 发送拥塞时服务端的推送（连同 sync）可能插到排着的回复前面
            var client = PushTestClient.Connected();
            var outcomes = new List<CallOutcome>();
            client.Call("buy", Array.Empty<object>(), (_, oc, _) => outcomes.Add(oc), true);

            client.Receive(new object[] { "sync", 1 });
            Assert.That(outcomes, Is.Empty);

            client.Receive(new object[] { "rsp", "ok" });
            Assert.That(outcomes, Is.EqualTo(new[] { CallOutcome.Completed }));
        }

        [Test]
        public void AwaitPush_PlainCallsStayFifoAligned()
        {
            // rpcs 等 sync 的期间，排在它后面的普通调用照常按回复顺序完成
            var client = PushTestClient.Connected();
            var order = new List<string>();
            client.Call("buy", Array.Empty<object>(), (_, _, _) => order.Add("buy"), true);
            client.Call("move", Array.Empty<object>(), (_, _, _) => order.Add("move"));

            client.Receive(new object[] { "rsp", "ok" });
            client.Receive(new object[] { "rsp", "ok" });
            Assert.That(order, Is.EqualTo(new[] { "move" }));

            client.Receive(new object[] { "sync", 1 });
            Assert.That(order, Is.EqualTo(new[] { "move", "buy" }));
        }

        [Test]
        public void AwaitPush_Rejected_CompletesImmediately_LaterSyncIgnored()
        {
            var client = PushTestClient.Connected();
            var done = new List<(CallOutcome Outcome, string Code)>();
            client.Call("buy", Array.Empty<object>(), (_, oc, code) => done.Add((oc, code)),
                true);

            client.Receive(new object[] { "rej", "buy", "RATE_LIMITED" });
            Assert.That(done, Is.EqualTo(new[] { (CallOutcome.Rejected, "RATE_LIMITED") }));

            client.Receive(new object[] { "sync", 1 });
            Assert.That(done, Has.Count.EqualTo(1));
        }

        [Test]
        public void AwaitPush_Failed_CompletesImmediately()
        {
            var client = PushTestClient.Connected();
            var done = new List<(CallOutcome Outcome, string Reason)>();
            client.Call("buy", Array.Empty<object>(), (_, oc, code) => done.Add((oc, code)),
                true);

            client.Receive(new object[] { "err", "buy", "RuntimeError: boom" });
            Assert.That(done, Is.EqualTo(new[] { (CallOutcome.Failed, "RuntimeError: boom") }));
        }

        [Test]
        public void AwaitPush_UnknownSyncIsIgnored()
        {
            var client = PushTestClient.Connected();
            Assert.DoesNotThrow(() => client.Receive(new object[] { "sync", 99 }));
        }

        [Test]
        public void AwaitPush_ClosedAfterRsp_CompletesAfterOnClosed()
        {
            // 提交确定发生了：断线时已收到 rsp 的按成功完成。放在 OnClosed 之后，连接拆完了才跑
            // 用户代码（续体里接着 Connect / CallSystem 不会撞上拆到一半的连接）；Session 层自己按
            // onAnswered 处理，不靠这里的先后
            var client = PushTestClient.Connected();
            var outcomes = new List<CallOutcome>();
            client.Call("buy", Array.Empty<object>(), (_, oc, _) => outcomes.Add(oc), true);
            client.Receive(new object[] { "rsp", "ok" });
            var seenInOnClosed = -1;
            client.OnClosed += _ => seenInOnClosed = outcomes.Count;

            client.RaiseClosed(HeTuCloseCode.Abnormal, "network lost");

            Assert.That(seenInOnClosed, Is.EqualTo(0));
            Assert.That(outcomes, Is.EqualTo(new[] { CallOutcome.Completed }));
        }

        [Test]
        public void AwaitPush_ReportsAnsweredWhenRspArrives()
        {
            // rsp 到了、sync 还没到时通知一次（Session 层据此在断线时把它当成功）；rej / err、
            // 普通调用都不通知
            var client = PushTestClient.Connected();
            var answered = new List<string>();
            client.Call("buy", Array.Empty<object>(), Ignore, true, _ => answered.Add("buy"));
            client.Call("sell", Array.Empty<object>(), Ignore, true, _ => answered.Add("sell"));
            client.Call("move", Array.Empty<object>(), Ignore, false, _ => answered.Add("move"));

            client.Receive(new object[] { "rsp", "ok" });
            Assert.That(answered, Is.EqualTo(new[] { "buy" }));
            client.Receive(new object[] { "rej", "sell", "RATE_LIMITED" });
            client.Receive(new object[] { "rsp", "ok" });
            Assert.That(answered, Is.EqualTo(new[] { "buy" }));
        }

        [Test]
        public void AwaitPush_ClosedBeforeRsp_IsNotSuccess()
        {
            var client = PushTestClient.Connected();
            var outcomes = new List<CallOutcome>();
            client.Call("buy", Array.Empty<object>(), (_, oc, _) => outcomes.Add(oc), true);

            client.RaiseClosed(HeTuCloseCode.Abnormal, "network lost");
            Assert.That(outcomes, Is.Empty, "没收到 rsp 不能当成功");

            client.Close();
            Assert.That(outcomes, Is.EqualTo(new[] { CallOutcome.Canceled }));
        }

        [Test]
        public void AwaitPush_CloseAfterRsp_Completes()
        {
            var client = PushTestClient.Connected();
            var outcomes = new List<CallOutcome>();
            client.Call("buy", Array.Empty<object>(), (_, oc, _) => outcomes.Add(oc), true);
            client.Receive(new object[] { "rsp", "ok" });

            client.Close();

            Assert.That(outcomes, Is.EqualTo(new[] { CallOutcome.Completed }));
        }

        [Test]
        public void AwaitPush_IdsRestartAfterReconnect()
        {
            var client = PushTestClient.Connected();
            client.Call("buy", Array.Empty<object>(), Ignore, true);
            client.Reconnect();
            client.Call("buy", Array.Empty<object>(), Ignore, true);

            Assert.That(client.Sent[0][1], Is.EqualTo(1));
            Assert.That(client.Sent[1][1], Is.EqualTo(1));
        }

        [Test]
        public void SyncFrame_IsDecodedAsStandardMessage()
        {
            var bytes = MessagePackSerializer.Serialize(new object[] { "sync", 7 });

            var decoded = (object[])new JsonbLayer().Decode(bytes);

            Assert.That(decoded[0], Is.EqualTo("sync"));
            Assert.That(decoded[1], Is.TypeOf<long>().And.EqualTo(7L));
        }

        private sealed class PushTestClient : HeTuClientBase
        {
            private Action<int, string> _onClose;

            private PushTestClient() =>
                SetupPipeline(new List<MessageProcessLayer> { new JsonbLayer() });

            public List<object[]> Sent { get; } = new();

            // 走一遍真实的连接入口（记下 onClose 好模拟断线），再当作已握手
            public static PushTestClient Connected()
            {
                var client = new PushTestClient();
                client.Reconnect();
                return client;
            }

            public void Reconnect()
            {
                ConnectSync("ws://test");
                State = ConnectionState.Connected;
            }

            public void RaiseClosed(int code, string reason) => _onClose(code, reason);

            public void Call(string systemName, object[] args,
                Action<JsonObject, CallOutcome, string> onResponse,
                bool awaitPush = false, Action<JsonObject> onAnswered = null) =>
                CallSystemSync(systemName, args, onResponse, awaitPush, onAnswered);

            public void Receive(object[] frame) => OnReceived(Pipeline.Encode(frame, out _));

            protected override void ConnectCore(string url, Action onConnected,
                Action<byte[]> onMessage, Action<int, string> onClose,
                Action<string> onError) =>
                _onClose = onClose;

            protected override void CloseCore()
            {
            }

            protected override void SendCore(byte[] data) =>
                Sent.Add((object[])Pipeline.Decode(data));
        }
    }
}
