using System;
using System.Collections.Generic;
using HeTu;
using NUnit.Framework;

namespace Tests.HeTu
{
    public class CallRejectTest
    {
        [Test]
        public void RejFrame_FiresOnCallRejected_AndRejectedOutcome()
        {
            var client = new RejectTestClient();
            Logger.Instance.SetLogger(_ => { }, _ => { }, _ => { });
            client.ForceConnected();

            string evtSys = null, evtCode = null;
            client.OnCallRejected += (sys, code) => { evtSys = sys; evtCode = code; };

            CallOutcome outcome = CallOutcome.Completed;
            string rejectCode = null;
            client.CallSystem("attack", new object[] { 1 },
                (_, oc, code) => { outcome = oc; rejectCode = code; });

            client.Receive(new object[] { "rej", "attack", "RATE_LIMITED" });

            Assert.AreEqual("attack", evtSys);
            Assert.AreEqual("RATE_LIMITED", evtCode);
            Assert.AreEqual(CallOutcome.Rejected, outcome);
            Assert.AreEqual("RATE_LIMITED", rejectCode);
        }

        [Test]
        public void NormalRsp_AfterReject_StaysFifoAligned()
        {
            var client = new RejectTestClient();
            Logger.Instance.SetLogger(_ => { }, _ => { }, _ => { });
            client.ForceConnected();

            var outcomes = new List<CallOutcome>();
            client.CallSystem("a", Array.Empty<object>(),
                (_, oc, _2) => outcomes.Add(oc));
            client.CallSystem("b", Array.Empty<object>(),
                (_, oc, _2) => outcomes.Add(oc));

            client.Receive(new object[] { "rej", "a", "RATE_LIMITED" });
            client.Receive(new object[] { "rsp", "ok" });

            Assert.AreEqual(CallOutcome.Rejected, outcomes[0]);
            Assert.AreEqual(CallOutcome.Completed, outcomes[1]);
        }

        [Test]
        public void ErrFrame_FiresFailedOutcome_WithReason_AndNullResponse()
        {
            var client = new RejectTestClient();
            Logger.Instance.SetLogger(_ => { }, _ => { }, _ => { });
            client.ForceConnected();

            CallOutcome outcome = CallOutcome.Completed;
            string reason = null;
            JsonObject resp = null;
            bool fired = false;
            client.CallSystem("crashing_system", Array.Empty<object>(),
                (r, oc, code) => { fired = true; outcome = oc; reason = code; resp = r; });

            client.Receive(new object[]
                { "err", "crashing_system", "RuntimeError: boom for test" });

            Assert.IsTrue(fired);
            Assert.AreEqual(CallOutcome.Failed, outcome);
            Assert.AreEqual("RuntimeError: boom for test", reason);
            // 失败时 resp 为 null，调用方的 resp?.ToDict() 天然安全、不再抛反序列化异常
            Assert.IsNull(resp);
        }

        [Test]
        public void NormalRsp_AfterErr_StaysFifoAligned()
        {
            var client = new RejectTestClient();
            Logger.Instance.SetLogger(_ => { }, _ => { }, _ => { });
            client.ForceConnected();

            var outcomes = new List<CallOutcome>();
            client.CallSystem("a", Array.Empty<object>(),
                (_, oc, _2) => outcomes.Add(oc));
            client.CallSystem("b", Array.Empty<object>(),
                (_, oc, _2) => outcomes.Add(oc));

            client.Receive(new object[] { "err", "a", "RuntimeError: boom" });
            client.Receive(new object[] { "rsp", "ok" });

            Assert.AreEqual(CallOutcome.Failed, outcomes[0]);
            Assert.AreEqual(CallOutcome.Completed, outcomes[1]);
        }

        private sealed class RejectTestClient : HeTuClientBase
        {
            public RejectTestClient() =>
                SetupPipeline(new List<MessageProcessLayer> { new JsonbLayer() });

            public void ForceConnected() => State = ConnectionState.Connected;

            public void CallSystem(string systemName, object[] args,
                Action<JsonObject, CallOutcome, string> onResponse) =>
                CallSystemSync(systemName, args, onResponse);

            public void Receive(object[] frame)
            {
                var bytes = Pipeline.Encode(frame, out _);
                OnReceived(bytes);
            }

            protected override void ConnectCore(string url, Action onConnected,
                Action<byte[]> onMessage, Action<string> onClose, Action<string> onError)
            {
            }

            protected override void CloseCore()
            {
            }

            protected override void SendCore(byte[] data)
            {
            }
        }
    }
}
