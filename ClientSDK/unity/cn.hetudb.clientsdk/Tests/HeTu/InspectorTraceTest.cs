using System;
using System.Collections.Generic;
using System.Diagnostics;
using HeTu;
using MessagePack;
using NUnit.Framework;

namespace Tests.HeTu
{
    [TestFixture]
    public class InspectorTraceTest
    {
        [Test]
        public void CreateFrameInfo_VisibleFrame_UsesTypeMethodFileAndLine()
        {
            var info = InspectorTraceStack.CreateFrameInfo(
                "ClientBase", "GetSync", "/a/b/ClientBase.cs", 357);

            Assert.AreEqual("ClientBase.GetSync (ClientBase.cs:357)",
                info.DisplayText);
            Assert.True(info.IsVisible);
            Assert.AreEqual("/a/b/ClientBase.cs", info.FilePath);
            Assert.AreEqual(357, info.Line);
        }

        [Test]
        public void CreateFrameInfo_InvisibleFrame_IsGreyAndNotOpenable()
        {
            var frame = InspectorTraceStack.CreateFrameInfo(
                "Entry", "Fire", null, 0);

            Assert.AreEqual("Entry.Fire (-:-)", frame.DisplayText);
            Assert.False(frame.IsVisible);
            Assert.IsNull(frame.FilePath);
            Assert.AreEqual(0, frame.Line);
        }

        [Test]
        public void IsUserCodeDeclaringType_DistinguishesSdkAndUserTypes()
        {
            Assert.False(InspectorTraceStack.IsUserCodeDeclaringType(
                typeof(HeTuClientBase)));
            Assert.False(InspectorTraceStack.IsUserCodeDeclaringType(
                typeof(string)));
            Assert.True(InspectorTraceStack.IsUserCodeDeclaringType(
                typeof(InspectorTraceTest)));
        }

        [Test]
        public void InterceptRequest_PopulatesCallerFrames()
        {
            var collector = new InspectorTraceCollector();
            var dispatcher = new CaptureDispatcher();
            collector.AddDispatcher(dispatcher);
            collector.Configure(true);

            collector.InterceptRequest("rpc", "login",
                new object[] { "rpc", "login" });

            Assert.NotNull(dispatcher.LastTrace);
            Assert.That(dispatcher.LastTrace.CallerFrames, Is.Not.Null.And.Not.Empty);
            Assert.That(dispatcher.LastTrace.CallerFrames[0].DisplayText,
                Is.Not.Empty);
        }

        [Test]
        public void InterceptMessageUpdate_UsesStackTraceFramesDirectly()
        {
            var collector = new InspectorTraceCollector();
            var dispatcher = new CaptureDispatcher();
            collector.AddDispatcher(dispatcher);
            collector.Configure(true);

            var trace = new StackTrace(true);
            collector.InterceptMessageUpdate("HP", null, trace,
                12, 9);

            Assert.NotNull(dispatcher.LastTrace);
            Assert.That(dispatcher.LastTrace.CallerFrames, Is.Not.Null.And.Not.Empty);
            Assert.That(dispatcher.LastTrace.CallerStack, Is.Not.Null.And.Not.Empty);
        }

        [Test]
        public void CompleteAfterSendIfNeeded_CompletesAfterSendTraceOnly()
        {
            var collector = new InspectorTraceCollector();
            var dispatcher = new CaptureDispatcher();
            collector.AddDispatcher(dispatcher);
            collector.Configure(true);

            var traceId = collector.InterceptRequest("unsub", "HP.id[1:None:1][:1]",
                new object[] { "unsub", "HP.id[1:None:1][:1]" },
                InspectorTraceCompletionMode.AfterSend);

            collector.CompleteAfterSendIfNeeded(traceId);

            Assert.NotNull(dispatcher.LastTrace);
            Assert.AreEqual("completed", dispatcher.LastTrace.Status);
        }

        [Test]
        public void Unsubscribe_EmitsInspectorTrace()
        {
            var client = new TestClient();
            var dispatcher = new CaptureDispatcher();
            const string subId = "HP.id[1:None:1][:1]";

            client.ConfigureInspector(true);
            client.AddInspectorDispatcher(dispatcher);
            client.ForceConnected();
            using var subscription = new TestSubscription(subId, "HP", client,
                new StackTrace(true));
            client.RegisterSubscription(subId, subscription);

            client.Unsubscribe(subId, "test");

            Assert.NotNull(dispatcher.LastTrace);
            Assert.AreEqual(HeTuClientBase.CommandUnsub, dispatcher.LastTrace.Type);
            Assert.AreEqual("completed", dispatcher.LastTrace.Status);
            Assert.That(dispatcher.LastTrace.CallerFrames, Is.Not.Null.And.Not.Empty);
        }

        [Test]
        public void OnReceived_UpdateTraceUsesSubscriptionCreationStack()
        {
            var client = new TestClient();
            var dispatcher = new CaptureDispatcher();
            const string subId = "HP.owner[123:None:1][:1]";

            client.ConfigureInspector(true);
            client.AddInspectorDispatcher(dispatcher);
            client.ForceConnected();
            using var subscription = new TestSubscription(subId, "HP", client,
                new StackTrace(true));
            client.RegisterSubscription(subId, subscription);

            var packet = MessagePackSerializer.Serialize(new object[]
            {
                HeTuClientBase.MessageUpdate,
                subId,
                new Dictionary<long, object> { [1L] = new Dictionary<string, object>() }
            });

            client.Receive(packet);

            Assert.NotNull(dispatcher.LastTrace);
            Assert.AreEqual(HeTuClientBase.MessageUpdate, dispatcher.LastTrace.Type);
            Assert.That(dispatcher.LastTrace.CallerFrames, Is.Not.Null.And.Not.Empty);
            Assert.AreEqual(subscription.CreationTrace.ToString(),
                dispatcher.LastTrace.CallerStack);
        }

        private sealed class CaptureDispatcher : IInspectorTraceDispatcher
        {
            public InspectorTraceEvent LastTrace { get; private set; }

            public void Dispatch(InspectorTraceEvent traceEvent) => LastTrace = traceEvent;
        }

        private sealed class TestClient : HeTuClientBase
        {
            public TestClient() =>
                SetupPipeline(new List<MessageProcessLayer> { new JsonbLayer() });

            public void ForceConnected() => State = ConnectionState.Connected;

            public void Receive(byte[] buffer) => OnReceived(buffer);

            public void RegisterSubscription(string subId, BaseSubscription subscription) =>
                Subscriptions.Add(subId, new WeakReference(subscription, false));

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

        private sealed class TestSubscription : BaseSubscription
        {
            public TestSubscription(string subscriptID, string componentName,
                HeTuClientBase client, StackTrace creationTrace) :
                base(subscriptID, componentName, client, creationTrace)
            {
            }

            public override void UpdateRows(JsonObject data)
            {
            }
        }
    }
}
