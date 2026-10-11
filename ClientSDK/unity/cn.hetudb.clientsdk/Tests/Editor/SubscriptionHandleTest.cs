using System;
using System.Collections.Generic;
using HeTu;
using NUnit.Framework;
using R3;

namespace Tests.HeTu
{
    // 同一个订阅（sub_id）被订阅多次：每次 Watch 拿到自己的句柄，共用一个远端订阅；
    // 各自 Dispose 只退掉自己，最后一个句柄 Dispose 才发 unsub。
    [TestFixture]
    public class SubscriptionHandleTest
    {
        private const string RowSubId = "Item.id[7:None:1][:1]";
        private const string RangeSubId = "Item.owner[1:1:1][:10]";
        private const string TableSubId = "Item.table";

        [SetUp]
        public void SilenceLogger() =>
            Logger.Instance.SetLogger(_ => { }, _ => { }, _ => { });

        [Test]
        public void WatchRow_SameRowTwice_EachCallerGetsOwnHandle()
        {
            var client = HandleTestClient.Connected();
            var a = client.WatchRow(Row(7, 10));
            var b = client.WatchRow(Row(7, 10));

            Assert.NotNull(a);
            Assert.NotNull(b);
            Assert.AreNotSame(a, b, "每个调用方应拿到自己的句柄");
            Assert.AreEqual(1, client.CountSent("sub"), "第二次订阅不该再发 sub");
            Assert.AreEqual(10, Value(b.Data));
            Assert.AreEqual(RowSubId, b.SubId);
        }

        [Test]
        public void WatchRow_DisposeOneHandle_OtherKeepsUpdating()
        {
            var client = HandleTestClient.Connected();
            var a = client.WatchRow(Row(7, 10));
            var b = client.WatchRow(Row(7, 10));

            var aUpdates = new List<int>();
            a.OnUpdate += s => aUpdates.Add(Value(s.Data));
            var bUpdates = new List<int>();
            b.OnUpdate += s => bUpdates.Add(Value(s.Data));
            var bSubject = new List<int>();
            b.Subject.Subscribe(row => bSubject.Add(Value(row)));

            a.Dispose();
            Assert.AreEqual(0, client.CountSent("unsub"), "还有句柄在用，不能退订");

            client.Receive(Update(RowSubId, 7, Row(7, 20)));

            CollectionAssert.IsEmpty(aUpdates, "已 Dispose 的句柄不再收事件");
            CollectionAssert.AreEqual(new[] { 20 }, bUpdates);
            CollectionAssert.AreEqual(new[] { 10, 20 }, bSubject);
            Assert.AreEqual(20, Value(b.Data));
        }

        [Test]
        public void WatchRow_DisposeLastHandle_SendsSingleUnsub()
        {
            var client = HandleTestClient.Connected();
            var a = client.WatchRow(Row(7, 10));
            var b = client.WatchRow(Row(7, 10));

            a.Dispose();
            a.Dispose(); // 重复 Dispose 只算一次
            Assert.AreEqual(0, client.CountSent("unsub"));

            b.Dispose();
            Assert.AreEqual(1, client.CountSent("unsub"));
            Assert.That(client.Sent[^1], Is.EqualTo(new object[] { "unsub", RowSubId }));

            // 都退了：再订阅要重新发 sub
            client.WatchRow(Row(7, 10));
            Assert.AreEqual(2, client.CountSent("sub"));
        }

        [Test]
        public void WatchRow_DisposedHandle_CompletesOnlyItsOwnSubject()
        {
            var client = HandleTestClient.Connected();
            var a = client.WatchRow(Row(7, 10));
            var b = client.WatchRow(Row(7, 10));

            var aCompleted = false;
            a.Subject.Subscribe(_ => { }, _ => aCompleted = true);
            var bCompleted = false;
            b.Subject.Subscribe(_ => { }, _ => bCompleted = true);

            a.Dispose();

            Assert.IsTrue(aCompleted);
            Assert.IsFalse(bCompleted);
        }

        [Test]
        public void WatchRow_ByOtherIndex_ResolvingToWatchedRow_GetsOwnHandle()
        {
            // 非 id 索引事先算不出 sub_id，照常发 sub；服务端对已订阅的行回同一个 sub_id
            var client = HandleTestClient.Connected();
            var a = client.WatchRow(Row(7, 10));

            RowSubscription<DictComponent> b = null;
            client.WatchRowSync<DictComponent>("owner", 1L, (s, _, _) => b = s, "Item");
            client.Receive(new object[] { "sub", RowSubId, Row(7, 10) });

            Assert.NotNull(b);
            Assert.AreNotSame(a, b);
            a.Dispose();
            Assert.AreEqual(0, client.CountSent("unsub"));
            client.Receive(Update(RowSubId, 7, Row(7, 30)));
            Assert.AreEqual(30, Value(b.Data));
        }

        [Test]
        public void WatchRow_HandlerThrows_OtherHandleStillNotified()
        {
            var client = HandleTestClient.Connected();
            var a = client.WatchRow(Row(7, 10));
            var b = client.WatchRow(Row(7, 10));

            a.OnUpdate += _ => throw new InvalidOperationException("boom");
            var bUpdates = 0;
            b.OnUpdate += _ => bUpdates++;

            client.Receive(Update(RowSubId, 7, Row(7, 20)));

            Assert.AreEqual(1, bUpdates, "一个调用方的回调抛异常不能挡住别的调用方");
        }

        [Test]
        public void WatchRange_DisposeOneHandle_OtherKeepsReceivingRowChanges()
        {
            var client = HandleTestClient.Connected();
            var a = client.WatchRange(Row(7, 10));
            var b = client.WatchRange(Row(7, 10));
            Assert.AreNotSame(a, b);

            var rowValues = new List<int>();
            b.ObserveRow(7).Subscribe(row => rowValues.Add(Value(row)));
            var inserts = new List<long>();
            b.OnInsert += (_, id) => inserts.Add(id);
            var removed = new List<long>();
            b.ObserveRemove().Subscribe(id => removed.Add(id));

            a.Dispose();
            Assert.AreEqual(0, client.CountSent("unsub"));

            client.Receive(Update(RangeSubId, 7, Row(7, 11)));
            client.Receive(Update(RangeSubId, 8, Row(8, 1)));
            client.Receive(Update(RangeSubId, 7, null));

            CollectionAssert.AreEqual(new[] { 11 }, rowValues);
            CollectionAssert.AreEqual(new long[] { 8 }, inserts);
            CollectionAssert.AreEqual(new long[] { 7 }, removed);
            CollectionAssert.AreEquivalent(new long[] { 8 }, b.Rows.Keys);

            b.Dispose();
            Assert.AreEqual(1, client.CountSent("unsub"));
        }

        [Test]
        public void WatchRange_LaterHandle_SeesCurrentRowsAndCanObserveThem()
        {
            var client = HandleTestClient.Connected();
            var a = client.WatchRange(Row(7, 10));
            client.Receive(Update(RangeSubId, 8, Row(8, 1)));

            var b = client.WatchRange(Row(7, 10));
            Assert.AreNotSame(a, b);
            var added = new List<long>();
            b.ObserveAdd().Subscribe(row => added.Add(row.ID));
            var row8 = new List<int>();
            b.ObserveRow(8).Subscribe(row => row8.Add(Value(row)));

            client.Receive(Update(RangeSubId, 8, Row(8, 2)));

            CollectionAssert.AreEquivalent(new long[] { 7, 8 }, added);
            CollectionAssert.AreEqual(new[] { 2 }, row8);
            Assert.AreEqual(2, a.Rows.Count);
        }

        [Test]
        public void WatchRange_TwoInFlight_ShareOneRemote()
        {
            // 两次订阅都在回复前发出：服务端对重复订阅回同一个 sub_id、不计数，
            // 所以客户端最后只能发一次 unsub
            var client = HandleTestClient.Connected();
            IndexSubscription<DictComponent> a = null, b = null;
            client.WatchRangeSync<DictComponent>("owner", 1L, 1L, 10,
                (s, _, _) => a = s, componentName: "Item");
            client.WatchRangeSync<DictComponent>("owner", 1L, 1L, 10,
                (s, _, _) => b = s, componentName: "Item");
            Assert.AreEqual(2, client.CountSent("sub"));
            client.Receive(new object[] { "sub", RangeSubId, new[] { Row(7, 10) } });
            client.Receive(new object[] { "sub", RangeSubId, new[] { Row(7, 10) } });
            Assert.NotNull(a);
            Assert.NotNull(b);
            Assert.AreNotSame(a, b);

            a.Dispose();
            Assert.AreEqual(0, client.CountSent("unsub"));
            b.Dispose();
            Assert.AreEqual(1, client.CountSent("unsub"));
        }

        [Test]
        public void WatchTable_DisposeOneHandle_OtherKeepsUpdating()
        {
            var client = HandleTestClient.Connected();
            IndexSubscription<DictComponent> a = null, b = null;
            client.WatchTableSync<DictComponent>((s, _, _) => a = s, "Item");
            client.Receive(new object[] { "sub", TableSubId, new[] { Row(7, 10) } });
            client.WatchTableSync<DictComponent>((s, _, _) => b = s, "Item");
            Assert.AreNotSame(a, b);

            var updates = new List<long>();
            b.OnUpdate += (_, id) => updates.Add(id);
            a.Dispose();
            client.Receive(Update(TableSubId, 7, Row(7, 11)));

            CollectionAssert.AreEqual(new long[] { 7 }, updates);
            Assert.AreEqual(0, client.CountSent("unsub"));
        }

        [Test]
        public void HandleFromPreviousConnection_DisposeKeepsNewSubscription()
        {
            // 断线后不用会话、手动重连再订阅同一行：旧连接的句柄晚些 Dispose 时，
            // 不能把新连接上同 sub_id 的订阅退掉
            var client = HandleTestClient.Connected();
            var old = client.WatchRow(Row(7, 10));
            client.RaiseClosed();
            client.Reconnect();
            var fresh = client.WatchRow(Row(7, 11));
            Assert.AreNotSame(old, fresh);

            old.Dispose();

            Assert.AreEqual(0, client.CountSent("unsub"), "旧句柄不该退掉新订阅");
            client.Receive(Update(RowSubId, 7, Row(7, 12)));
            Assert.AreEqual(12, Value(fresh.Data));

            fresh.Dispose();
            Assert.AreEqual(1, client.CountSent("unsub"));
        }

        [Test]
        public void Dispose_UserDisposableThrows_StillReleasesHandle()
        {
            // DisposeBag 里用户挂的东西 Dispose 时抛异常，句柄也要交回，不然订阅永远退不掉
            var client = HandleTestClient.Connected();
            var row = client.WatchRow(Row(7, 10));
            row.DisposeBag.Add(new ThrowingDisposable());
            var range = client.WatchRange(Row(7, 10));
            range.DisposeBag.Add(new ThrowingDisposable());

            Assert.Throws<InvalidOperationException>(row.Dispose);
            Assert.Throws<InvalidOperationException>(range.Dispose);

            Assert.AreEqual(2, client.CountSent("unsub"), "抛异常也要退订");
        }

        [Test]
        public void RowRestore_LastHandleDisposedInFlight_DropsAndUnsubscribes()
        {
            // 会话恢复的 sub 在途时最后一个句柄 Dispose：核心还 stale，当时不发 unsub。
            // 回复到了不能把已关闭的核心登记回表、报恢复成功，服务端因这次恢复又订上了，要退掉
            var client = HandleTestClient.Connected();
            var core = new ManagedRowCore<DictComponent>(RowSubId, "Item",
                Dict(Row(7, 10)), client, null);
            var handle = core.CreateHandle(null);
            core.Suspend();
            ManagedRowCore<DictComponent> restored = null;
            var answered = false;
            client.WatchRowCoreSync<DictComponent>("id", 7L, (c, _, _) =>
            {
                restored = c;
                answered = true;
            }, "Item", core, null);

            handle.Dispose();
            Assert.AreEqual(0, client.CountSent("unsub"));
            client.Receive(new object[] { "sub", RowSubId, Row(7, 10) });

            Assert.IsTrue(answered);
            Assert.IsNull(restored, "已关闭的核心不算恢复成功");
            Assert.AreEqual(1, client.CountSent("unsub"));
            client.WatchRow(Row(7, 11));
            Assert.AreEqual(2, client.CountSent("sub"), "表里不能留已关闭的核心");
        }

        [Test]
        public void RangeRestore_LastHandleDisposedInFlight_DropsAndUnsubscribes()
        {
            var client = HandleTestClient.Connected();
            var core = new ManagedIndexCore<DictComponent>(RangeSubId, "Item",
                new List<DictComponent> { Dict(Row(7, 10)) }, client, null);
            var handle = core.CreateHandle(null);
            core.Suspend();
            ManagedIndexCore<DictComponent> restored = null;
            var answered = false;
            client.WatchRangeCoreSync<DictComponent>("owner", 1L, 1L, 10, (c, _, _) =>
            {
                restored = c;
                answered = true;
            }, false, true, "Item", core, null);

            handle.Dispose();
            client.Receive(new object[] { "sub", RangeSubId, new[] { Row(7, 10) } });

            Assert.IsTrue(answered);
            Assert.IsNull(restored);
            Assert.AreEqual(1, client.CountSent("unsub"));
        }

        [Test]
        public void RowRestore_SameSubIdAlreadyWatchedByRawClient_HandlesKeepUpdating()
        {
            // 重连后、会话恢复前，有人用裸客户端订了同一行：表里已有一个核心。会话的核心不能
            // 就此停在 stale，两边的句柄都要继续收推送，全部释放后才退订
            var client = HandleTestClient.Connected();
            var raw = client.WatchRow(Row(7, 20));
            var core = new ManagedRowCore<DictComponent>(RowSubId, "Item",
                Dict(Row(7, 10)), client, null);
            var sess = core.CreateHandle(null);
            core.Suspend();
            var resynced = 0;
            sess.OnResynced += () => resynced++;
            ManagedRowCore<DictComponent> restored = null;

            client.WatchRowCoreSync<DictComponent>("id", 7L, (c, _, _) => restored = c,
                "Item", core, null);

            Assert.AreSame(core, restored, "会话的核心恢复后仍是同一个");
            Assert.IsFalse(sess.IsStale);
            Assert.AreEqual(20, Value(sess.Data));
            Assert.AreEqual(1, resynced);
            client.Receive(Update(RowSubId, 7, Row(7, 30)));
            Assert.AreEqual(30, Value(sess.Data));
            Assert.AreEqual(30, Value(raw.Data));
            sess.Dispose();
            Assert.AreEqual(0, client.CountSent("unsub"));
            raw.Dispose();
            Assert.AreEqual(1, client.CountSent("unsub"));
        }

        [Test]
        public void RangeRestore_ReplyFindsCoreFromEarlierRawWatch_HandlesKeepUpdating()
        {
            // 裸客户端的订阅先发、恢复后发，裸的回复先到登记了核心，恢复的回复到达时撞上它
            var client = HandleTestClient.Connected();
            IndexSubscription<DictComponent> raw = null;
            client.WatchRangeSync<DictComponent>("owner", 1L, 1L, 10,
                (s, _, _) => raw = s, componentName: "Item");
            var core = new ManagedIndexCore<DictComponent>(RangeSubId, "Item",
                new List<DictComponent> { Dict(Row(7, 10)) }, client, null);
            core.ConfigureRestoreQuery("owner", 1L, 1L, 10, false, true);
            var sess = core.CreateHandle(null);
            core.Suspend();
            ManagedIndexCore<DictComponent> restored = null;
            client.WatchRangeCoreSync<DictComponent>("owner", 1L, 1L, 10,
                (c, _, _) => restored = c, false, true, "Item", core, null);

            var rows = new[] { Row(7, 20), Row(8, 1) };
            client.Receive(new object[] { "sub", RangeSubId, rows });
            var rawRow8 = new List<int>();
            raw.ObserveRow(8).Subscribe(r => rawRow8.Add(Value(r)));
            client.Receive(new object[] { "sub", RangeSubId, rows });

            Assert.AreSame(core, restored);
            Assert.IsFalse(sess.IsStale);
            Assert.AreEqual(20, Value(sess.Rows[7]));
            client.Receive(Update(RangeSubId, 8, Row(8, 2)));
            Assert.AreEqual(2, Value(sess.Rows[8]));
            CollectionAssert.AreEqual(new[] { 2 }, rawRow8, "裸句柄已有的行流不能断");
            sess.Dispose();
            Assert.AreEqual(0, client.CountSent("unsub"));
            raw.Dispose();
            Assert.AreEqual(1, client.CountSent("unsub"));
        }

        // 以下是"有 sub 在途时退订"的竞态：服务端对同一连接的重复订阅回同一个 sub_id、不计数，
        // 在途的 sub 若落到要退的订阅上，先发出的 unsub 会把它一起退掉（服务端先处理完 sub 时
        // 客户端拿到一个服务端已没有的订阅；先处理 unsub 时回 null）。unsub 要等在途的 sub 回完。

        [Test]
        public void UnsubWhileSubInFlight_ReplyLandsOnSameSubscription_KeepsIt()
        {
            var client = HandleTestClient.Connected();
            var a = client.WatchRow(Row(7, 10));
            RowSubscription<DictComponent> b = null;
            client.WatchRowSync<DictComponent>("owner", 1L, (s, _, _) => b = s, "Item");

            a.Dispose();
            Assert.AreEqual(0, client.CountSent("unsub"), "有 sub 在途时 unsub 先不发");

            client.Receive(new object[] { "sub", RowSubId, Row(7, 10) });
            Assert.NotNull(b);
            Assert.AreEqual(0, client.CountSent("unsub"), "在途回复落到了这个订阅上，不能退");
            client.Receive(Update(RowSubId, 7, Row(7, 30)));
            Assert.AreEqual(30, Value(b.Data));

            b.Dispose();
            Assert.AreEqual(1, client.CountSent("unsub"));
        }

        [Test]
        public void UnsubWhileSubInFlight_ReplyLandsElsewhere_SendsUnsubAfterReply()
        {
            var client = HandleTestClient.Connected();
            var a = client.WatchRow(Row(7, 10));
            RowSubscription<DictComponent> b = null;
            client.WatchRowSync<DictComponent>("owner", 2L, (s, _, _) => b = s, "Item");

            a.Dispose();
            Assert.AreEqual(0, client.CountSent("unsub"));

            client.Receive(new object[] { "sub", "Item.id[8:None:1][:1]", Row(8, 1) });
            Assert.NotNull(b);
            Assert.AreEqual(1, client.CountSent("unsub"));
            Assert.That(client.Sent[^1], Is.EqualTo(new object[] { "unsub", RowSubId }));
        }

        [Test]
        public void UnsubWhileSubInFlight_NullReply_SendsUnsubAfterReply()
        {
            var client = HandleTestClient.Connected();
            var a = client.WatchRow(Row(7, 10));
            var answered = false;
            client.WatchRowSync<DictComponent>("owner", 3L, (_, _, _) => answered = true,
                "Item");

            a.Dispose();
            Assert.AreEqual(0, client.CountSent("unsub"));

            client.Receive(new object[] { "sub", null, null });
            Assert.IsTrue(answered);
            Assert.AreEqual(1, client.CountSent("unsub"));
        }

        [Test]
        public void TwoRangesInFlight_FirstDisposedOnArrival_SecondStillSubscribed()
        {
            var client = HandleTestClient.Connected();
            IndexSubscription<DictComponent> a = null, b = null;
            client.WatchRangeSync<DictComponent>("owner", 1L, 1L, 10,
                (s, _, _) => a = s, componentName: "Item");
            client.WatchRangeSync<DictComponent>("owner", 1L, 1L, 10,
                (s, _, _) => b = s, componentName: "Item");

            client.Receive(new object[] { "sub", RangeSubId, new[] { Row(7, 10) } });
            a.Dispose();
            Assert.AreEqual(0, client.CountSent("unsub"), "第二个订阅还在途");

            client.Receive(new object[] { "sub", RangeSubId, new[] { Row(7, 10) } });
            Assert.NotNull(b);
            Assert.AreEqual(0, client.CountSent("unsub"));
            client.Receive(Update(RangeSubId, 7, Row(7, 11)));
            Assert.AreEqual(11, Value(b.Rows[7]));

            b.Dispose();
            Assert.AreEqual(1, client.CountSent("unsub"));
        }

        [Test]
        public void PendingUnsub_ReplyLandsThenDisposedInCallback_SendsOnce()
        {
            // 在途回复落到挂起的订阅上，回调里马上 Dispose：当场发出的 unsub 和挂着的那条
            // 只能发一条
            var client = HandleTestClient.Connected();
            var a = client.WatchRow(Row(7, 10));
            client.WatchRowSync<DictComponent>("owner", 1L, (s, _, _) => s.Dispose(),
                "Item");
            a.Dispose();

            client.Receive(new object[] { "sub", RowSubId, Row(7, 10) });

            Assert.AreEqual(1, client.CountSent("unsub"));
        }

        [Test]
        public void PendingUnsub_DroppedWhenConnectionCloses()
        {
            var client = HandleTestClient.Connected();
            var a = client.WatchRow(Row(7, 10));
            client.WatchRowSync<DictComponent>("owner", 1L, (_, _, _) => { }, "Item");
            a.Dispose();

            client.RaiseClosed();
            client.Reconnect();
            // 新连接上再订、回复：旧连接挂着的 unsub 不能补发
            client.WatchRange(Row(7, 10));

            Assert.AreEqual(0, client.CountSent("unsub"));
        }

        [Test]
        public void CallInFlight_DoesNotDelayUnsub()
        {
            var client = HandleTestClient.Connected();
            var a = client.WatchRow(Row(7, 10));
            client.Call("move");

            a.Dispose();

            Assert.AreEqual(1, client.CountSent("unsub"), "只有在途的 sub 才推迟 unsub");
        }

        private static Dictionary<string, object> Row(long id, int value) =>
            new() { ["id"] = id, ["value"] = value };

        private static int Value(DictComponent row) => Convert.ToInt32(row["value"]);

        private static DictComponent Dict(Dictionary<string, object> row)
        {
            var dict = new DictComponent();
            foreach (var (key, value) in row)
                dict[key] = value;
            return dict;
        }

        private static object[] Update(string subId, long rowId,
            Dictionary<string, object> row) =>
            new object[]
            {
                "updt", subId, new Dictionary<long, object> { [rowId] = row }
            };

        private sealed class ThrowingDisposable : IDisposable
        {
            public void Dispose() => throw new InvalidOperationException("boom");
        }

        private sealed class HandleTestClient : HeTuClientBase
        {
            private Action<int, string> _onClose;

            private HandleTestClient() =>
                SetupPipeline(new List<MessageProcessLayer> { new JsonbLayer() });

            public List<object[]> Sent { get; } = new();

            public static HandleTestClient Connected()
            {
                var client = new HandleTestClient();
                client.Reconnect();
                return client;
            }

            // 走一遍真实的连接入口（记下 onClose 好模拟断线），再当作已握手
            public void Reconnect()
            {
                ConnectSync("ws://test");
                State = ConnectionState.Connected;
            }

            public void RaiseClosed() => _onClose(HeTuCloseCode.Abnormal, "network lost");

            public void Call(string systemName) =>
                CallSystemSync(systemName, Array.Empty<object>(), (_, _, _) => { });

            public int CountSent(string command) =>
                Sent.FindAll(frame => (string)frame[0] == command).Count;

            // 订阅 Item 的 id=7 行；已订阅过时同步回调，否则用 reply 当服务端回复
            public RowSubscription<DictComponent> WatchRow(
                Dictionary<string, object> reply)
            {
                RowSubscription<DictComponent> result = null;
                var answered = false;
                WatchRowSync<DictComponent>("id", 7L, (s, _, _) =>
                {
                    result = s;
                    answered = true;
                }, "Item");
                if (!answered)
                    Receive(new object[] { "sub", RowSubId, reply });
                return result;
            }

            // 订阅 Item 的 owner=[1,1] 范围；已订阅过时同步回调，否则用 reply 当初始行
            public IndexSubscription<DictComponent> WatchRange(
                params Dictionary<string, object>[] reply)
            {
                IndexSubscription<DictComponent> result = null;
                var answered = false;
                WatchRangeSync<DictComponent>("owner", 1L, 1L, 10, (s, _, _) =>
                {
                    result = s;
                    answered = true;
                }, componentName: "Item");
                if (!answered)
                    Receive(new object[] { "sub", RangeSubId, reply });
                return result;
            }

            public void Receive(object[] frame) =>
                OnReceived(Pipeline.Encode(frame, out _));

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
