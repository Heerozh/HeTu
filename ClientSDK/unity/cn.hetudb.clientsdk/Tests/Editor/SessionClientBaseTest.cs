using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using HeTu;
using NUnit.Framework;

namespace Tests.HeTu
{
    [TestFixture]
    public sealed class SessionClientBaseTest
    {
        [Test]
        public void Start_BootstrapsBeforeReady()
        {
            var transport = new FakeTransport("c1");
            var scheduler = new FakeScheduler();
            var operations = new List<string>();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { transport }),
                scheduler,
                (_, succeed, _) =>
                {
                    operations.Add("bootstrap");
                    succeed();
                });

            session.Start();
            Assert.AreEqual(HeTuSessionState.Connecting, session.State);

            transport.RaiseConnected();

            Assert.AreEqual(HeTuSessionState.Ready, session.State);
            CollectionAssert.AreEqual(
                new[] { "connect", "bootstrap" },
                transport.Operations.Concat(operations));
        }

        [Test]
        public void CallBeforeReady_IsQueuedUntilSessionReady()
        {
            var transport = new FakeTransport("c1");
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { transport }),
                scheduler);
            JsonObject response = null;

            session.Start();
            session.CallSystem("login", new object[] { 1 }, x => response = x,
                _ => Assert.Fail("call should not fail"));

            Assert.AreEqual(0, transport.Calls.Count);

            transport.RaiseConnected();

            Assert.AreEqual(1, transport.Calls.Count);
            Assert.IsNull(response);
        }

        [Test]
        public void SentCallThenDisconnect_FailsAsUnknownOutcome_WithoutRetry()
        {
            var first = new FakeTransport("c1")
            {
                HoldCallsOpen = true
            };
            var second = new FakeTransport("c2");
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { first, second }),
                scheduler);
            Exception failure = null;

            session.Start();
            first.RaiseConnected();
            session.CallSystem("mutate", new object[] { 1 }, _ => { },
                ex => failure = ex);

            first.RaiseClosed("network lost");
            scheduler.RunNext();
            second.RaiseConnected();

            Assert.IsInstanceOf<CallOutcomeUnknownException>(failure);
            Assert.AreEqual(1, first.Calls.Count);
            Assert.AreEqual(0, second.Calls.Count);
        }

        [Test]
        public void WatchRowBeforeReady_CompletesAfterInitialSnapshot()
        {
            var transport = new FakeTransport("c1");
            transport.RowResults[("id", 7L)] =
                new TestComponent { ID = 7, Value = 10 };
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { transport }),
                scheduler);
            RowSubscription<TestComponent> subscription = null;

            session.Start();
            session.WatchRow<TestComponent>("id", 7L, null,
                sub => subscription = sub,
                _ => Assert.Fail("watch should not fail"));

            Assert.IsNull(subscription);
            Assert.AreEqual(0, transport.WatchedRows.Count);

            transport.RaiseConnected();

            Assert.NotNull(subscription);
            Assert.AreEqual(10, subscription.Data.Value);
            CollectionAssert.AreEqual(
                new[] { ("id", (object)7L) },
                transport.WatchedRows);
        }

        [Test]
        public void WatchRow_WithSameSubId_ReturnsSameSubscription()
        {
            var transport = new FakeTransport("c1");
            transport.RowResults[("id", 7L)] =
                new TestComponent { ID = 7, Value = 10 };
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { transport }),
                scheduler);

            session.Start();
            transport.RaiseConnected();

            RowSubscription<TestComponent> first = null;
            RowSubscription<TestComponent> second = null;
            session.WatchRow<TestComponent>("id", 7L, null,
                sub => first = sub,
                _ => Assert.Fail("watch should not fail"));
            session.WatchRow<TestComponent>("id", 7L, null,
                sub => second = sub,
                _ => Assert.Fail("watch should not fail"));

            Assert.NotNull(first);
            Assert.NotNull(second);
            Assert.AreSame(first, second);
            Assert.AreEqual(10, first.Data.Value);
            Assert.AreEqual(10, second.Data.Value);
            CollectionAssert.AreEqual(
                new[] { ("id", (object)7L) },
                transport.WatchedRows);
        }

        [Test]
        public void WatchRange_WithSameSubId_ReturnsSameSubscription()
        {
            var transport = new FakeTransport("c1");
            transport.RangeResults[("value", 0, 10, 10, false, true)] =
                new List<TestComponent>
                {
                    new() { ID = 1, Value = 1 }
                };
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { transport }),
                scheduler);

            session.Start();
            transport.RaiseConnected();

            IndexSubscription<TestComponent> first = null;
            IndexSubscription<TestComponent> second = null;
            session.WatchRange<TestComponent>(
                "value", 0, 10, 10, null,
                sub => first = sub,
                _ => Assert.Fail("watch should not fail"));
            session.WatchRange<TestComponent>(
                "value", 0, 10, 10, null,
                sub => second = sub,
                _ => Assert.Fail("watch should not fail"));

            Assert.NotNull(first);
            Assert.NotNull(second);
            Assert.AreSame(first, second);
            Assert.AreEqual(1, first.Rows.Count);
            Assert.AreEqual(1, transport.WatchedRanges.Count);
        }

        [Test]
        public void WatchRow_WithIdIndex_RestoresAfterReconnect()
        {
            var first = new FakeTransport("c1");
            first.RowResults[("id", 7L)] = new TestComponent { ID = 7, Value = 10 };
            var second = new FakeTransport("c2");
            second.RowResults[("id", 7L)] = new TestComponent { ID = 7, Value = 20 };
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { first, second }),
                scheduler);

            session.Start();
            first.RaiseConnected();

            RowSubscription<TestComponent> subscription = null;
            session.WatchRow<TestComponent>("id", 7L, null,
                sub => subscription = sub,
                _ => Assert.Fail("watch should not fail"));
            var resynced = 0;
            subscription.OnResynced += () => resynced++;

            first.RaiseClosed("network lost");
            scheduler.RunNext();
            second.RaiseConnected();

            Assert.AreEqual(20, subscription.Data.Value);
            Assert.AreEqual(1, resynced);
            CollectionAssert.AreEqual(
                new[] { ("id", (object)7L) },
                second.WatchedRows);
        }

        [Test]
        public void WatchRow_ResolvedByNonId_RestoresUsingBoundRowId()
        {
            var first = new FakeTransport("c1");
            first.RowResults[("owner", 123L)] =
                new TestComponent { ID = 7, Value = 10 };
            var second = new FakeTransport("c2");
            second.RowResults[("id", 7L)] =
                new TestComponent { ID = 7, Value = 30 };
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { first, second }),
                scheduler);

            session.Start();
            first.RaiseConnected();

            RowSubscription<TestComponent> subscription = null;
            session.WatchRow<TestComponent>("owner", 123L, null,
                sub => subscription = sub,
                _ => Assert.Fail("watch should not fail"));

            first.RaiseClosed("network lost");
            scheduler.RunNext();
            second.RaiseConnected();

            Assert.AreEqual(7, subscription.Data.ID);
            CollectionAssert.AreEqual(
                new[] { ("id", (object)7L) },
                second.WatchedRows);
        }

        [Test]
        public void WatchRange_RestoresAndReplacesSnapshotAfterReconnect()
        {
            var first = new FakeTransport("c1");
            first.RangeResults[("value", 0, 10, 10, false, true)] =
                new List<TestComponent>
                {
                    new() { ID = 1, Value = 1 },
                    new() { ID = 2, Value = 2 }
                };
            var second = new FakeTransport("c2");
            second.RangeResults[("value", 0, 10, 10, false, true)] =
                new List<TestComponent>
                {
                    new() { ID = 2, Value = 20 },
                    new() { ID = 3, Value = 3 }
                };
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { first, second }),
                scheduler);

            session.Start();
            first.RaiseConnected();

            IndexSubscription<TestComponent> subscription = null;
            session.WatchRange<TestComponent>(
                "value", 0, 10, 10, null,
                sub => subscription = sub,
                _ => Assert.Fail("watch should not fail"));
            var resynced = 0;
            subscription.OnResynced += () => resynced++;

            first.RaiseClosed("network lost");
            scheduler.RunNext();
            second.RaiseConnected();

            CollectionAssert.AreEquivalent(new long[] { 2, 3 },
                subscription.Rows.Keys);
            Assert.AreEqual(20, subscription.Rows[2].Value);
            Assert.AreEqual(1, resynced);
        }

        [Test]
        public void WatchRange_Resync_PreservesExistingRowSubject()
        {
            var first = new FakeTransport("c1");
            first.RangeResults[("value", 0, 10, 10, false, true)] =
                new List<TestComponent>
                {
                    new() { ID = 2, Value = 2 }
                };
            var second = new FakeTransport("c2");
            second.RangeResults[("value", 0, 10, 10, false, true)] =
                new List<TestComponent>
                {
                    new() { ID = 2, Value = 20 }
                };
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { first, second }),
                scheduler);

            session.Start();
            first.RaiseConnected();

            IndexSubscription<TestComponent> subscription = null;
            session.WatchRange<TestComponent>(
                "value", 0, 10, 10, null,
                sub => subscription = sub,
                _ => Assert.Fail("watch should not fail"));

            var replaceSubjectsField = typeof(IndexSubscription<TestComponent>)
                .GetField("_replaceSubjects",
                    BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(replaceSubjectsField);
            var replaceSubjects = replaceSubjectsField.GetValue(subscription);
            var itemProperty = replaceSubjects.GetType().GetProperty("Item");
            Assert.NotNull(itemProperty);
            var subjectBefore = itemProperty.GetValue(replaceSubjects,
                new object[] { 2L });

            first.RaiseClosed("network lost");
            scheduler.RunNext();
            second.RaiseConnected();
            var subjectAfter = itemProperty.GetValue(replaceSubjects,
                new object[] { 2L });

            Assert.AreSame(subjectBefore, subjectAfter);
        }

        [Test]
        public void DisposedSubscriptionWhileDisconnected_IsNotRestored()
        {
            var first = new FakeTransport("c1");
            first.RowResults[("id", 7L)] = new TestComponent { ID = 7, Value = 10 };
            var second = new FakeTransport("c2");
            second.RowResults[("id", 7L)] = new TestComponent { ID = 7, Value = 20 };
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { first, second }),
                scheduler);

            session.Start();
            first.RaiseConnected();

            RowSubscription<TestComponent> subscription = null;
            session.WatchRow<TestComponent>("id", 7L, null,
                sub => subscription = sub,
                _ => Assert.Fail("watch should not fail"));

            first.RaiseClosed("network lost");
            subscription.Dispose();
            scheduler.RunNext();
            second.RaiseConnected();

            Assert.AreEqual(0, second.WatchedRows.Count);
        }

        [Test]
        public void RestoreOrder_BootstrapRunsBeforeSubscriptions()
        {
            var first = new FakeTransport("c1");
            first.RowResults[("id", 7L)] = new TestComponent { ID = 7, Value = 10 };
            var second = new FakeTransport("c2");
            second.RowResults[("id", 7L)] = new TestComponent { ID = 7, Value = 20 };
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { first, second }),
                scheduler,
                (transport, succeed, _) =>
                {
                    ((FakeTransport)transport).Operations.Add("bootstrap");
                    succeed();
                });

            session.Start();
            first.RaiseConnected();
            session.WatchRow<TestComponent>("id", 7L, null, _ => { },
                _ => Assert.Fail("watch should not fail"));

            first.RaiseClosed("network lost");
            scheduler.RunNext();
            second.RaiseConnected();

            CollectionAssert.AreEqual(
                new[] { "connect", "bootstrap", "watch-row" },
                second.Operations);
        }

        [Test]
        public void WatchRow_Miss_DoesNotRetainIntent()
        {
            var first = new FakeTransport("c1");
            var second = new FakeTransport("c2");
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { first, second }),
                scheduler);
            RowSubscription<TestComponent> subscription = null;

            session.Start();
            first.RaiseConnected();
            session.WatchRow<TestComponent>("owner", 123L, null,
                sub => subscription = sub,
                _ => Assert.Fail("watch should not fail"));

            Assert.IsNull(subscription);

            first.RaiseClosed("network lost");
            scheduler.RunNext();
            second.RaiseConnected();

            Assert.AreEqual(0, second.WatchedRows.Count);
        }

        [Test]
        public void Restore_LateCancelAfterReconnect_DoesNotCascadeIntoNewTransport()
        {
            var first = new FakeTransport("c1");
            first.RowResults[("id", 7L)] = new TestComponent { ID = 7, Value = 10 };
            var second = new FakeTransport("c2") { HoldWatchCallbacks = true };
            var third = new FakeTransport("c3");
            third.RowResults[("id", 7L)] = new TestComponent { ID = 7, Value = 30 };
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { first, second, third }),
                scheduler);

            session.Start();
            first.RaiseConnected();

            RowSubscription<TestComponent> subscription = null;
            session.WatchRow<TestComponent>("id", 7L, null,
                sub => subscription = sub,
                _ => Assert.Fail("watch should not fail"));
            Assert.NotNull(subscription);

            // 第一次断线 → 重连到 second，second 把 Restore 的 WatchRow 回调挂起不返回，
            // 模拟真实底层把 callback 留在 ResponseQueue 里等下一轮 CancelAll 触发。
            first.RaiseClosed("network lost");
            scheduler.RunNext();
            second.RaiseConnected();
            Assert.AreEqual(1, second.HeldWatchCount,
                "second 应正在 Restore，WatchRow 回调被挂起");
            Assert.AreEqual(HeTuSessionState.RestoringSubscriptions, session.State);

            // 第二次断线 → 重连到 third，third 立刻完成 Restore 进入 Ready。
            second.RaiseClosed("network lost again");
            scheduler.RunNext();
            third.RaiseConnected();
            Assert.AreEqual(HeTuSessionState.Ready, session.State);
            Assert.IsFalse(third.IsDisposed);

            // 现在 second 那条 stale 的 WatchRow 回调以 canceled=true 触发，
            // 模拟真实底层 ResponseQueue.CancelAll 在 third 的 ConnectSync 中冲掉它。
            // 修复前：会级联到 HandleRecoverableFailure，把刚 Ready 的 third 当场 Dispose
            // 并再排一次 reconnect。
            second.ReleaseHeldWatches(canceled: true);

            Assert.IsFalse(third.IsDisposed,
                "stale cancel 不应当级联 Dispose 当前 transport");
            Assert.AreEqual(0, scheduler.PendingCount,
                "不应当排出额外的 reconnect");
            Assert.AreEqual(HeTuSessionState.Ready, session.State);
        }

        [Test]
        public void CallSystemAfterClose_Throws()
        {
            var transport = new FakeTransport("c1");
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { transport }), scheduler);

            session.Start();
            transport.RaiseConnected();
            session.Close();

            Assert.Throws<ObjectDisposedException>(() =>
                session.CallSystem("noop", Array.Empty<object>(),
                    _ => { }, _ => { }));
        }

        [Test]
        public void WatchRowAfterClose_Throws()
        {
            var transport = new FakeTransport("c1");
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { transport }), scheduler);

            session.Start();
            transport.RaiseConnected();
            session.Close();

            Assert.Throws<ObjectDisposedException>(() =>
                session.WatchRow<TestComponent>("id", 1L, null,
                    _ => { }, _ => { }));
        }

        [Test]
        public void WatchRangeAfterClose_Throws()
        {
            var transport = new FakeTransport("c1");
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { transport }), scheduler);

            session.Start();
            transport.RaiseConnected();
            session.Close();

            Assert.Throws<ObjectDisposedException>(() =>
                session.WatchRange<TestComponent>("value", 0, 10, 10, null,
                    _ => { }, _ => { }));
        }

        [Test]
        public void Close_LateWatchSuccess_DisposesSubscriptionInsteadOfOrphaning()
        {
            var transport = new FakeTransport("c1") { HoldWatchCallbacks = true };
            transport.RowResults[("id", 7L)] = new TestComponent { ID = 7, Value = 10 };
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { transport }), scheduler);

            session.Start();
            transport.RaiseConnected();

            RowSubscription<TestComponent> received = null;
            Exception failure = null;
            session.WatchRow<TestComponent>("id", 7L, null,
                sub => received = sub,
                ex => failure = ex);
            Assert.AreEqual(1, transport.HeldWatchCount);

            // Close 期间 watch 还在 in-flight。
            session.Close();
            Assert.IsInstanceOf<OperationCanceledException>(failure);

            // 服务端晚到的成功响应。
            transport.ReleaseHeldWatches(canceled: false);

            // 用户回调不能被二次触发。
            Assert.IsNull(received,
                "Close 后晚到的成功响应不应再回调用户");
            // 新生成的 subscription 必须 Dispose，否则就是孤儿（服务器仍在推送）。
            Assert.NotNull(transport.LastIssuedSubscription);
            Assert.IsTrue(transport.LastIssuedSubscription.IsDisposed,
                "Close 后晚到的 subscription 必须 Dispose，不能留在 session 里");
        }

        [Test]
        public void Reconnect_UsesExponentialBackoff()
        {
            var ts = new[]
            {
                new FakeTransport("c1"),
                new FakeTransport("c2"),
                new FakeTransport("c3"),
                new FakeTransport("c4")
            };
            var q = new Queue<FakeTransport>(ts);
            var scheduler = new FakeScheduler();
            var session = new HeTuSessionClientBase(
                () => q.Dequeue(),
                scheduler,
                bootstrap: null,
                reconnectDelay: TimeSpan.FromSeconds(1),
                maxReconnectDelay: TimeSpan.FromSeconds(30));

            session.Start();
            ts[0].RaiseClosed("e");
            scheduler.RunNext();
            ts[1].RaiseClosed("e");
            scheduler.RunNext();
            ts[2].RaiseClosed("e");
            scheduler.RunNext();
            ts[3].RaiseClosed("e");

            CollectionAssert.AreEqual(
                new[]
                {
                    TimeSpan.FromSeconds(1),
                    TimeSpan.FromSeconds(2),
                    TimeSpan.FromSeconds(4),
                    TimeSpan.FromSeconds(8)
                },
                scheduler.ScheduledDelays);
        }

        [Test]
        public void Reconnect_BackoffResetsAfterReady()
        {
            var ts = new[]
            {
                new FakeTransport("c1"),
                new FakeTransport("c2"),
                new FakeTransport("c3"),
                new FakeTransport("c4")
            };
            var q = new Queue<FakeTransport>(ts);
            var scheduler = new FakeScheduler();
            var session = new HeTuSessionClientBase(
                () => q.Dequeue(),
                scheduler,
                bootstrap: null,
                reconnectDelay: TimeSpan.FromSeconds(1),
                maxReconnectDelay: TimeSpan.FromSeconds(30));

            session.Start();
            ts[0].RaiseClosed("e");
            scheduler.RunNext();
            ts[1].RaiseClosed("e");
            scheduler.RunNext();
            ts[2].RaiseConnected();
            Assert.AreEqual(HeTuSessionState.Ready, session.State);
            ts[2].RaiseClosed("e");
            scheduler.RunNext();
            ts[3].RaiseClosed("e");

            CollectionAssert.AreEqual(
                new[]
                {
                    TimeSpan.FromSeconds(1),
                    TimeSpan.FromSeconds(2),
                    TimeSpan.FromSeconds(1),
                    TimeSpan.FromSeconds(2)
                },
                scheduler.ScheduledDelays);
        }

        [Test]
        public void Reconnect_BackoffCapsAtMax()
        {
            var ts = new FakeTransport[6];
            for (var i = 0; i < ts.Length; i++)
                ts[i] = new FakeTransport($"c{i}");
            var q = new Queue<FakeTransport>(ts);
            var scheduler = new FakeScheduler();
            var session = new HeTuSessionClientBase(
                () => q.Dequeue(),
                scheduler,
                bootstrap: null,
                reconnectDelay: TimeSpan.FromSeconds(1),
                maxReconnectDelay: TimeSpan.FromSeconds(4));

            session.Start();
            for (var i = 0; i < ts.Length - 1; i++)
            {
                ts[i].RaiseClosed("e");
                scheduler.RunNext();
            }
            ts[^1].RaiseClosed("e");

            CollectionAssert.AreEqual(
                new[]
                {
                    TimeSpan.FromSeconds(1),
                    TimeSpan.FromSeconds(2),
                    TimeSpan.FromSeconds(4),
                    TimeSpan.FromSeconds(4),
                    TimeSpan.FromSeconds(4),
                    TimeSpan.FromSeconds(4)
                },
                scheduler.ScheduledDelays);
        }

        [Test]
        public void Reconnect_AfterMaxAttempts_EntersFaultedAndStopsRetrying()
        {
            var ts = new[]
            {
                new FakeTransport("c1"),
                new FakeTransport("c2"),
                new FakeTransport("c3"),
                new FakeTransport("never-used")
            };
            var q = new Queue<FakeTransport>(ts);
            var scheduler = new FakeScheduler();
            var session = new HeTuSessionClientBase(
                () => q.Dequeue(),
                scheduler,
                bootstrap: null,
                reconnectDelay: TimeSpan.FromSeconds(1),
                maxReconnectDelay: TimeSpan.FromSeconds(30),
                maxReconnectAttempts: 3);

            var observed = new List<HeTuSessionState>();
            session.StateChanged += observed.Add;

            session.Start();
            ts[0].RaiseClosed("fail 1");
            scheduler.RunNext();
            ts[1].RaiseClosed("fail 2");
            scheduler.RunNext();
            ts[2].RaiseClosed("fail 3");

            Assert.AreEqual(HeTuSessionState.Faulted, session.State);
            Assert.Contains(HeTuSessionState.Faulted, observed);
            Assert.AreEqual(0, scheduler.PendingCount,
                "no further reconnect should be scheduled after Faulted");
            Assert.AreEqual(0, ts[3].ConnectCount,
                "the 4th transport should never be constructed");
        }

        [Test]
        public void Faulted_FailsPendingCallsAndWatchesAndRejectsNewWork()
        {
            var ts = new[] { new FakeTransport("c1") };
            var q = new Queue<FakeTransport>(ts);
            var scheduler = new FakeScheduler();
            var session = new HeTuSessionClientBase(
                () => q.Dequeue(),
                scheduler,
                bootstrap: null,
                reconnectDelay: TimeSpan.FromSeconds(1),
                maxReconnectDelay: TimeSpan.FromSeconds(30),
                maxReconnectAttempts: 1);

            session.Start();

            Exception callFailure = null;
            Exception watchFailure = null;
            session.CallSystem("noop", Array.Empty<object>(),
                _ => Assert.Fail("call should not succeed"),
                ex => callFailure = ex);
            session.WatchRow<TestComponent>("id", 1L, null,
                _ => Assert.Fail("watch should not succeed"),
                ex => watchFailure = ex);

            ts[0].RaiseClosed("fatal");

            Assert.AreEqual(HeTuSessionState.Faulted, session.State);
            Assert.NotNull(callFailure, "queued CallSystem must fail when entering Faulted");
            Assert.NotNull(watchFailure, "queued WatchRow must fail when entering Faulted");

            Assert.Throws<ObjectDisposedException>(() =>
                session.CallSystem("noop", Array.Empty<object>(),
                    _ => { }, _ => { }));
        }

        [Test]
        public void Close_CancelsScheduledReconnectAndStopsSession()
        {
            var first = new FakeTransport("c1");
            var second = new FakeTransport("c2");
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { first, second }),
                scheduler);

            session.Start();
            first.RaiseConnected();
            first.RaiseClosed("network lost");

            Assert.AreEqual(1, scheduler.PendingCount);

            session.Close();
            scheduler.RunNext();

            Assert.AreEqual(HeTuSessionState.Stopped, session.State);
            Assert.AreEqual(0, second.ConnectCount);
        }

        // ----- Code-review 4 个问题对应的 TDD 用例 -----

        // 问题 #1：Ready 触发顺序破坏 FIFO。
        [Test]
        public void Ready_HandlerInvokedAfterPendingCallsFlushed()
        {
            var transport = new FakeTransport("c1");
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { transport }),
                scheduler);

            session.Start();
            session.CallSystem("queued-before-ready", Array.Empty<object>(),
                _ => { }, _ => { });

            session.Ready += () =>
            {
                session.CallSystem("issued-from-ready-handler",
                    Array.Empty<object>(), _ => { }, _ => { });
            };

            transport.RaiseConnected();

            Assert.AreEqual(2, transport.Calls.Count);
            Assert.AreEqual("queued-before-ready",
                transport.Calls[0].SystemName,
                "排队请求必须先于 Ready 回调里新发的请求送出");
            Assert.AreEqual("issued-from-ready-handler",
                transport.Calls[1].SystemName);
        }

        // 问题 #2：用户回调抛异常会打断 Close 的清理循环。
        [Test]
        public void Close_OnePendingCallFailedHandlerThrows_OthersStillNotified()
        {
            var transport = new FakeTransport("c1");
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { transport }),
                scheduler);

            session.Start();

            var firstFailed = false;
            var secondFailed = false;
            session.CallSystem("first", Array.Empty<object>(), _ => { },
                _ =>
                {
                    firstFailed = true;
                    throw new InvalidOperationException("user handler boom");
                });
            session.CallSystem("second", Array.Empty<object>(), _ => { },
                _ => secondFailed = true);

            Assert.DoesNotThrow(() => session.Close(),
                "Close 不应把用户回调里的异常向外抛");
            Assert.IsTrue(firstFailed, "first pending 必须被通知");
            Assert.IsTrue(secondFailed,
                "first 的 OnFailed 抛异常后,second 仍必须被通知");
            Assert.AreEqual(HeTuSessionState.Stopped, session.State,
                "Close 必须能跑完到 Stopped 终态");
        }

        // 问题 #3：transport-level 重试耗尽时未触发 Faulted 事件。
        [Test]
        public void Reconnect_AfterMaxAttempts_FiresFaultedEvent()
        {
            var transport = new FakeTransport("c1");
            var scheduler = new FakeScheduler();
            var session = new HeTuSessionClientBase(
                () => transport,
                scheduler,
                bootstrap: null,
                reconnectDelay: TimeSpan.FromSeconds(1),
                maxReconnectDelay: TimeSpan.FromSeconds(30),
                maxReconnectAttempts: 1);

            Exception observed = null;
            session.Faulted += ex => observed = ex;

            session.Start();
            transport.RaiseClosed("fatal close reason");

            Assert.AreEqual(HeTuSessionState.Faulted, session.State);
            Assert.IsNotNull(observed,
                "transport 重试耗尽进入 Faulted 终态前必须 invoke Faulted 事件");
            StringAssert.Contains("fatal close reason", observed.Message);
        }

        // 问题 #4：bootstrap/restore 失败时 transport 只 Dispose 未 Close。
        [Test]
        public void Bootstrap_Failure_ClosesUnderlyingTransport()
        {
            var transport = new FakeTransport("c1");
            var fallback = new FakeTransport("c2");
            var scheduler = new FakeScheduler();
            var session = CreateSession(
                new Queue<FakeTransport>(new[] { transport, fallback }),
                scheduler,
                (_, _, fail) =>
                    fail(new InvalidOperationException("bootstrap failed")));

            session.Start();
            transport.RaiseConnected();

            Assert.IsFalse(transport.IsConnected,
                "应用层 bootstrap 失败时 session 必须主动 Close 还活着的 transport, "
                + "否则要等到 TCP keepalive 服务器才会回收连接");
        }

        private static HeTuSessionClientBase CreateSession(
            Queue<FakeTransport> transports,
            FakeScheduler scheduler,
            HeTuSessionBootstrap bootstrap = null)
        {
            return new HeTuSessionClientBase(
                () => transports.Dequeue(),
                scheduler,
                bootstrap,
                TimeSpan.Zero);
        }

        private sealed class TestComponent : IBaseComponent
        {
            public long ID { get; set; }
            public int Value { get; set; }
        }

        private sealed class FakeTransport : IHeTuSessionTransport
        {
            private readonly FakeRemoteClient _remoteClient = new();

            public FakeTransport(string name)
            {
                Name = name;
                _remoteClient.ForceConnected();
            }

            public string Name { get; }
            public bool HoldCallsOpen { get; set; }
            public bool HoldWatchCallbacks { get; set; }
            public bool IsConnected { get; private set; }
            public bool IsDisposed { get; private set; }
            public int ConnectCount { get; private set; }
            public List<string> Operations { get; } = new();
            public List<CallRecord> Calls { get; } = new();
            public List<(string Index, object Value)> WatchedRows { get; } = new();
            public List<(string Index, object Left, object Right, int Limit, bool Desc,
                bool Force)> WatchedRanges { get; } = new();
            public Dictionary<(string Index, object Value), TestComponent> RowResults
            {
                get;
            } = new();
            public Dictionary<(string Index, object Left, object Right, int Limit,
                bool Desc, bool Force), List<TestComponent>> RangeResults { get; } = new();
            public BaseSubscription LastIssuedSubscription { get; private set; }
            public int HeldWatchCount => _heldWatches.Count;
            private readonly List<Action<bool>> _heldWatches = new();

            public void ReleaseHeldWatches(bool canceled)
            {
                var snapshot = _heldWatches.ToArray();
                _heldWatches.Clear();
                foreach (var responder in snapshot)
                    responder(canceled);
            }

            public event Action Connected;
            public event Action<string> Closed;

            public void Connect()
            {
                ConnectCount++;
                Operations.Add("connect");
            }

            public void Close() => IsConnected = false;

            public void RaiseConnected()
            {
                IsConnected = true;
                Connected?.Invoke();
            }

            public void RaiseClosed(string reason)
            {
                IsConnected = false;
                Closed?.Invoke(reason);
            }

            public void CallSystem(string systemName, object[] args,
                Action<JsonObject, bool> onResponse)
            {
                Calls.Add(new CallRecord(systemName, args));
                if (!HoldCallsOpen)
                    onResponse(null, false);
            }

            public void WatchRow<T>(
                string index, object value,
                Action<RowSubscription<T>, bool, Exception> onResponse,
                string componentName = null,
                RowSubscription<T> reusable = null)
                where T : IBaseComponent
            {
                Operations.Add("watch-row");
                WatchedRows.Add((index, value));

                void Respond(bool canceled)
                {
                    if (canceled)
                    {
                        onResponse(null, true, null);
                        return;
                    }

                    if (!RowResults.TryGetValue((index, value), out var result))
                    {
                        onResponse(null, false, null);
                        return;
                    }

                    var compName = componentName ?? typeof(T).Name;
                    var row = (T)(IBaseComponent)result;
                    var subId = HeTuClientBase.MakeSubId(
                        compName, "id", row.ID, null, 1, false);
                    RowSubscription<T> resolved;
                    if (reusable != null)
                    {
                        reusable.Rebind(subId, row, _remoteClient);
                        resolved = reusable;
                    }
                    else
                    {
                        resolved = new RowSubscription<T>(
                            subId, compName, row, _remoteClient);
                    }

                    LastIssuedSubscription = resolved;
                    onResponse(resolved, false, null);
                }

                if (HoldWatchCallbacks)
                {
                    _heldWatches.Add(Respond);
                    return;
                }

                Respond(false);
            }

            public void WatchRange<T>(
                string index, object left, object right, int limit,
                Action<IndexSubscription<T>, bool, Exception> onResponse,
                bool desc = false, bool force = true, string componentName = null,
                IndexSubscription<T> reusable = null)
                where T : IBaseComponent
            {
                Operations.Add("watch-range");
                var key = (index, left, right, limit, desc, force);
                WatchedRanges.Add(key);
                var rows = RangeResults[key].Cast<T>().ToList();
                componentName ??= typeof(T).Name;
                var subId = HeTuClientBase.MakeSubId(
                    componentName, index, left, right, limit, desc);
                if (reusable != null)
                {
                    reusable.Rebind(subId, rows, _remoteClient);
                    onResponse(reusable, false, null);
                    return;
                }

                var subscription = new IndexSubscription<T>(
                    subId, componentName, rows, _remoteClient);
                subscription.ConfigureRestoreQuery(index, left, right, limit, desc,
                    force);
                onResponse(subscription, false, null);
            }

            public void Dispose() => IsDisposed = true;

            public readonly struct CallRecord
            {
                public CallRecord(string systemName, object[] args)
                {
                    SystemName = systemName;
                    Args = args;
                }

                public string SystemName { get; }
                public object[] Args { get; }
            }
        }

        private sealed class FakeScheduler : IHeTuSessionScheduler
        {
            private readonly Queue<ScheduledAction> _scheduled = new();

            public int PendingCount => _scheduled.Count(x => !x.IsDisposed);
            public List<TimeSpan> ScheduledDelays { get; } = new();

            public IDisposable Schedule(TimeSpan delay, Action action)
            {
                ScheduledDelays.Add(delay);
                var scheduled = new ScheduledAction(action);
                _scheduled.Enqueue(scheduled);
                return scheduled;
            }

            public void RunNext()
            {
                while (_scheduled.Count > 0)
                {
                    var scheduled = _scheduled.Dequeue();
                    if (scheduled.IsDisposed)
                        continue;
                    scheduled.Run();
                    return;
                }
            }

            private sealed class ScheduledAction : IDisposable
            {
                private readonly Action _action;

                public ScheduledAction(Action action) => _action = action;

                public bool IsDisposed { get; private set; }

                public void Run()
                {
                    if (!IsDisposed)
                        _action();
                }

                public void Dispose() => IsDisposed = true;
            }
        }

        private sealed class FakeRemoteClient : HeTuClientBase
        {
            public void ForceConnected() => State = ConnectionState.Connected;

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
