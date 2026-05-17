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
            public bool IsConnected { get; private set; }
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
                if (!RowResults.TryGetValue((index, value), out var result))
                {
                    onResponse(null, false, null);
                    return;
                }

                componentName ??= typeof(T).Name;
                var row = (T)(IBaseComponent)result;
                var subId = HeTuClientBase.MakeSubId(
                    componentName, "id", row.ID, null, 1, false);
                if (reusable != null)
                {
                    reusable.Rebind(subId, row, _remoteClient);
                    onResponse(reusable, false, null);
                    return;
                }

                onResponse(
                    new RowSubscription<T>(subId, componentName, row, _remoteClient),
                    false,
                    null);
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

            public void Dispose()
            {
            }

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

            public IDisposable Schedule(TimeSpan delay, Action action)
            {
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
