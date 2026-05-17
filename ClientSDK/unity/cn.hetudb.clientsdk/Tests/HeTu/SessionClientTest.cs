using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using HeTu;
using NUnit.Framework;

namespace Tests.HeTu
{
    [TestFixture]
    public class SessionClientTest
    {
        [Test]
        public async Task CallBeforeReady_WaitsUntilBootstrapCompletes()
        {
            var connection = new FakeSessionConnection("c1");
            var bootstrapGate = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var session = CreateSession(
                new Queue<FakeSessionConnection>(new[] { connection }),
                async (_, ct) =>
                {
                    await bootstrapGate.Task;
                    ct.ThrowIfCancellationRequested();
                });

            var startTask = session.StartAsync();
            var callTask = session.CallSystem("login", 1);

            Assert.AreEqual(0, connection.Calls.Count);

            bootstrapGate.TrySetResult(true);
            await startTask;
            await callTask;

            CollectionAssert.AreEqual(
                new[] { "login" },
                connection.Calls.Select(call => call.SystemName));
        }

        [Test]
        public async Task SentCallThenDisconnect_CompletesAsUnknownOutcome_WithoutRetry()
        {
            var first = new FakeSessionConnection("c1")
            {
                HoldCallsOpen = true
            };
            var second = new FakeSessionConnection("c2");
            var session = CreateSession(
                new Queue<FakeSessionConnection>(new[] { first, second }));

            await session.StartAsync();
            var callTask = session.CallSystem("mutate", 1);
            await WaitUntilAsync(() => first.Calls.Count == 1);

            first.Close("network lost");
            await WaitUntilAsync(() => session.State == SessionState.Ready &&
                                       second.OpenCount == 1);

            Assert.ThrowsAsync<CallOutcomeUnknownException>(async () =>
                await callTask);
            Assert.AreEqual(0, second.Calls.Count);
        }

        [Test]
        public async Task WatchRowById_RestoresAfterReconnect()
        {
            var first = new FakeSessionConnection("c1");
            first.RowByIdResults[7] = new TestComponent { ID = 7, Value = 10 };

            var second = new FakeSessionConnection("c2");
            second.RowByIdResults[7] = new TestComponent { ID = 7, Value = 20 };

            var session = CreateSession(
                new Queue<FakeSessionConnection>(new[] { first, second }));
            await session.StartAsync();

            using var sub = await session.WatchRowById<TestComponent>(7);
            Assert.AreEqual(10, sub.Data.Value);

            var resyncCount = 0;
            sub.OnResynced += () => resyncCount++;

            first.Close("network lost");
            await WaitUntilAsync(() => sub.Data.Value == 20);

            Assert.AreEqual(1, resyncCount);
            CollectionAssert.AreEqual(new long[] { 7 }, first.WatchedRowIds);
            CollectionAssert.AreEqual(new long[] { 7 }, second.WatchedRowIds);
        }

        [Test]
        public async Task WatchFirst_RestoresUsingOriginalIntent_NotPreviousRemoteSubId()
        {
            var first = new FakeSessionConnection("c1");
            first.FirstResults[("owner", 123L)] =
                new TestComponent { ID = 7, Value = 10 };

            var second = new FakeSessionConnection("c2");
            second.FirstResults[("owner", 123L)] =
                new TestComponent { ID = 9, Value = 30 };

            var session = CreateSession(
                new Queue<FakeSessionConnection>(new[] { first, second }));
            await session.StartAsync();

            using var sub = await session.WatchFirst<TestComponent>("owner", 123L);
            Assert.AreEqual(7, sub.Data.ID);

            first.Close("network lost");
            await WaitUntilAsync(() => sub.Data.ID == 9);

            CollectionAssert.AreEqual(
                new[] { ("owner", (object)123L) },
                first.WatchedFirstQueries);
            CollectionAssert.AreEqual(
                new[] { ("owner", (object)123L) },
                second.WatchedFirstQueries);
        }

        [Test]
        public async Task WatchRange_RestoresAndReplacesSnapshotAfterReconnect()
        {
            var first = new FakeSessionConnection("c1");
            first.RangeResults[("value", 0, 10, 10, false, true)] =
                new List<TestComponent>
                {
                    new() { ID = 1, Value = 1 },
                    new() { ID = 2, Value = 2 }
                };

            var second = new FakeSessionConnection("c2");
            second.RangeResults[("value", 0, 10, 10, false, true)] =
                new List<TestComponent>
                {
                    new() { ID = 2, Value = 20 },
                    new() { ID = 3, Value = 3 }
                };

            var session = CreateSession(
                new Queue<FakeSessionConnection>(new[] { first, second }));
            await session.StartAsync();

            using var sub = await session.WatchRange<TestComponent>(
                "value", 0, 10, 10);
            CollectionAssert.AreEquivalent(new long[] { 1, 2 }, sub.Rows.Keys);

            var resyncCount = 0;
            sub.OnResynced += () => resyncCount++;

            first.Close("network lost");
            await WaitUntilAsync(() => sub.Rows.ContainsKey(3));

            Assert.AreEqual(1, resyncCount);
            CollectionAssert.AreEquivalent(new long[] { 2, 3 }, sub.Rows.Keys);
            Assert.AreEqual(20, sub.Rows[2].Value);
        }

        [Test]
        public async Task DisposedSubscriptionWhileDisconnected_IsNotRestored()
        {
            var first = new FakeSessionConnection("c1");
            first.RowByIdResults[7] = new TestComponent { ID = 7, Value = 10 };
            var second = new FakeSessionConnection("c2");
            second.RowByIdResults[7] = new TestComponent { ID = 7, Value = 20 };

            var session = CreateSession(
                new Queue<FakeSessionConnection>(new[] { first, second }));
            await session.StartAsync();

            var sub = await session.WatchRowById<TestComponent>(7);
            first.Close("network lost");
            await WaitUntilAsync(() => session.State == SessionState.Reconnecting);
            sub.Dispose();

            await WaitUntilAsync(() => session.State == SessionState.Ready &&
                                       second.OpenCount == 1);

            Assert.AreEqual(0, second.WatchedRowIds.Count);
        }

        [Test]
        public async Task RestoreOrder_BootstrapRunsBeforeSubscriptions()
        {
            var first = new FakeSessionConnection("c1");
            first.RowByIdResults[7] = new TestComponent { ID = 7, Value = 10 };
            var second = new FakeSessionConnection("c2");
            second.RowByIdResults[7] = new TestComponent { ID = 7, Value = 20 };

            var session = CreateSession(
                new Queue<FakeSessionConnection>(new[] { first, second }),
                (conn, _) =>
                {
                    conn.Operations.Add("bootstrap");
                    return Task.CompletedTask;
                });
            await session.StartAsync();
            using var _ = await session.WatchRowById<TestComponent>(7);

            first.Close("network lost");
            await WaitUntilAsync(() => second.Operations.Contains("watch-row"));

            CollectionAssert.AreEqual(
                new[] { "open", "bootstrap", "watch-row" },
                second.Operations);
        }

        private static HeTuSessionClient CreateSession(
            Queue<FakeSessionConnection> connections,
            Func<FakeSessionConnection, CancellationToken, Task> bootstrap = null)
        {
            return new HeTuSessionClient(
                () => connections.Dequeue(),
                bootstrap == null
                    ? null
                    : (conn, ct) => bootstrap((FakeSessionConnection)conn, ct),
                TimeSpan.Zero);
        }

        private static async Task WaitUntilAsync(Func<bool> predicate)
        {
            var deadline = DateTime.UtcNow.AddSeconds(2);
            while (!predicate())
            {
                if (DateTime.UtcNow >= deadline)
                    Assert.Fail("Condition was not met before timeout.");
                await Task.Delay(10);
            }
        }

        private sealed class TestComponent : IBaseComponent
        {
            public long ID { get; set; }
            public int Value { get; set; }
        }

        private sealed class FakeSessionConnection : IHeTuSessionConnection
        {
            private readonly TaskCompletionSource<string> _closed = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly FakeRemoteClient _remoteClient = new();

            public FakeSessionConnection(string name)
            {
                Name = name;
                _remoteClient.ForceConnected();
            }

            public string Name { get; }
            public bool HoldCallsOpen { get; set; }
            public int OpenCount { get; private set; }
            public List<string> Operations { get; } = new();
            public List<CallRecord> Calls { get; } = new();
            public List<long> WatchedRowIds { get; } = new();
            public List<(string Index, object Value)> WatchedFirstQueries { get; } = new();
            public List<(string Index, object Left, object Right, int Limit, bool Desc,
                bool Force)> WatchedRanges { get; } = new();
            public Dictionary<long, TestComponent> RowByIdResults { get; } = new();
            public Dictionary<(string Index, object Value), TestComponent> FirstResults
            {
                get;
            } = new();
            public Dictionary<(string Index, object Left, object Right, int Limit,
                bool Desc, bool Force), List<TestComponent>> RangeResults { get; } = new();

            public Task OpenAsync(CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();
                OpenCount++;
                Operations.Add("open");
                return Task.CompletedTask;
            }

            public Task<string> WaitClosedAsync(
                CancellationToken cancellationToken = default) =>
                _closed.Task;

            public async Task<JsonObject> CallSystemAsync(string systemName,
                object[] args, CancellationToken cancellationToken = default)
            {
                Calls.Add(new CallRecord(systemName, args));
                if (HoldCallsOpen)
                {
                    var never = new TaskCompletionSource<JsonObject>(
                        TaskCreationOptions.RunContinuationsAsynchronously);
                    return await never.Task;
                }

                return null;
            }

            public Task<RowSubscription<T>> WatchRowByIdAsync<T>(
                long id, string componentName = null,
                CancellationToken cancellationToken = default)
                where T : IBaseComponent
            {
                Operations.Add("watch-row");
                WatchedRowIds.Add(id);
                var row = (T)(IBaseComponent)RowByIdResults[id];
                return Task.FromResult(new RowSubscription<T>(
                    $"{Name}.row.{id}", componentName ?? typeof(T).Name, row,
                    _remoteClient));
            }

            public Task<RowSubscription<T>> WatchFirstAsync<T>(
                string index, object value, string componentName = null,
                CancellationToken cancellationToken = default)
                where T : IBaseComponent
            {
                Operations.Add("watch-first");
                WatchedFirstQueries.Add((index, value));
                var row = (T)(IBaseComponent)FirstResults[(index, value)];
                return Task.FromResult(new RowSubscription<T>(
                    $"{Name}.first.{index}.{value}", componentName ?? typeof(T).Name, row,
                    _remoteClient));
            }

            public Task<IndexSubscription<T>> WatchRangeAsync<T>(
                string index, object left, object right, int limit,
                bool desc = false, bool force = true, string componentName = null,
                CancellationToken cancellationToken = default)
                where T : IBaseComponent
            {
                Operations.Add("watch-range");
                WatchedRanges.Add((index, left, right, limit, desc, force));
                var rows = RangeResults[(index, left, right, limit, desc, force)]
                    .Cast<T>()
                    .ToList();
                return Task.FromResult(new IndexSubscription<T>(
                    $"{Name}.range.{index}", componentName ?? typeof(T).Name, rows,
                    _remoteClient));
            }

            public void Close(string reason) => _closed.TrySetResult(reason);

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;

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
