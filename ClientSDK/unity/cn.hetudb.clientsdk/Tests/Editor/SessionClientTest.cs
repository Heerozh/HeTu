using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using HeTu;
using NUnit.Framework;
using UnityEngine.TestTools;

namespace Tests.HeTu
{
    [TestFixture]
    public class SessionClientTest
    {
        [UnityTest]
        public IEnumerator CallBeforeReady_WaitsUntilBootstrapCompletes() =>
            RunTask(CallBeforeReady_WaitsUntilBootstrapCompletesAsync());

        private async Task CallBeforeReady_WaitsUntilBootstrapCompletesAsync()
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

        [UnityTest]
        public IEnumerator WatchBeforeReady_WaitsUntilInitialSnapshotIsAvailable() =>
            RunTask(WatchBeforeReady_WaitsUntilInitialSnapshotIsAvailableAsync());

        private async Task WatchBeforeReady_WaitsUntilInitialSnapshotIsAvailableAsync()
        {
            var connection = new FakeSessionConnection("c1");
            connection.RowByIdResults[7] = new TestComponent { ID = 7, Value = 10 };
            var bootstrapGate = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var session = CreateSession(
                new Queue<FakeSessionConnection>(new[] { connection }),
                async (_, ct) =>
                {
                    await bootstrapGate.Task;
                    ct.ThrowIfCancellationRequested();
                });

            _ = session.StartAsync();
            var watchTask = session.WatchRowById<TestComponent>(7);

            Assert.False(watchTask.IsCompleted);
            Assert.AreEqual(0, connection.WatchedRowIds.Count);

            bootstrapGate.TrySetResult(true);
            using var sub = await watchTask;

            Assert.AreEqual(10, sub.Data.Value);
            CollectionAssert.AreEqual(new long[] { 7 }, connection.WatchedRowIds);
        }

        [UnityTest]
        public IEnumerator DuplicateIntent_SharesSingleRemoteSubscription() =>
            RunTask(DuplicateIntent_SharesSingleRemoteSubscriptionAsync());

        private async Task DuplicateIntent_SharesSingleRemoteSubscriptionAsync()
        {
            var connection = new FakeSessionConnection("c1");
            connection.RowByIdResults[7] = new TestComponent { ID = 7, Value = 10 };
            var session = CreateSession(
                new Queue<FakeSessionConnection>(new[] { connection }));
            await session.StartAsync();

            using var first = await session.WatchRowById<TestComponent>(7);
            using var second = await session.WatchRowById<TestComponent>(7);

            Assert.AreEqual(10, first.Data.Value);
            Assert.AreEqual(10, second.Data.Value);
            CollectionAssert.AreEqual(new long[] { 7 }, connection.WatchedRowIds);
        }

        [UnityTest]
        public IEnumerator SentCallThenDisconnect_CompletesAsUnknownOutcome_WithoutRetry() =>
            RunTask(SentCallThenDisconnect_CompletesAsUnknownOutcome_WithoutRetryAsync());

        private async Task SentCallThenDisconnect_CompletesAsUnknownOutcome_WithoutRetryAsync()
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
                await WaitUntilAsync(() => session.State == HeTuSessionState.Ready &&
                                       second.OpenCount == 1);

            var unknownOutcome = false;
            try
            {
                await callTask;
            }
            catch (CallOutcomeUnknownException)
            {
                unknownOutcome = true;
            }

            Assert.True(unknownOutcome);
            Assert.AreEqual(0, second.Calls.Count);
        }

        [UnityTest]
        public IEnumerator WatchRowById_RestoresAfterReconnect() =>
            RunTask(WatchRowById_RestoresAfterReconnectAsync());

        private async Task WatchRowById_RestoresAfterReconnectAsync()
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

        [UnityTest]
        public IEnumerator WatchRow_RestoresUsingOriginalIntent_NotPreviousRemoteSubId() =>
            RunTask(WatchRow_RestoresUsingOriginalIntent_NotPreviousRemoteSubIdAsync());

        private async Task WatchRow_RestoresUsingOriginalIntent_NotPreviousRemoteSubIdAsync()
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

            using var sub = await session.WatchRow<TestComponent>("owner", 123L);
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

        [UnityTest]
        public IEnumerator WatchRange_RestoresAndReplacesSnapshotAfterReconnect() =>
            RunTask(WatchRange_RestoresAndReplacesSnapshotAfterReconnectAsync());

        private async Task WatchRange_RestoresAndReplacesSnapshotAfterReconnectAsync()
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

        [UnityTest]
        public IEnumerator DisposedSubscriptionWhileDisconnected_IsNotRestored() =>
            RunTask(DisposedSubscriptionWhileDisconnected_IsNotRestoredAsync());

        private async Task DisposedSubscriptionWhileDisconnected_IsNotRestoredAsync()
        {
            var first = new FakeSessionConnection("c1");
            first.RowByIdResults[7] = new TestComponent { ID = 7, Value = 10 };
            var second = new FakeSessionConnection("c2");
            second.RowByIdResults[7] = new TestComponent { ID = 7, Value = 20 };

            var session = CreateSession(
                new Queue<FakeSessionConnection>(new[] { first, second }),
                reconnectDelay: TimeSpan.FromMilliseconds(50));
            await session.StartAsync();

            var sub = await session.WatchRowById<TestComponent>(7);
            first.Close("network lost");
            await WaitUntilAsync(() => session.State == HeTuSessionState.Reconnecting);
            sub.Dispose();

            await WaitUntilAsync(() => session.State == HeTuSessionState.Ready &&
                                       second.OpenCount == 1);

            Assert.AreEqual(0, second.WatchedRowIds.Count);
        }

        [UnityTest]
        public IEnumerator RestoreOrder_BootstrapRunsBeforeSubscriptions() =>
            RunTask(RestoreOrder_BootstrapRunsBeforeSubscriptionsAsync());

        private async Task RestoreOrder_BootstrapRunsBeforeSubscriptionsAsync()
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
            Func<FakeSessionConnection, CancellationToken, Task> bootstrap = null,
            TimeSpan? reconnectDelay = null)
        {
            return new HeTuSessionClient(
                () => connections.Dequeue(),
                bootstrap == null
                    ? null
                    : (conn, ct) => bootstrap((FakeSessionConnection)conn, ct),
                reconnectDelay ?? TimeSpan.Zero);
        }

        private static IEnumerator RunTask(Task task)
        {
            while (!task.IsCompleted)
                yield return null;

            if (task.Exception != null)
                throw task.Exception;
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

            public Task<RowSubscription<T>> WatchRowAsync<T>(
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

            public ValueTask DisposeAsync() => new();

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
