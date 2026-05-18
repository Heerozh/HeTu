using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
#if !UNITY_6000_0_OR_NEWER
using Cysharp.Threading.Tasks;
#endif
using HeTu;
using NUnit.Framework;
using UnityEngine.TestTools;

namespace Tests.HeTu
{
    [TestFixture]
    public sealed class SessionClientFacadeTest
    {
        [UnityTest]
        public IEnumerator Connect_CompletesWhenCoreBecomesReady() =>
            RunTask(Connect_CompletesWhenCoreBecomesReadyAsync());

        private async Task Connect_CompletesWhenCoreBecomesReadyAsync()
        {
            var transport = new FakeTransport("c1");
            var scheduler = new FakeScheduler();
            using var session = CreateSession(transport, scheduler);

            var connectTask = ToTask(session.Connect());
            Assert.False(connectTask.IsCompleted);

            transport.RaiseConnected();
            await connectTask;

            Assert.AreEqual(HeTuSessionState.Ready, session.State);
        }

        [UnityTest]
        public IEnumerator CallSystem_CompletesFromCoreCallback() =>
            RunTask(CallSystem_CompletesFromCoreCallbackAsync());

        private async Task CallSystem_CompletesFromCoreCallbackAsync()
        {
            var transport = new FakeTransport("c1");
            var scheduler = new FakeScheduler();
            using var session = CreateSession(transport, scheduler);

            var connectTask = ToTask(session.Connect());
            transport.RaiseConnected();
            await connectTask;

            var callTask = ToTask(session.CallSystem("login", 1));
            await callTask;

            CollectionAssert.AreEqual(
                new[] { "login" },
                transport.Calls.Select(call => call.SystemName));
        }

        [UnityTest]
        public IEnumerator WatchRow_CompletesFromCoreCallback() =>
            RunTask(WatchRow_CompletesFromCoreCallbackAsync());

        private async Task WatchRow_CompletesFromCoreCallbackAsync()
        {
            var transport = new FakeTransport("c1");
            transport.RowResults[("id", 7L)] =
                new TestComponent { ID = 7, Value = 10 };
            var scheduler = new FakeScheduler();
            using var session = CreateSession(transport, scheduler);

            var connectTask = ToTask(session.Connect());
            transport.RaiseConnected();
            await connectTask;

            using var subscription =
                await ToTask(session.WatchRow<TestComponent>("id", 7L));

            Assert.AreEqual(10, subscription.Data.Value);
        }

        [UnityTest]
        public IEnumerator Close_CancelsPendingConnectAwaiter() =>
            RunTask(Close_CancelsPendingConnectAwaiterAsync());

        private async Task Close_CancelsPendingConnectAwaiterAsync()
        {
            var transport = new FakeTransport("c1");
            var scheduler = new FakeScheduler();
            using var session = CreateSession(transport, scheduler);

            var connectTask = ToTask(session.Connect());
            session.Close();

            var canceled = false;
            try
            {
                await connectTask;
            }
            catch (TaskCanceledException)
            {
                canceled = true;
            }

            Assert.True(canceled);
        }

        private static HeTuSessionClient CreateSession(
            FakeTransport transport,
            FakeScheduler scheduler)
        {
            return new HeTuSessionClient(
                new HeTuSessionClientBase(
                    () => transport,
                    scheduler,
                    null,
                    TimeSpan.Zero));
        }

        private static IEnumerator RunTask(Task task)
        {
            while (!task.IsCompleted)
                yield return null;

            if (task.Exception != null)
                throw task.Exception;
        }

#if UNITY_6000_0_OR_NEWER
        private static async Task ToTask(Awaitable awaitable) =>
            await awaitable;

        private static async Task<T> ToTask<T>(Awaitable<T> awaitable) =>
            await awaitable;
#else
        private static Task ToTask(UniTask task) => task.AsTask();

        private static Task<T> ToTask<T>(UniTask<T> task) => task.AsTask();
#endif

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
            public bool IsConnected { get; private set; }
            public List<CallRecord> Calls { get; } = new();
            public Dictionary<(string Index, object Value), TestComponent> RowResults
            {
                get;
            } = new();

            public event Action Connected;
            public event Action<string> Closed;

            public void Connect()
            {
            }

            public void Close() => IsConnected = false;

            public void RaiseConnected()
            {
                IsConnected = true;
                Connected?.Invoke();
            }

            public void CallSystem(string systemName, object[] args,
                Action<JsonObject, bool> onResponse)
            {
                Calls.Add(new CallRecord(systemName, args));
                onResponse(null, false);
            }

            public void WatchRow<T>(
                string index, object value,
                Action<RowSubscription<T>, bool, Exception> onResponse,
                string componentName = null,
                RowSubscription<T> reusable = null)
                where T : IBaseComponent
            {
                componentName ??= typeof(T).Name;
                var row = (T)(IBaseComponent)RowResults[(index, value)];
                var subId = HeTuClientBase.MakeSubId(
                    componentName, "id", row.ID, null, 1, false);
                if (reusable != null)
                {
                    reusable.Rebind(subId, row, _remoteClient);
                    onResponse(reusable, false, null);
                    return;
                }

                onResponse(
                    new RowSubscription<T>(
                        subId,
                        componentName,
                        row,
                        _remoteClient),
                    false,
                    null);
            }

            public void WatchRange<T>(
                string index, object left, object right, int limit,
                Action<IndexSubscription<T>, bool, Exception> onResponse,
                bool desc = false, bool force = true, string componentName = null,
                IndexSubscription<T> reusable = null)
                where T : IBaseComponent =>
                throw new NotSupportedException();

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
            public IDisposable Schedule(TimeSpan delay, Action action) =>
                new NoopDisposable();

            private sealed class NoopDisposable : IDisposable
            {
                public void Dispose()
                {
                }
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
