using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using HeTu;
using NUnit.Framework;
using UnityEngine;
using UnityEngine.TestTools;
#if !UNITY_6000_0_OR_NEWER
using Cysharp.Threading.Tasks;
#endif

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

        [UnityTest]
        public IEnumerator Connect_TimesOut_When_TransportNeverConnects() =>
            RunTask(Connect_TimesOut_When_TransportNeverConnectsAsync());

        private async Task Connect_TimesOut_When_TransportNeverConnectsAsync()
        {
            var transport = new FakeTransport("c1");
            var scheduler = new FakeScheduler();
            using var session = CreateSession(transport, scheduler);

            // transport 不 RaiseConnected：timeout 200ms 内应触发并 Close 会话。
            var connectTask =
                ToTask(session.Connect(TimeSpan.FromMilliseconds(200)));

            TimeoutException caught = null;
            try
            {
                await connectTask;
            }
            catch (TimeoutException ex)
            {
                caught = ex;
            }

            Assert.IsNotNull(caught,
                "Connect 在 transport 不响应时必须抛 TimeoutException");
            Assert.AreEqual(HeTuSessionState.Stopped, session.State,
                "Connect 超时必须把 session Close 掉");
        }

        [UnityTest]
        public IEnumerator Connect_TimeoutNoOp_After_Ready() =>
            RunTask(Connect_TimeoutNoOp_After_ReadyAsync());

        private async Task Connect_TimeoutNoOp_After_ReadyAsync()
        {
            var transport = new FakeTransport("c1");
            var scheduler = new FakeScheduler();
            using var session = CreateSession(transport, scheduler);

            var connectTask =
                ToTask(session.Connect(TimeSpan.FromMilliseconds(150)));
            transport.RaiseConnected();
            await connectTask;
            Assert.AreEqual(HeTuSessionState.Ready, session.State);

            // 等过超时窗口；State 必须仍是 Ready（迟到的 timer 不应误关已成功的 session）。
            await Task.Delay(400);
            Assert.AreEqual(HeTuSessionState.Ready, session.State,
                "Ready 之后才 fire 的 timeout 必须是 no-op");
        }

        [UnityTest]
        public IEnumerator Connect_ThrowsCapturedFault_When_CoreEntersFaulted() =>
            RunTask(Connect_ThrowsCapturedFault_When_CoreEntersFaultedAsync());

        private async Task Connect_ThrowsCapturedFault_When_CoreEntersFaultedAsync()
        {
            var transport = new FakeTransport("c1");
            var scheduler = new FakeScheduler();
            // maxReconnectAttempts=1：第一次 close 即耗尽进 Faulted 终态。
            var core = new HeTuSessionClientBase(
                () => transport,
                scheduler,
                null,
                TimeSpan.Zero,
                TimeSpan.Zero,
                1);
            using var session = new HeTuSessionClient(core);

            var connectTask = ToTask(session.Connect());
            transport.RaiseClosed("network down");

            Exception caught = null;
            try
            {
                await connectTask;
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            Assert.IsNotNull(caught,
                "core 进 Faulted 终态时 Connect 的 await 必须抛出");
            StringAssert.Contains("network down", caught.Message,
                "抛出的异常应携带 Faulted 事件捕获到的原始 fault");
            Assert.AreEqual(HeTuSessionState.Faulted, session.State);
        }

        // 主用例:启动时匿名 Connect → Ready → 用户登录后 SetBootstrap →
        // 断线时 SDK 自动重连,跑这个新装的 bootstrap。
        [UnityTest]
        public IEnumerator SetBootstrap_AfterReady_NextReconnectRunsNewBootstrap() =>
            RunTask(SetBootstrap_AfterReady_NextReconnectRunsNewBootstrapAsync());

        private async Task
            SetBootstrap_AfterReady_NextReconnectRunsNewBootstrapAsync()
        {
            var ts = new[]
            {
                new FakeTransport("c1"),
                new FakeTransport("c2")
            };
            var q = new Queue<FakeTransport>(ts);
            var scheduler = new FakeScheduler();
            var core = new HeTuSessionClientBase(
                () => q.Dequeue(),
                scheduler,
                bootstrap: null,
                TimeSpan.Zero,
                TimeSpan.Zero,
                maxReconnectAttempts: 0);
            using var session = new HeTuSessionClient(core);

            var connectTask = ToTask(session.Connect());
            ts[0].RaiseConnected();
            await connectTask;
            Assert.AreEqual(HeTuSessionState.Ready, session.State);

            var calls = 0;
            session.SetBootstrap(_ =>
            {
                calls++;
                return CompletedAwaitable();
            });

            ts[0].RaiseClosed("network");
            // 真实计时器 FakeScheduler 异步触发 reconnectDelay=0,
            // 让一帧给 factory dequeue ts[1] 并订阅其 Connected
            await Task.Yield();
            await Task.Yield();
            ts[1].RaiseConnected();

            // bootstrap 通过 async Awaitable 桥接,continuation 需要一帧来回
            await Task.Yield();
            await Task.Yield();

            Assert.AreEqual(1, calls,
                "断线重连必须跑用 SetBootstrap 设置的新 bootstrap");
            Assert.AreEqual(HeTuSessionState.Ready, session.State);
        }

#if UNITY_6000_0_OR_NEWER
        private static Awaitable CompletedAwaitable()
        {
            var tcs = new AwaitableCompletionSource();
            tcs.SetResult();
            return tcs.Awaitable;
        }
#else
        private static UniTask CompletedAwaitable() => UniTask.CompletedTask;
#endif

        private static HeTuSessionClient CreateSession(
            FakeTransport transport,
            FakeScheduler scheduler) =>
            new(
                new HeTuSessionClientBase(
                    () => transport,
                    scheduler,
                    null,
                    TimeSpan.Zero));

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
            public int Value { get; set; }
            public long ID { get; set; }
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
            public List<CallRecord> Calls { get; } = new();

            public Dictionary<(string Index, object Value), TestComponent> RowResults
            {
                get;
            } = new();

            public bool IsConnected { get; private set; }

            public event Action Connected;
            public event Action<string> Closed;

            public void Connect()
            {
            }

            public void Close() => IsConnected = false;

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

        // 用真实 Unity 计时器替代旧版无操作 FakeScheduler——Connect 超时改走
        // _core.WaitForReady 经过 scheduler，而 PlayMode 里测试要的就是真实时序。
        private sealed class FakeScheduler : IHeTuSessionScheduler
        {
            public IDisposable Schedule(TimeSpan delay, Action action)
            {
                var scheduled = new ScheduledAction();
#if UNITY_6000_0_OR_NEWER
                _ = RunAsync(delay, action, scheduled);
#else
                RunAsync(delay, action, scheduled).Forget();
#endif
                return scheduled;
            }

#if UNITY_6000_0_OR_NEWER
            private static async Awaitable RunAsync(
#else
            private static async UniTask RunAsync(
#endif
                TimeSpan delay, Action action, ScheduledAction scheduled)
            {
#if UNITY_6000_0_OR_NEWER
                await Awaitable.WaitForSecondsAsync((float)delay.TotalSeconds);
#else
                await UniTask.Delay(delay);
#endif
                if (!scheduled.IsDisposed) action();
            }

            private sealed class ScheduledAction : IDisposable
            {
                public bool IsDisposed { get; private set; }
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
