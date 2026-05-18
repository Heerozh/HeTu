// <copyright>
// Copyright 2026, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity逻辑会话层</summary>


#if !UNITY_6000_0_OR_NEWER
using Cysharp.Threading.Tasks;
#endif
using System;
using System.Threading;
using UnityEngine;

namespace HeTu
{
    /// <summary>
    ///     自动管理重连、bootstrap 与订阅恢复的 Unity 逻辑会话层。单件，通过
    ///     <see cref="Instance" /> 获取——底层共享 <see cref="HeTuClient.Instance" />
    ///     的物理连接，所以一个进程同时只能有一个会话。
    /// </summary>
    public sealed class HeTuSessionClient : IDisposable
    {
#if UNITY_6000_0_OR_NEWER
        private static AwaitableCompletionSource<T> NewCompletionSource<T>() => new();
        private static Awaitable<T> AwaitFrom<T>(AwaitableCompletionSource<T> tcs) =>
            tcs.Awaitable;
#else
        private static UniTaskCompletionSource<T> NewCompletionSource<T>() => new();
        private static UniTask<T> AwaitFrom<T>(UniTaskCompletionSource<T> tcs) =>
            tcs.Task;
#endif

        private static readonly HeTuSessionClient s_instance = new();

        /// <summary>
        ///     单件实例。
        /// </summary>
        public static HeTuSessionClient Instance => s_instance;

        private HeTuSessionClientBase _core;

        private HeTuSessionClient() { }

        internal HeTuSessionClient(HeTuSessionClientBase core)
        {
            _core = core;
            WireFacadeEvents(core);
        }

        /// <summary>
        ///     当前会话状态；尚未 Connect 时为
        ///     <see cref="HeTuSessionState.Stopped" />。
        /// </summary>
        public HeTuSessionState State =>
            _core?.State ?? HeTuSessionState.Stopped;

        /// <summary>
        ///     会话状态每次变化都触发一次；最常用于驱动 UI（断线 → 显示 syncing...
        ///     条幅 / Ready → 隐藏）。
        /// </summary>
        public event Action<HeTuSessionState> StateChanged;

        /// <summary>
        ///     每次"本次连接失败"触发一次：socket 关闭、bootstrap 异常、restore 异常
        ///     都会进来，携带本次失败的异常。是否已经是终态请同时观察
        ///     <see cref="State" /> == <see cref="HeTuSessionState.Faulted" />。
        /// </summary>
        public event Action<Exception> Faulted;

        private void WireFacadeEvents(HeTuSessionClientBase core)
        {
            // 给每个 core 单独装 forwarder：旧 core 被替换后会随它一起 GC 掉，
            // 不需要显式取消订阅。
            core.StateChanged += state => StateChanged?.Invoke(state);
            core.Faulted += ex => Faulted?.Invoke(ex);
        }

        /// <summary>
        ///     首次连接超时默认值。游戏客户端用 30s 一般够覆盖正常握手 + bootstrap
        ///     的时间预算；想关掉超时就传 <see cref="TimeSpan.Zero" />。
        /// </summary>
        public static readonly TimeSpan DefaultConnectTimeout =
            TimeSpan.FromSeconds(30);

        /// <summary>
        ///     首次重连延迟默认值（指数退避起点）。
        /// </summary>
        public static readonly TimeSpan DefaultReconnectDelay =
            TimeSpan.FromSeconds(1);

        /// <summary>
        ///     重连延迟上限默认值。指数退避会从
        ///     <see cref="DefaultReconnectDelay" /> 翻倍直到这个值封顶。
        /// </summary>
        public static readonly TimeSpan DefaultMaxReconnectDelay =
            TimeSpan.FromSeconds(30);

        /// <summary>
        ///     连续重试次数上限默认值。20 次按 1/2/4/8/16/30s 退避大约覆盖 8 分钟，
        ///     足以扛过常规热重启；想让玩家挂着等长维护就显式传 <c>0</c>（不限次）。
        /// </summary>
        public const int DefaultMaxReconnectAttempts = 20;

        /// <summary>
        ///     启动逻辑会话：连接 <paramref name="url" />、跑 <paramref name="bootstrap" />、
        ///     恢复所有存活订阅。await 返回时
        ///     <see cref="State" /> == <see cref="HeTuSessionState.Ready" />。
        /// </summary>
        /// <param name="url">河图服务端 URL，例如 <c>ws://127.0.0.1:2466/hetu/MyGame</c>。</param>
        /// <param name="authKey">可选预共享密钥（服务端启用 <c>--authkey</c> 时必填）。</param>
        /// <param name="bootstrap">每次握手成功后的初始化委托，典型用于登录。</param>
        /// <param name="reconnectDelay">
        ///     首次重连前等待时间（指数退避起点）。<c>null</c> 用
        ///     <see cref="DefaultReconnectDelay" />（1s）。
        /// </param>
        /// <param name="maxReconnectDelay">
        ///     退避上限。<c>null</c> 用 <see cref="DefaultMaxReconnectDelay" />（30s）。
        /// </param>
        /// <param name="maxReconnectAttempts">
        ///     连续失败到此数即进入 <see cref="HeTuSessionState.Faulted" /> 终态。
        ///     默认 <see cref="DefaultMaxReconnectAttempts" />（20）；
        ///     <c>0</c> = 不限次，配合 Faulted 事件做 UI 反馈即可。
        /// </param>
        /// <param name="connectTimeout">
        ///     本次 Connect 的整体超时（从 Connecting 到 Ready）。超时会自动 Close
        ///     并把 await 抛 <see cref="TimeoutException" />。<c>null</c> 用
        ///     <see cref="DefaultConnectTimeout" />（30s）；<see cref="TimeSpan.Zero" />
        ///     关闭超时，await 一直等到 Ready 或 Faulted。
        /// </param>
#if UNITY_6000_0_OR_NEWER
        public Awaitable Connect(
            string url,
            string authKey = null,
            Func<HeTuClient, Awaitable> bootstrap = null,
            TimeSpan? reconnectDelay = null,
            TimeSpan? maxReconnectDelay = null,
            int maxReconnectAttempts = DefaultMaxReconnectAttempts,
            TimeSpan? connectTimeout = null)
#else
        public UniTask Connect(
            string url,
            string authKey = null,
            Func<HeTuClient, UniTask> bootstrap = null,
            TimeSpan? reconnectDelay = null,
            TimeSpan? maxReconnectDelay = null,
            int maxReconnectAttempts = DefaultMaxReconnectAttempts,
            TimeSpan? connectTimeout = null)
#endif
        {
            if (_core != null
                && _core.State != HeTuSessionState.Stopped
                && _core.State != HeTuSessionState.Faulted)
                throw new InvalidOperationException(
                    "HeTuSessionClient 已经在运行中。请先 Close() 再调用 Connect。");

            var client = HeTuClient.Instance;
            _core = new HeTuSessionClientBase(
                () => new UnityHeTuSessionTransport(client, url, authKey),
                new UnityHeTuSessionScheduler(),
                bootstrap == null
                    ? null
                    : (_, succeed, fail) =>
                        RunBootstrapAsync(client, bootstrap, succeed, fail),
                reconnectDelay ?? DefaultReconnectDelay,
                maxReconnectDelay ?? DefaultMaxReconnectDelay,
                maxReconnectAttempts);
            WireFacadeEvents(_core);

            return ConnectCore(connectTimeout ?? DefaultConnectTimeout);
        }

        // 测试通过内部 ctor 注入预构建的 core，无需 url 配置；走这两个重载触发 Start。
        // 无参 = 不开 timeout，便于 fake transport 显式控制时序；TimeSpan 重载
        // 让 ScheduleConnectTimeout 跑在测试可控的时长上。
#if UNITY_6000_0_OR_NEWER
        internal Awaitable Connect()
#else
        internal UniTask Connect()
#endif
        {
            return ConnectCore(TimeSpan.Zero);
        }

#if UNITY_6000_0_OR_NEWER
        internal Awaitable Connect(TimeSpan connectTimeout)
#else
        internal UniTask Connect(TimeSpan connectTimeout)
#endif
        {
            return ConnectCore(connectTimeout);
        }

#if UNITY_6000_0_OR_NEWER
        private Awaitable ConnectCore(TimeSpan connectTimeout)
#else
        private UniTask ConnectCore(TimeSpan connectTimeout)
#endif
        {
            if (_core.State == HeTuSessionState.Ready)
                return CompletedAwaitable();

            var tcs = NewCompletionSource<bool>();
            // 最近一次 Faulted 事件携带的异常；用于把"重试用尽"那条 await 抛出去时
            // 能带上真实原因（StateChanged 信号本身不带 fault）。
            Exception capturedFault = null;
            void OnReady() => tcs.TrySetResult(true);
            void OnFaulted(Exception ex) => capturedFault = ex;
            void OnStateChanged(HeTuSessionState state)
            {
                if (state == HeTuSessionState.Stopped)
                    tcs.TrySetCanceled();
                else if (state == HeTuSessionState.Faulted)
                    tcs.TrySetException(capturedFault ?? new InvalidOperationException(
                        "Session faulted: reconnect attempts exhausted."));
            }

            _core.Ready += OnReady;
            _core.Faulted += OnFaulted;
            _core.StateChanged += OnStateChanged;
            _core.Start();

            if (connectTimeout > TimeSpan.Zero)
            {
                var coreAtSchedule = _core;
                ScheduleConnectTimeout(connectTimeout, () =>
                    HandleConnectTimeout(
                        coreAtSchedule, OnStateChanged, tcs, connectTimeout));
            }

            return AwaitConnectAsync(tcs, OnReady, OnStateChanged, OnFaulted);
        }

#if UNITY_6000_0_OR_NEWER
        private void HandleConnectTimeout(
            HeTuSessionClientBase coreAtSchedule,
            Action<HeTuSessionState> onStateChanged,
            AwaitableCompletionSource<bool> tcs,
            TimeSpan timeout)
#else
        private void HandleConnectTimeout(
            HeTuSessionClientBase coreAtSchedule,
            Action<HeTuSessionState> onStateChanged,
            UniTaskCompletionSource<bool> tcs,
            TimeSpan timeout)
#endif
        {
            // 旧定时器：用户已经走完一轮 Connect/Close/Connect 又起了新 core，
            // 这个 timer 不应该误伤新 session。
            if (_core != coreAtSchedule) return;
            // 已经 Ready：timer 迟到了，session 已经成功，不能误关。
            if (_core.State == HeTuSessionState.Ready) return;
            // 已经 Stopped / Faulted：tcs 早被对应 OnStateChanged 分支占走了；
            // 下面的 Close + TrySetException 都是 no-op，让它过去也没副作用。

            // 关键顺序：Unity 的 AwaitableCompletionSource 在 TrySet 内部同步唤醒
            // awaiter；若顺序是 (TrySetException → Close)，test 在 Close 之前就拿到
            // 控制权看到 State 还是 Connecting。
            // 反过来 (Close → TrySetException) 时 Close 触发的 Stopped 又会被
            // OnStateChanged 抢成 Canceled。所以先把 OnStateChanged 摘了再 Close。
            _core.StateChanged -= onStateChanged;
            _core.Close();
            tcs.TrySetException(new TimeoutException(
                $"Session did not become Ready within " +
                $"{timeout.TotalSeconds:F0}s."));
        }

#if UNITY_6000_0_OR_NEWER
        private async Awaitable AwaitConnectAsync(
            AwaitableCompletionSource<bool> tcs,
            Action onReady,
            Action<HeTuSessionState> onStateChanged,
            Action<Exception> onFaulted)
#else
        private async UniTask AwaitConnectAsync(
            UniTaskCompletionSource<bool> tcs,
            Action onReady,
            Action<HeTuSessionState> onStateChanged,
            Action<Exception> onFaulted)
#endif
        {
            try
            {
                await AwaitFrom(tcs);
            }
            finally
            {
                _core.Ready -= onReady;
                _core.StateChanged -= onStateChanged;
                _core.Faulted -= onFaulted;
            }
        }

#if UNITY_6000_0_OR_NEWER
        private void ScheduleConnectTimeout(TimeSpan timeout, Action onTimeout) =>
            _ = RunConnectTimeoutAsync(timeout, onTimeout);

        private async Awaitable RunConnectTimeoutAsync(
            TimeSpan timeout, Action onTimeout)
        {
            await Awaitable.WaitForSecondsAsync((float)timeout.TotalSeconds);
            onTimeout();
        }
#else
        private void ScheduleConnectTimeout(TimeSpan timeout, Action onTimeout) =>
            RunConnectTimeoutAsync(timeout, onTimeout).Forget();

        private async UniTask RunConnectTimeoutAsync(
            TimeSpan timeout, Action onTimeout)
        {
            await UniTask.Delay(timeout);
            onTimeout();
        }
#endif

#if UNITY_6000_0_OR_NEWER
        public Awaitable<JsonObject> CallSystem(string systemName,
            params object[] args)
#else
        public UniTask<JsonObject> CallSystem(string systemName,
            params object[] args)
#endif
        {
            var tcs = NewCompletionSource<JsonObject>();
            _core.CallSystem(
                systemName,
                args,
                response => tcs.TrySetResult(response),
                ex => tcs.TrySetException(ex));
            return AwaitFrom(tcs);
        }

#if UNITY_6000_0_OR_NEWER
        public Awaitable<RowSubscription<T>> WatchRow<T>(
#else
        public UniTask<RowSubscription<T>> WatchRow<T>(
#endif
            string index, object value, string componentName = null)
            where T : IBaseComponent
        {
            var tcs = NewCompletionSource<RowSubscription<T>>();
            _core.WatchRow<T>(
                index,
                value,
                componentName,
                sub => tcs.TrySetResult(sub),
                ex => tcs.TrySetException(ex));
            return AwaitFrom(tcs);
        }

#if UNITY_6000_0_OR_NEWER
        public Awaitable<IndexSubscription<T>> WatchRange<T>(
#else
        public UniTask<IndexSubscription<T>> WatchRange<T>(
#endif
            string index, object left, object right, int limit,
            bool desc = false, bool force = true, string componentName = null)
            where T : IBaseComponent
        {
            var tcs = NewCompletionSource<IndexSubscription<T>>();
            _core.WatchRange<T>(
                index,
                left,
                right,
                limit,
                componentName,
                sub => tcs.TrySetResult(sub),
                ex => tcs.TrySetException(ex),
                desc,
                force);
            return AwaitFrom(tcs);
        }

        /// <summary>
        ///     关闭当前会话，取消所有进行中的调用与订阅。Close 后再次调用 Connect
        ///     会建立全新的会话。
        /// </summary>
        public void Close() => _core?.Close();

        /// <summary>
        ///     等价于 <see cref="Close" />。单件实例本身不会被销毁。
        /// </summary>
        public void Dispose() => _core?.Dispose();

#if UNITY_6000_0_OR_NEWER
        private static async Awaitable RunBootstrapAsync(
            HeTuClient client,
            Func<HeTuClient, Awaitable> bootstrap,
            Action succeed,
            Action<Exception> fail)
#else
        private static async UniTaskVoid RunBootstrapAsync(
            HeTuClient client,
            Func<HeTuClient, UniTask> bootstrap,
            Action succeed,
            Action<Exception> fail)
#endif
        {
            try
            {
                await bootstrap(client);
                succeed();
            }
            catch (Exception ex)
            {
                fail(ex);
            }
        }

#if UNITY_6000_0_OR_NEWER
        private static async Awaitable CompletedAwaitable()
        {
        }
#else
        private static UniTask CompletedAwaitable() => UniTask.CompletedTask;
#endif
    }

    internal sealed class UnityHeTuSessionTransport : IHeTuSessionTransport
    {
        private readonly string _authKey;
        private readonly HeTuClient _client;
        private readonly string _url;

        public UnityHeTuSessionTransport(
            HeTuClient client,
            string url,
            string authKey)
        {
            _client = client;
            _url = url;
            _authKey = authKey;
            _client.OnConnected += HandleConnected;
            _client.OnClosed += HandleClosed;
        }

        public bool IsConnected => _client.IsConnected;

        public event Action Connected;
        public event Action<string> Closed;

        public void Connect()
        {
#if UNITY_6000_0_OR_NEWER
            _ = ConnectAsync();
#else
            ConnectAsync().Forget();
#endif
        }

        public void Close() => _client.Close();

        public void CallSystem(string systemName, object[] args,
            Action<JsonObject, bool> onResponse) =>
            _client.CallSystemSync(systemName, args, onResponse);

        public void WatchRow<T>(
            string index,
            object value,
            Action<RowSubscription<T>, bool, Exception> onResponse,
            string componentName = null,
            RowSubscription<T> reusable = null)
            where T : IBaseComponent =>
            _client.WatchRowSync(index, value, onResponse, componentName, reusable);

        public void WatchRange<T>(
            string index,
            object left,
            object right,
            int limit,
            Action<IndexSubscription<T>, bool, Exception> onResponse,
            bool desc = false,
            bool force = true,
            string componentName = null,
            IndexSubscription<T> reusable = null)
            where T : IBaseComponent =>
            _client.WatchRangeSync(index, left, right, limit, onResponse,
                desc, force, componentName, reusable);

        public void Dispose()
        {
            _client.OnConnected -= HandleConnected;
            _client.OnClosed -= HandleClosed;
        }

        private void HandleConnected() => Connected?.Invoke();

        private void HandleClosed(string reason) => Closed?.Invoke(reason);

#if UNITY_6000_0_OR_NEWER
        private async Awaitable ConnectAsync()
#else
        private async UniTask ConnectAsync()
#endif
        {
            try
            {
                await _client.Connect(_url, _authKey);
            }
            catch (Exception ex)
            {
                Closed?.Invoke(ex.Message);
            }
        }
    }

    internal sealed class UnityHeTuSessionScheduler : IHeTuSessionScheduler
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
            if (!scheduled.IsDisposed)
                action();
        }

        private sealed class ScheduledAction : IDisposable
        {
            public bool IsDisposed { get; private set; }

            public void Dispose() => IsDisposed = true;
        }
    }
}
