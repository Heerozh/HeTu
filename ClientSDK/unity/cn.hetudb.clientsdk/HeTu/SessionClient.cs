// <copyright>
// Copyright 2026, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity逻辑会话层</summary>


#if !UNITY_6000_0_OR_NEWER
using Cysharp.Threading.Tasks;
#endif
using System;
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

        /// <summary>
        ///     单件实例。
        /// </summary>
        public static HeTuSessionClient Instance { get; } = new();

        private HeTuSessionClientBase _core;

        private HeTuSessionClient()
        {
        }

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
        ///     **仅作用于"曾经 Ready 之后的重连"**。首次 Ready 之前的任何 close /
        ///     bootstrap 异常都直接进 <see cref="HeTuSessionState.Faulted" /> 终态
        ///     （重试同一份凭据 / URL 无意义）。Ready 之后再断线，会按本值循环重连，
        ///     连续失败到此数才转 Faulted。默认
        ///     <see cref="DefaultMaxReconnectAttempts" />（20）；
        ///     <c>0</c> = post-Ready 不限次重连。
        /// </param>
        /// <param name="connectTimeout">
        ///     本次 Connect 的整体超时（从 Connecting 到 Ready）。超时会自动 Close
        ///     并把 await 抛 <see cref="TimeoutException" />（InnerException 携带
        ///     最近一次 Faulted 事件的异常）。<c>null</c> 用
        ///     <see cref="DefaultConnectTimeout" />（30s）；<see cref="TimeSpan.Zero" />
        ///     关闭超时，await 等到 Ready 或 Faulted 为止。
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
                    : _ => RunBootstrapAsync(client, bootstrap),
                reconnectDelay ?? DefaultReconnectDelay,
                maxReconnectDelay ?? DefaultMaxReconnectDelay,
                maxReconnectAttempts);
            WireFacadeEvents(_core);

            return ConnectCore(connectTimeout ?? DefaultConnectTimeout);
        }

        // 测试通过内部 ctor 注入预构建的 core，无需 url 配置；走这两个重载触发 Start。
        // 无参 = 不开 timeout，便于 fake transport 显式控制时序；TimeSpan 重载
        // 让超时跑在测试可控的时长上。
#if UNITY_6000_0_OR_NEWER
        internal Awaitable Connect() => ConnectCore(TimeSpan.Zero);

        internal Awaitable Connect(TimeSpan connectTimeout) =>
            ConnectCore(connectTimeout);

        private Awaitable ConnectCore(TimeSpan connectTimeout)
#else
        internal UniTask Connect() => ConnectCore(TimeSpan.Zero);
        internal UniTask Connect(TimeSpan connectTimeout) =>
            ConnectCore(connectTimeout);

        private UniTask ConnectCore(TimeSpan connectTimeout)
#endif
        {
            _core.Start();
            return FutureToAwaitable(_core.WaitForReady(connectTimeout));
        }

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
        ///     替换"下次重连"用的 bootstrap。典型场景:启动时
        ///     <see cref="Connect" /> 不传 bootstrap(匿名连接),用户成功登录后
        ///     再调用本方法装上"重连时自动重登"逻辑——SDK 在 Ready 之后断线会
        ///     自动重连,届时跑这里设置的委托。<paramref name="bootstrap" /> 传
        ///     <c>null</c> 表示清空(例如用户退登)。
        ///     <para>
        ///         必须先 <see cref="Connect" /> 过一次,否则抛
        ///         <see cref="InvalidOperationException" />。
        ///     </para>
        /// </summary>
#if UNITY_6000_0_OR_NEWER
        public void SetBootstrap(Func<HeTuClient, Awaitable> bootstrap)
#else
        public void SetBootstrap(Func<HeTuClient, UniTask> bootstrap)
#endif
        {
            if (_core == null)
                throw new InvalidOperationException(
                    "HeTuSessionClient 尚未 Connect, 不能 SetBootstrap。");

            if (bootstrap == null)
            {
                _core.SetBootstrap(null);
                return;
            }

            var client = HeTuClient.Instance;
            _core.SetBootstrap(_ => RunBootstrapAsync(client, bootstrap));
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
        private static Future RunBootstrapAsync(
            HeTuClient client,
            Func<HeTuClient, Awaitable> bootstrap)
        {
            var p = new Promise();
            _ = BridgeBootstrap(client, bootstrap, p);
            return p.Future;
        }

        private static async Awaitable BridgeBootstrap(
            HeTuClient client,
            Func<HeTuClient, Awaitable> bootstrap,
            Promise p)
        {
            try
            {
                await bootstrap(client);
                p.TryComplete();
            }
            catch (Exception ex)
            {
                Debug.LogError("Bootstrap code throw a error, connection closed: " + ex);
                p.TryFail(ex);
            }
        }
#else
        private static Future RunBootstrapAsync(
            HeTuClient client,
            Func<HeTuClient, UniTask> bootstrap)
        {
            var p = new Promise();
            BridgeBootstrap(client, bootstrap, p).Forget();
            return p.Future;
        }

        private static async UniTaskVoid BridgeBootstrap(
            HeTuClient client,
            Func<HeTuClient, UniTask> bootstrap,
            Promise p)
        {
            try
            {
                await bootstrap(client);
                p.TryComplete();
            }
            catch (Exception ex)
            {
                Debug.LogError("Bootstrap code throw a error, connection closed: " + ex);
                p.TryFail(ex);
            }
        }
#endif

        // Future → awaitable 单点桥接：已完成的 Future 直接构造已结算的 TCS；
        // 未完成的 Future 用 Then/Catch 把结果搬运过去。把 Connect 那一堆 TCS+三个
        // event handler+timeout 的拼装收成"一行 await"。
#if UNITY_6000_0_OR_NEWER
        private static Awaitable FutureToAwaitable(Future f)
        {
            var tcs = new AwaitableCompletionSource();
            f.Then(() => tcs.TrySetResult())
                .Catch(ex => tcs.TrySetException(ex));
            return tcs.Awaitable;
        }
#else
        private static UniTask FutureToAwaitable(Future f)
        {
            var tcs = new UniTaskCompletionSource();
            f.Then(() => tcs.TrySetResult())
             .Catch(ex => tcs.TrySetException(ex));
            return tcs.Task;
        }
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
