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

        internal HeTuSessionClient(HeTuSessionClientBase core) => _core = core;

        /// <summary>
        ///     当前会话状态；尚未 Connect 时为
        ///     <see cref="HeTuSessionState.Stopped" />。
        /// </summary>
        public HeTuSessionState State =>
            _core?.State ?? HeTuSessionState.Stopped;

        /// <summary>
        ///     启动逻辑会话：连接 <paramref name="url" />、跑 <paramref name="bootstrap" />、
        ///     恢复所有存活订阅。await 返回时
        ///     <see cref="State" /> == <see cref="HeTuSessionState.Ready" />。
        /// </summary>
        /// <param name="url">河图服务端 URL，例如 <c>ws://127.0.0.1:2466/hetu/MyGame</c>。</param>
        /// <param name="authKey">可选预共享密钥（服务端启用 <c>--authkey</c> 时必填）。</param>
        /// <param name="bootstrap">每次握手成功后的初始化委托，典型用于登录。</param>
        /// <param name="reconnectDelay">重连前等待时间，默认 1 秒。</param>
#if UNITY_6000_0_OR_NEWER
        public Awaitable Connect(
            string url,
            string authKey = null,
            Func<HeTuClient, Awaitable> bootstrap = null,
            TimeSpan? reconnectDelay = null)
#else
        public UniTask Connect(
            string url,
            string authKey = null,
            Func<HeTuClient, UniTask> bootstrap = null,
            TimeSpan? reconnectDelay = null)
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
                reconnectDelay);

            return ConnectCore();
        }

        // 测试通过内部 ctor 注入预构建的 core，无需 url 配置；走这个重载触发 Start。
#if UNITY_6000_0_OR_NEWER
        internal Awaitable Connect()
#else
        internal UniTask Connect()
#endif
        {
            return ConnectCore();
        }

#if UNITY_6000_0_OR_NEWER
        private Awaitable ConnectCore()
#else
        private UniTask ConnectCore()
#endif
        {
            if (_core.State == HeTuSessionState.Ready)
                return CompletedAwaitable();

            var tcs = NewCompletionSource<bool>();
            void OnReady() => tcs.TrySetResult(true);
            void OnStateChanged(HeTuSessionState state)
            {
                if (state == HeTuSessionState.Stopped)
                    tcs.TrySetCanceled();
            }

            _core.Ready += OnReady;
            _core.StateChanged += OnStateChanged;
            _core.Start();
            return AwaitConnectAsync(tcs, OnReady, OnStateChanged);
        }

#if UNITY_6000_0_OR_NEWER
        private async Awaitable AwaitConnectAsync(
            AwaitableCompletionSource<bool> tcs,
            Action onReady,
            Action<HeTuSessionState> onStateChanged)
#else
        private async UniTask AwaitConnectAsync(
            UniTaskCompletionSource<bool> tcs,
            Action onReady,
            Action<HeTuSessionState> onStateChanged)
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
            }
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
