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
    ///     自动管理重连、bootstrap 与订阅恢复的 Unity 逻辑会话层。
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

        private readonly HeTuSessionClientBase _core;

#if UNITY_6000_0_OR_NEWER
        public HeTuSessionClient(
            string url,
            string authKey = null,
            Func<HeTuClient, Awaitable> bootstrap = null,
            TimeSpan? reconnectDelay = null)
#else
        public HeTuSessionClient(
            string url,
            string authKey = null,
            Func<HeTuClient, UniTask> bootstrap = null,
            TimeSpan? reconnectDelay = null)
#endif
        {
            var client = HeTuClient.Instance;
            _core = new HeTuSessionClientBase(
                () => new UnityHeTuSessionTransport(client, url, authKey),
                new UnityHeTuSessionScheduler(),
                bootstrap == null
                    ? null
                    : (_, succeed, fail) =>
                        RunBootstrapAsync(client, bootstrap, succeed, fail),
                reconnectDelay);
        }

        internal HeTuSessionClient(HeTuSessionClientBase core) => _core = core;

        public HeTuSessionState State => _core.State;

#if UNITY_6000_0_OR_NEWER
        public Awaitable Connect()
#else
        public UniTask Connect()
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

        public void Close() => _core.Close();

        public void Dispose() => _core.Dispose();

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
