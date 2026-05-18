// <copyright>
// Copyright 2026, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的通用逻辑会话层</summary>


using System;
using System.Collections.Generic;
using System.Linq;

namespace HeTu
{
    /// <summary>
    ///     逻辑会话状态。
    /// </summary>
    public enum HeTuSessionState
    {
        Stopped,
        Connecting,
        Bootstrapping,
        RestoringSubscriptions,
        Ready,
        Reconnecting,
        Faulted
    }

    /// <summary>
    ///     调用已发送，但在收到响应前连接丢失，结果无法判定。
    /// </summary>
    public sealed class CallOutcomeUnknownException : Exception
    {
        public CallOutcomeUnknownException(string systemName) :
            base($"System call '{systemName}' was sent, but the connection closed before a response was received.")
        {
        }
    }

    internal interface IHeTuSessionTransport : IDisposable
    {
        event Action Connected;
        event Action<string> Closed;

        bool IsConnected { get; }

        void Connect();
        void Close();

        void CallSystem(
            string systemName,
            object[] args,
            Action<JsonObject, bool> onResponse);

        void WatchRow<T>(
            string index,
            object value,
            Action<RowSubscription<T>, bool, Exception> onResponse,
            string componentName = null,
            RowSubscription<T> reusable = null)
            where T : IBaseComponent;

        void WatchRange<T>(
            string index,
            object left,
            object right,
            int limit,
            Action<IndexSubscription<T>, bool, Exception> onResponse,
            bool desc = false,
            bool force = true,
            string componentName = null,
            IndexSubscription<T> reusable = null)
            where T : IBaseComponent;
    }

    internal interface IHeTuSessionScheduler
    {
        IDisposable Schedule(TimeSpan delay, Action action);
    }

    internal delegate Future HeTuSessionBootstrap(IHeTuSessionTransport transport);

    /// <summary>
    ///     不依赖任何 awaitable 类型的逻辑会话核心。
    /// </summary>
    internal sealed class HeTuSessionClientBase : IDisposable
    {
        private readonly HeTuSessionBootstrap _bootstrap;
        private readonly HashSet<PendingCall> _inFlightCalls = new();
        private readonly Queue<PendingCall> _pendingCalls = new();
        private readonly Dictionary<string, PendingWatch> _pendingWatchesByKey = new();
        private readonly List<PendingWatch> _pendingWatches = new();
        private readonly TimeSpan _reconnectDelay;
        private readonly TimeSpan _maxReconnectDelay;
        private readonly int _maxReconnectAttempts;
        private readonly IHeTuSessionScheduler _scheduler;
        private readonly Dictionary<string, BaseSubscription> _subscriptions = new();
        private readonly Func<IHeTuSessionTransport> _transportFactory;

        private Promise _activeStepPromise;
        private bool _closed;
        private int _consecutiveFailures;
        private TimeSpan _currentReconnectDelay;
        private bool _hasBeenReady;
        private Exception _lastFault;
        private IDisposable _scheduledReconnect;
        private IHeTuSessionTransport _transport;
        private bool _transportClosedItself;

        public HeTuSessionClientBase(
            Func<IHeTuSessionTransport> transportFactory,
            IHeTuSessionScheduler scheduler,
            HeTuSessionBootstrap bootstrap = null,
            TimeSpan? reconnectDelay = null,
            TimeSpan? maxReconnectDelay = null,
            int maxReconnectAttempts = 0)
        {
            _transportFactory = transportFactory;
            _scheduler = scheduler;
            _bootstrap = bootstrap;
            _reconnectDelay = reconnectDelay ?? TimeSpan.FromSeconds(1);
            // 未指定 max 时退化为固定延迟（不做指数退避），保持调用方旧行为。
            _maxReconnectDelay = maxReconnectDelay ?? _reconnectDelay;
            // 0 = 无限重试（旧行为）；正数 = 连续失败到此数即进入 Faulted 终态。
            _maxReconnectAttempts = maxReconnectAttempts;
            _currentReconnectDelay = _reconnectDelay;
            // 自订阅以追踪最近一次 Faulted 异常——WaitForReady / 后续重构都依赖
            // _lastFault 在 SetState(Faulted) 之前已被赋值。委托在构造时第一个挂上，
            // 所以一定排在用户后挂的 Faulted handler 前面，事件触发顺序对齐。
            Faulted += ex => _lastFault = ex;
        }

        public HeTuSessionState State { get; private set; } =
            HeTuSessionState.Stopped;

        /// <summary>
        ///     本 core 是否曾经达到过 <see cref="HeTuSessionState.Ready" />；
        ///     用来区分 "首次连接没成功" 和 "进游戏后断线"——前者不重试，后者才重试。
        /// </summary>
        internal bool HasBeenReady => _hasBeenReady;

        public event Action Ready;
        public event Action<HeTuSessionState> StateChanged;
        public event Action<Exception> Faulted;

        public void Start()
        {
            if (_closed || State != HeTuSessionState.Stopped)
                return;

            RunSessionLifecycle();
        }

        public void Close()
        {
            if (_closed)
                return;

            _closed = true;
            _scheduledReconnect?.Dispose();
            _scheduledReconnect = null;

            // 显式声明为 Exception，避免 SafeInvokeUserCallback<T>(Action<T>, T)
            // 在 Action<Exception> + OperationCanceledException 组合下的类型推断歧义。
            Exception canceled = new OperationCanceledException("Session closed.");
            while (_pendingCalls.Count > 0)
                SafeInvokeUserCallback(
                    _pendingCalls.Dequeue().OnFailed, canceled);

            foreach (var pending in _inFlightCalls.ToArray())
                SafeInvokeUserCallback(pending.OnFailed, canceled);
            _inFlightCalls.Clear();

            foreach (var pending in _pendingWatches.ToArray())
            {
                RemovePendingWatch(pending);
                pending.Fail(canceled);
            }

            foreach (var subscription in _subscriptions.Values.ToArray())
                SafeInvokeUserCallback(subscription.Dispose);

            _subscriptions.Clear();
            CleanupTransport(closeTransport: true);
            SetState(HeTuSessionState.Stopped);
        }

        public void Dispose() => Close();

        /// <summary>
        ///     返回一个在 Session 进入 <see cref="HeTuSessionState.Ready" /> 后完成
        ///     的 Future。失败语义：
        ///     <list type="bullet">
        ///         <item>已被 <see cref="Close" /> → <see cref="ObjectDisposedException" />；</item>
        ///         <item>已进入 Faulted → 携带最近一次 Faulted 事件的异常；</item>
        ///         <item>等待过程中 Stopped → <see cref="OperationCanceledException" />；</item>
        ///         <item>等待过程中 Faulted → 同样携带 fault；</item>
        ///         <item>超时 → 主动 <see cref="Close" /> 并返回 <see cref="TimeoutException" />，
        ///             InnerException 携带最近一次 fault。</item>
        ///     </list>
        ///     timeout=<see cref="TimeSpan.Zero" /> 表示不开超时（一直等到 Ready
        ///     或显式 Close）。timeout 只在"首次 Ready 之前"有效：
        ///     <see cref="HasBeenReady" /> 之后 timer 命中会被忽略。
        /// </summary>
        public Future WaitForReady(TimeSpan timeout)
        {
            // Faulted 在内部会同时置 _closed=true，先判 Faulted 才能拿到真异常；
            // 否则 ObjectDisposed 会盖掉 fault。
            if (State == HeTuSessionState.Ready)
                return Future.Completed;
            if (State == HeTuSessionState.Faulted)
                return Future.Failed(_lastFault ?? new InvalidOperationException(
                    "Session faulted: reconnect attempts exhausted."));
            if (_closed)
                return Future.Failed(new ObjectDisposedException(
                    nameof(HeTuSessionClientBase)));

            var p = new Promise();
            Action onReady = null;
            Action<HeTuSessionState> onState = null;
            IDisposable timer = null;

            void Unwire()
            {
                if (onReady != null) Ready -= onReady;
                if (onState != null) StateChanged -= onState;
                timer?.Dispose();
            }

            onReady = () =>
            {
                Unwire();
                p.TryComplete();
            };
            onState = st =>
            {
                if (st == HeTuSessionState.Faulted)
                {
                    Unwire();
                    p.TryFail(_lastFault ?? new InvalidOperationException(
                        "Session faulted: reconnect attempts exhausted."));
                }
                else if (st == HeTuSessionState.Stopped)
                {
                    Unwire();
                    p.TryFail(new OperationCanceledException("Session closed."));
                }
            };

            Ready += onReady;
            StateChanged += onState;

            if (timeout > TimeSpan.Zero)
            {
                timer = _scheduler.Schedule(timeout, () =>
                {
                    // 曾经 Ready：connect 早就成功返回了，后续状态变化跟
                    // connectTimeout 无关——不能让一个 30s 的初始超时误关游戏中的会话。
                    if (HasBeenReady) return;
                    // 先把 onState 摘掉再 Close：否则 Close 触发的 Stopped 会被 onState
                    // 抢成 OperationCanceled，覆盖掉真正的 TimeoutException。
                    Unwire();
                    Close();
                    var msg = _lastFault != null
                        ? $"Connect timed out after {timeout.TotalSeconds:F0}s: "
                          + $"{_lastFault.Message}"
                        : $"Connect timed out after {timeout.TotalSeconds:F0}s.";
                    p.TryFail(new TimeoutException(msg, _lastFault));
                });
            }

            return p.Future;
        }

        public void CallSystem(
            string systemName,
            object[] args,
            Action<JsonObject> onCompleted,
            Action<Exception> onFailed)
        {
            ThrowIfClosed();
            var pending = new PendingCall(systemName, args, onCompleted, onFailed);
            if (State == HeTuSessionState.Ready && _transport != null)
            {
                DispatchCall(pending);
                return;
            }

            _pendingCalls.Enqueue(pending);
        }

        public void WatchRow<T>(
            string index,
            object value,
            string componentName,
            Action<RowSubscription<T>> onCompleted,
            Action<Exception> onFailed)
            where T : IBaseComponent
        {
            ThrowIfClosed();
            componentName ??= typeof(T).Name;
            var knownSubId = index == HeTuClientBase.IndexId
                ? HeTuClientBase.MakeSubId(
                    componentName, HeTuClientBase.IndexId, value, null, 1, false)
                : null;

            if (knownSubId != null &&
                TryGetSubscription<RowSubscription<T>>(knownSubId, out var existing))
            {
                onCompleted(existing);
                return;
            }

            if (knownSubId != null &&
                _pendingWatchesByKey.TryGetValue(knownSubId, out var pending))
            {
                if (pending is PendingWatch<RowSubscription<T>> typed)
                {
                    typed.AddWaiter(onCompleted, onFailed);
                    return;
                }

                throw new InvalidCastException(
                    $"Subscription '{knownSubId}' already exists with type {pending.DataType}.");
            }

            var watch = new PendingWatch<RowSubscription<T>>(
                knownSubId,
                typeof(T),
                (tx, promise) => tx.WatchRow<T>(
                    index,
                    value,
                    (sub, canceled, ex) =>
                    {
                        if (canceled) return;
                        if (ex != null) promise.TryFail(ex);
                        else promise.TryComplete(sub);
                    },
                    componentName),
                onCompleted,
                onFailed);
            AddPendingWatch(watch);
            DispatchPendingIfReady(watch);
        }

        public void WatchRange<T>(
            string index,
            object left,
            object right,
            int limit,
            string componentName,
            Action<IndexSubscription<T>> onCompleted,
            Action<Exception> onFailed,
            bool desc = false,
            bool force = true)
            where T : IBaseComponent
        {
            ThrowIfClosed();
            componentName ??= typeof(T).Name;
            var subId = HeTuClientBase.MakeSubId(
                componentName, index, left, right, limit, desc);

            if (TryGetSubscription<IndexSubscription<T>>(subId, out var existing))
            {
                onCompleted(existing);
                return;
            }

            if (_pendingWatchesByKey.TryGetValue(subId, out var pending))
            {
                if (pending is PendingWatch<IndexSubscription<T>> typed)
                {
                    typed.AddWaiter(onCompleted, onFailed);
                    return;
                }

                throw new InvalidCastException(
                    $"Subscription '{subId}' already exists with type {pending.DataType}.");
            }

            var watch = new PendingWatch<IndexSubscription<T>>(
                subId,
                typeof(T),
                (tx, promise) => tx.WatchRange<T>(
                    index,
                    left,
                    right,
                    limit,
                    (sub, canceled, ex) =>
                    {
                        if (canceled) return;
                        if (ex != null) promise.TryFail(ex);
                        else promise.TryComplete(sub);
                    },
                    desc,
                    force,
                    componentName),
                onCompleted,
                onFailed);
            AddPendingWatch(watch);
            DispatchPendingIfReady(watch);
        }

        // ---------- 状态机：单条 Future 链 ----------
        // Start → ConnectTransport → RunBootstrap → RestoreAllSubscriptions → MarkReady
        //                                                                    ↘ Catch(HandleSessionFailure)
        // 每一步把自己的 Promise 注册为 _activeStepPromise；OnTransportClosed
        // 通过它把"在飞那一步"失败掉，让失败统一从 .Catch 一处出口走。
        // MarkReady 之后 _activeStepPromise 设回 null，断线走 HandleSessionFailure
        // 直连路径。
        private void RunSessionLifecycle()
        {
            if (_closed) return;
            ConnectTransport()
                .Then(RunBootstrap)
                .Then(RestoreAllSubscriptions)
                .Then(MarkReady)
                .Catch(HandleSessionFailure);
        }

        private Future ConnectTransport()
        {
            if (_closed) return Future.Failed(
                new ObjectDisposedException(nameof(HeTuSessionClientBase)));

            CleanupTransport(closeTransport: false);
            _transport = _transportFactory();
            _transportClosedItself = false;
            var p = new Promise();
            _activeStepPromise = p;
            _transport.Connected += () => p.TryComplete();
            _transport.Closed += OnTransportClosed;
            SetState(HeTuSessionState.Connecting);
            _transport.Connect();
            return p.Future;
        }

        private Future RunBootstrap()
        {
            if (_closed) return Future.Completed;
            SetState(HeTuSessionState.Bootstrapping);
            if (_bootstrap == null)
            {
                _activeStepPromise = null;
                return Future.Completed;
            }

            var p = new Promise();
            _activeStepPromise = p;
            Future bootstrapFuture;
            try
            {
                bootstrapFuture = _bootstrap(_transport);
            }
            catch (Exception ex)
            {
                p.TryFail(ex);
                return p.Future;
            }

            if (bootstrapFuture == null)
            {
                p.TryComplete();
                return p.Future;
            }

            bootstrapFuture
                .Then(() => p.TryComplete())
                .Catch(ex => p.TryFail(ex));
            return p.Future;
        }

        private Future RestoreAllSubscriptions()
        {
            if (_closed) return Future.Completed;
            SetState(HeTuSessionState.RestoringSubscriptions);

            var p = new Promise();
            _activeStepPromise = p;
            // 若 transport.Restore 的回调同步触发，按订阅数线性涨栈在递归实现里会
            // StackOverflow。fold 成 Future 链后 continuation 在堆里而非栈里串联，
            // 栈深恒定。
            var subs = _subscriptions.Values.ToArray();
            var head = Future.Completed;
            foreach (var sub in subs)
                head = head.Then(() => RestoreOne(sub));
            head
                .Then(() => p.TryComplete())
                .Catch(ex => p.TryFail(ex));
            return p.Future;
        }

        private Future RestoreOne(BaseSubscription subscription)
        {
            if (_closed) return Future.Completed;
            if (subscription.IsDisposed) return Future.Completed;

            var p = new Promise();
            ((IRestorableSubscription)subscription).Restore(
                _transport,
                isActive =>
                {
                    if (!isActive)
                        UnregisterSubscription(subscription);
                    p.TryComplete();
                },
                ex => p.TryFail(ex));
            return p.Future;
        }

        private Future MarkReady()
        {
            if (_closed) return Future.Completed;
            // 清掉 _activeStepPromise——之后再来的 Closed 事件就走 HandleSessionFailure
            // 直连路径，不再尝试失败一个已完成的 step。
            _activeStepPromise = null;
            BecomeReady();
            return Future.Completed;
        }

        private void OnTransportClosed(string reason)
        {
            if (_closed) return;

            _transportClosedItself = true;
            var fault = new InvalidOperationException(
                $"Connection closed: {reason ?? "(unknown)"}");

            // 链在飞：把当前 step 失败掉，由 .Catch(HandleSessionFailure) 统一处理。
            if (_activeStepPromise != null && _activeStepPromise.TryFail(fault))
                return;

            // 没有活动 step（已 Ready）：直走 HandleSessionFailure。
            HandleSessionFailure(fault);
        }

        private void BecomeReady()
        {
            _scheduledReconnect?.Dispose();
            _scheduledReconnect = null;
            // 成功 Ready 之后重置退避和失败计数器：下一次断线从初始延迟重新开始，
            // maxReconnectAttempts 也按 "本次会话期内连续失败" 重新计数。
            _currentReconnectDelay = _reconnectDelay;
            _consecutiveFailures = 0;
            // 标记 "本 core 至少 Ready 过一次"——之后的 close / restore 失败才走 reconnect。
            _hasBeenReady = true;
            SetState(HeTuSessionState.Ready);
            // 先把排队请求派发完再触发 Ready，避免用户在 Ready 回调里新发的
            // CallSystem/WatchRow 抢在排队请求之前破坏 FIFO 顺序。
            FlushPendingCalls();
            DispatchPendingWatches();
            SafeInvokeUserCallback(Ready);
        }

        private void FlushPendingCalls()
        {
            while (_pendingCalls.Count > 0)
                DispatchCall(_pendingCalls.Dequeue());
        }

        private void DispatchCall(PendingCall pending)
        {
            _inFlightCalls.Add(pending);
            _transport.CallSystem(
                pending.SystemName,
                pending.Args,
                (response, canceled) =>
                {
                    if (!_inFlightCalls.Remove(pending))
                        return;

                    if (canceled)
                    {
                        pending.OnFailed(new OperationCanceledException(
                            $"System call '{pending.SystemName}' was canceled."));
                    }
                    else
                    {
                        pending.OnCompleted(response);
                    }
                });
        }

        private void DispatchPendingWatches()
        {
            foreach (var pending in _pendingWatches.ToArray())
                DispatchPendingIfReady(pending);
        }

        private void DispatchPendingIfReady(PendingWatch pending)
        {
            if (State != HeTuSessionState.Ready || _transport == null ||
                pending.IsInFlight)
            {
                return;
            }

            pending.Dispatch(this, _transport);
        }

        // 统一失败入口。来源：(a) 链的 .Catch；(b) OnTransportClosed 在 Ready 后
        // 直走的路径。两者都已经把 fault 准备好。closeTransport 由
        // _transportClosedItself 决定——对端自己关了就不用再发 Close 帧，
        // 否则 bootstrap/restore 抛出时 socket 还活着，必须主动关掉。
        private void HandleSessionFailure(Exception fault)
        {
            if (_closed) return;

            _activeStepPromise = null;
            var closeTransport = !_transportClosedItself;
            _transportClosedItself = false;
            MarkConnectionLost(closeTransport);
            SafeInvokeUserCallback(Faulted, fault);
            if (_closed) return;
            // 首次 Ready 前的失败 = 凭据/配置/URL 错，重同样一份没意义。
            if (!_hasBeenReady)
            {
                EnterFaulted(fault);
                return;
            }
            if (ExhaustedRetries())
            {
                EnterFaulted(fault);
                return;
            }
            ScheduleReconnect();
        }

        private bool ExhaustedRetries()
        {
            _consecutiveFailures++;
            return _maxReconnectAttempts > 0 &&
                   _consecutiveFailures >= _maxReconnectAttempts;
        }

        private void EnterFaulted(Exception fault)
        {
            if (_closed)
                return;

            _closed = true;
            _scheduledReconnect?.Dispose();
            _scheduledReconnect = null;

            // 与 Close 一致地清理 pending 与现存订阅；只是终态用 Faulted 区分
            // "用户主动关闭" 与 "重试用尽自动退出"。
            while (_pendingCalls.Count > 0)
                SafeInvokeUserCallback(_pendingCalls.Dequeue().OnFailed, fault);

            foreach (var pending in _inFlightCalls.ToArray())
                SafeInvokeUserCallback<Exception>(
                    pending.OnFailed,
                    new CallOutcomeUnknownException(pending.SystemName));
            _inFlightCalls.Clear();

            foreach (var pending in _pendingWatches.ToArray())
            {
                RemovePendingWatch(pending);
                pending.Fail(fault);
            }

            foreach (var subscription in _subscriptions.Values.ToArray())
                SafeInvokeUserCallback(subscription.Dispose);
            _subscriptions.Clear();

            CleanupTransport(closeTransport: true);
            SetState(HeTuSessionState.Faulted);
        }

        private void MarkConnectionLost(bool closeTransport)
        {
            foreach (var pending in _inFlightCalls.ToArray())
                SafeInvokeUserCallback<Exception>(
                    pending.OnFailed,
                    new CallOutcomeUnknownException(pending.SystemName));
            _inFlightCalls.Clear();

            foreach (var subscription in _subscriptions.Values.ToArray())
                ((IRestorableSubscription)subscription).Suspend();

            foreach (var pending in _pendingWatches)
                pending.MarkRetryable();

            CleanupTransport(closeTransport: closeTransport);
        }

        private void ScheduleReconnect()
        {
            if (_closed)
                return;

            SetState(HeTuSessionState.Reconnecting);
            _scheduledReconnect?.Dispose();
            var delay = _currentReconnectDelay;
            _scheduledReconnect = _scheduler.Schedule(delay, RunSessionLifecycle);
            // 指数退避：下一次失败时使用翻倍的延迟，封顶在 _maxReconnectDelay。
            // BecomeReady 会把 _currentReconnectDelay 重置回 _reconnectDelay。
            var nextMs = Math.Min(
                delay.TotalMilliseconds * 2,
                _maxReconnectDelay.TotalMilliseconds);
            _currentReconnectDelay = TimeSpan.FromMilliseconds(nextMs);
        }

        private void CleanupTransport(bool closeTransport)
        {
            if (_transport == null)
                return;

            // Connected 用的是 lambda，无法 -=；ConnectTransport 一旦 promise 完成
            // 就不再有意义。Closed 用的是命名方法，必须显式取消订阅，否则旧 transport
            // 的 Closed 事件会触发新一轮 OnTransportClosed。
            _transport.Closed -= OnTransportClosed;
            if (closeTransport)
                _transport.Close();
            _transport.Dispose();
            _transport = null;
        }

        private bool TryGetSubscription<TSubscription>(
            string subId,
            out TSubscription subscription)
            where TSubscription : BaseSubscription
        {
            subscription = null;
            if (!_subscriptions.TryGetValue(subId, out var existing))
                return false;

            if (existing is not TSubscription typed)
                throw new InvalidCastException(
                    $"Subscription '{subId}' already exists with type {existing.GetType()}.");

            subscription = typed;
            return true;
        }

        private TSubscription RegisterSubscription<TSubscription>(
            TSubscription subscription)
            where TSubscription : BaseSubscription
        {
            if (_subscriptions.TryGetValue(subscription.SubId, out var existing))
            {
                if (existing is not TSubscription typed)
                    throw new InvalidCastException(
                        $"Subscription '{subscription.SubId}' already exists with type {existing.GetType()}.");
                return typed;
            }

            _subscriptions.Add(subscription.SubId, subscription);
            subscription.Disposed += HandleSubscriptionDisposed;
            return subscription;
        }

        private void UnregisterSubscription(BaseSubscription subscription)
        {
            _subscriptions.Remove(subscription.SubId);
            subscription.Disposed -= HandleSubscriptionDisposed;
        }

        private void HandleSubscriptionDisposed(BaseSubscription subscription) =>
            UnregisterSubscription(subscription);

        private void AddPendingWatch(PendingWatch watch)
        {
            _pendingWatches.Add(watch);
            if (watch.Key != null)
                _pendingWatchesByKey.Add(watch.Key, watch);
        }

        private void RemovePendingWatch(PendingWatch watch)
        {
            _pendingWatches.Remove(watch);
            if (watch.Key != null)
                _pendingWatchesByKey.Remove(watch.Key);
        }

        private void CompletePending<TSubscription>(
            PendingWatch watch,
            TSubscription subscription)
            where TSubscription : BaseSubscription
        {
            RemovePendingWatch(watch);
            // Close 后晚到的成功响应：session 已不再管理订阅，必须把这条 subscription
            // 立即 Dispose，否则它会被 RegisterSubscription 写回已清空的 _subscriptions
            // 成为孤儿（服务器还会持续推送）。
            if (_closed)
            {
                subscription?.Dispose();
                return;
            }
            if (subscription == null)
            {
                watch.Complete(null);
                return;
            }

            watch.Complete(RegisterSubscription(subscription));
        }

        private void FailPending(PendingWatch watch, Exception exception)
        {
            RemovePendingWatch(watch);
            if (_closed)
                return;
            watch.Fail(exception);
        }

        private void ThrowIfClosed()
        {
            if (_closed)
                throw new ObjectDisposedException(nameof(HeTuSessionClientBase));
        }

        private void SetState(HeTuSessionState state)
        {
            if (State == state)
                return;

            State = state;
            SafeInvokeUserCallback(StateChanged, state);
        }

        // 在内部清理流程里调用用户委托一定要走这个包装：用户回调（Unity 里典型的
        // MissingReferenceException）抛出后会打断循环，留下半清空的队列和未推进
        // 的状态，让 session 卡死。
        private static void SafeInvokeUserCallback(Action action)
        {
            if (action == null)
                return;
            try
            {
                action();
            }
            catch (Exception ex)
            {
                Logger.Instance.Error(
                    $"[HeTuSession] user callback threw: {ex}");
            }
        }

        private static void SafeInvokeUserCallback<T>(Action<T> action, T arg)
        {
            if (action == null)
                return;
            try
            {
                action(arg);
            }
            catch (Exception ex)
            {
                Logger.Instance.Error(
                    $"[HeTuSession] user callback threw: {ex}");
            }
        }

        private sealed class PendingCall
        {
            public PendingCall(
                string systemName,
                object[] args,
                Action<JsonObject> onCompleted,
                Action<Exception> onFailed)
            {
                SystemName = systemName;
                Args = args;
                OnCompleted = onCompleted;
                OnFailed = onFailed;
            }

            public string SystemName { get; }
            public object[] Args { get; }
            public Action<JsonObject> OnCompleted { get; }
            public Action<Exception> OnFailed { get; }
        }

        private abstract class PendingWatch
        {
            protected PendingWatch(string key, Type dataType)
            {
                Key = key;
                DataType = dataType;
            }

            public string Key { get; }
            public Type DataType { get; }
            public bool IsInFlight { get; protected set; }

            public abstract void Dispatch(
                HeTuSessionClientBase owner,
                IHeTuSessionTransport transport);

            public abstract void Complete(BaseSubscription subscription);
            public abstract void Fail(Exception exception);

            public void MarkRetryable() => IsInFlight = false;
        }

        // Row/Range 唯一的差异是 transport.WatchRow vs transport.WatchRange 这一次
        // 调用。把它折成一个 dispatch lambda 注入进来，剩下的"deduplication / 在飞
        // 标志 / waiter 列表 / 完成时通知所有 waiter"逻辑就完全共用。
        private sealed class PendingWatch<TSub> : PendingWatch
            where TSub : BaseSubscription
        {
            private readonly Action<IHeTuSessionTransport, Promise<TSub>> _dispatch;
            private readonly List<Waiter> _waiters = new();

            public PendingWatch(
                string key,
                Type dataType,
                Action<IHeTuSessionTransport, Promise<TSub>> dispatch,
                Action<TSub> firstOnCompleted,
                Action<Exception> firstOnFailed) :
                base(key, dataType)
            {
                _dispatch = dispatch;
                _waiters.Add(new Waiter(firstOnCompleted, firstOnFailed));
            }

            public void AddWaiter(
                Action<TSub> onCompleted,
                Action<Exception> onFailed) =>
                _waiters.Add(new Waiter(onCompleted, onFailed));

            public override void Dispatch(
                HeTuSessionClientBase owner,
                IHeTuSessionTransport transport)
            {
                IsInFlight = true;
                var p = new Promise<TSub>();
                _dispatch(transport, p);
                p.Future
                    .Then(sub =>
                    {
                        IsInFlight = false;
                        owner.CompletePending(this, sub);
                    })
                    .Catch(ex =>
                    {
                        IsInFlight = false;
                        owner.FailPending(this, ex);
                    });
            }

            public override void Complete(BaseSubscription subscription)
            {
                var typed = subscription as TSub;
                var snapshot = _waiters.ToArray();
                _waiters.Clear();
                foreach (var waiter in snapshot)
                    SafeInvokeUserCallback(waiter.OnCompleted, typed);
            }

            public override void Fail(Exception exception)
            {
                var snapshot = _waiters.ToArray();
                _waiters.Clear();
                foreach (var waiter in snapshot)
                    SafeInvokeUserCallback(waiter.OnFailed, exception);
            }

            private readonly struct Waiter
            {
                public Waiter(Action<TSub> onCompleted, Action<Exception> onFailed)
                {
                    OnCompleted = onCompleted;
                    OnFailed = onFailed;
                }

                public Action<TSub> OnCompleted { get; }
                public Action<Exception> OnFailed { get; }
            }
        }
    }
}
