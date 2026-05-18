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

    internal delegate void HeTuSessionBootstrap(
        IHeTuSessionTransport transport,
        Action onSucceeded,
        Action<Exception> onFailed);

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
        private readonly IHeTuSessionScheduler _scheduler;
        private readonly Dictionary<string, BaseSubscription> _subscriptions = new();
        private readonly Func<IHeTuSessionTransport> _transportFactory;

        private bool _closed;
        private IDisposable _scheduledReconnect;
        private IHeTuSessionTransport _transport;

        public HeTuSessionClientBase(
            Func<IHeTuSessionTransport> transportFactory,
            IHeTuSessionScheduler scheduler,
            HeTuSessionBootstrap bootstrap = null,
            TimeSpan? reconnectDelay = null)
        {
            _transportFactory = transportFactory;
            _scheduler = scheduler;
            _bootstrap = bootstrap;
            _reconnectDelay = reconnectDelay ?? TimeSpan.FromSeconds(1);
        }

        public HeTuSessionState State { get; private set; } =
            HeTuSessionState.Stopped;

        public event Action Ready;
        public event Action<HeTuSessionState> StateChanged;
        public event Action<Exception> Faulted;

        public void Start()
        {
            if (_closed || State != HeTuSessionState.Stopped)
                return;

            ConnectNewTransport();
        }

        public void Close()
        {
            if (_closed)
                return;

            _closed = true;
            _scheduledReconnect?.Dispose();
            _scheduledReconnect = null;

            while (_pendingCalls.Count > 0)
                _pendingCalls.Dequeue().OnFailed(
                    new OperationCanceledException("Session closed."));

            foreach (var pending in _inFlightCalls.ToArray())
                pending.OnFailed(new OperationCanceledException("Session closed."));
            _inFlightCalls.Clear();

            foreach (var pending in _pendingWatches.ToArray())
            {
                RemovePendingWatch(pending);
                pending.Fail(new OperationCanceledException("Session closed."));
            }

            foreach (var subscription in _subscriptions.Values.ToArray())
                subscription.Dispose();

            _subscriptions.Clear();
            CleanupTransport(closeTransport: true);
            SetState(HeTuSessionState.Stopped);
        }

        public void Dispose() => Close();

        public void CallSystem(
            string systemName,
            object[] args,
            Action<JsonObject> onCompleted,
            Action<Exception> onFailed)
        {
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
                if (pending is PendingRowWatch<T> typed)
                {
                    typed.AddWaiter(onCompleted, onFailed);
                    return;
                }

                throw new InvalidCastException(
                    $"Subscription '{knownSubId}' already exists with type {pending.DataType}.");
            }

            var watch = new PendingRowWatch<T>(
                knownSubId, index, value, componentName, onCompleted, onFailed);
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
                if (pending is PendingRangeWatch<T> typed)
                {
                    typed.AddWaiter(onCompleted, onFailed);
                    return;
                }

                throw new InvalidCastException(
                    $"Subscription '{subId}' already exists with type {pending.DataType}.");
            }

            var watch = new PendingRangeWatch<T>(
                subId, index, left, right, limit, desc, force, componentName,
                onCompleted, onFailed);
            AddPendingWatch(watch);
            DispatchPendingIfReady(watch);
        }

        private void ConnectNewTransport()
        {
            if (_closed)
                return;

            CleanupTransport(closeTransport: false);
            _transport = _transportFactory();
            _transport.Connected += HandleTransportConnected;
            _transport.Closed += HandleTransportClosed;
            SetState(HeTuSessionState.Connecting);
            _transport.Connect();
        }

        private void HandleTransportConnected()
        {
            if (_closed)
                return;

            SetState(HeTuSessionState.Bootstrapping);
            if (_bootstrap == null)
            {
                RestoreSubscriptions();
                return;
            }

            try
            {
                _bootstrap(_transport, RestoreSubscriptions, HandleRecoverableFailure);
            }
            catch (Exception ex)
            {
                HandleRecoverableFailure(ex);
            }
        }

        private void RestoreSubscriptions()
        {
            if (_closed)
                return;

            SetState(HeTuSessionState.RestoringSubscriptions);
            RestoreSubscriptionAt(_subscriptions.Values.ToArray(), 0);
        }

        private void RestoreSubscriptionAt(BaseSubscription[] subscriptions, int index)
        {
            if (_closed)
                return;

            if (index >= subscriptions.Length)
            {
                BecomeReady();
                return;
            }

            var subscription = subscriptions[index];
            if (subscription.IsDisposed)
            {
                RestoreSubscriptionAt(subscriptions, index + 1);
                return;
            }

            ((IRestorableSubscription)subscription).Restore(
                _transport,
                isActive =>
                {
                    if (!isActive)
                        UnregisterSubscription(subscription);
                    RestoreSubscriptionAt(subscriptions, index + 1);
                },
                HandleRecoverableFailure);
        }

        private void BecomeReady()
        {
            _scheduledReconnect?.Dispose();
            _scheduledReconnect = null;
            SetState(HeTuSessionState.Ready);
            Ready?.Invoke();
            FlushPendingCalls();
            DispatchPendingWatches();
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

        private void HandleTransportClosed(string _)
        {
            if (_closed)
                return;

            MarkConnectionLost();
            ScheduleReconnect();
        }

        private void HandleRecoverableFailure(Exception exception)
        {
            if (_closed)
                return;

            MarkConnectionLost();
            ScheduleReconnect();
            Faulted?.Invoke(exception);
        }

        private void MarkConnectionLost()
        {
            foreach (var pending in _inFlightCalls.ToArray())
                pending.OnFailed(
                    new CallOutcomeUnknownException(pending.SystemName));
            _inFlightCalls.Clear();

            foreach (var subscription in _subscriptions.Values)
                ((IRestorableSubscription)subscription).Suspend();

            foreach (var pending in _pendingWatches)
                pending.MarkRetryable();

            CleanupTransport(closeTransport: false);
        }

        private void ScheduleReconnect()
        {
            if (_closed)
                return;

            SetState(HeTuSessionState.Reconnecting);
            _scheduledReconnect?.Dispose();
            _scheduledReconnect = _scheduler.Schedule(
                _reconnectDelay,
                ConnectNewTransport);
        }

        private void CleanupTransport(bool closeTransport)
        {
            if (_transport == null)
                return;

            _transport.Connected -= HandleTransportConnected;
            _transport.Closed -= HandleTransportClosed;
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
            watch.Fail(exception);
        }

        private void SetState(HeTuSessionState state)
        {
            if (State == state)
                return;

            State = state;
            StateChanged?.Invoke(state);
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

        private sealed class PendingRowWatch<T> : PendingWatch
            where T : IBaseComponent
        {
            private readonly string _componentName;
            private readonly string _index;
            private readonly List<RowWaiter> _waiters = new();
            private readonly object _value;

            public PendingRowWatch(
                string key,
                string index,
                object value,
                string componentName,
                Action<RowSubscription<T>> onCompleted,
                Action<Exception> onFailed) :
                base(key, typeof(T))
            {
                _index = index;
                _value = value;
                _componentName = componentName;
                AddWaiter(onCompleted, onFailed);
            }

            public void AddWaiter(
                Action<RowSubscription<T>> onCompleted,
                Action<Exception> onFailed) =>
                _waiters.Add(new RowWaiter(onCompleted, onFailed));

            public override void Dispatch(
                HeTuSessionClientBase owner,
                IHeTuSessionTransport transport)
            {
                IsInFlight = true;
                transport.WatchRow<T>(
                    _index,
                    _value,
                    (subscription, canceled, exception) =>
                    {
                        IsInFlight = false;
                        if (canceled)
                            return;
                        if (exception != null)
                        {
                            owner.FailPending(this, exception);
                            return;
                        }

                        owner.CompletePending(this, subscription);
                    },
                    _componentName);
            }

            public override void Complete(BaseSubscription subscription)
            {
                var typed = subscription as RowSubscription<T>;
                foreach (var waiter in _waiters)
                    waiter.OnCompleted(typed);
                _waiters.Clear();
            }

            public override void Fail(Exception exception)
            {
                foreach (var waiter in _waiters)
                    waiter.OnFailed(exception);
                _waiters.Clear();
            }

            private readonly struct RowWaiter
            {
                public RowWaiter(
                    Action<RowSubscription<T>> onCompleted,
                    Action<Exception> onFailed)
                {
                    OnCompleted = onCompleted;
                    OnFailed = onFailed;
                }

                public Action<RowSubscription<T>> OnCompleted { get; }
                public Action<Exception> OnFailed { get; }
            }
        }

        private sealed class PendingRangeWatch<T> : PendingWatch
            where T : IBaseComponent
        {
            private readonly string _componentName;
            private readonly bool _desc;
            private readonly bool _force;
            private readonly string _index;
            private readonly object _left;
            private readonly int _limit;
            private readonly object _right;
            private readonly List<RangeWaiter> _waiters = new();

            public PendingRangeWatch(
                string key,
                string index,
                object left,
                object right,
                int limit,
                bool desc,
                bool force,
                string componentName,
                Action<IndexSubscription<T>> onCompleted,
                Action<Exception> onFailed) :
                base(key, typeof(T))
            {
                _index = index;
                _left = left;
                _right = right;
                _limit = limit;
                _desc = desc;
                _force = force;
                _componentName = componentName;
                AddWaiter(onCompleted, onFailed);
            }

            public void AddWaiter(
                Action<IndexSubscription<T>> onCompleted,
                Action<Exception> onFailed) =>
                _waiters.Add(new RangeWaiter(onCompleted, onFailed));

            public override void Dispatch(
                HeTuSessionClientBase owner,
                IHeTuSessionTransport transport)
            {
                IsInFlight = true;
                transport.WatchRange<T>(
                    _index,
                    _left,
                    _right,
                    _limit,
                    (subscription, canceled, exception) =>
                    {
                        IsInFlight = false;
                        if (canceled)
                            return;
                        if (exception != null)
                        {
                            owner.FailPending(this, exception);
                            return;
                        }

                        owner.CompletePending(this, subscription);
                    },
                    _desc,
                    _force,
                    _componentName);
            }

            public override void Complete(BaseSubscription subscription)
            {
                var typed = subscription as IndexSubscription<T>;
                foreach (var waiter in _waiters)
                    waiter.OnCompleted(typed);
                _waiters.Clear();
            }

            public override void Fail(Exception exception)
            {
                foreach (var waiter in _waiters)
                    waiter.OnFailed(exception);
                _waiters.Clear();
            }

            private readonly struct RangeWaiter
            {
                public RangeWaiter(
                    Action<IndexSubscription<T>> onCompleted,
                    Action<Exception> onFailed)
                {
                    OnCompleted = onCompleted;
                    OnFailed = onFailed;
                }

                public Action<IndexSubscription<T>> OnCompleted { get; }
                public Action<Exception> OnFailed { get; }
            }
        }
    }
}
