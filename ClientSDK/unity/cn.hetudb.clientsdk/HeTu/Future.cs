// <copyright>
// Copyright 2026, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的内部Future/Promise——把callback串成线性链。</summary>


using System;
using System.Collections.Generic;

namespace HeTu
{
    /// <summary>
    ///     轻量级callback聚合器（void版本）。不依赖Task/Awaitable/UniTask；
    ///     一次性完成、同步触发已完成态的continuation、单个handler抛异常不影响后续。
    /// </summary>
    internal sealed class Future
    {
        private enum State { Pending, Completed, Failed }

        private static readonly Future s_completed = CreateCompletedSingleton();

        /// <summary>预完成单例，直接return它表示"已完成、无须等"。</summary>
        public static Future Completed => s_completed;

        /// <summary>创建一个已失败态的Future（携带异常）。</summary>
        public static Future Failed(Exception ex)
        {
            var f = new Future();
            f.SetFailed(ex);
            return f;
        }

        private State _state = State.Pending;
        private Exception _exception;
        private List<Action> _onSuccess;
        private List<Action<Exception>> _onFail;
        private List<Action> _onFinally;

        public bool IsCompleted => _state != State.Pending;
        public bool IsFailed => _state == State.Failed;
        public Exception Exception => _exception;

        /// <summary>成功后跑同步动作，返回的Future镜像本条链的后续结果。</summary>
        public Future Then(Action next)
        {
            var result = new Future();
            ChainTo(
                () =>
                {
                    SafeInvoke(next);
                    result.SetCompleted();
                },
                ex => result.SetFailed(ex));
            return result;
        }

        /// <summary>成功后跑异步动作（返回Future），结果合并到本条链。</summary>
        public Future Then(Func<Future> next)
        {
            var result = new Future();
            ChainTo(
                () =>
                {
                    Future inner;
                    try
                    {
                        inner = next();
                    }
                    catch (Exception ex)
                    {
                        result.SetFailed(ex);
                        return;
                    }

                    if (inner == null)
                    {
                        result.SetCompleted();
                        return;
                    }

                    inner.ChainTo(
                        () => result.SetCompleted(),
                        ex => result.SetFailed(ex));
                },
                ex => result.SetFailed(ex));
            return result;
        }

        public Future Catch(Action<Exception> handler)
        {
            if (handler == null) return this;
            if (_state == State.Failed)
                SafeInvoke(handler, _exception);
            else if (_state == State.Pending)
                (_onFail ??= new List<Action<Exception>>()).Add(handler);
            return this;
        }

        public Future Finally(Action handler)
        {
            if (handler == null) return this;
            if (_state != State.Pending)
                SafeInvoke(handler);
            else
                (_onFinally ??= new List<Action>()).Add(handler);
            return this;
        }

        internal void SetCompleted()
        {
            if (_state != State.Pending) return;
            _state = State.Completed;
            var success = _onSuccess;
            var fin = _onFinally;
            _onSuccess = null;
            _onFinally = null;
            _onFail = null;
            if (success != null)
                foreach (var a in success) SafeInvoke(a);
            if (fin != null)
                foreach (var a in fin) SafeInvoke(a);
        }

        internal void SetFailed(Exception ex)
        {
            if (_state != State.Pending) return;
            _exception = ex ?? new Exception("Unknown failure.");
            _state = State.Failed;
            var fail = _onFail;
            var fin = _onFinally;
            _onFail = null;
            _onFinally = null;
            _onSuccess = null;
            if (fail != null)
                foreach (var a in fail) SafeInvoke(a, _exception);
            if (fin != null)
                foreach (var a in fin) SafeInvoke(a);
        }

        // 内部钩子：Then用它把链式continuation挂到本Future上。
        // success/fail分别只在对应分支触发；同步态时立即调用。
        // internal而非private——Future<T>.Then(Func<T,Future>)需要从外部调用。
        internal void ChainTo(Action onSuccess, Action<Exception> onFail)
        {
            if (_state == State.Completed)
            {
                onSuccess();
            }
            else if (_state == State.Failed)
            {
                onFail(_exception);
            }
            else
            {
                (_onSuccess ??= new List<Action>()).Add(onSuccess);
                (_onFail ??= new List<Action<Exception>>()).Add(onFail);
            }
        }

        private static Future CreateCompletedSingleton()
        {
            var f = new Future();
            f._state = State.Completed;
            return f;
        }

        // 单个用户continuation抛异常不应该卡死同一队列里后续的handler。
        internal static void SafeInvoke(Action a)
        {
            if (a == null) return;
            try
            {
                a();
            }
            catch (Exception ex)
            {
                Logger.Instance.Error($"continuation threw: {ex}");
            }
        }

        internal static void SafeInvoke(Action<Exception> a, Exception arg)
        {
            if (a == null) return;
            try
            {
                a(arg);
            }
            catch (Exception ex)
            {
                Logger.Instance.Error($"continuation threw: {ex}");
            }
        }
    }

    /// <summary>
    ///     带值的Future。语义与无值版本一致；Then提供值，链式Then<U>支持类型转换。
    /// </summary>
    internal sealed class Future<T>
    {
        private enum State { Pending, Completed, Failed }

        public static Future<T> Failed(Exception ex)
        {
            var f = new Future<T>();
            f.SetFailed(ex);
            return f;
        }

        private State _state = State.Pending;
        private T _value;
        private Exception _exception;
        private List<Action<T>> _onSuccess;
        private List<Action<Exception>> _onFail;
        private List<Action> _onFinally;

        public bool IsCompleted => _state != State.Pending;
        public bool IsFailed => _state == State.Failed;
        public T Value => _value;
        public Exception Exception => _exception;

        public Future Then(Action<T> next)
        {
            var result = new Future();
            ChainTo(
                v =>
                {
                    SafeInvoke(next, v);
                    result.SetCompleted();
                },
                ex => result.SetFailed(ex));
            return result;
        }

        public Future<U> Then<U>(Func<T, Future<U>> next)
        {
            var result = new Future<U>();
            ChainTo(
                v =>
                {
                    Future<U> inner;
                    try
                    {
                        inner = next(v);
                    }
                    catch (Exception ex)
                    {
                        result.SetFailed(ex);
                        return;
                    }

                    if (inner == null)
                    {
                        result.SetFailed(new InvalidOperationException(
                            "Then returned a null Future."));
                        return;
                    }

                    inner.ChainTo(
                        u => result.SetCompleted(u),
                        ex => result.SetFailed(ex));
                },
                ex => result.SetFailed(ex));
            return result;
        }

        public Future Then(Func<T, Future> next)
        {
            var result = new Future();
            ChainTo(
                v =>
                {
                    Future inner;
                    try
                    {
                        inner = next(v);
                    }
                    catch (Exception ex)
                    {
                        result.SetFailed(ex);
                        return;
                    }

                    if (inner == null)
                    {
                        result.SetCompleted();
                        return;
                    }

                    inner.ChainTo(
                        () => result.SetCompleted(),
                        ex => result.SetFailed(ex));
                },
                ex => result.SetFailed(ex));
            return result;
        }

        public Future<T> Catch(Action<Exception> handler)
        {
            if (handler == null) return this;
            if (_state == State.Failed)
                Future.SafeInvoke(handler, _exception);
            else if (_state == State.Pending)
                (_onFail ??= new List<Action<Exception>>()).Add(handler);
            return this;
        }

        public Future<T> Finally(Action handler)
        {
            if (handler == null) return this;
            if (_state != State.Pending)
                Future.SafeInvoke(handler);
            else
                (_onFinally ??= new List<Action>()).Add(handler);
            return this;
        }

        internal void SetCompleted(T value)
        {
            if (_state != State.Pending) return;
            _value = value;
            _state = State.Completed;
            var success = _onSuccess;
            var fin = _onFinally;
            _onSuccess = null;
            _onFinally = null;
            _onFail = null;
            if (success != null)
                foreach (var a in success) SafeInvoke(a, _value);
            if (fin != null)
                foreach (var a in fin) Future.SafeInvoke(a);
        }

        internal void SetFailed(Exception ex)
        {
            if (_state != State.Pending) return;
            _exception = ex ?? new Exception("Unknown failure.");
            _state = State.Failed;
            var fail = _onFail;
            var fin = _onFinally;
            _onFail = null;
            _onFinally = null;
            _onSuccess = null;
            if (fail != null)
                foreach (var a in fail) Future.SafeInvoke(a, _exception);
            if (fin != null)
                foreach (var a in fin) Future.SafeInvoke(a);
        }

        // ChainTo需要给值给success分支；fail分支与无值版本一致。
        internal void ChainTo(Action<T> onSuccess, Action<Exception> onFail)
        {
            if (_state == State.Completed)
            {
                onSuccess(_value);
            }
            else if (_state == State.Failed)
            {
                onFail(_exception);
            }
            else
            {
                (_onSuccess ??= new List<Action<T>>()).Add(onSuccess);
                (_onFail ??= new List<Action<Exception>>()).Add(onFail);
            }
        }

        private static void SafeInvoke(Action<T> a, T arg)
        {
            if (a == null) return;
            try
            {
                a(arg);
            }
            catch (Exception ex)
            {
                Logger.Instance.Error($"continuation threw: {ex}");
            }
        }
    }

    /// <summary>写端：把Future从Pending翻到Completed/Failed。</summary>
    internal sealed class Promise
    {
        private readonly Future _future = new();
        public Future Future => _future;

        public bool TryComplete()
        {
            if (_future.IsCompleted) return false;
            _future.SetCompleted();
            return true;
        }

        public bool TryFail(Exception ex)
        {
            if (_future.IsCompleted) return false;
            _future.SetFailed(ex);
            return true;
        }
    }

    /// <summary>带值Future的写端。</summary>
    internal sealed class Promise<T>
    {
        private readonly Future<T> _future = new();
        public Future<T> Future => _future;

        public bool TryComplete(T value)
        {
            if (_future.IsCompleted) return false;
            _future.SetCompleted(value);
            return true;
        }

        public bool TryFail(Exception ex)
        {
            if (_future.IsCompleted) return false;
            _future.SetFailed(ex);
            return true;
        }
    }
}
