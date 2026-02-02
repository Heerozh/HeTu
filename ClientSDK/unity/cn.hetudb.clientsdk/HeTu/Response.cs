// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的请求管理库</summary>.


using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace HeTu
{
    public class ResponseManager
    {
        public delegate void Callback(List<object> args, bool cancel = false);

        private readonly ConcurrentQueue<Callback> _requestCallbacks = new();

        public void EnqueueCallback(Callback cb) =>
            _requestCallbacks.Enqueue(cb);

        public void CompleteNext(List<object> response)
        {
            if (_requestCallbacks.TryDequeue(out var cb))
                cb(response);
        }

        public void CancelAll(string reason)
        {
            if (_requestCallbacks.IsEmpty) return;
            Logger.Instance.Info($"[HeTuClient] {reason}, 取消所有等待任务...");
            foreach (var cb in _requestCallbacks)
                cb(null, true);
            _requestCallbacks.Clear();
        }
    }
}
