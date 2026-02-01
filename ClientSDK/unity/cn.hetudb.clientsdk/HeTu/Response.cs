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
        private readonly ConcurrentQueue<Action<List<object>>> _requestCallbacks = new();

        public void EnqueueCallback(Action<List<object>> cb) =>
            _requestCallbacks.Enqueue(cb);

        public void CompleteNext(List<object> response)
        {
            if (_requestCallbacks.TryDequeue(out var cb))
                cb(response);
        }

        public void CancelAll(string reason)
        {
            Logger.Instance.Info($"[HeTuClient] {reason}, 取消所有等待任务...");
            // foreach (var cb in _requestCallbacks)
            //     cb(null);
            _requestCallbacks.Clear();
        }
    }
}
