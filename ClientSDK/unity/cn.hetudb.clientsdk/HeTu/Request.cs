// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的请求管理库</summary>.

#if UNITY_6000_0_OR_NEWER
using System.Threading.Tasks;
using UnityEngine;
#else
using Cysharp.Threading.Tasks;
#endif
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace HeTu
{
    public class RequestManager
    {
#if UNITY_6000_0_OR_NEWER
        readonly ConcurrentQueue<TaskCompletionSource<List<object>>> _waitingSubTasks =
 new();
#else
        private readonly ConcurrentQueue<UniTaskCompletionSource<List<object>>>
            _requests = new();
#endif

#if UNITY_6000_0_OR_NEWER
        public static TaskCompletionSource<T> CreateTcs<T>()
        {
            var tcs = new TaskCompletionSource<T>();
#else
        public static UniTaskCompletionSource<T> CreateTcs<T>()
        {
            var tcs = new UniTaskCompletionSource<T>();
#endif
            return tcs;
        }


        public void Create()
        {
            var tcs = CreateTcs<List<object>>();
            _requests.Enqueue(tcs);
        }

        public bool CompleteNext(List<object> response) =>
            _requests.TryDequeue(out var tcs) && tcs.TrySetResult(response);

        public void CancelAll(string reason)
        {
            Logger.Instance.Info($"[HeTuClient] {reason}, 取消所有等待任务...");
            foreach (var tcs in _requests)
                tcs.TrySetCanceled();
            _requests.Clear();
        }
    }
}
