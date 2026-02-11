// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的请求管理库</summary>.


using System.Collections.Concurrent;

namespace HeTu
{
    /// <summary>
    ///     管理请求-响应回调队列。
    ///     <para>
    ///         每次发送需要等待服务端返回的请求时，都会按发送顺序入队一个回调；
    ///         收到响应后按 FIFO 顺序触发对应回调。
    ///     </para>
    /// </summary>
    public class ResponseManager
    {
        /// <summary>
        ///     请求完成回调。
        /// </summary>
        /// <param name="args">服务端返回的数据包。</param>
        /// <param name="cancel">是否为取消信号。</param>
        public delegate void Callback(object[] args, bool cancel = false);

        private readonly ConcurrentQueue<Callback> _requestCallbacks = new();

        /// <summary>
        ///     入队一个等待响应的回调。
        /// </summary>
        /// <param name="cb">请求完成后的回调函数。</param>
        public void EnqueueCallback(Callback cb) =>
            _requestCallbacks.Enqueue(cb);

        /// <summary>
        ///     完成队列中的下一个请求回调。
        /// </summary>
        /// <param name="response">服务端返回的响应数据。</param>
        public void CompleteNext(object[] response)
        {
            if (_requestCallbacks.TryDequeue(out var cb))
                cb(response);
        }

        /// <summary>
        ///     取消当前所有等待中的请求。
        /// </summary>
        /// <param name="reason">取消原因（用于日志）。</param>
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
