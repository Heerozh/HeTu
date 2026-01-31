// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Client库</summary>


using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HeTu
{
    /// <summary>
    ///     河图Client基础类，不包含网络和平台相关操作
    /// </summary>
    public abstract class HeTuClientBase
    {
        protected readonly ConcurrentQueue<byte[]> OfflineQueue = new();
        protected readonly MessagePipeline Pipeline = new();
        protected readonly RequestManager Requests = new();

        protected readonly SubscriptionManager Subscriptions = new();
        protected string State = "Disconnected";

        // 本地调用System时的回调。调用时立即就会回调，是否成功调用未知。
        public Dictionary<string, Action<object[]>> SystemLocalCallbacks = new();

        // 连接成功时的回调
        public event Action OnConnected;

        // 实际Websocket连接方法
        protected abstract void _connect(string url, Action onConnected,
            Action<byte[]> onMessage, Action onClose, Action<string> onError);

        // 实际关闭ws连接的方法
        protected abstract void _close();

        // 实际往ws发送数据的方法
        protected abstract void _send(byte[] data);

        public void SetupPipeline(string compressor, string cipher)
        {
            if (!string.IsNullOrEmpty(compressor))
            {
                switch (compressor)
                {
                    case "zlib":
                        Pipeline.AddLayer(new ZlibLayer());
                        Logger.Instance.Info("[HeTuClient] 已启用Zlib压缩层。");
                        break;
                    default:
                        Logger.Instance.Error(
                            $"[HeTuClient] 未知的压缩算法：{compressor}，将不启用压缩层。");
                        break;
                }
            }

            if (!string.IsNullOrEmpty(cipher))
            {
                switch (cipher)
                {
                    case "ChaCha20-Poly1305":
                        Pipeline.AddLayer(new CryptoLayer());
                        break;
                    default:
                        Logger.Instance.Error($"[HeTuClient] 未知的加密算法：{cipher}，将不启用加密层。");
                        break;
                }
            }

            Pipeline.AddLayer(new JsonbLayer());
        }

        // 连接到河图url的Core方法，外部还需要异步包装一下
        protected void ConnectSync(string url)
        {
            if (Pipeline.NumLayers == 0)
                throw new InvalidOperationException(
                    "Pipeline未设置，请先调用SetupPipeline");

            // 前置清理
            Logger.Instance.Info($"[HeTuClient] 正在连接到：{url}...");
            Subscriptions.Clean();

            // 初始化WebSocket以及事件
            State = "ReadyForConnect";
            _connect(url, () =>
                {
                    Logger.Instance.Info("[HeTuClient] 连接成功。");
                    State = "Connected";
                    OnConnected?.Invoke();
                    foreach (var data in OfflineQueue)
                        _send(data);
                    OfflineQueue.Clear();
                },
                msg => { _OnReceived(msg); },
                () =>
                {
                    State = "Disconnected";
                    Requests.CancelAll("连接断开");
                    Logger.Instance.Info("[HeTuClient] 连接断开，收到了服务器Close消息。");
                }, errMsg =>
                {
                    switch (State)
                    {
                        case "ReadyForConnect":
                            Logger.Instance.Error($"[HeTuClient] 连接失败: {errMsg}");
                            break;
                        case "Connected":
                            Logger.Instance.Error($"[HeTuClient] 接受消息时发生异常: {errMsg}");
                            break;
                    }
                }
            );
        }

        // 关闭河图连接
        public virtual void Close()
        {
            Logger.Instance.Info("[HeTuClient] 主动调用了Close");
            Requests.CancelAll("主动调用了Close");
            _close();
        }

        protected void _SendSync(object payload)
        {
            var buffer = Pipeline.Encode(payload);

            if (State == "Connected")
            {
                _send(buffer); // 后台线程发送
            }
            else
            {
                Logger.Instance.Info("尝试发送数据但连接未建立，将加入队列在建立后发送。");
                OfflineQueue.Enqueue(buffer);
            }
        }

        // 调用System方法，但是不处理返回值
        protected void CallSystemSync(string systemName, object[] args,
            Action<object>[] onResponse)
        {
            var payload = new object[] { "sys", systemName }.Concat(args);
            _SendSync(payload);
            SystemLocalCallbacks.TryGetValue(systemName, out var callbacks);
            callbacks?.Invoke(args);
        }

        public RowSubscription<T> GetSync<T>(
            string index, object value, string componentName = null)
            where T : IBaseComponent
        {
            componentName ??= typeof(T).Name;
            // 如果index是id，我们可以事先判断是否已经订阅过
            if (index == "id")
            {
                var predictID = _makeSubID(
                    componentName, "id", value, null, 1, false);
                if (_subscriptions.TryGetValue(predictID, out var subscribed))
                    if (subscribed.Target is RowSubscription<T> casted)
                        return casted;
                    else
                        throw new InvalidCastException(
                            $"[HeTuClient] 已订阅该数据，但之前订阅使用的是{typeof(T)}类型");
            }

            // 向服务器订阅
            var payload = new[] { "sub", componentName, "select", value, where };
            _Send(payload);
            _logDebug?.Invoke(
                $"[HeTuClient] 发送Select订阅: {componentName}.{where}[{value}:]");

            // 等待服务器结果
#if UNITY_6000_0_OR_NEWER
            var tcs = new TaskCompletionSource<List<object>>();
#else
            var tcs = new UniTaskCompletionSource<List<object>>();
#endif
            _waitingSubTasks.Enqueue(tcs);
            List<object> subMsg;
            try
            {
                // await UniTask会调用task.GetResult()，如果cancel了，会抛出异常
                subMsg = await tcs.Task;
            }
            catch (OperationCanceledException e)
            {
                // NUnit不把取消信号视为错误，所以这里要LogError一下让测试不通过
                _logError?.Invoke($"[HeTuClient] 订阅数据过程中遇到取消信号: {e}");
                throw;
            }

            var subID = (string)subMsg[1];
            // 如果没有查询到值
            if (subID is null) return null;
            // 如果依然是重复订阅，直接返回副本
            if (_subscriptions.TryGetValue(subID, out var stillSubscribed))
                if (stillSubscribed.Target is RowSubscription<T> casted)
                    return casted;
                else
                    throw new InvalidCastException(
                        $"[HeTuClient] 已订阅该数据，但之前订阅使用的是{typeof(T)}类型");

            var data = ((JObject)subMsg[2]).ToObject<T>();
            var newSub = new RowSubscription<T>(subID, componentName, data);
            _subscriptions[subID] = new WeakReference(newSub, false);
            _logInfo?.Invoke($"[HeTuClient] 成功订阅了 {subID}");
            return newSub;
        }

        protected virtual void _OnReceived(byte[] buffer)
        {
            // 解码消息
            buffer = _protocol?.Decrypt(buffer) ?? buffer;
            buffer = _protocol?.Decompress(buffer) ?? buffer;
            var decoded = Encoding.UTF8.GetString(buffer);
            // 处理消息
            // Logger.Instance.Info($"[HeTuClient] 收到消息: {decoded}");
            var structuredMsg = JsonConvert.DeserializeObject<List<object>>(decoded);
            if (structuredMsg is null) return;
            switch (structuredMsg[0])
            {
                case "rsp":
                    OnResponse?.Invoke((JObject)structuredMsg[1]);
                    break;
                case "sub":
                    if (!_waitingSubTasks.TryDequeue(out var tcs))
                        break;
                    tcs.TrySetResult(structuredMsg);
                    break;
                case "updt":
                    var subID = (string)structuredMsg[1];
                    if (!_subscriptions.TryGetValue(subID, out var pSubscribed))
                        break;
                    if (pSubscribed.Target is not BaseSubscription subscribed)
                        break;
                    var rows = ((JObject)structuredMsg[2])
                        .ToObject<Dictionary<long, JObject>>();
                    foreach (var (rowID, data) in rows)
                        subscribed.Update(rowID, data);
                    break;
            }
        }
    }
}
