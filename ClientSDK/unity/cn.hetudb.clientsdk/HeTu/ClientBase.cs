// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Client库</summary>


using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace HeTu
{
    /// <summary>
    ///     河图Client基础类，不包含网络和平台相关操作
    /// </summary>
    public abstract class HeTuClientBase
    {
        protected readonly ConcurrentQueue<byte[]> OfflineQueue = new();
        protected readonly MessagePipeline Pipeline = new();
        protected readonly ResponseManager ResponseQueue = new();

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
                _OnReceived,
                () =>
                {
                    State = "Disconnected";
                    ResponseQueue.CancelAll("连接断开");
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
            ResponseQueue.CancelAll("主动调用了Close");
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
            Action<JsonObject> onResponse)
        {
            var payload = new object[] { "sys", systemName }.Concat(args);
            _SendSync(payload);
            ResponseQueue.EnqueueCallback(response =>
            {
                onResponse((JsonObject)response[1]);
            });
            SystemLocalCallbacks.TryGetValue(systemName, out var callbacks);
            callbacks?.Invoke(args);
        }

        private static string _makeSubID(string table, string index, object left,
            object right,
            int limit, bool desc) =>
            $"{table}.{index}[{left}:{right ?? "None"}:{(desc ? -1 : 1)}][:{limit}]";

        public void GetSync<T>(
            string index, object value, Action<RowSubscription<T>> onResponse,
            string componentName = null)
            where T : IBaseComponent
        {
            componentName ??= typeof(T).Name;
            // 如果index是id，我们可以事先判断是否已经订阅过
            if (index == "id")
            {
                var predictID = _makeSubID(
                    componentName, "id", value, null, 1, false);
                if (Subscriptions.TryGet(predictID, out var subscribed))
                    if (subscribed is RowSubscription<T> casted)
                        onResponse(casted);
                    else
                        throw new InvalidCastException(
                            $"[HeTuClient] 已订阅该数据，但之前订阅使用的是{subscribed.GetType()}类型");
            }

            // 向服务器订阅
            var payload = new[] { "sub", componentName, "get", index, value };
            _SendSync(payload);
            Logger.Instance.Debug(
                $"[HeTuClient] 发送Get订阅: {componentName}.{index}[{value}:]");

            // 等待服务器结果
            ResponseQueue.EnqueueCallback(response =>
            {
                var subID = (string)response[1];
                // 如果没有查询到值
                if (subID is null)
                {
                    onResponse(null);
                    return;
                }

                // 如果依然是重复订阅，直接返回副本
                if (Subscriptions.TryGet(subID, out var stillSubscribed))
                    if (stillSubscribed is RowSubscription<T> casted)
                    {
                        onResponse(casted);
                        return;
                    }
                    else
                        throw new InvalidCastException(
                            $"[HeTuClient] 已订阅该数据，但之前订阅使用的是{stillSubscribed.GetType()}类型");

                var data = ((JsonObject)response[2]).To<T>();
                var newSub = new RowSubscription<T>(subID, componentName, data);
                Subscriptions.Add(subID, new WeakReference(newSub, false));
                Logger.Instance.Info($"[HeTuClient] 成功订阅了 {subID}");
                onResponse(newSub);
            });
        }

        public void GetSync(
            string index, object value, Action<RowSubscription<DictComponent>> onResponse,
            string componentName = null) =>
            GetSync<DictComponent>(index, value, onResponse, componentName);

        public void Range<T>(
            string index, object left, object right, int limit,
            Action<IndexSubscription<T>> onResponse,
            bool desc = false, bool force = true,
            string componentName = null)
            where T : IBaseComponent
        {
            componentName ??= typeof(T).Name;

            // 先要组合sub_id看看是否已订阅过
            var predictID = _makeSubID(
                componentName, index, left, right, limit, desc);
            if (Subscriptions.TryGet(predictID, out var subscribed))
                if (subscribed is IndexSubscription<T> casted)
                {
                    onResponse(casted);
                    return;
                }
                else
                    throw new InvalidCastException(
                        $"[HeTuClient] 已订阅该数据，但之前订阅使用的是{subscribed.GetType()}类型");

            // 发送订阅请求
            var payload = new[]
            {
                "sub", componentName, "range", index, left, right, limit, desc, force
            };
            _SendSync(payload);
            Logger.Instance.Debug($"[HeTuClient] 发送Query订阅: {predictID}");

            // 等待服务器结果
            ResponseQueue.EnqueueCallback(response =>
            {
                var subID = (string)response[1];
                // 如果没有查询到值
                if (subID is null)
                {
                    onResponse(null);
                    return;
                }

                // 如果依然是重复订阅，直接返回副本
                if (Subscriptions.TryGet(subID, out var stillSubscribed))
                    if (stillSubscribed is IndexSubscription<T> casted)
                    {
                        onResponse(casted);
                        return;
                    }
                    else
                        throw new InvalidCastException(
                            $"[HeTuClient] 已订阅该数据，但之前订阅使用的是{stillSubscribed.GetType()}类型");

                var rawRows = (JsonObject)response[2];
                var rows = rawRows.ToList<T>();
                var newSub = new IndexSubscription<T>(subID, componentName, rows);

                Subscriptions.Add(subID, new WeakReference(newSub, false));
                Logger.Instance.Info($"[HeTuClient] 成功订阅了 {subID}");
                onResponse(newSub);
            });
        }

        public void Range(
            string componentName, string index, object left, object right, int limit,
            Action<IndexSubscription<DictComponent>> onResponse,
            bool desc = false, bool force = true) =>
            Range(index, left, right, limit, onResponse, desc, force, componentName);

        internal void _unsubscribe(string subID, string from)
        {
            if (!_subscriptions.ContainsKey(subID)) return;
            _subscriptions.Remove(subID);
            var payload = new object[] { "unsub", subID };
            _Send(payload);
            _logInfo?.Invoke($"[HeTuClient] 因BaseSubscription {from}，已取消订阅 {subID}");
        }

        protected virtual void _OnReceived(byte[] buffer)
        {
            // 解码消息
            // Logger.Instance.Info($"[HeTuClient] 收到消息: {decoded}");
            var structuredMsg = Pipeline.Decode(buffer);
            switch (structuredMsg[0])
            {
                case "rsp":
                case "sub":
                    // 这2个都是round trip响应，所以有对应的请求等待队列
                    ResponseQueue.CompleteNext(structuredMsg);
                    break;
                case "updt":
                    // 这个是主动推送，需要根据subID找到对应的订阅对象
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
