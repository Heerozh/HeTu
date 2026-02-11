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
    public enum ConnectionState
    {
        Disconnected,
        ReadyForConnect,
        Connected
    }

    /// <summary>
    ///     河图Client基础类，不包含网络和平台相关操作
    /// </summary>
    public abstract class HeTuClientBase : IDisposable
    {
        private const string CommandRpc = "rpc";
        private const string CommandSub = "sub";
        private const string CommandUnsub = "unsub";
        private const string QueryGet = "get";
        private const string QueryRange = "range";
        private const string MessageResponse = "rsp";
        private const string MessageUpdate = "updt";
        private const string IndexId = "id";

        protected readonly ConcurrentQueue<ValueTuple<object, ResponseManager.Callback>>
            OfflineQueue = new();

        protected readonly MessagePipeline Pipeline = new();
        protected readonly ResponseManager ResponseQueue = new();

        protected readonly SubscriptionManager Subscriptions = new();
        protected ConnectionState State = ConnectionState.Disconnected;

        // 调用System时的本地回调，也就是System对应的客户端逻辑
        public Dictionary<string, Action<object[]>> SystemLocalCallbacks = new();

        protected HeTuClientBase() => SetupPipeline(
            new List<MessageProcessLayer>
            {
                new JsonbLayer(), new ZlibLayer(), new CryptoLayer()
            });

        public virtual void Dispose()
        {
            Pipeline.Dispose();
            GC.SuppressFinalize(this);
        }

        // 连接成功时的回调
        public event Action OnConnected;

        // 连接关闭时的回调，如果string不为null，表示异常关闭。
        // 如果连接未建立，本方法不会被调用。
        public event Action<string> OnClosed;

        // 实际Websocket连接方法
        protected abstract void ConnectCore(string url, Action onConnected,
            Action<byte[]> onMessage, Action<string> onClose, Action<string> onError);

        // 实际关闭ws连接的方法
        protected abstract void CloseCore();

        // 实际往ws发送数据的方法
        protected abstract void SendCore(byte[] data);

        // 设置封包的编码/解码协议，协议要和你的河图服务器中的配置一致
        public void SetupPipeline(List<MessageProcessLayer> layers)
        {
            Pipeline.Clean();
            foreach (var layer in layers)
                Pipeline.AddLayer(layer);
        }

        // 连接到河图url的Core方法，外部还需要异步包装一下
        protected void ConnectSync(string url)
        {
            if (Pipeline.NumLayers == 0)
                Logger.Instance.Debug("Pipeline为空，可能是忘了调用SetupPipeline");

            // 前置清理
            Logger.Instance.Info($"[HeTuClient] 正在连接到：{url}...");
            Subscriptions.Clean();
            ResponseQueue.CancelAll("重新连接");

            // 初始化WebSocket以及事件
            State = ConnectionState.ReadyForConnect;
            var handshakeDone = false;
            ConnectCore(url,
                SendClientHandshake,
                msg =>
                {
                    if (handshakeDone)
                    {
                        OnReceived(msg);
                        return;
                    }

                    HandleHandshakeMessage(msg);
                    handshakeDone = true;
                },
                HandleClosed,
                HandleError);
        }

        private void SendClientHandshake()
        {
            var helloMsg = Pipeline.ClientHello();
            SendCore(helloMsg);
        }

        private void HandleHandshakeMessage(byte[] msg)
        {
            var serverHandshake = Pipeline.Decode(msg) as object[];
            Pipeline.Handshake(serverHandshake?.Cast<byte[]>().ToArray());

            Logger.Instance.Info("[HeTuClient] 连接成功。");
            State = ConnectionState.Connected;

            // 连接完成开始发送离线消息队列中的消息
            OnConnected?.Invoke();
            // OnConnected后Pipeline才可用
            FlushOfflineQueue();
        }

        private void FlushOfflineQueue()
        {
            foreach (var (payload, callback) in OfflineQueue)
            {
                var buffer = Pipeline.Encode(payload);
                SendCore(buffer);
                if (callback != null)
                    ResponseQueue.EnqueueCallback(callback);
            }

            OfflineQueue.Clear();
        }

        private void HandleClosed(string errMsg)
        {
            State = ConnectionState.Disconnected;
            if (errMsg == null)
                Logger.Instance.Info("[HeTuClient] 连接断开，收到了服务器Close消息。");
            OnClosed?.Invoke(errMsg);
        }

        private void HandleError(string errMsg)
        {
            switch (State)
            {
                case ConnectionState.ReadyForConnect:
                    Logger.Instance.Error($"[HeTuClient] 连接失败: {errMsg}");
                    break;
                case ConnectionState.Connected:
                    Logger.Instance.Error($"[HeTuClient] 接受消息时发生异常: {errMsg}");
                    break;
            }
        }

        // 关闭河图连接
        public virtual void Close()
        {
            Logger.Instance.Info("[HeTuClient] 主动调用了Close");
            ResponseQueue.CancelAll("主动调用了Close");
            CloseCore();
        }

        private void SendOrQueueRequest(object payload, ResponseManager.Callback callback)
        {
            if (State == ConnectionState.Connected)
            {
                var buffer = Pipeline.Encode(payload);
                SendCore(buffer); // 后台线程发送
                if (callback != null)
                    ResponseQueue.EnqueueCallback(callback);
            }
            else
            {
                Logger.Instance.Info("尝试发送数据但连接未建立，将加入队列在建立后发送。");
                OfflineQueue.Enqueue((payload, callback));
            }
        }

        private bool EnsureConnected(string operationName)
        {
            if (State != ConnectionState.Disconnected)
                return true;

            Logger.Instance.Error($"[HeTuClient] {operationName}失败，请先调用Connect");
            return false;
        }

        // 调用System方法，但是不处理返回值
        protected void CallSystemSync(string systemName, object[] args,
            Action<JsonObject, bool> onResponse)
        {
            if (!EnsureConnected("CallSystem"))
            {
                onResponse(null, true);
                return;
            }

            var payload = new object[] { CommandRpc, systemName }.Concat(args).ToArray();
            SendOrQueueRequest(payload, (response, cancel) =>
            {
                if (cancel)
                    onResponse(null, true);
                else
                    onResponse((JsonObject)response[1], false);
            });
            SystemLocalCallbacks.TryGetValue(systemName, out var callbacks);
            callbacks?.Invoke(args);
        }

        private static string MakeSubId(string table, string index, object left,
            object right,
            int limit, bool desc) =>
            $"{table}.{index}[{left}:{right ?? "None"}:{(desc ? -1 : 1)}][:{limit}]";

        private static string BuildSubscriptionTypeMismatchMessage(Type actualType,
            Type expectedComponentType) =>
            $"[HeTuClient] 已订阅该数据，但之前订阅使用的是{actualType}类型，你不能再用{expectedComponentType}类型订阅了";

        private static TSubscription CastSubscriptionOrThrow<TSubscription, TComponent>(
            BaseSubscription subscribed)
            where TSubscription : BaseSubscription
            where TComponent : IBaseComponent
        {
            if (subscribed is TSubscription casted)
                return casted;

            throw new InvalidCastException(
                BuildSubscriptionTypeMismatchMessage(subscribed.GetType(),
                    typeof(TComponent)));
        }

        private bool TryGetExistingSubscription<TSubscription, TComponent>(string subId,
            out TSubscription subscription)
            where TSubscription : BaseSubscription
            where TComponent : IBaseComponent
        {
            subscription = null;
            if (!Subscriptions.TryGet(subId, out var existing))
                return false;

            subscription = CastSubscriptionOrThrow<TSubscription, TComponent>(existing);
            return true;
        }

        private static string CaptureCreationSource()
        {
            string creationSource = null;
#if DEBUG
            creationSource = Environment.StackTrace;
#endif
            return creationSource;
        }

        public void GetSync<T>(
            string index, object value,
            Action<RowSubscription<T>, bool, Exception> onResponse,
            string componentName = null)
            where T : IBaseComponent
        {
            if (!EnsureConnected("Get"))
            {
                onResponse(null, true, null);
                return;
            }

            componentName ??= typeof(T).Name;
            // 如果index是id，我们可以事先判断是否已经订阅过
            if (index == IndexId)
            {
                var predictID = MakeSubId(componentName, IndexId, value, null, 1, false);
                if (TryGetExistingSubscription<RowSubscription<T>, T>(predictID,
                        out var existingRowSubscription))
                {
                    onResponse(existingRowSubscription, false, null);
                    return;
                }
            }

            var creationSource = CaptureCreationSource();

            // 向服务器订阅
            Logger.Instance.Debug(
                $"[HeTuClient] 发送Get订阅: {componentName}.{index}[{value}:]");
            var payload = new[] { CommandSub, componentName, QueryGet, index, value };
            SendOrQueueRequest(payload, (response, cancel) =>
            {
                if (cancel)
                {
                    onResponse(null, true, null);
                    return;
                }

                RowSubscription<T> rowSubscription = null;
                try
                {
                    var subID = (string)response[1];
                    // 如果查询到值
                    if (subID != null)
                    {
                        // 如果依然是重复订阅，直接返回副本
                        if (TryGetExistingSubscription<RowSubscription<T>, T>(subID,
                                out var existingSubscription))
                        {
                            rowSubscription = existingSubscription;
                        }
                        else
                        {
                            var data = ((JsonObject)response[2]).To<T>();
                            // ReSharper disable once NotDisposedResource
                            rowSubscription = new RowSubscription<T>(
                                subID, componentName, data, this, creationSource);
                            Subscriptions.Add(subID,
                                new WeakReference(rowSubscription, false));
                            Logger.Instance.Info($"[HeTuClient] 成功订阅了 {subID}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    onResponse(null, false, ex);
                    return;
                }

                onResponse(rowSubscription, false, null);
            });
        }

        public void GetSync(
            string index, object value,
            Action<RowSubscription<DictComponent>, bool, Exception> onResponse,
            string componentName = null) =>
            GetSync<DictComponent>(index, value, onResponse, componentName);

        public void RangeSync<T>(
            string index, object left, object right, int limit,
            Action<IndexSubscription<T>, bool, Exception> onResponse,
            bool desc = false, bool force = true,
            string componentName = null)
            where T : IBaseComponent
        {
            if (!EnsureConnected("Range"))
            {
                onResponse(null, true, null);
                return;
            }

            componentName ??= typeof(T).Name;

            // 先要组合sub_id看看是否已订阅过
            var predictID = MakeSubId(
                componentName, index, left, right, limit, desc);
            if (TryGetExistingSubscription<IndexSubscription<T>, T>(predictID,
                    out var existingIndexSubscription))
            {
                onResponse(existingIndexSubscription, false, null);
                return;
            }

            var creationSource = CaptureCreationSource();

            // 发送订阅请求
            Logger.Instance.Debug($"[HeTuClient] 发送Range订阅: {predictID}");
            var payload = new[]
            {
                CommandSub, componentName, QueryRange, index, left, right, limit,
                desc, force
            };
            SendOrQueueRequest(payload, (response, cancel) =>
            {
                if (cancel)
                {
                    onResponse(null, true, null);
                    return;
                }

                IndexSubscription<T> idxSubscription = null;
                try
                {
                    var subID = (string)response[1];
                    // 如果查询到值
                    if (subID != null)
                    {
                        // 如果依然是重复订阅，直接返回副本
                        if (TryGetExistingSubscription<IndexSubscription<T>, T>(subID,
                                out var existingSubscription))
                        {
                            idxSubscription = existingSubscription;
                        }
                        else
                        {
                            var rawRows = (JsonObject)response[2];
                            var rows = rawRows.ToList<T>();
                            // ReSharper disable once NotDisposedResource
                            idxSubscription = new IndexSubscription<T>(
                                subID, componentName, rows, this, creationSource);
                            Subscriptions.Add(subID,
                                new WeakReference(idxSubscription, false));
                            Logger.Instance.Info($"[HeTuClient] 成功订阅了 {subID}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    onResponse(null, false, ex);
                    return;
                }

                onResponse(idxSubscription, false, null);
            });
        }

        public void RangeSync(
            string componentName, string index, object left, object right, int limit,
            Action<IndexSubscription<DictComponent>, bool, Exception> onResponse,
            bool desc = false, bool force = true) =>
            RangeSync(index, left, right, limit, onResponse, desc, force, componentName);

        public void Unsubscribe(string subID, string from)
        {
            if (State == ConnectionState.Disconnected)
                return;
            if (!Subscriptions.Contains(subID)) return;
            Subscriptions.Remove(subID);
            var payload = new object[] { CommandUnsub, subID };
            SendOrQueueRequest(payload, null);
            Logger.Instance.Info($"[HeTuClient] 因BaseSubscription {from}，已取消订阅 {subID}");
        }

        protected virtual void OnReceived(byte[] buffer)
        {
            // 解码消息
            // Logger.Instance.Info($"[HeTuClient] 收到消息: {decoded}");
            if (Pipeline.Decode(buffer) is not object[] structuredMsg ||
                structuredMsg.Length == 0)
                return;

            var messageType = structuredMsg[0] as string;
            switch (messageType)
            {
                case MessageResponse:
                case CommandSub:
                    // 这2个都是round trip响应，所以有对应的请求等待队列
                    ResponseQueue.CompleteNext(structuredMsg);
                    break;
                case MessageUpdate:
                    // 这个是主动推送，需要根据subID找到对应的订阅对象
                    var subID = (string)structuredMsg[1];
                    if (!Subscriptions.TryGet(subID, out var subscribed))
                        break;
                    var rows = (JsonObject)structuredMsg[2];
                    subscribed.UpdateRows(rows);
                    break;
            }
        }
    }
}
