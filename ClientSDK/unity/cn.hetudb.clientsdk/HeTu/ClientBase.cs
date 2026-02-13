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
    ///     客户端连接状态。
    /// </summary>
    public enum ConnectionState
    {
        /// <summary>
        ///     未连接。
        /// </summary>
        Disconnected,

        /// <summary>
        ///     连接已建立但尚未完成握手。
        /// </summary>
        ReadyForConnect,

        /// <summary>
        ///     已连接并完成握手。
        /// </summary>
        Connected
    }

    /// <summary>
    ///     河图Client基础类，不包含网络和平台相关操作
    /// </summary>
    public abstract class HeTuClientBase : IDisposable
    {
        internal const string CommandRpc = "rpc";
        internal const string CommandSub = "sub";
        internal const string CommandUnsub = "unsub";
        internal const string QueryGet = "get";
        internal const string QueryRange = "range";
        internal const string MessageResponse = "rsp";
        internal const string MessageUpdate = "updt";
        internal const string MessageSubed = "sub";
        internal const string IndexId = "id";
        protected readonly InspectorTraceCollector InspectorCollector = new();

        protected readonly ConcurrentQueue<
                ValueTuple<object, ResponseManager.Callback, string>>
            OfflineQueue = new();

        protected readonly MessagePipeline Pipeline = new();
        protected readonly ResponseManager ResponseQueue = new();

        protected readonly SubscriptionManager Subscriptions = new();
        protected ConnectionState State = ConnectionState.Disconnected;

        /// <summary>
        ///     System 调用的本地回调表。也就是System对应的客户端逻辑。
        ///     Key 为系统名，Value 为调用时同步触发的回调。
        /// </summary>
        public Dictionary<string, Action<object[]>> SystemLocalCallbacks = new();

        protected HeTuClientBase() => SetupPipeline(
            new List<MessageProcessLayer>
            {
                new JsonbLayer(), new ZlibLayer(), new CryptoLayer()
            });

        public bool IsConnected => State == ConnectionState.Connected;

        /// <summary>
        ///     动态开关：是否启用 Inspector 拦截。
        /// </summary>
        public bool InspectorEnabled => InspectorCollector.Enabled;


        public virtual void Dispose()
        {
            Pipeline.Dispose();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///     配置 Inspector 拦截开关与采样率。
        ///     默认关闭，采样率默认 1。
        /// </summary>
        public void ConfigureInspector(bool enabled) =>
            InspectorCollector.Configure(enabled);

        /// <summary>
        ///     注册 Inspector 事件分发器。
        /// </summary>
        public void AddInspectorDispatcher(IInspectorTraceDispatcher dispatcher) =>
            InspectorCollector.AddDispatcher(dispatcher);

        /// <summary>
        ///     移除 Inspector 事件分发器。
        /// </summary>
        public void RemoveInspectorDispatcher(IInspectorTraceDispatcher dispatcher) =>
            InspectorCollector.RemoveDispatcher(dispatcher);

        /// <summary>
        ///     握手完成并可收发业务消息时触发。
        /// </summary>
        public event Action OnConnected;

        /// <summary>
        ///     连接关闭时触发。如果连接未建立，本方法不会被调用。
        /// </summary>
        /// <remarks>
        ///     参数为 <see langword="null" /> 表示正常关闭；否则为错误信息。
        /// </remarks>
        public event Action<string> OnClosed;

        // 实际Websocket连接方法
        protected abstract void ConnectCore(string url, Action onConnected,
            Action<byte[]> onMessage, Action<string> onClose, Action<string> onError);

        // 实际关闭ws连接的方法
        protected abstract void CloseCore();

        // 实际往ws发送数据的方法
        protected abstract void SendCore(byte[] data);

        /// <summary>
        ///     配置消息处理管道。要和你的河图服务器中的配置一致
        /// </summary>
        /// <param name="layers">处理层列表（按顺序执行）。</param>
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
            foreach (var (payload, callback, traceId) in OfflineQueue)
            {
                var buffer = Pipeline.Encode(payload, out var metrics);
                InspectorCollector.UpdateRequestSize(traceId,
                    metrics.SourceSizeBytes,
                    metrics.TransportSizeBytes);
                SendCore(buffer);
                if (callback != null)
                    ResponseQueue.EnqueueCallback(callback, traceId);
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

        /// <summary>
        ///     主动关闭连接，并取消所有等待中的响应。
        /// </summary>
        public virtual void Close()
        {
            Logger.Instance.Info("[HeTuClient] 主动调用了Close");
            ResponseQueue.CancelAll("主动调用了Close");
            CloseCore();
        }

        private void SendOrQueueRequest(object payload, ResponseManager.Callback callback,
            string traceId = null)
        {
            if (State == ConnectionState.Connected)
            {
                var buffer = Pipeline.Encode(payload, out var metrics);
                InspectorCollector.UpdateRequestSize(traceId,
                    metrics.SourceSizeBytes,
                    metrics.TransportSizeBytes);
                SendCore(buffer); // 后台线程发送
                if (callback != null)
                    ResponseQueue.EnqueueCallback(callback, traceId);
            }
            else
            {
                Logger.Instance.Info("尝试发送数据但连接未建立，将加入队列在建立后发送。");
                OfflineQueue.Enqueue((payload, callback, traceId));
            }
        }

        private bool EnsureConnected(string operationName)
        {
            if (State != ConnectionState.Disconnected)
                return true;

            Logger.Instance.Error($"[HeTuClient] {operationName}失败，请先调用Connect");
            return false;
        }

        /// <summary>
        ///     发起 System RPC 调用。
        /// </summary>
        /// <param name="systemName">系统名。</param>
        /// <param name="args">参数列表。</param>
        /// <param name="onResponse">响应回调，第二参数为是否取消。</param>
        protected void CallSystemSync(string systemName, object[] args,
            Action<JsonObject, bool> onResponse)
        {
            if (!EnsureConnected("CallSystem"))
            {
                onResponse(null, true);
                return;
            }

            var payload = new object[] { CommandRpc, systemName }.Concat(args).ToArray();
            var traceId = InspectorCollector.InterceptRequest("callsystem", systemName,
                payload);
            SendOrQueueRequest(payload, (response, cancel) =>
            {
                if (cancel)
                {
                    InspectorCollector.CompleteRequest(traceId, "canceled");
                    onResponse(null, true);
                }
                else
                {
                    var responsePayload = response != null && response.Length > 1
                        ? response[1]
                        : null;
                    InspectorCollector.CompleteRequest(traceId, "completed",
                        responsePayload);
                    onResponse((JsonObject)responsePayload, false);
                }
            }, traceId);
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

        /// <summary>
        ///     发起单行订阅（Get）。
        /// </summary>
        /// <typeparam name="T">组件类型。</typeparam>
        /// <param name="index">索引字段名。</param>
        /// <param name="value">索引值。</param>
        /// <param name="onResponse">回调：订阅对象、是否取消、异常信息。</param>
        /// <param name="componentName">组件名；为空时取 <typeparamref name="T" /> 类型名。</param>
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
            var payload = new[] { CommandSub, componentName, QueryGet, index, value };
            var traceId = InspectorCollector.InterceptRequest(QueryGet, componentName,
                payload);
            SendOrQueueRequest(payload, (response, cancel) =>
            {
                if (cancel)
                {
                    InspectorCollector.CompleteRequest(traceId, "canceled");
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
                        }
                    }
                }
                catch (Exception ex)
                {
                    InspectorCollector.CompleteRequest(traceId, "failed", ex.Message);
                    onResponse(null, false, ex);
                    return;
                }

                var responsePayload = response.Length > 2 ? response[2] : null;
                InspectorCollector.CompleteRequest(traceId, "completed",
                    responsePayload);
                onResponse(rowSubscription, false, null);
            }, traceId);
        }

        /// <summary>
        ///     发起字典组件单行订阅（Get）。
        /// </summary>
        public void GetSync(
            string index, object value,
            Action<RowSubscription<DictComponent>, bool, Exception> onResponse,
            string componentName = null) =>
            GetSync<DictComponent>(index, value, onResponse, componentName);

        /// <summary>
        ///     发起范围订阅（Range）。
        /// </summary>
        /// <typeparam name="T">组件类型。</typeparam>
        /// <param name="index">索引字段名。</param>
        /// <param name="left">范围左边界。</param>
        /// <param name="right">范围右边界。</param>
        /// <param name="limit">最大返回条数。</param>
        /// <param name="onResponse">回调：订阅对象、是否取消、异常信息。</param>
        /// <param name="desc">是否降序。</param>
        /// <param name="force">无数据时是否仍保持订阅。</param>
        /// <param name="componentName">组件名；为空时取 <typeparamref name="T" /> 类型名。</param>
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
            var payload = new[]
            {
                CommandSub, componentName, QueryRange, index, left, right, limit,
                desc, force
            };
            var traceId = InspectorCollector.InterceptRequest(QueryRange,
                componentName, payload);
            SendOrQueueRequest(payload, (response, cancel) =>
            {
                if (cancel)
                {
                    InspectorCollector.CompleteRequest(traceId, "canceled");
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
                        }
                    }
                }
                catch (Exception ex)
                {
                    InspectorCollector.CompleteRequest(traceId, "failed", ex.Message);
                    onResponse(null, false, ex);
                    return;
                }

                var responsePayload = response.Length > 2 ? response[2] : null;
                InspectorCollector.CompleteRequest(traceId, "completed",
                    responsePayload);
                onResponse(idxSubscription, false, null);
            }, traceId);
        }

        /// <summary>
        ///     发起字典组件范围订阅（Range）。
        /// </summary>
        public void RangeSync(
            string componentName, string index, object left, object right, int limit,
            Action<IndexSubscription<DictComponent>, bool, Exception> onResponse,
            bool desc = false, bool force = true) =>
            RangeSync(index, left, right, limit, onResponse, desc, force, componentName);

        /// <summary>
        ///     取消指定订阅。
        /// </summary>
        /// <param name="subID">订阅 ID。</param>
        /// <param name="from">取消来源说明（用于日志）。</param>
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
            if (Pipeline.Decode(buffer, out var decodeMetrics) is not object[]
                    structuredMsg ||
                structuredMsg.Length == 0)
                return;

            var messageType = structuredMsg[0] as string;
            switch (messageType)
            {
                case MessageResponse:
                case MessageSubed:
                    // 这2个都是round trip响应，所以有对应的请求等待队列
                    ResponseQueue.CompleteNext(structuredMsg);
                    break;
                case MessageUpdate:
                    // 这个是主动推送，需要根据subID找到对应的订阅对象
                    var subID = (string)structuredMsg[1];
                    if (!Subscriptions.TryGet(subID, out var subscribed))
                        break;
                    var rows = (JsonObject)structuredMsg[2];
                    var target = subID?.Split('.')[0] ?? "unknown";
                    InspectorCollector.InterceptMessageUpdate(target, rows,
                        decodeMetrics.SourceSizeBytes,
                        decodeMetrics.TransportSizeBytes);
                    subscribed.UpdateRows(rows);
                    break;
            }
        }
    }
}
