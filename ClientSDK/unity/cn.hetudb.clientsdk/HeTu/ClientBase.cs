// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Client库</summary>


using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;

namespace HeTu
{
    /// <summary>
    ///     System 调用结果。
    /// </summary>
    public enum CallOutcome
    {
        /// <summary>正常完成。</summary>
        Completed,

        /// <summary>因连接取消/重连而取消。</summary>
        Canceled,

        /// <summary>被服务端守卫拒绝（如限流）。</summary>
        Rejected,

        /// <summary>
        ///     服务端执行失败（System 抛异常或非法调用）。
        ///     仅在 server debug 模式下回传并带原因；release 模式服务端会直接断开连接。
        /// </summary>
        Failed
    }

    /// <summary>
    ///     System 调用被服务端守卫拒绝（如限流）时抛出。
    /// </summary>
    public sealed class HeTuCallRejectedException : Exception
    {
        public string SystemName { get; }
        public string Code { get; }

        public HeTuCallRejectedException(string systemName, string code)
            : base($"System '{systemName}' rejected by server: {code}")
        {
            SystemName = systemName;
            Code = code;
        }
    }

    /// <summary>
    ///     System 在服务端执行失败（抛异常或非法调用）时抛出。
    ///     <see cref="Reason" /> 仅在 server debug 模式下有内容（透传服务端真实异常），
    ///     release 模式下服务端直接断开连接、不会发送本帧。
    /// </summary>
    public sealed class HeTuCallFailedException : Exception
    {
        public string SystemName { get; }
        public string Reason { get; }

        public HeTuCallFailedException(string systemName, string reason)
            : base($"System '{systemName}' failed on server: {reason}")
        {
            SystemName = systemName;
            Reason = reason;
        }
    }

    /// <summary>
    ///     WebSocket close 码。连接关闭后可在 <see cref="HeTuClientBase.OnClosed" /> 里读
    ///     <see cref="HeTuClientBase.LastCloseCode" /> 与之比较。
    ///     WebSocket close codes; compare with
    ///     <see cref="HeTuClientBase.LastCloseCode" /> inside
    ///     <see cref="HeTuClientBase.OnClosed" />.
    /// </summary>
    public static class HeTuCloseCode
    {
        /// <summary>正常关闭。Normal closure.</summary>
        public const int Normal = 1000;

        /// <summary>
        ///     没收到 close 帧就断了（网络断开、连接失败等）。
        ///     Closed without a close frame (network loss, connect failure, ...).
        /// </summary>
        public const int Abnormal = 1006;

        /// <summary>
        ///     被顶号：同一账号在别处登录，服务端断开了本连接。请提示玩家，并且不要自动重连——
        ///     重连会重新登录，把对方顶掉。与服务端 <c>CLOSE_KICKED</c> 一致。
        ///     Kicked: the same account logged in elsewhere. Tell the player and do not
        ///     reconnect automatically — reconnecting logs in again and kicks the other
        ///     side.
        /// </summary>
        public const int Kicked = 4001;

        /// <summary>
        ///     请求被拒：客户端越界（调了服务器上不存在的 System、无权限、参数数量不对、报文非法、
        ///     超出收发频率上限等），常见于客户端比服务器新。重连后再发同样的请求只会再被断开，
        ///     所以不要自动重连。服务器不回具体原因，看服务器日志。与服务端 <c>CLOSE_REJECTED</c> 一致。
        ///     Rejected: the client broke the protocol (unknown System, no permission,
        ///     malformed message, rate limit...), e.g. a client newer than the server.
        ///     Do not reconnect automatically: the same request would be rejected again.
        ///     See the server log for the reason.
        /// </summary>
        public const int Rejected = 4002;

        /// <summary>
        ///     服务端内部出错（后端连不上、System 执行抛异常等），可以退避重连。
        ///     Server internal error (backend unreachable, System raised...);
        ///     reconnecting with backoff is fine.
        /// </summary>
        public const int InternalError = 1011;

        /// <summary>
        ///     是不是终态的 close 码：4000-4999 段都由河图定义为终态（如 <see cref="Kicked" />、
        ///     <see cref="Rejected" />），不应自动重连；其余（断网的 <see cref="Abnormal" />、
        ///     <see cref="InternalError" /> 等）可以重连。以后新增的终态码也在这一段，老版本 SDK
        ///     同样不会重连。
        ///     Whether the close code is terminal: HeTu defines every code in 4000-4999
        ///     as terminal (no automatic reconnect); other codes may be retried.
        /// </summary>
        public static bool IsTerminal(int code) => code >= 4000 && code <= 4999;
    }

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
        internal const string CommandRpcSync = "rpcs";
        internal const string CommandSub = "sub";
        internal const string CommandUnsub = "unsub";
        internal const string QueryGet = "get";
        internal const string QueryRange = "range";
        internal const string QueryTable = "table";
        internal const string MessageResponse = "rsp";
        internal const string MessageUpdate = "updt";
        internal const string MessageSubed = "sub";
        internal const string MessageReject = "rej";
        internal const string MessageError = "err";
        internal const string MessageSync = "sync";
        internal const string IndexId = "id";
        protected readonly InspectorTraceCollector InspectorCollector = new();

        protected readonly MessagePipeline Pipeline = new();
        protected readonly ResponseManager ResponseQueue = new();

        protected readonly SubscriptionManager Subscriptions = new();
        protected ConnectionState State = ConnectionState.Disconnected;

        // 等推送的调用（rpcs）：sync_id → 还没完成的调用。sync 帧不占回复顺序，按 id 认
        private readonly Dictionary<int, PendingPushCall> _pendingPushCalls = new();

        // 本连接上一个 rpcs 的 sync_id：按连接递增，重连归零（服务端只收 0..int.MaxValue）
        private int _syncSeq;

        // 已发出、还没收到回复的 sub 请求数。服务端对同一连接的重复订阅回同一个 sub_id、
        // 不计数，一条 unsub 就删：在途的 sub 可能落到要退的订阅上（按非 id 索引的 WatchRow
        // 事先算不出 sub_id），这时发出 unsub 会把它一起退掉。所以 unsub 只在没有 sub 在途时
        // 才发，否则先挂在 _pendingUnsubs，等在途的 sub 都回复了再决定（FlushPendingUnsubs）。
        private int _inFlightSubs;
        private readonly List<(string SubId, string From)> _pendingUnsubs = new();

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
        ///     最近一次连接关闭的 close 码（见 <see cref="HeTuCloseCode" />），在
        ///     <see cref="OnClosed" /> 触发前设好；还没关闭过、或重新连接后为 0。
        ///     例如等于 <see cref="HeTuCloseCode.Kicked" /> 时提示"账号已在别处登录"。
        ///     Close code of the last closed connection, set before
        ///     <see cref="OnClosed" /> fires; 0 before any close or after reconnecting.
        /// </summary>
        public int LastCloseCode { get; private set; }

        /// <summary>
        ///     动态开关：是否启用 Inspector 拦截。
        /// </summary>
        public bool InspectorEnabled => InspectorCollector.Enabled;


        public virtual void Dispose()
        {
            FinishPushCallsOnClose();
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
        ///     close 码见 <see cref="LastCloseCode" />。
        /// </remarks>
        public event Action<string> OnClosed;

        /// <summary>
        ///     System 调用被服务端守卫拒绝（如限流）时触发的通用回调。
        ///     参数为 (systemName, code)。用户可在此统一弹窗/提示。
        /// </summary>
        public event Action<string, string> OnCallRejected;

        // 实际Websocket连接方法。onClose(close 码, 原因)：原因为 null 表示正常关闭；
        // 没收到 close 帧就断了的用 HeTuCloseCode.Abnormal
        protected abstract void ConnectCore(string url, Action onConnected,
            Action<byte[]> onMessage, Action<int, string> onClose,
            Action<string> onError);

        // 实际关闭ws连接的方法
        protected abstract void CloseCore();

        // 实际往ws发送数据的方法
        protected abstract void SendCore(byte[] data);

        // 在本客户端的协议线程上执行 action：订阅表、共享核心、挂起的 unsub 等只能在那条线程上
        // 动。默认就地执行（Unity 只在主线程上用）；Headless 投到泵线程。
        internal virtual void RunSerialized(Action action) => action();

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

        /// <summary>
        ///     设置加密层握手认证 key。为空则使用 legacy 握手。
        /// </summary>
        protected void ConfigureCryptoAuthKey(string authKey)
        {
            foreach (var layer in Pipeline.Layers)
            {
                if (layer is CryptoLayer crypto)
                    crypto.SetAuthKey(authKey);
            }
        }

        // 连接到河图url的Core方法，外部还需要异步包装一下
        protected void ConnectSync(string url)
        {
            if (Pipeline.NumLayers == 0)
                Logger.Instance.Debug("Pipeline为空，可能是忘了调用SetupPipeline");

            // 前置清理
            Logger.Instance.Info($"正在连接到：{url}...");
            Subscriptions.Clean();
            _pendingUnsubs.Clear();
            FinishPushCallsOnClose();
            ResponseQueue.CancelAll("重新连接");
            _inFlightSubs = 0;
            _pendingPushCalls.Clear();
            _syncSeq = 0;
            LastCloseCode = 0;

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

            Logger.Instance.Info("<color=green>连接成功。</color>");
            State = ConnectionState.Connected;

            OnConnected?.Invoke();
        }

        private void HandleClosed(int code, string errMsg)
        {
            State = ConnectionState.Disconnected;
            LastCloseCode = code;
            Subscriptions.Clean();
            _pendingUnsubs.Clear();
            if (code == HeTuCloseCode.Kicked)
                Logger.Instance.Info("连接断开：账号已在别处登录（被顶号）。");
            else if (code == HeTuCloseCode.Rejected)
                Logger.Instance.Error(
                    "连接断开：请求被服务器拒绝（调了服务器上不存在的 System、无权限、报文非法或超出" +
                    "频率上限，常见于客户端比服务器新），原因见服务器日志。");
            else if (errMsg == null)
                Logger.Instance.Info("连接断开，收到了服务器Close消息。");
            OnClosed?.Invoke(errMsg);
            // 放在 OnClosed 之后：连接拆完了才跑用户代码（续体里接着 Connect / CallSystem 不会撞上
            // 拆到一半的连接）
            FinishPushCallsOnClose();
        }

        private void HandleError(string errMsg)
        {
            switch (State)
            {
                case ConnectionState.ReadyForConnect:
                    Logger.Instance.Error($"连接失败: {errMsg}");
                    break;
                case ConnectionState.Connected:
                    Logger.Instance.Error($"接受消息时发生异常: {errMsg}");
                    break;
            }
        }

        /// <summary>
        ///     主动关闭连接，并取消所有等待中的响应。
        /// </summary>
        public virtual void Close()
        {
            Logger.Instance.Info("主动调用了Close");
            FinishPushCallsOnClose();
            _pendingUnsubs.Clear();
            ResponseQueue.CancelAll("主动调用了Close");
            _inFlightSubs = 0;
            Subscriptions.Clean();
            CloseCore();
        }

        private void SendRequest(object payload, ResponseManager.Callback callback,
            string traceId = null)
        {
            // 这里不检查State == ConnectionState.Connected，请确保调用前使用过 EnsureConnected()
            var buffer = Pipeline.Encode(payload, out var metrics);
            InspectorCollector.UpdateRequestSize(traceId,
                metrics.SourceSizeBytes,
                metrics.TransportSizeBytes);
            SendCore(buffer); // 后台线程发送
            if (callback != null)
                ResponseQueue.EnqueueCallback(callback, traceId);
            else
                InspectorCollector.CompleteAfterSendIfNeeded(traceId);
        }

        private bool EnsureConnected(string operationName)
        {
            if (State == ConnectionState.Connected)
                return true;

            Logger.Instance.Error($"{operationName}失败，连接尚未就绪");
            return false;
        }

        /// <summary>
        ///     发起 System RPC 调用。
        /// </summary>
        /// <param name="systemName">系统名。</param>
        /// <param name="args">参数列表。</param>
        /// <param name="onResponse">响应回调，第二参数为调用结果，第三参数为拒绝码。</param>
        /// <param name="awaitPush">
        ///     为 true 时发等推送的调用（rpcs）：回了 rsp 之后还要等服务端的 ["sync", id]（这次调用
        ///     引起的订阅推送都已先到）才回调 Completed；rej / err 立即回调。断线时已收到 rsp 的按
        ///     成功回调（见 <see cref="FinishPushCallsOnClose" />）。
        /// </param>
        /// <param name="onAnswered">
        ///     等推送的调用收到 rsp、还在等 sync 时通知一次（参数为回复内容）。Session 层据此在断线时
        ///     自己把这个调用当成功，不靠本类断线收尾的先后。
        /// </param>
        internal protected void CallSystemSync(string systemName, object[] args,
            Action<JsonObject, CallOutcome, string> onResponse, bool awaitPush = false,
            Action<JsonObject> onAnswered = null)
        {
            if (!EnsureConnected("CallSystem"))
            {
                onResponse(null, CallOutcome.Canceled, null);
                return;
            }

            PendingPushCall pushCall = null;
            object[] payload;
            if (awaitPush)
            {
                _syncSeq = _syncSeq == int.MaxValue ? 1 : _syncSeq + 1;
                pushCall = new PendingPushCall(_syncSeq, onResponse);
                payload = new object[] { CommandRpcSync, pushCall.SyncId, systemName }
                    .Concat(args).ToArray();
            }
            else
            {
                payload = new object[] { CommandRpc, systemName }.Concat(args).ToArray();
            }

            var traceId = InspectorCollector.InterceptRequest("callsystem", systemName,
                payload);
            if (pushCall != null)
                pushCall.TraceId = traceId;
            SendRequest(payload, (response, cancel) =>
            {
                if (cancel)
                {
                    ForgetPushCall(pushCall);
                    InspectorCollector.CompleteRequest(traceId, "canceled");
                    onResponse(null, CallOutcome.Canceled, null);
                    return;
                }

                // 服务端守卫拒绝：response = ["rej", systemName, code]
                if (response != null && response.Length > 0 &&
                    response[0] as string == MessageReject)
                {
                    ForgetPushCall(pushCall);
                    var code = response.Length > 2 ? response[2] as string : "REJECTED";
                    InspectorCollector.CompleteRequest(traceId, "rejected", code);
                    OnCallRejected?.Invoke(systemName, code);
                    onResponse(null, CallOutcome.Rejected, code);
                    return;
                }

                // 服务端执行失败：response = ["err", systemName, reason]（仅 debug 模式）。
                // 失败原因走独立通道，response 置 null，调用方的 resp?.ToDict() 不会再
                // 对失败字符串做反序列化而抛异常。
                if (response != null && response.Length > 0 &&
                    response[0] as string == MessageError)
                {
                    ForgetPushCall(pushCall);
                    var reason = response.Length > 2 ? response[2] as string : null;
                    InspectorCollector.CompleteRequest(traceId, "failed", reason);
                    onResponse(null, CallOutcome.Failed, reason);
                    return;
                }

                var responsePayload = response != null && response.Length > 1
                    ? response[1]
                    : null;
                if (pushCall != null)
                {
                    // 等推送的调用：rsp 到了先存着，等 sync（发送拥塞时 sync 也可能先到）
                    pushCall.Response = responsePayload;
                    pushCall.RspArrived = true;
                    onAnswered?.Invoke((JsonObject)responsePayload);
                    if (pushCall.Synced)
                        CompletePushCall(pushCall);
                    return;
                }

                InspectorCollector.CompleteRequest(traceId, "completed",
                    responsePayload);
                onResponse((JsonObject)responsePayload, CallOutcome.Completed, null);
            }, traceId);
            // 发出去了才登记：参数没法序列化时 SendRequest 在编码时就抛出，不能留下永远不会完成的
            // 登记（回复与 sync 都要等之后处理收到的消息，晚于这里）
            if (pushCall != null)
                _pendingPushCalls[pushCall.SyncId] = pushCall;
            SystemLocalCallbacks.TryGetValue(systemName, out var callbacks);
            callbacks?.Invoke(args);
        }

        // ["sync", sync_id]：等推送的调用引起的订阅推送都已先到。不占回复顺序，按 id 认；认不出的
        // （已因 rej / err 结束，或断线前的旧 id）忽略
        private void HandleSync(object[] message)
        {
            // JsonbLayer 按标准消息解出来的 sync_id 是 long
            if (message.Length < 2 || message[1] is not long syncId ||
                syncId < 0 || syncId > int.MaxValue ||
                !_pendingPushCalls.TryGetValue((int)syncId, out var call))
                return;
            call.Synced = true;
            if (call.RspArrived)
                CompletePushCall(call);
        }

        private void CompletePushCall(PendingPushCall call)
        {
            // 已经完成过的不再回调：断线收尾逐个完成时，前一个的续体里又 Close()（重入收尾）
            if (!ForgetPushCall(call))
                return;
            InspectorCollector.CompleteRequest(call.TraceId, "completed", call.Response);
            call.OnResponse((JsonObject)call.Response, CallOutcome.Completed, null);
        }

        // 撤掉登记，返回它这时还登记着没有
        private bool ForgetPushCall(PendingPushCall call)
        {
            if (call == null ||
                !_pendingPushCalls.TryGetValue(call.SyncId, out var registered) ||
                registered != call)
                return false;
            _pendingPushCalls.Remove(call.SyncId);
            return true;
        }

        /// <summary>
        ///     连接断开（OnClosed 之后）、主动关闭、重连前、释放时调用：已收到 rsp 的等推送调用按
        ///     成功完成——提交确定发生了，重连后订阅会恢复。没收到 rsp 的留给回复队列照旧取消。
        ///     子类有自己的取消（如 Unity 的 HeTuClient 按连接取消等待者）时，要在取消之前调用它，
        ///     否则这些调用会先被判成取消。Session 层不靠这里：它按 onAnswered 自己处理。
        /// </summary>
        protected void FinishPushCallsOnClose()
        {
            if (_pendingPushCalls.Count == 0)
                return;
            var answered = _pendingPushCalls.Values.Where(c => c.RspArrived).ToArray();
            foreach (var call in answered)
                CompletePushCall(call);
        }

        // 带 group 的查询键以它开头。服务端的 sub_id 以组件名开头，组件名里不会有控制字符，所以
        // 这种键永远不等于任何服务端 sub_id——两者在同一张表里查（SubscriptionManager.TryGet
        // 先按服务端 sub_id 查，再按客户端键的别名查）
        private const string GroupKeyMarker = "\u0001";

        // 客户端拼的订阅键，发 sub 之前用来查重。单列查询的格式同服务端 sub_id（只是边界的格式化
        // 可能不同，对不上时靠 SubscriptionManager 的别名表）。带 group（复合索引的分组值）时是
        // GroupKeyMarker + 单列格式 + {键=类型标记,...}：键按 Ordinal 排序；整数、bool、刚好是
        // 整数的浮点记作 n:<十进制整数>（bool 记 1 / 0，服务端把 True 和 1 当作同一组），string
        // 记作 s:<UTF-8 字节数>:<原文>，byte[] 记作 b:<小写 hex>。同一组语义相同的值得到同一个键，
        // 不同的组得到不同的键（键里不含 '='，见 CopyGroup；left / right 的既有格式不变）
        internal static string MakeSubId(string table, string index, object left,
            object right,
            int limit, bool desc,
            IReadOnlyDictionary<string, object> group = null)
        {
            var subId =
                $"{table}.{index}[{left}:{right ?? "None"}:{(desc ? -1 : 1)}][:{limit}]";
            if (group == null || group.Count == 0)
                return subId;

            var keys = group.Keys.ToList();
            keys.Sort(StringComparer.Ordinal);
            var sb = new StringBuilder(GroupKeyMarker,
                subId.Length + 16 * keys.Count + 2);
            sb.Append(subId).Append('{');
            for (var i = 0; i < keys.Count; i++)
            {
                if (i > 0)
                    sb.Append(',');
                var key = keys[i];
                sb.Append(key).Append('=').Append(GroupValueTag(key, group[key]));
            }

            return sb.Append('}').ToString();
        }

        /// <summary>
        ///     检查并拷贝复合索引的分组值 {分组列: 值}，订阅入口一进来就调。null 和空字典都返回
        ///     null（不带 group）。不合法抛 <see cref="ArgumentException" />，请求不会发出去：服务端
        ///     会以 4002 断开连接，会话随之进入终态。值只收整数类型、bool、刚好是整数（且在 64 位
        ///     整数范围内）的 double / float、string、byte[]；"整数列给了字符串"这类要看列类型的错误
        ///     留给服务端。拷贝是新的普通 Dictionary（MessagePack 能确定地序列化，InspectorTrace
        ///     认得 IDictionary），按键的 Ordinal 顺序插入，byte[] 也复制一份：之后的 payload、
        ///     恢复参数、pending 闭包、查询键都用它，调用方事后改字典不影响已发出的订阅。
        /// </summary>
        internal static Dictionary<string, object> CopyGroup(string index,
            IReadOnlyDictionary<string, object> group)
        {
            if (group == null || group.Count == 0)
                return null;
            // WatchRow 在 id 上走预测的 sub_id，group 会被丢掉；id 也不可能是复合索引
            if (index == IndexId)
                throw new ArgumentException(
                    "id 不是复合索引，不能带 group。The id index takes no group.",
                    nameof(group));

            var pairs = group.ToList();
            pairs.Sort((a, b) => string.CompareOrdinal(a.Key, b.Key));
            var copy = new Dictionary<string, object>(pairs.Count);
            foreach (var (key, value) in pairs)
            {
                // 分组列名是 Python 标识符；键里有 '=' 会让查询键不再单射
                if (string.IsNullOrEmpty(key) || key.IndexOf('=') >= 0)
                    throw new ArgumentException(
                        $"group 的键 \"{key}\" 不是分组列名（不能为空、不能含 '='）。" +
                        "Group keys must be group column names.", nameof(group));
                GroupValueTag(key, value); // 只为检查类型
                var stored = value is byte[] bytes ? (byte[])bytes.Clone() : value;
                if (!copy.TryAdd(key, stored))
                    throw new ArgumentException(
                        $"group 的键 \"{key}\" 重复了。Duplicate group key.", nameof(group));
            }

            return copy;
        }

        // 分组值在查询键里的类型标记（见 MakeSubId），同时就是分组值的类型白名单：拼不出标记的
        // 抛 ArgumentException
        private static string GroupValueTag(string key, object value)
        {
            switch (value)
            {
                case string s:
                    return "s:" + Encoding.UTF8.GetByteCount(s)
                        .ToString(CultureInfo.InvariantCulture) + ":" + s;
                case byte[] bytes:
                    return "b:" + ToLowerHex(bytes);
                case bool b:
                    return b ? "n:1" : "n:0";
                case int v:
                    return IntegerTag(v);
                case long v:
                    return IntegerTag(v);
                case short v:
                    return IntegerTag(v);
                case sbyte v:
                    return IntegerTag(v);
                case byte v:
                    return IntegerTag(v);
                case ushort v:
                    return IntegerTag(v);
                case uint v:
                    return IntegerTag(v);
                case ulong v:
                    return "n:" + v.ToString(CultureInfo.InvariantCulture);
                case double d when IsIntegral(d):
                    return IntegralTag(d);
                case float f when IsIntegral(f):
                    return IntegralTag(f);
                default:
                    var shown = value == null
                        ? "null"
                        : $"{value}（{value.GetType().Name}）";
                    throw new ArgumentException(
                        $"group 的值 {key}={shown} 不能用作分组值：只收整数、bool、刚好是整数的" +
                        $"浮点、string、byte[]。Invalid group value for \"{key}\".", "group");
            }
        }

        private static string IntegerTag(long value) =>
            "n:" + value.ToString(CultureInfo.InvariantCulture);

        // 刚好是整数、并且在 64 位整数的范围 [-2^63, 2^64) 里。超出的不可能是任何整数列的合法值，
        // 浮点又不能做分组列，发出去只会被服务端断开
        private static bool IsIntegral(double value) =>
            Math.Floor(value) == value &&
            value >= -9223372036854775808.0 && value < 18446744073709551616.0;

        private static string IntegralTag(double value) =>
            value < 9223372036854775808.0
                ? IntegerTag((long)value)
                : "n:" + ((ulong)value).ToString(CultureInfo.InvariantCulture);

        private static string ToLowerHex(byte[] bytes)
        {
            const string digits = "0123456789abcdef";
            var chars = new char[bytes.Length * 2];
            for (var i = 0; i < bytes.Length; i++)
            {
                chars[2 * i] = digits[bytes[i] >> 4];
                chars[2 * i + 1] = digits[bytes[i] & 0xF];
            }

            return new string(chars);
        }

        /// <summary>整表订阅的 sub_id，与服务端 SubscriptionBroker.subscribe_table 一致。</summary>
        internal static string MakeTableSubId(string table) => $"{table}.table";

        private static string BuildSubscriptionTypeMismatchMessage(Type actualType,
            Type expectedComponentType) =>
            $"[HeTuClient] 已订阅该数据，但之前订阅使用的是{actualType}类型，你不能再用{expectedComponentType}类型订阅了";

        private bool TryGetExistingCore<TCore, TComponent>(string subId,
            out TCore core)
            where TCore : ManagedSubscriptionCore
            where TComponent : IBaseComponent
        {
            core = null;
            if (!Subscriptions.TryGet(subId, out var existing))
                return false;
            // 已退订的核心不能再发句柄（正常路径上退订时已从表里移除，这里兜底）
            if (existing.IsClosed)
            {
                Subscriptions.Remove(existing.SubId);
                return false;
            }

            core = existing as TCore ?? throw new InvalidCastException(
                BuildSubscriptionTypeMismatchMessage(existing.GetType(),
                    typeof(TComponent)));
            return true;
        }

        internal static StackTrace CaptureCreationTrace()
        {
#if DEBUG
            return new StackTrace(1, true);
#else
            return null;
#endif
        }

        /// <summary>
        ///     先按条件找出第一行，随后按这行RowID订阅。
        ///     (按RowID订阅的快速模式，不处理索引值的变化)
        /// </summary>
        /// <remarks>
        ///     订阅推送是尽力而为的最终一致：正常负载下约 99% 的情况能收到最新数据；
        ///     服务端 Redis 压力过大（副本复制延迟超过约 100ms）时，可能残留旧数据，
        ///     直到该行下次变更。需要强一致的判断请放在服务端 System 里。
        ///     Best-effort and eventually consistent: about 99% of the time you get
        ///     the latest data; when the server's Redis is overloaded (replica lag
        ///     above ~100 ms), stale data may remain until the row changes again.
        ///     Make strongly consistent decisions in a server System.
        /// </remarks>
        /// <typeparam name="T">组件类型。</typeparam>
        /// <param name="index">索引字段名。</param>
        /// <param name="value">索引值。</param>
        /// <param name="onResponse">回调：订阅对象、是否取消、异常信息。</param>
        /// <param name="componentName">组件名；为空时取 <typeparamref name="T" /> 类型名。</param>
        /// <param name="group">
        ///     复合索引（服务端 <c>property_field(groupby=...)</c>）的分组值 {分组列: 值}，要给出
        ///     全部分组列；单列索引不给。值只能是整数、bool、刚好是整数的浮点、string、byte[]，
        ///     不合法、或在 <c>id</c> 上给了 group 时抛 <see cref="ArgumentException" />。字典会被
        ///     拷贝，之后再改不影响这次订阅。需要支持复合索引的服务器。
        ///     Group values {group column: value} of a composite (groupby) index; all
        ///     group columns are required. Leave null for single-column indexes. Throws
        ///     <see cref="ArgumentException" /> on invalid values. The dictionary is
        ///     copied.
        /// </param>
        public void WatchRowSync<T>(
            string index, object value,
            Action<RowSubscription<T>, bool, Exception> onResponse,
            string componentName = null,
            IReadOnlyDictionary<string, object> group = null)
            where T : IBaseComponent
        {
            // 一进来就检查、拷贝 group：调用方事后改字典不影响这次订阅
            var groupCopy = CopyGroup(index, group);
            // 每个调用方发一个自己的句柄，同一订阅的句柄共用一个核心
            var creationTrace = CaptureCreationTrace();
            WatchRowCoreSync<T>(index, value,
                (core, canceled, ex) =>
                    onResponse(core?.CreateHandle(creationTrace), canceled, ex),
                componentName, null, creationTrace, groupCopy);
        }

        // 核心级的 WatchRow：已订阅过就回已有的核心，否则发 sub、建核心并登记。回调拿到的
        // 核心可能还没有句柄，由调用方发句柄。reusable：会话恢复时把回复绑回原来的核心。
        // group：调用方已用 CopyGroup 检查、拷贝过（id 上不会有 group）
        internal void WatchRowCoreSync<T>(
            string index, object value,
            Action<ManagedRowCore<T>, bool, Exception> onResponse,
            string componentName,
            ManagedRowCore<T> reusable,
            StackTrace creationTrace,
            IReadOnlyDictionary<string, object> group = null)
            where T : IBaseComponent
        {
            if (group is { Count: 0 })
                group = null;
            if (!EnsureConnected("WatchRowSync"))
            {
                onResponse(null, true, null);
                return;
            }

            componentName ??= typeof(T).Name;
            // 如果index是id，我们可以事先判断是否已经订阅过
            if (index == IndexId)
            {
                var predictID = MakeSubId(componentName, IndexId, value, null, 1, false);
                if (TryGetExistingCore<ManagedRowCore<T>, T>(predictID,
                        out var existingCore) && existingCore != reusable)
                {
                    onResponse(TakeOver(reusable, existingCore), false, null);
                    return;
                }
            }

            // 向服务器订阅。只在有 group 时才发第 6 项，旧服务器照旧收 5 项
            var payload = group == null
                ? new[] { CommandSub, componentName, QueryGet, index, value }
                : new[] { CommandSub, componentName, QueryGet, index, value, group };
            var traceId = InspectorCollector.InterceptRequest(QueryGet, componentName,
                payload);
            SendSubRequest(payload, (response, cancel) =>
            {
                if (cancel)
                {
                    InspectorCollector.CompleteRequest(traceId, "canceled");
                    onResponse(null, true, null);
                    return;
                }

                ManagedRowCore<T> rowCore = null;
                try
                {
                    var subID = (string)response[1];
                    // 如果查询到值；依然是重复订阅时沿用已有的核心
                    if (subID != null && reusable is { IsClosed: true })
                        DropClosedRestore(subID);
                    else if (subID != null &&
                             TryGetExistingCore<ManagedRowCore<T>, T>(subID,
                                 out var existingCore) && existingCore != reusable)
                        rowCore = TakeOver(reusable, existingCore);
                    else if (subID != null)
                    {
                        var data = ((JsonObject)response[2]).To<T>();
                        if (reusable == null)
                        {
                            rowCore = new ManagedRowCore<T>(
                                subID, componentName, data, this, creationTrace);
                        }
                        else
                        {
                            reusable.Rebind(subID, data, this);
                            rowCore = reusable;
                        }

                        Subscriptions.Add(subID, rowCore);
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
                onResponse(rowCore, false, null);
            }, traceId);
        }

        /// <summary>
        ///     同 WatchRowSync<T>，但使用默认字典类型。
        /// </summary>
        /// <param name="group">复合索引的分组值，见 WatchRowSync&lt;T&gt;。</param>
        public void WatchRowSync(
            string index, object value,
            Action<RowSubscription<DictComponent>, bool, Exception> onResponse,
            string componentName = null,
            IReadOnlyDictionary<string, object> group = null) =>
            WatchRowSync<DictComponent>(index, value, onResponse, componentName, group);

        /// <summary>
        ///     订阅索引范围数据。（Range)
        /// </summary>
        /// <remarks>
        ///     订阅推送是尽力而为的最终一致：正常负载下约 99% 的情况能收到最新数据；
        ///     服务端 Redis 压力过大（副本复制延迟超过约 100ms）时，可能残留旧数据，
        ///     直到该行下次变更。需要强一致的判断请放在服务端 System 里。
        ///     Best-effort and eventually consistent: about 99% of the time you get
        ///     the latest data; when the server's Redis is overloaded (replica lag
        ///     above ~100 ms), stale data may remain until the row changes again.
        ///     Make strongly consistent decisions in a server System.
        /// </remarks>
        /// <typeparam name="T">组件类型。</typeparam>
        /// <param name="index">索引字段名。</param>
        /// <param name="left">范围左边界。</param>
        /// <param name="right">范围右边界。</param>
        /// <param name="limit">最大返回条数。</param>
        /// <param name="onResponse">回调：订阅对象、是否取消、异常信息。</param>
        /// <param name="desc">是否降序。</param>
        /// <param name="force">无数据时是否仍保持订阅。</param>
        /// <param name="componentName">组件名；为空时取 <typeparamref name="T" /> 类型名。</param>
        /// <param name="group">
        ///     复合索引（服务端 <c>property_field(groupby=...)</c>）的分组值 {分组列: 值}，要给出
        ///     全部分组列，例如 <c>level</c> 按 <c>zone</c> 分组时，订阅 zone=3 里 level 在
        ///     [left, right] 的行要给 <c>{ ["zone"] = 3 }</c>；单列索引不给。值只能是整数、bool、
        ///     刚好是整数的浮点、string、byte[]，不合法时抛 <see cref="ArgumentException" />。
        ///     字典会被拷贝，之后再改不影响这次订阅及其重连恢复。需要支持复合索引的服务器。
        ///     Group values {group column: value} of a composite (groupby) index; all
        ///     group columns are required. Leave null for single-column indexes. Throws
        ///     <see cref="ArgumentException" /> on invalid values. The dictionary is
        ///     copied.
        /// </param>
        public void WatchRangeSync<T>(
            string index, object left, object right, int limit,
            Action<IndexSubscription<T>, bool, Exception> onResponse,
            bool desc = false, bool force = true,
            string componentName = null,
            IReadOnlyDictionary<string, object> group = null)
            where T : IBaseComponent
        {
            // 一进来就检查、拷贝 group：调用方事后改字典不影响这次订阅
            var groupCopy = CopyGroup(index, group);
            var creationTrace = CaptureCreationTrace();
            WatchRangeCoreSync<T>(index, left, right, limit,
                (core, canceled, ex) =>
                    onResponse(core?.CreateHandle(creationTrace), canceled, ex),
                desc, force, componentName, null, creationTrace, groupCopy);
        }

        // 核心级的 WatchRange，见 WatchRowCoreSync。group：调用方已用 CopyGroup 检查、拷贝过
        internal void WatchRangeCoreSync<T>(
            string index, object left, object right, int limit,
            Action<ManagedIndexCore<T>, bool, Exception> onResponse,
            bool desc, bool force,
            string componentName,
            ManagedIndexCore<T> reusable,
            StackTrace creationTrace,
            IReadOnlyDictionary<string, object> group = null)
            where T : IBaseComponent
        {
            if (group is { Count: 0 })
                group = null;
            if (!EnsureConnected("WatchRangeSync"))
            {
                onResponse(null, true, null);
                return;
            }

            componentName ??= typeof(T).Name;

            // 先要组合sub_id看看是否已订阅过
            var predictID = MakeSubId(
                componentName, index, left, right, limit, desc, group);
            if (TryGetExistingCore<ManagedIndexCore<T>, T>(predictID,
                    out var existingCore) && existingCore != reusable)
            {
                onResponse(TakeOver(reusable, existingCore), false, null);
                return;
            }

            // 发送订阅请求。只在有 group 时才发第 10 项，旧服务器照旧收 9 项
            var payload = group == null
                ? new[]
                {
                    CommandSub, componentName, QueryRange, index, left, right, limit,
                    desc, force
                }
                : new[]
                {
                    CommandSub, componentName, QueryRange, index, left, right, limit,
                    desc, force, group
                };
            var traceId = InspectorCollector.InterceptRequest(QueryRange,
                componentName, payload);
            SendSubRequest(payload, (response, cancel) =>
            {
                if (cancel)
                {
                    InspectorCollector.CompleteRequest(traceId, "canceled");
                    onResponse(null, true, null);
                    return;
                }

                ManagedIndexCore<T> idxCore = null;
                try
                {
                    var subID = (string)response[1];
                    // 如果查询到值；依然是重复订阅时沿用已有的核心
                    if (subID != null && reusable is { IsClosed: true })
                        DropClosedRestore(subID);
                    else if (subID != null &&
                             TryGetExistingCore<ManagedIndexCore<T>, T>(subID,
                                 out var existingCore) && existingCore != reusable)
                        idxCore = TakeOver(reusable, existingCore);
                    else if (subID != null)
                    {
                        var rawRows = (JsonObject)response[2];
                        var rows = rawRows.ToList<T>();
                        if (reusable == null)
                        {
                            idxCore = new ManagedIndexCore<T>(
                                subID, componentName, rows, this, creationTrace);
                        }
                        else
                        {
                            reusable.Rebind(subID, rows, this);
                            idxCore = reusable;
                        }

                        idxCore.ConfigureRestoreQuery(
                            index, left, right, limit, desc, force, group);
                        Subscriptions.Add(subID, idxCore);
                    }

                    // 客户端拼的 predictID 可能和服务端的 sub_id 格式不同（带 group 时一定不同），
                    // 记下来供下次查重
                    if (idxCore != null)
                        Subscriptions.AddAlias(predictID, subID);
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
                onResponse(idxCore, false, null);
            }, traceId);
        }

        /// <summary>
        ///     按字典类型，订阅索引范围数据。（Range)
        /// </summary>
        /// <param name="group">复合索引的分组值，见 WatchRangeSync&lt;T&gt;。</param>
        public void WatchRangeSync(
            string componentName, string index, object left, object right, int limit,
            Action<IndexSubscription<DictComponent>, bool, Exception> onResponse,
            bool desc = false, bool force = true,
            IReadOnlyDictionary<string, object> group = null) =>
            WatchRangeSync(index, left, right, limit, onResponse, desc, force,
                componentName, group);

        /// <summary>
        ///     订阅整张表。（Table）
        ///     与 WatchRange 语义独立：不管表有多少行，服务端只占一个订阅、只订一个频道，
        ///     初始返回你有权限看到的全部行，之后按行推送增量。适合"行多、行小、很少变"的表，
        ///     如所有玩家的名字。代价是该表任何写入都会触发通知，高频写入的表请用 WatchRange。
        ///     服务端组件必须声明 table_sub=True，否则返回 null；服务端还按
        ///     MAX_TABLE_SUBSCRIPTION_ROWS 限制行数，超过也返回 null。
        ///     The server component must be declared with table_sub=True, otherwise null.
        /// </summary>
        /// <remarks>
        ///     订阅推送是尽力而为的最终一致：正常负载下约 99% 的情况能收到最新数据；
        ///     服务端 Redis 压力过大（副本复制延迟超过约 100ms）时，可能残留旧数据，
        ///     直到该行下次变更。需要强一致的判断请放在服务端 System 里。
        ///     Best-effort and eventually consistent: about 99% of the time you get
        ///     the latest data; when the server's Redis is overloaded (replica lag
        ///     above ~100 ms), stale data may remain until the row changes again.
        ///     Make strongly consistent decisions in a server System.
        /// </remarks>
        /// <typeparam name="T">组件类型。</typeparam>
        /// <param name="onResponse">回调：订阅对象、是否取消、异常信息。</param>
        /// <param name="componentName">组件名；为空时取 <typeparamref name="T" /> 类型名。</param>
        public void WatchTableSync<T>(
            Action<IndexSubscription<T>, bool, Exception> onResponse,
            string componentName = null)
            where T : IBaseComponent
        {
            var creationTrace = CaptureCreationTrace();
            WatchTableCoreSync<T>(
                (core, canceled, ex) =>
                    onResponse(core?.CreateHandle(creationTrace), canceled, ex),
                componentName, null, creationTrace);
        }

        // 核心级的 WatchTable，见 WatchRowCoreSync
        internal void WatchTableCoreSync<T>(
            Action<ManagedIndexCore<T>, bool, Exception> onResponse,
            string componentName,
            ManagedIndexCore<T> reusable,
            StackTrace creationTrace)
            where T : IBaseComponent
        {
            if (!EnsureConnected("WatchTableSync"))
            {
                onResponse(null, true, null);
                return;
            }

            componentName ??= typeof(T).Name;

            // 先要组合sub_id看看是否已订阅过
            var predictID = MakeTableSubId(componentName);
            if (TryGetExistingCore<ManagedIndexCore<T>, T>(predictID,
                    out var existingCore) && existingCore != reusable)
            {
                onResponse(TakeOver(reusable, existingCore), false, null);
                return;
            }

            // 发送订阅请求
            var payload = new object[] { CommandSub, componentName, QueryTable };
            var traceId = InspectorCollector.InterceptRequest(QueryTable,
                componentName, payload);
            SendSubRequest(payload, (response, cancel) =>
            {
                if (cancel)
                {
                    InspectorCollector.CompleteRequest(traceId, "canceled");
                    onResponse(null, true, null);
                    return;
                }

                ManagedIndexCore<T> tblCore = null;
                try
                {
                    var subID = (string)response[1];
                    // 无权限或行数超限时服务端返回null；依然是重复订阅时沿用已有的核心
                    if (subID != null && reusable is { IsClosed: true })
                        DropClosedRestore(subID);
                    else if (subID != null &&
                             TryGetExistingCore<ManagedIndexCore<T>, T>(subID,
                                 out var existingCore) && existingCore != reusable)
                        tblCore = TakeOver(reusable, existingCore);
                    else if (subID != null)
                    {
                        var rawRows = (JsonObject)response[2];
                        var rows = rawRows.ToList<T>();
                        if (reusable == null)
                        {
                            tblCore = new ManagedIndexCore<T>(
                                subID, componentName, rows, this, creationTrace);
                        }
                        else
                        {
                            reusable.Rebind(subID, rows, this);
                            tblCore = reusable;
                        }

                        tblCore.ConfigureRestoreTable();
                        Subscriptions.Add(subID, tblCore);
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
                onResponse(tblCore, false, null);
            }, traceId);
        }

        /// <summary>
        ///     按字典类型，订阅整张表。（Table）
        /// </summary>
        public void WatchTableSync(
            string componentName,
            Action<IndexSubscription<DictComponent>, bool, Exception> onResponse) =>
            WatchTableSync(onResponse, componentName);

        /// <summary>
        ///     取消指定订阅。有订阅请求还没收到回复时，unsub 推迟到它们都回复之后再发；
        ///     期间同一 sub_id 又被订上的话就不发了。
        ///     If subscribe requests are still awaiting replies, the unsub is deferred
        ///     until they are answered, and dropped if the same sub_id got subscribed
        ///     again meanwhile.
        /// </summary>
        /// <param name="subID">订阅 ID。</param>
        /// <param name="from">取消来源说明（用于日志）。</param>
        public void Unsubscribe(string subID, string from)
        {
            if (!Subscriptions.Contains(subID)) return;
            Subscriptions.Remove(subID);
            UnsubscribeRemote(subID, from);
        }

        // 会话恢复（reusable）撞上表里同 sub_id 的另一个核心 existing（重连后、恢复前有人用
        // 裸 HeTuClient 订了同一订阅）：两者对应服务端同一个订阅。用 existing 的数据重绑
        // reusable（它的句柄收到差异和 OnResynced），并入 existing 的句柄（看到的数据不变），
        // 登记 reusable 取代它，会话的核心实例不变。不是恢复时照常沿用已有的核心
        private ManagedRowCore<T> TakeOver<T>(ManagedRowCore<T> reusable,
            ManagedRowCore<T> existing)
            where T : IBaseComponent
        {
            if (reusable == null)
                return existing;
            if (reusable.IsClosed)
                return null;
            reusable.Rebind(existing.SubId, existing.Data, this);
            reusable.AbsorbHandlesFrom(existing);
            Subscriptions.Add(existing.SubId, reusable);
            return reusable;
        }

        private ManagedIndexCore<T> TakeOver<T>(ManagedIndexCore<T> reusable,
            ManagedIndexCore<T> existing)
            where T : IBaseComponent
        {
            if (reusable == null)
                return existing;
            if (reusable.IsClosed)
                return null;
            reusable.Rebind(existing.SubId, existing.Rows.Values.ToList(), this);
            reusable.AbsorbHandlesFrom(existing);
            Subscriptions.Add(existing.SubId, reusable);
            return reusable;
        }

        // 恢复的 sub 在途时，核心的最后一个句柄已 Dispose（当时 stale，没发 unsub）：回复不登记、
        // 不算恢复成功；这次恢复让服务端又订上了，表里没有别的核心持有这个 sub_id 就退掉
        private void DropClosedRestore(string subID)
        {
            if (!Subscriptions.Contains(subID))
                UnsubscribeRemote(subID, "Dispose");
        }

        private void UnsubscribeRemote(string subID, string from)
        {
            if (State != ConnectionState.Connected)
                return;
            if (_inFlightSubs > 0)
            {
                // 有 sub 在途，它可能落到这个订阅上：等它回复后再决定发不发
                if (!_pendingUnsubs.Exists(p => p.SubId == subID))
                    _pendingUnsubs.Add((subID, from));
                return;
            }

            SendUnsub(subID, from);
        }

        private void SendUnsub(string subID, string from)
        {
            // 同一 sub_id 若还挂着一条（在途回复落到它上面后又被当场退订），这条发了它就不用再发
            _pendingUnsubs.RemoveAll(p => p.SubId == subID);
            var payload = new object[] { CommandUnsub, subID };
            var traceId = InspectorCollector.InterceptRequest(CommandUnsub, subID,
                payload, InspectorTraceCompletionMode.AfterSend);
            SendRequest(payload, null, traceId);
            Logger.Instance.Info($"因BaseSubscription {from}，已取消订阅 {subID}");
        }

        // 发 sub 请求：计入在途数；回复（或取消）处理完后看看挂着的 unsub 能不能发
        private void SendSubRequest(object payload, ResponseManager.Callback callback,
            string traceId)
        {
            SendRequest(payload, (response, cancel) =>
            {
                // 重连 / Close 时计数已归零，那之后才回调的不再减成负数
                if (_inFlightSubs > 0)
                    _inFlightSubs--;
                try
                {
                    callback(response, cancel);
                }
                finally
                {
                    FlushPendingUnsubs();
                }
            }, traceId);
            // 编码失败时 SendRequest 直接抛出、回调不会登记，所以发出去了才计数
            _inFlightSubs++;
        }

        // 在途的 sub 都回复了：挂着的 unsub 里，sub_id 又被登记了的（在途的回复落到了它上面，
        // 服务端那边还订着）丢掉，其余发出
        private void FlushPendingUnsubs()
        {
            if (_inFlightSubs > 0 || _pendingUnsubs.Count == 0)
                return;
            var pending = _pendingUnsubs.ToArray();
            _pendingUnsubs.Clear();
            if (State != ConnectionState.Connected)
                return;
            foreach (var (subID, from) in pending)
            {
                if (!Subscriptions.Contains(subID))
                    SendUnsub(subID, from);
            }
        }

        // 核心的最后一个句柄 Dispose 时调用：只退登记着的这个核心。旧连接留下的核心晚些
        // 才 Dispose 时，表里同 sub_id 的可能已是重连后新订的，不能把它退掉。
        internal void Unsubscribe(ManagedSubscriptionCore core)
        {
            if (!Subscriptions.TryGet(core.SubId, out var registered) ||
                registered != core)
                return;
            Unsubscribe(core.SubId, "Dispose");
        }

        protected virtual void OnReceived(byte[] buffer)
        {
            // 解码消息
            // Logger.Instance.Info($"收到消息: {decoded}");
            if (Pipeline.Decode(buffer, out var decodeMetrics) is not object[]
                    structuredMsg ||
                structuredMsg.Length == 0)
                return;

            var messageType = structuredMsg[0] as string;
            switch (messageType)
            {
                case MessageResponse:
                case MessageSubed:
                case MessageReject:
                case MessageError:
                    // 这些都是 round trip 响应，有对应的请求等待队列
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
                        subscribed.CreationTrace,
                        decodeMetrics.SourceSizeBytes,
                        decodeMetrics.TransportSizeBytes);
                    subscribed.UpdateRows(rows);
                    break;
                case MessageSync:
                    // 等推送的调用引起的推送都已先到（按 id 认，不占回复顺序）
                    HandleSync(structuredMsg);
                    break;
            }
        }

        // 一次还没完成的等推送调用（rpcs）
        private sealed class PendingPushCall
        {
            public PendingPushCall(int syncId,
                Action<JsonObject, CallOutcome, string> onResponse)
            {
                SyncId = syncId;
                OnResponse = onResponse;
            }

            public int SyncId { get; }
            public Action<JsonObject, CallOutcome, string> OnResponse { get; }
            public string TraceId { get; set; }

            // 按顺序的回复（rsp）已到：payload 先存着，等 sync
            public bool RspArrived { get; set; }
            public object Response { get; set; }

            // ["sync", id] 已到
            public bool Synced { get; set; }
        }
    }
}
