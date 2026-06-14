using System;
using System.Threading;
using System.Threading.Tasks;

namespace HeTu
{
    /// <summary>纯 .NET 的 HeTu 客户端：每实例一条独立连接（无单例）。
    /// 所有协议访问经 SerialExecutor 串行化（见 spec §7）。</summary>
    public sealed class HeadlessHeTuClient : HeTuClientBase
    {
        private readonly SerialExecutor _pump = new();
        private WebSocketTransport _socket;

        // ---- 传输接缝：事件统一转投到泵线程 ----
        protected override void ConnectCore(string url, Action onConnected,
            Action<byte[]> onMessage, Action<string> onClose, Action<string> onError)
        {
            _socket = new WebSocketTransport();
            _socket.OnOpen += () => _pump.Post(onConnected);
            _socket.OnMessage += data => _pump.Post(() => onMessage(data));
            _socket.OnClose += reason => _pump.Post(() => onClose(reason));
            _socket.OnError += msg => _pump.Post(() => onError(msg));
            _socket.Connect(url);
        }

        protected override void SendCore(byte[] data) => _socket?.Send(data);

        protected override void CloseCore()
        {
            _socket?.Close();
            _socket = null;
            State = ConnectionState.Disconnected;
        }

        public override void Dispose()
        {
            // 先停并排空泵（其 Dispose 会 join 泵线程），此后不再有任何动作访问基类状态，
            // 从而 Pipeline 可安全地最后释放，不被排空中的动作触碰（spec §7 单线程不变量延伸到 teardown）。
            _pump.Dispose();
            // 关闭并释放底层 socket（释放其 CTS/信号量/ws）。
            _socket?.Close();
            _socket?.Dispose();
            _socket = null;
            State = ConnectionState.Disconnected;
            // 取消仍在等待的响应回调，避免 await 中的 CallSystem/WatchXxx 永久挂起（泵已停，无竞态）。
            ResponseQueue.CancelAll("disposed");
            // 最后释放 Pipeline。
            base.Dispose();
        }

        /// <summary>主动关闭连接：路由到泵线程执行，保持 spec §7 单线程不变量
        /// （base.Close 会改动 ResponseQueue/Subscriptions/State，不能在调用方线程并发执行）。</summary>
        public override void Close() => _pump.Post(base.Close);

        // ---- Task 外观 ----
        public Task Connect(string url, string authKey = null, CancellationToken ct = default)
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            void Cleanup() { OnConnected -= OnConn; OnClosed -= OnClose; }
            void OnConn() { Cleanup(); tcs.TrySetResult(); }
            void OnClose(string reason) { Cleanup(); tcs.TrySetException(new Exception(reason ?? "connection closed")); }
            OnConnected += OnConn;
            OnClosed += OnClose;
            if (ct.CanBeCanceled)
                // CloseCore 写 _socket/State，必须与泵上的 SendCore/状态读取串行化，故改投泵线程。
                ct.Register(() => { Cleanup(); tcs.TrySetCanceled(); _pump.Post(CloseCore); });
            _pump.Post(() =>
            {
                if (authKey != null) ConfigureCryptoAuthKey(authKey);
                ConnectSync(url);
            });
            return tcs.Task;
        }

        /// <summary>等待连接关闭：完成于基类 OnClosed，返回关闭原因（null = 正常关闭）。
        /// 若调用时已处于断开态则立即完成，避免永久挂起。</summary>
        public Task<string> WaitClosedAsync()
        {
            var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            void OnClose(string reason) { OnClosed -= OnClose; tcs.TrySetResult(reason); }
            OnClosed += OnClose;
            // 订阅时若已断开，OnClosed 不会再触发，立即完成以免永久挂起。
            if (State == ConnectionState.Disconnected)
            {
                OnClosed -= OnClose;
                tcs.TrySetResult(null);
            }
            return tcs.Task;
        }

        public Task<JsonObject> CallSystem(string systemName, params object[] args)
        {
            var tcs = new TaskCompletionSource<JsonObject>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pump.Post(() => CallSystemSync(systemName, args, (resp, outcome, code) =>
            {
                switch (outcome)
                {
                    case CallOutcome.Canceled:
                        tcs.TrySetCanceled();
                        break;
                    case CallOutcome.Rejected:
                        tcs.TrySetException(
                            new HeTuCallRejectedException(systemName, code));
                        break;
                    default:
                        tcs.TrySetResult(resp);
                        break;
                }
            }));
            return tcs.Task;
        }

        public Task<IndexSubscription<T>> WatchRange<T>(string index, object left, object right,
            int limit, bool desc = false, bool force = true, string componentName = null)
            where T : IBaseComponent
        {
            var tcs = new TaskCompletionSource<IndexSubscription<T>>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pump.Post(() => WatchRangeSync<T>(index, left, right, limit, (sub, cancel, ex) =>
            {
                if (cancel) tcs.TrySetCanceled();
                else if (ex != null) tcs.TrySetException(ex);
                else tcs.TrySetResult(sub);
            }, desc, force, componentName));
            return tcs.Task;
        }

        public Task<RowSubscription<T>> WatchRow<T>(string index, object value,
            string componentName = null) where T : IBaseComponent
        {
            var tcs = new TaskCompletionSource<RowSubscription<T>>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pump.Post(() => WatchRowSync<T>(index, value, (sub, cancel, ex) =>
            {
                if (cancel) tcs.TrySetCanceled();
                else if (ex != null) tcs.TrySetException(ex);
                else tcs.TrySetResult(sub);
            }, componentName));
            return tcs.Task;
        }
    }
}
