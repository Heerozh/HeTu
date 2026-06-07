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
            base.Dispose();
            _socket?.Dispose();
            _pump.Dispose();
        }

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
                ct.Register(() => { Cleanup(); tcs.TrySetCanceled(); CloseCore(); });
            _pump.Post(() =>
            {
                if (authKey != null) ConfigureCryptoAuthKey(authKey);
                ConnectSync(url);
            });
            return tcs.Task;
        }

        public Task<JsonObject> CallSystem(string systemName, params object[] args)
        {
            var tcs = new TaskCompletionSource<JsonObject>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pump.Post(() => CallSystemSync(systemName, args, (resp, cancel) =>
            {
                if (cancel) tcs.TrySetCanceled(); else tcs.TrySetResult(resp);
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
