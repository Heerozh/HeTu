using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

namespace HeTu
{
    /// <summary>纯 .NET 的 WebSocket 传输：包 ClientWebSocket，事件化收/发/关；发送串行化。
    /// 事件在线程池线程触发，由 HeadlessHeTuClient 转投到 SerialExecutor。
    /// 单次使用：每个实例只应 Connect 一次（每连接一个实例，重复 Connect 会泄漏前一个 socket）。</summary>
    internal sealed class WebSocketTransport : IDisposable
    {
        private ClientWebSocket _ws;
        private CancellationTokenSource _cts;
        private readonly SemaphoreSlim _sendLock = new(1, 1);

        public event Action OnOpen;
        public event Action<byte[]> OnMessage;
        public event Action<string> OnClose;   // null = 正常关闭
        public event Action<string> OnError;

        public WebSocketState State => _ws?.State ?? WebSocketState.None;

        public void Connect(string url) => _ = ConnectAsync(url);

        private async Task ConnectAsync(string url)
        {
            _ws = new ClientWebSocket();
            _cts = new CancellationTokenSource();
            try
            {
                await _ws.ConnectAsync(new Uri(url), _cts.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                OnError?.Invoke(ex.Message);
                OnClose?.Invoke(ex.Message);
                return;
            }
            OnOpen?.Invoke();
            _ = ReceiveLoop();
        }

        private async Task ReceiveLoop()
        {
            var buf = new byte[16 * 1024];
            using var ms = new MemoryStream();
            try
            {
                while (_ws.State == WebSocketState.Open)
                {
                    ms.SetLength(0);
                    WebSocketReceiveResult r;
                    do
                    {
                        r = await _ws.ReceiveAsync(new ArraySegment<byte>(buf), _cts.Token)
                            .ConfigureAwait(false);
                        if (r.MessageType == WebSocketMessageType.Close)
                        {
                            OnClose?.Invoke(null);
                            return;
                        }
                        ms.Write(buf, 0, r.Count);
                    } while (!r.EndOfMessage);
                    OnMessage?.Invoke(ms.ToArray());
                }
                // State left Open (e.g. Aborted after Close()) — still fire OnClose
                OnClose?.Invoke(null);
            }
            catch (OperationCanceledException) { OnClose?.Invoke(null); }
            catch (Exception ex) { OnError?.Invoke(ex.Message); OnClose?.Invoke(ex.Message); }
        }

        public void Send(byte[] data) => _ = SendAsync(data);

        private async Task SendAsync(byte[] data)
        {
            await _sendLock.WaitAsync().ConfigureAwait(false);
            try
            {
                // 捕获到局部，避免 Connect 竞态下 _cts 仍为 null（State!=Open 时整体跳过，
                // 不访问 _cts.Token，故连接前/关闭后 Send 静默丢弃，不误发 OnError）。
                var ws = _ws;
                var cts = _cts;
                if (ws is { State: WebSocketState.Open } && cts != null)
                    await ws.SendAsync(new ArraySegment<byte>(data),
                        WebSocketMessageType.Binary, true, cts.Token).ConfigureAwait(false);
            }
            catch (Exception ex) { OnError?.Invoke(ex.Message); }
            finally { _sendLock.Release(); }
        }

        public void Close()
        {
            try { _cts?.Cancel(); _ws?.Abort(); } catch { /* ignore */ }
        }

        public void Dispose()
        {
            Close();
            _ws?.Dispose();
            _cts?.Dispose();
            _sendLock.Dispose();
        }
    }
}
