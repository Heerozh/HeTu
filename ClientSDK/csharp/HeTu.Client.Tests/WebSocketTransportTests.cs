using System;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using HeTu;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using NUnit.Framework;

namespace HeTu.Client.Tests
{
    public class WebSocketTransportTests
    {
        private WebApplication _app;
        private string _wsUrl;

        [SetUp]
        public async Task StartEchoServer()
        {
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseUrls("http://127.0.0.1:0");   // 端口 0：OS 分配，消除 FreePort 的 TOCTOU
            _app = builder.Build();
            _app.UseWebSockets();
            _app.Map("/ws", async (HttpContext ctx) =>
            {
                if (!ctx.WebSockets.IsWebSocketRequest) { ctx.Response.StatusCode = 400; return; }
                using var ws = await ctx.WebSockets.AcceptWebSocketAsync();
                var buf = new byte[4096];
                try
                {
                    while (ws.State == WebSocketState.Open)
                    {
                        var r = await ws.ReceiveAsync(buf, CancellationToken.None);
                        if (r.MessageType == WebSocketMessageType.Close)
                        {
                            await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
                            break;
                        }
                        await ws.SendAsync(new ArraySegment<byte>(buf, 0, r.Count),
                            WebSocketMessageType.Binary, true, CancellationToken.None);
                    }
                }
                // 客户端 Abort（不走 close 握手）会让 ReceiveAsync 抛 WebSocketException，
                // 属测试预期；干净吞掉以免 Kestrel 打出 fail: 级噪声日志。
                catch (WebSocketException) { }
                catch (OperationCanceledException) { }
            });
            await _app.StartAsync();
            // 读取 OS 实际分配的绑定地址（含真实端口）
            var addr = _app.Urls.First();                    // "http://127.0.0.1:<port>"
            _wsUrl = addr.Replace("http://", "ws://") + "/ws";
        }

        [TearDown]
        public async Task StopEchoServer() => await _app.DisposeAsync();

        [Test]
        public async Task ConnectSendReceiveClose()
        {
            using var t = new WebSocketTransport();
            var opened = new TaskCompletionSource();
            var echoed = new TaskCompletionSource<byte[]>();
            var closed = new TaskCompletionSource<string>();
            t.OnOpen += () => opened.TrySetResult();
            t.OnMessage += b => echoed.TrySetResult(b);
            t.OnClose += r => closed.TrySetResult(r);

            t.Connect(_wsUrl);
            Assert.That(await Task.WhenAny(opened.Task, Task.Delay(5000)) == opened.Task, Is.True, "应触发 OnOpen");

            t.Send(new byte[] { 1, 2, 3, 4 });
            var got = await Task.WhenAny(echoed.Task, Task.Delay(5000)) == echoed.Task
                ? echoed.Task.Result : null;
            Assert.That(got, Is.EqualTo(new byte[] { 1, 2, 3, 4 }), "应原样回显");

            t.Close();
            await Task.WhenAny(closed.Task, Task.Delay(5000));
            Assert.That(closed.Task.IsCompleted, Is.True, "Close 后应触发 OnClose");
        }

        [Test]
        public async Task BadUrlRaisesError()
        {
            using var t = new WebSocketTransport();
            var err = new TaskCompletionSource<string>();
            t.OnError += m => err.TrySetResult(m);
            t.Connect("ws://127.0.0.1:1/nope"); // 拒绝连接的端口
            Assert.That(await Task.WhenAny(err.Task, Task.Delay(5000)) == err.Task, Is.True,
                "连接失败应触发 OnError");
        }
    }
}
