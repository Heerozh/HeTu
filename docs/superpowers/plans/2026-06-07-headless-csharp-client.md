# HeTu.Client（headless 纯 .NET 客户端库）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `HeTu/ClientSDK/csharp/` 新建一个纯 .NET（net8.0）客户端库 `HeTu.Client`，链接复用 unity 包的协议源码，提供 `HeadlessHeTuClient`（基于 `System.Net.WebSockets.ClientWebSocket`），可在无 Unity 进程内开多条并发连接、握手、`CallSystem`、`WatchRange`。

**Architecture:** unity 包 `ClientSDK/unity/cn.hetudb.clientsdk/HeTu/` 里的协议层是唯一真源；本库用 `<Compile Link>` 链接其中的纯文件（排除 `UnityClient.cs`/`SessionClient.cs`/`AssemblyInfo.cs`），加 4 个 NuGet 包，新增 `WebSocketTransport`（WS I/O）、`SerialExecutor`（每连接单线程串行）、`HeadlessHeTuClient`（继承 `HeTuClientBase` + Task 外观）。unity 包仅 `ZlibLayer.cs` 加一处 `#if`。

**Tech Stack:** C# / net8.0、`System.Net.WebSockets.ClientWebSocket`；NuGet `MessagePack`、`BouncyCastle.Cryptography`、`SharpZipLib`、`R3`；测试 NUnit（`<RollForward>Major</RollForward>` 以在本沙箱 net10 运行时执行）。

**配套 spec:** `HeTu/docs/superpowers/specs/2026-06-07-headless-csharp-client-design.md`

## 环境与前置

- 本沙箱：dotnet 10 SDK + NuGet 可用，可构建 net8.0、可经 RollForward 跑 NUnit 测试。仅 10.x 运行时。
- 所有命令工作目录默认 `/workspace-Game/HeTu/ClientSDK/csharp/`（除非注明）。`/workspace-Game/HeTu` 是符号链接 → `/HeTu`；git 操作用 `git -C /workspace-Game/HeTu ...`（当前分支 `dev`）。
- Task 4（集成冒烟）需要一个运行中的 HeTu 应用，标注为目标机/有服务端时执行。

## File Structure

| 文件 | 职责 |
|---|---|
| `csharp/HeTu.Client/HeTu.Client.csproj` | 库工程：链接共享源码 + NuGet 依赖 |
| `csharp/HeTu.Client/InternalsVisibleTo.cs` | 暴露 internal 给测试程序集 |
| `csharp/HeTu.Client/WebSocketTransport.cs` | `ClientWebSocket` 封装：事件化 open/message/close/error，发送串行 |
| `csharp/HeTu.Client/SerialExecutor.cs` | 每连接单线程泵：串行化对 Pipeline/加密的访问 |
| `csharp/HeTu.Client/HeadlessHeTuClient.cs` | 继承 `HeTuClientBase`：实现传输三方法 + Task 外观 |
| `csharp/HeTu.Client.Tests/HeTu.Client.Tests.csproj` | NUnit 测试工程 |
| `csharp/HeTu.Client.Tests/SerialExecutorTests.cs` | 串行执行单测 |
| `csharp/HeTu.Client.Tests/WebSocketTransportTests.cs` | 传输层单测（Kestrel WS echo） |
| `csharp/HeTu.Client.Tests/IntegrationSmokeTests.cs` | 集成冒烟（需服务端，`[Explicit]`） |
| `csharp/.gitignore` | 忽略 bin/obj |
| `unity/.../HeTu/ZlibLayer.cs` | **修改**：zlib 库 `using` 的 `#if`（见 Task 1 Step 2） |

---

### Task 1: 工程脚手架 + 链接共享源码可编译（验证整个复用假设）

**Files:**
- Create: `/workspace-Game/HeTu/ClientSDK/csharp/HeTu.Client/HeTu.Client.csproj`
- Create: `/workspace-Game/HeTu/ClientSDK/csharp/.gitignore`
- Modify: `/workspace-Game/HeTu/ClientSDK/unity/cn.hetudb.clientsdk/HeTu/ZlibLayer.cs`（顶部 using）

- [ ] **Step 1: 写库 csproj**

`csharp/HeTu.Client/HeTu.Client.csproj`：

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
    <AssemblyName>HeTu.Client</AssemblyName>
    <RootNamespace>HeTu</RootNamespace>
    <Nullable>disable</Nullable>
    <LangVersion>latest</LangVersion>
    <ImplicitUsings>disable</ImplicitUsings>
  </PropertyGroup>

  <!-- 链接共享纯协议源码（不拷贝）；排除 Unity 适配器与会重复的 AssemblyInfo -->
  <ItemGroup>
    <Compile Include="..\..\unity\cn.hetudb.clientsdk\HeTu\*.cs"
             Exclude="..\..\unity\cn.hetudb.clientsdk\HeTu\UnityClient.cs;..\..\unity\cn.hetudb.clientsdk\HeTu\SessionClient.cs;..\..\unity\cn.hetudb.clientsdk\HeTu\AssemblyInfo.cs"
             Link="Shared\%(Filename)%(Extension)" />
  </ItemGroup>

  <ItemGroup>
    <PackageReference Include="MessagePack" Version="2.5.198" />
    <PackageReference Include="BouncyCastle.Cryptography" Version="2.4.0" />
    <PackageReference Include="SharpZipLib" Version="1.4.2" />
    <PackageReference Include="R3" Version="1.2.9" />
  </ItemGroup>
</Project>
```

- [ ] **Step 2: 改 `ZlibLayer.cs` 顶部 using（`#if` 切库，Unity 分支不变）**

把文件第 9-11 行：

```csharp
using Unity.SharpZipLib.Zip.Compression;
using Unity.SharpZipLib.Zip.Compression.Streams;
using UnityEngine;
```

替换为：

```csharp
#if UNITY_2022_3_OR_NEWER
using Unity.SharpZipLib.Zip.Compression;
using Unity.SharpZipLib.Zip.Compression.Streams;
using UnityEngine;
#else
using ICSharpCode.SharpZipLib.Zip.Compression;
using ICSharpCode.SharpZipLib.Zip.Compression.Streams;
using Debug = System.Diagnostics.Debug;
#endif
```

（文件内两处 `Debug.Assert(...)` 在两分支下都成立；Unity 客户端仍走真分支。）

- [ ] **Step 3: 写 `.gitignore`**

`csharp/.gitignore`：

```
bin/
obj/
*.user
```

- [ ] **Step 4: 构建库，验证链接的共享源码 + NuGet 全部编过**

Run:
```bash
cd /workspace-Game/HeTu/ClientSDK/csharp/HeTu.Client && dotnet build -c Release
```
Expected: `Build succeeded.` `0 Error(s)`。

排错指引（若失败）：
- 报错来自 `UnityClient.cs`/`SessionClient.cs` → 确认它们在 `Exclude` 列表里。
- `Subscription.cs` 报 `UnityEngine`/`[Preserve]` 缺失 → 确认未定义任何 `UNITY_*` 符号（这些在 `#if UNITY_2022_3_OR_NEWER` 内，本应被排除）。
- `JsonbLayer`/`CryptoLayer` 报 MessagePack/BouncyCastle API 不匹配 → 该 NuGet 大版本与 unity 包不一致，调 `Version` 对齐（见 spec §8）。
- 重复程序集属性 → 确认 `AssemblyInfo.cs` 在 `Exclude` 里。

- [ ] **Step 5: 提交**

```bash
git -C /workspace-Game/HeTu add ClientSDK/csharp/HeTu.Client/HeTu.Client.csproj ClientSDK/csharp/.gitignore ClientSDK/unity/cn.hetudb.clientsdk/HeTu/ZlibLayer.cs
git -C /workspace-Game/HeTu commit -m "ENH: HeTu.Client 工程脚手架 + 链接共享协议源码可编译" -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: SerialExecutor + 单测

**Files:**
- Create: `csharp/HeTu.Client/SerialExecutor.cs`
- Create: `csharp/HeTu.Client/InternalsVisibleTo.cs`
- Create: `csharp/HeTu.Client.Tests/HeTu.Client.Tests.csproj`
- Test: `csharp/HeTu.Client.Tests/SerialExecutorTests.cs`

- [ ] **Step 1: 写测试工程 csproj**

`csharp/HeTu.Client.Tests/HeTu.Client.Tests.csproj`：

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
    <RollForward>Major</RollForward>   <!-- 本沙箱仅 net10 运行时 -->
    <Nullable>disable</Nullable>
    <IsPackable>false</IsPackable>
    <ImplicitUsings>disable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
    <FrameworkReference Include="Microsoft.AspNetCore.App" /> <!-- Task 3 的 Kestrel echo 用 -->
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.11.1" />
    <PackageReference Include="NUnit" Version="4.2.2" />
    <PackageReference Include="NUnit3TestAdapter" Version="4.6.0" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\HeTu.Client\HeTu.Client.csproj" />
  </ItemGroup>
</Project>
```

- [ ] **Step 2: 暴露 internal 给测试**

`csharp/HeTu.Client/InternalsVisibleTo.cs`：

```csharp
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("HeTu.Client.Tests")]
```

- [ ] **Step 3: 写失败测试**

`csharp/HeTu.Client.Tests/SerialExecutorTests.cs`：

```csharp
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using HeTu;
using NUnit.Framework;

namespace HeTu.Client.Tests
{
    public class SerialExecutorTests
    {
        [Test]
        public void AllActionsRunOnOneThreadAndComplete()
        {
            using var exec = new SerialExecutor();
            var threadIds = new ConcurrentBag<int>();
            var done = new CountdownEvent(200);

            // 从多个线程并发 Post，验证它们都在同一根线程串行执行
            Parallel.For(0, 200, i =>
                exec.Post(() =>
                {
                    threadIds.Add(Thread.CurrentThread.ManagedThreadId);
                    done.Signal();
                }));

            Assert.That(done.Wait(5000), Is.True, "200 个动作应全部执行");
            Assert.That(threadIds, Has.Count.EqualTo(200));
            Assert.That(new HashSet<int>(threadIds), Has.Count.EqualTo(1),
                "所有动作必须在同一根泵线程上执行（串行）");
        }
    }
}
```

- [ ] **Step 4: 跑测试看它失败**

Run:
```bash
cd /workspace-Game/HeTu/ClientSDK/csharp && dotnet test HeTu.Client.Tests -c Release
```
Expected: 编译失败 `SerialExecutor 未定义`（类型不存在）。

- [ ] **Step 5: 实现 SerialExecutor**

`csharp/HeTu.Client/SerialExecutor.cs`：

```csharp
using System;
using System.Collections.Concurrent;
using System.Threading;

namespace HeTu
{
    /// <summary>每连接一个：把所有动作排到单线程串行执行，匹配 SDK 的单线程假设
    /// （CryptoLayer/Pipeline 非线程安全，收发须串行）。</summary>
    internal sealed class SerialExecutor : IDisposable
    {
        private readonly BlockingCollection<Action> _q = new();
        private readonly Thread _thread;

        public SerialExecutor()
        {
            _thread = new Thread(Run) { IsBackground = true, Name = "HeTuClientPump" };
            _thread.Start();
        }

        public void Post(Action action)
        {
            if (action != null && !_q.IsAddingCompleted) _q.Add(action);
        }

        private void Run()
        {
            foreach (var a in _q.GetConsumingEnumerable())
            {
                try { a(); }
                catch (Exception ex) { Logger.Instance.Error($"pump action threw: {ex}"); }
            }
        }

        public void Dispose()
        {
            try { _q.CompleteAdding(); } catch { /* 已释放 */ }
        }
    }
}
```

- [ ] **Step 6: 跑测试看它通过**

Run:
```bash
cd /workspace-Game/HeTu/ClientSDK/csharp && dotnet test HeTu.Client.Tests -c Release
```
Expected: `Passed! - Failed: 0, Passed: 1`。

- [ ] **Step 7: 提交**

```bash
git -C /workspace-Game/HeTu add ClientSDK/csharp/HeTu.Client/SerialExecutor.cs ClientSDK/csharp/HeTu.Client/InternalsVisibleTo.cs ClientSDK/csharp/HeTu.Client.Tests/HeTu.Client.Tests.csproj ClientSDK/csharp/HeTu.Client.Tests/SerialExecutorTests.cs
git -C /workspace-Game/HeTu commit -m "ENH: HeTu.Client SerialExecutor 单线程泵 + 单测" -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: WebSocketTransport + Kestrel echo 单测

**Files:**
- Create: `csharp/HeTu.Client/WebSocketTransport.cs`
- Test: `csharp/HeTu.Client.Tests/WebSocketTransportTests.cs`

- [ ] **Step 1: 写失败测试（含一个进程内 Kestrel WS echo 服务）**

`csharp/HeTu.Client.Tests/WebSocketTransportTests.cs`：

```csharp
using System;
using System.Net;
using System.Net.Sockets;
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

        private static int FreePort()
        {
            var l = new TcpListener(IPAddress.Loopback, 0);
            l.Start();
            var port = ((IPEndPoint)l.LocalEndpoint).Port;
            l.Stop();
            return port;
        }

        [SetUp]
        public async Task StartEchoServer()
        {
            var port = FreePort();
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
            _app = builder.Build();
            _app.UseWebSockets();
            _app.Map("/ws", async (HttpContext ctx) =>
            {
                if (!ctx.WebSockets.IsWebSocketRequest) { ctx.Response.StatusCode = 400; return; }
                using var ws = await ctx.WebSockets.AcceptWebSocketAsync();
                var buf = new byte[4096];
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
            });
            await _app.StartAsync();
            _wsUrl = $"ws://127.0.0.1:{port}/ws";
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
```

- [ ] **Step 2: 跑测试看它失败**

Run:
```bash
cd /workspace-Game/HeTu/ClientSDK/csharp && dotnet test HeTu.Client.Tests -c Release --filter WebSocketTransportTests
```
Expected: 编译失败 `WebSocketTransport 未定义`。

- [ ] **Step 3: 实现 WebSocketTransport**

`csharp/HeTu.Client/WebSocketTransport.cs`：

```csharp
using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

namespace HeTu
{
    /// <summary>纯 .NET 的 WebSocket 传输：包 ClientWebSocket，事件化收/发/关；发送串行化。
    /// 事件在线程池线程触发，由 HeadlessHeTuClient 转投到 SerialExecutor。</summary>
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
                if (_ws is { State: WebSocketState.Open })
                    await _ws.SendAsync(new ArraySegment<byte>(data),
                        WebSocketMessageType.Binary, true, _cts.Token).ConfigureAwait(false);
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
```

- [ ] **Step 4: 跑测试看它通过**

Run:
```bash
cd /workspace-Game/HeTu/ClientSDK/csharp && dotnet test HeTu.Client.Tests -c Release --filter WebSocketTransportTests
```
Expected: `Passed! - Failed: 0, Passed: 2`。

- [ ] **Step 5: 提交**

```bash
git -C /workspace-Game/HeTu add ClientSDK/csharp/HeTu.Client/WebSocketTransport.cs ClientSDK/csharp/HeTu.Client.Tests/WebSocketTransportTests.cs
git -C /workspace-Game/HeTu commit -m "ENH: HeTu.Client WebSocketTransport(ClientWebSocket) + Kestrel echo 单测" -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: HeadlessHeTuClient（传输三方法 + Task 外观）

**Files:**
- Create: `csharp/HeTu.Client/HeadlessHeTuClient.cs`

> 本类需依赖前面验证过的 `HeTuClientBase` 成员（已在源码确认）：`protected abstract ConnectCore/CloseCore/SendCore`、`protected ConnectSync(url)`、`protected ConfigureCryptoAuthKey(authKey)`、`protected ConnectionState State`、`public event Action OnConnected`、`public event Action<string> OnClosed`、`internal protected CallSystemSync(name,args,Action<JsonObject,bool>)`、`public WatchRowSync<T>(...)`、`public WatchRangeSync<T>(...)`。

- [ ] **Step 1: 实现 HeadlessHeTuClient**

`csharp/HeTu.Client/HeadlessHeTuClient.cs`：

```csharp
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
```

- [ ] **Step 2: 构建（行为留待 Task 5 集成验证）**

Run:
```bash
cd /workspace-Game/HeTu/ClientSDK/csharp/HeTu.Client && dotnet build -c Release
```
Expected: `Build succeeded.` `0 Error(s)`。

排错：若某成员可见性不符（如 `CallSystemSync` 不可访问）→ 它是 `internal protected`，本类同程序集且为子类，应可访问；确认本类确实在 `HeTu.Client` 程序集内、`namespace HeTu`。

- [ ] **Step 3: 提交**

```bash
git -C /workspace-Game/HeTu add ClientSDK/csharp/HeTu.Client/HeadlessHeTuClient.cs
git -C /workspace-Game/HeTu commit -m "ENH: HeTu.Client HeadlessHeTuClient(传输实现 + Task 外观)" -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: 集成冒烟（需运行中的 HeTu 服务端）

> **执行环境**：需要一个可连的 HeTu 应用（如 ssw-lobby）。本测试用 `[Explicit]` 标注、默认不在普通 `dotnet test` 跑；从环境变量读连接信息，缺失则跳过。在目标机（或本沙箱起一个 lobby）执行。

**Files:**
- Create: `csharp/HeTu.Client.Tests/IntegrationSmokeTests.cs`

- [ ] **Step 1: 写集成冒烟测试**

`csharp/HeTu.Client.Tests/IntegrationSmokeTests.cs`：

```csharp
using System;
using System.Threading.Tasks;
using HeTu;
using NUnit.Framework;

namespace HeTu.Client.Tests
{
    [Explicit("需要运行中的 HeTu 服务端；设置 HETU_URL / HETU_AUTHKEY 后运行")]
    public class IntegrationSmokeTests
    {
        [Test]
        public async Task ConnectAndCallSystem()
        {
            var url = Environment.GetEnvironmentVariable("HETU_URL");      // ws://127.0.0.1:2466/hetu/<inst>
            var authKey = Environment.GetEnvironmentVariable("HETU_AUTHKEY");
            if (string.IsNullOrEmpty(url)) Assert.Ignore("未设置 HETU_URL");

            using var c = new HeadlessHeTuClient();
            await c.Connect(url, authKey).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.That(c.IsConnected, Is.True, "握手后应为已连接");

            // 调一个无需登录的 System 或 echo；具体名以目标 app 为准。
            // 这里以最小可达性为目标：连接 + 握手成功即视为冒烟通过。
        }
    }
}
```

- [ ] **Step 2: 构建（确认能编译；运行需服务端）**

Run:
```bash
cd /workspace-Game/HeTu/ClientSDK/csharp && dotnet build HeTu.Client.Tests -c Release
```
Expected: `Build succeeded.`（普通 `dotnet test` 会因 `[Explicit]` 跳过本测试。）

- [ ] **Step 3: （目标机/有服务端时）运行集成冒烟**

Run:
```bash
HETU_URL='ws://127.0.0.1:2466/hetu/<instance>' HETU_AUTHKEY='<config.yml 的 auth_key>' \
  dotnet test /workspace-Game/HeTu/ClientSDK/csharp/HeTu.Client.Tests -c Release --filter IntegrationSmokeTests
```
Expected: `Passed! - Passed: 1`（连接 + 握手成功）。
注：真正的 login/CallSystem/WatchRange 端到端验证在压测工具（client 仓 plan）的小规模自测里完成。

- [ ] **Step 4: 提交**

```bash
git -C /workspace-Game/HeTu add ClientSDK/csharp/HeTu.Client.Tests/IntegrationSmokeTests.cs
git -C /workspace-Game/HeTu commit -m "TEST: HeTu.Client 集成冒烟(Explicit, 需服务端)" -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## 验收（库）

- `dotnet build -c Release`（库）成功 → 链接共享源码 + 4 个 NuGet + `ZlibLayer` `#if` 全部编过。
- `dotnet test HeTu.Client.Tests`（非 Explicit）全绿：SerialExecutor 串行、WebSocketTransport 收发关。
- `HeadlessHeTuClient` 可多实例化（无单例）。
- Unity 客户端不受影响（`ZlibLayer` 仍走 `#if UNITY_2022_3_OR_NEWER` 真分支）——由 client 仓 plan 的 Unity 编译间接确认，或在 Unity 编辑器构建一次确认。
