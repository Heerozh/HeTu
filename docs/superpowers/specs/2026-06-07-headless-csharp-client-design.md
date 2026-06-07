# HeTu.Client · 纯 .NET（headless）客户端库设计

- 日期：2026-06-07
- 仓库：`HeTu`（分支 `dev`）
- 配套使用方 spec：`ssw_re_client/doc/superpowers/specs/2026-06-07-chat-stress-test-design.md`（聊天压测工具，本库首个使用者）
- 状态：设计待评审（heeroz）

## 1. 目标

提供一个 **纯 .NET（net8.0）的 HeTu 客户端库 `HeTu.Client`**，在没有 Unity 引擎的进程里
（控制台 / 服务 / 测试 / 压测）也能：建立 WebSocket 连接并完成握手、`CallSystem` 调用、
`WatchRow` / `WatchRange` 订阅，并能在**一个进程内开多条并发连接**。

核心约束：**不产生第二份协议栈**。Unity 客户端
（`ClientSDK/unity/cn.hetudb.clientsdk/HeTu/`）里的协议层源码是唯一真源，本库通过
`<Compile Link>` **链接共享**这些 `.cs`，绝不拷贝；Unity 包除一处极小的 `#if` 外**零改动**。

`ClientSDK/` 下现已有 `js/`、`unity/`，本库作为第三个兄弟 `csharp/` 落地，是天然的纯 .NET 客户端目标。

## 2. 非目标（首期不做）

- **不**把 Unity 包改造成双用途（不往干净的 UPM 包里塞 NuGet 依赖 / Task API / `#if !UNITY` 杂质）。
- **不**重写任何协议逻辑（握手 / 加密 / 压缩 / 序列化 / 订阅分发全部复用）。
- **不**复用 `HeTuSessionClientBase` 的自动重连 / 订阅恢复封装做 Task 外观（压测要如实暴露断连，
  使用方直接驱动 `HeTuClientBase`，见 §6.2）。`SessionClientBase.cs` 仍会被链接编译（它纯净），
  只是首期不包成 Task facade。
- **不**支持 WebGL；**不**发布 NuGet 包（首期用 `ProjectReference` 引用源码工程即可）。
- **不**做可注入 WebSocket 的测试 seam（首期靠对真实本地服务端的冒烟验证，见 §9）。

## 3. 目录与工程结构

```
HeTu/ClientSDK/
├── js/                       # 既有（空，todo）
├── unity/cn.hetudb.clientsdk # 既有 Unity UPM 包（协议层真源所在）
└── csharp/                   # 新增
    └── HeTu.Client/
        ├── HeTu.Client.csproj
        ├── HeadlessHeTuClient.cs   # 新增：ClientWebSocket 传输 + Task 外观
        └── (链接进来的共享源码以 Shared\ 虚拟目录呈现)
```

`HeTu.Client.csproj` 骨架：

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
    <AssemblyName>HeTu.Client</AssemblyName>   <!-- 区别于 unity asmdef 的 HeTu.ClientSDK -->
    <RootNamespace>HeTu</RootNamespace>        <!-- 共享文件均为 namespace HeTu -->
    <Nullable>disable</Nullable>
    <LangVersion>latest</LangVersion>
  </PropertyGroup>

  <!-- 链接共享纯协议源码（不拷贝）；排除 Unity 适配器与会重复的 AssemblyInfo -->
  <ItemGroup>
    <Compile Include="..\..\unity\cn.hetudb.clientsdk\HeTu\*.cs"
             Exclude="..\..\unity\cn.hetudb.clientsdk\HeTu\UnityClient.cs;
                      ..\..\unity\cn.hetudb.clientsdk\HeTu\SessionClient.cs;
                      ..\..\unity\cn.hetudb.clientsdk\HeTu\AssemblyInfo.cs"
             Link="Shared\%(Filename)%(Extension)" />
  </ItemGroup>

  <ItemGroup>
    <PackageReference Include="MessagePack" Version="2.5.*" />        <!-- 版本需与 unity 端 MessagePack 行为一致，plan 时核对 -->
    <PackageReference Include="BouncyCastle.Cryptography" Version="2.*" />
    <PackageReference Include="SharpZipLib" Version="1.4.*" />
    <PackageReference Include="R3" Version="1.*" />
  </ItemGroup>
</Project>
```

> 关键：`HeadlessHeTuClient.cs` 必须编进**与共享源码同一个程序集** `HeTu.Client`——因为它要继承
> `HeTuClientBase` 并调用 `internal protected` 的 `CallSystemSync`、以及使用 `internal` 的
> `Future`/`Promise` 等类型。使用方（压测工具）只引用本程序集的 **public** 表面。

## 4. 共享源码清单（已逐文件核实 Unity 依赖）

| 处理 | 文件 | 说明 |
|---|---|---|
| **原样链接**（纯净，仅 `System.*`） | `ClientBase.cs`(`HeTuClientBase`) `SessionClientBase.cs` `Future.cs` `Pipeline.cs` `PipelineBuffer.cs` `JsonbLayer.cs` `CryptoLayer.cs` `Response.cs` `Logger.cs` `InspectorTrace.cs` `Annotations.cs` | `Future.cs`/`InspectorTrace.cs` 里出现的 `UniTask`/`Awaitable` 经核实**只是注释或字符串字面量**，非真实依赖 |
| **链接 + R3 NuGet** | `Subscription.cs` | Unity 部分（`using UnityEngine` / `[Preserve]` / `AddTo(GameObject)`）已被 `#if UNITY_2022_3_OR_NEWER` 包住；非 Unity 编译下自动排除，R3 由 NuGet 提供 |
| **排除**（Unity 适配器 / 防冲突） | `UnityClient.cs` `SessionClient.cs` `AssemblyInfo.cs` | 前两者是 Unity 适配器（`new WebSocket()`/`Application.exitCancellationToken`/`Debug`/Awaitable）；`AssemblyInfo.cs` 排除以免与 SDK 自动生成的程序集属性重复（其内若有 `InternalsVisibleTo` 按需在本工程另行声明） |
| **唯一 SDK 内改动** | `ZlibLayer.cs` | 见 §5 |

## 5. 唯一的 SDK 改动：`ZlibLayer.cs` 的 `#if`

`ZlibLayer` 当前硬依赖 `Unity.SharpZipLib`（Unity 重打包版）和 `UnityEngine.Debug.Assert`。
`Unity.SharpZipLib` 与 NuGet `ICSharpCode.SharpZipLib` 是**同一库不同命名空间**，API
（`Deflater`/`Inflater`/`DeflaterOutputStream`）一致。改动仅限文件顶部 `using`，算法主体一字不动：

```csharp
// 原（第 9-11 行）：
//   using Unity.SharpZipLib.Zip.Compression;
//   using Unity.SharpZipLib.Zip.Compression.Streams;
//   using UnityEngine;
// 改为：
#if UNITY_2022_3_OR_NEWER
using Unity.SharpZipLib.Zip.Compression;
using Unity.SharpZipLib.Zip.Compression.Streams;
using UnityEngine;                       // 提供 Debug.Assert
#else
using ICSharpCode.SharpZipLib.Zip.Compression;
using ICSharpCode.SharpZipLib.Zip.Compression.Streams;
using Debug = System.Diagnostics.Debug;  // 同名 Assert(bool)
#endif
```

文件内两处 `Debug.Assert(...)`（约第 153、163 行）在两个分支下都解析正确。
**Unity 客户端构建不受影响**（仍走 `#if` 真分支）。

## 6. 新增代码：`HeadlessHeTuClient`

### 6.1 传输层：实现三个抽象方法

`HeTuClientBase` 把网络收发抽象成三个方法（已核实）：

```csharp
protected abstract void ConnectCore(string url, Action onConnected,
    Action<byte[]> onMessage, Action<string> onClose, Action<string> onError);
protected abstract void CloseCore();
protected abstract void SendCore(byte[] data);
```

`HeadlessHeTuClient : HeTuClientBase` 用 `System.Net.WebSockets.ClientWebSocket` 实现：

- `ConnectCore`：`new ClientWebSocket()` → `ConnectAsync(uri, ct)`；成功后 `onConnected()`，
  并启动**单条接收循环** task：`ReceiveAsync` 累积成完整消息帧后调 `onMessage(bytes)`；
  正常关闭 `onClose(null)`，异常 `onClose(reason)` / `onError(msg)`。
- `SendCore`：`ws.SendAsync(data, Binary, endOfMessage:true, ct)`；**串行化**（见 §7）。
- `CloseCore`：取消接收循环 + `CloseAsync` / `Abort`，置 `State = Disconnected`。

注意：握手由基类驱动（基类 `ConnectSync` 内先 `SendCore(ClientHello)`，首包当作握手响应处理），
本类不参与握手语义，只负责字节收发。

### 6.2 公共 API（Task 外观，镜像 UnityClient 但用 `Task`/`TaskCompletionSource`）

```csharp
public sealed class HeadlessHeTuClient : HeTuClientBase
{
    public Task Connect(string url, string authKey = null, CancellationToken ct = default);
    public Task<JsonObject> CallSystem(string systemName, params object[] args);
    public Task<RowSubscription<T>>  WatchRow<T>(string index, object value, string componentName = null) where T : IBaseComponent;
    public Task<IndexSubscription<T>> WatchRange<T>(string index, object left, object right, int limit,
        bool desc = false, bool force = true, string componentName = null) where T : IBaseComponent;
    public Task<string> WaitClosedAsync();   // 完成于 OnClosed
    // OnConnected / OnClosed 事件继承自基类
}
```

实现要点：
- `Connect`：`ConfigureCryptoAuthKey(authKey)` → `ConnectSync(url)` → 返回一个 TCS，
  在基类 `OnConnected`（握手完成）置完成、在 `OnClosed`/错误置异常，支持 `ct` 与超时。
- `CallSystem`/`WatchRow`/`WatchRange`：包装基类的 `CallSystemSync`/`WatchRowSync`/`WatchRangeSync`
  回调到 TCS（与 `UnityClient` 写法同构，只是 `Awaitable`→`Task`）。
- **无单例**：每条连接 `new HeadlessHeTuClient()`。Unity 的"一进程一会话"限制只源于
  `HeTuClient.Instance`/`HeTuSessionClient.Instance` 单例，本类不引入单例，多实例并发即多连接。

订阅数据变化沿用既有响应式 API（`IndexSubscription<T>` 的 `ObserveAdd()`/`UpdateRows`、R3），
与 Unity 端 `ChatController` 完全一致。

## 7. 线程模型与正确性（必须做对）

Unity 端一切在主线程串行执行；headless 下 `ClientWebSocket` 的接收循环在线程池线程上跑，
而 `CallSystem`/`WatchRange`（编码 + 发送）可能从另一个线程（定时器/调用方）发起。
`CryptoLayer`（ChaCha20 方向性 nonce/序列）、`MessagePipeline`、`SubscriptionManager` 等
**不是线程安全的**，并发的 encode/decode 会写乱加密流，产生诡异错误。

**不变量**：对**同一个 `HeadlessHeTuClient` 实例**的所有管道访问（编码发送、解码分发）必须串行。

**实现方式**（plan 阶段定细节，二选一）：
- 每连接一个**串行执行器/actor**（如 `Channel<Func>` 或专用单线程 `SynchronizationContext`）：
  接收循环把"解码这帧"投递进去，发送也投递进去——所有管道操作单线程化；socket 的
  `ReceiveAsync`/`SendAsync` 异步 I/O 本身不进串行段，只串行 encode/decode 与"一次只一个未完成
  `SendAsync`"。
- 或：用每连接 `SemaphoreSlim(1,1)` 守住"encode→send"与"receive→decode"两段。

`ResponseManager` 已是 `ConcurrentQueue`，但仍需保证响应**按序**完成（接收单循环即天然有序）。

## 8. 依赖与版本

| NuGet | 用途 | 备注 |
|---|---|---|
| `MessagePack` | `JsonbLayer` 序列化 | 版本须与 unity 端（`MessagePack.Unity`）行为一致；plan 时核对 `[Key]`/`MessagePackObject` 解析与 union/resolver 设置 |
| `BouncyCastle.Cryptography` | `CryptoLayer`（X25519 + ChaCha20-Poly1305） | `Org.BouncyCastle.*` 命名空间，与 SDK 现用一致 |
| `SharpZipLib` | `ZlibLayer`（zlib，`#if` 非 Unity 分支） | `ICSharpCode.SharpZipLib` |
| `R3` | `Subscription` 响应式 | 纯 .NET 版（非 `R3.Unity`） |

目标框架 `net8.0`（`ClientWebSocket`/`ArrayPool`/`Span` 齐备）。如将来需被更老运行时复用可降到
`netstandard2.1`，首期不需要。

## 9. 测试策略

- **纯逻辑单测**（无网络，sandbox 可跑）：`Pipeline`（认证 key 下 encode→decode 往返）、
  `Future`/`Promise` 链、`Subscription` 行更新合并等可独立验证。
- **集成冒烟**（需本地 HeTu 服务端，由 heeroz 在有网环境跑）：起一个 HeTu 示例/测试应用，
  `HeadlessHeTuClient` 连接 → 握手 → `CallSystem` 往返 → `WatchRange` 收到推送。
  sandbox 无网络，集成验证随压测工具一起在 heeroz 机器进行。
- **回归**：确认 **Unity 客户端仍正常编译/运行**（`ZlibLayer` 的 `#if` 未破坏 Unity 分支）。

## 10. 验收标准

1. `HeTu.Client` 以 `net8.0` 干净编译（链接共享源码 + 4 个 NuGet + 2 处排除 + `ZlibLayer` `#if`）。
2. 一个 headless 进程能 `new HeadlessHeTuClient()` × N 条并发连接，各自完成握手 + `CallSystem("login", ...)`。
3. 能 `WatchRange<T>` 订阅并收到服务端推送（由压测工具的"自己发自己收"链路间接验证）。
4. Unity 客户端不受影响，照常编译运行。
5. 多连接并发下无加密流损坏（§7 串行不变量生效）。
