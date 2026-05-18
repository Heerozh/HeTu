# 河图 Unity Client SDK

HeTu（河图）是一个面向实时应用的轻量化后端引擎。它将游戏服务器能力与数据库模型结合，适用于
MMO、多人联机、实时模拟与 AI Agent 持久化场景。

- 官网：<https://hetudb.cn>
- 服务器仓库：<https://github.com/Heerozh/hetu>

## 功能概览

- WebSocket 长连接与握手流程
- 可插拔消息管道（默认：MessagePack + Zlib + Crypto）
- System RPC 调用
- 单行订阅（WatchRow）与范围订阅（WatchRange）
- 可选逻辑会话层（自动重连、重新 bootstrap、自动恢复订阅）
- 与 Unity 生命周期友好的异步 API
- 可选 R3 响应式订阅支持

## 安装

1. 将本包添加到 Unity Package Manager（UPM）。
2. 菜单打开 `HeTu/Setup Wizard...`。
3. 按向导顺序安装依赖：
    - NuGet 依赖（MessagePack、BouncyCastle 等）
    - UPM 依赖（Unity 版本相关）
    - 可选依赖（R3）

> 若首次导入后自动弹出向导，这是预期行为。

## 快速开始

```csharp
using HeTu;
using UnityEngine;

public class NetBootstrap : MonoBehaviour
{
  private async void Start()
  {
    await HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu/pytest");
    HeTuClient.Instance.CallSystem("login", 123, true).Forget();

    // 如果关心断线，再显式等待关闭
    var err = await HeTuClient.Instance.WaitClosedAsync();
    Debug.Log($"disconnect: {err ?? "normal"}");
  }

  private void OnDestroy()
  {
    HeTuClient.Instance.Close();
  }
}
```

## 核心 API

### 推荐入口：逻辑会话

- `HeTuSessionClient.Instance` —— 单件，和 `HeTuClient.Instance` 一样一个进程一个。
    - 管理持续存在的逻辑会话，而不是只管理一条当前 WebSocket。
    - 断线后会重新建立连接、先执行 `bootstrap`，再恢复仍然存活的订阅。
    - `WatchRow` / `WatchRange` 返回的就是普通 `RowSubscription` /
      `IndexSubscription`；同一 canonical `sub_id` 会复用同一个订阅对象。
    - 未发送的 `CallSystem` 会等待 session ready 后再发；已经发出但未收到响应就断线的调用会抛出
      `CallOutcomeUnknownException`，不会自动重试副作用请求。

```csharp
await HeTuSessionClient.Instance.Connect(
    "ws://127.0.0.1:2466/hetu/pytest",
    bootstrap: async client =>
    {
        await client.CallSystem("login", userToken);
    });

using var hp = await HeTuSessionClient.Instance
    .WatchRow<HP>("owner", userId);
using var players = await HeTuSessionClient.Instance
    .WatchRange<Player>("room_id", roomId, roomId, 100);
```

`HeTuSessionClient` 的内部逻辑由 awaitable-free 的 session core 驱动；Unity 包装层只负责把它桥接成
Unity 6000+ 的 `Awaitable` 或 Unity 2022.3 的 `UniTask`，因此在 WebGL 上不会依赖 `.NET Task`。
当前会话仍在运行时再次 `Connect` 会抛 `InvalidOperationException`，需要先 `Close()` 才能换 URL
重新建立会话。

`Connect` 的可选参数（带默认值）：

| 参数                     | 默认            | 说明                                            |
|------------------------|---------------|-----------------------------------------------|
| `reconnectDelay`       | `1s`          | 重连退避起点。                                       |
| `maxReconnectDelay`    | `30s`         | 退避上限；指数翻倍封顶在这里。                              |
| `maxReconnectAttempts` | `20`           | 连续失败次数上限，`0` = 不限次。游戏端常用 `0`，让玩家挂着等维护结束。      |
| `connectTimeout`       | `30s`         | 首次到 Ready 的整体超时；超时自动 Close 并抛 `TimeoutException`。`TimeSpan.Zero` 关闭。 |

`Faulted` 事件每次连接失败（socket 断开、bootstrap 异常、restore 异常）都会触发一次，
携带本次失败的异常，是否终态请读 `State`（`Faulted` 状态才是终态）。

### 连接

- `Connect(url)`：建立单条物理连接并等待握手完成。
- `WaitClosedAsync()`：等待当前物理连接断开。
- `Close()`：主动断开并取消所有挂起请求。
- `OnConnected`：握手完成后触发。
- `OnClosed`：连接断开时触发，参数为空表示正常断开。

### RPC

- `CallSystem(systemName, params object[] args)`
    - 发送系统调用。
    - 可通过 `SystemLocalCallbacks[systemName]` 注册本地前置逻辑。

### 数据订阅

- `WatchRow<T>(index, value, componentName = null)`：先按条件找到第一行，再持续观察该行本身。
  若该行后续不再满足原条件，订阅不会切换到新的匹配行；首次解析成功后，Session 重连也会按该行
  的 `id` 恢复，而不会重新执行最初的 first 查询。按主键时可直接使用 `WatchRow("id", rowId)`。
- `WatchRange<T>(index, left, right, limit, desc = false, force = true, componentName = null)`
  ：持续观察索引范围。
- `RowSubscription` / `IndexSubscription` 在 Session 断线期间会把 `IsStale` 置为 `true`，
  恢复完成后触发 `OnResynced`。
- 非泛型重载返回 `DictComponent`，适合动态字段访问。

### 生命周期管理

- 订阅对象实现 `IDisposable`，**必须**调用 `Dispose()` 反订阅。
- 在 Unity 中推荐 `subscription.AddTo(gameObject)` 绑定生命周期自动释放。

## 数据类型建议

- 推荐使用服务器生成的强类型组件（`IBaseComponent`）。
- 动态场景可使用 `DictComponent`。
- `JsonObject` 提供延迟反序列化：
    - `To<T>()`
    - `ToList<T>()`
    - `ToDict<TKey, TValue>()`

## 测试

测试位于 `Tests/HeTu/HeTuClientTest.cs`，运行前请先启动 HeTu 服务端测试入口（文件注释中有提示）。

## 版本与兼容

- Unity：`2022.3+`
- Unity 6000+ 使用 `Awaitable`
- Unity 2022.3 使用 `UniTask`（按向导安装）
