# 河图 Unity Client SDK

HeTu（河图）是一个面向实时应用的轻量化后端引擎。它将游戏服务器能力与数据库模型结合，适用于 MMO、多人联机、实时模拟与 AI Agent 持久化场景。

- 官网：<https://hetudb.cn>
- 服务器仓库：<https://github.com/Heerozh/hetu>

## 功能概览

- WebSocket 长连接与握手流程
- 可插拔消息管道（默认：MessagePack + Zlib + Crypto）
- System RPC 调用
- 单行订阅（Get）与范围订阅（Range）
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
    HeTuClient.Instance.OnConnected += () =>
    {
      HeTuClient.Instance.CallSystem("login", 123, true).Forget();
    };

    // 连接会一直等待到断开后返回
    var err = await HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu/pytest");
    Debug.Log($"disconnect: {err ?? "normal"}");
  }

  private void OnDestroy()
  {
    HeTuClient.Instance.Close();
  }
}
```

## 核心 API

### 连接

- `Connect(url)`：建立连接并等待直到断开。
- `Close()`：主动断开并取消所有挂起请求。
- `OnConnected`：握手完成后触发。
- `OnClosed`：连接断开时触发，参数为空表示正常断开。

### RPC

- `CallSystem(systemName, params object[] args)`
  - 发送系统调用。
  - 可通过 `SystemLocalCallbacks[systemName]` 注册本地前置逻辑。

### 数据订阅

- `Get<T>(index, value, componentName = null)`：订阅单行数据。
- `Range<T>(index, left, right, limit, desc = false, force = true, componentName = null)`：订阅范围数据。
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
