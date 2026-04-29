---
title: "Unity 客户端 SDK"
description: "将 Unity 连接到 HeTu 服务器：`System` RPC、行订阅以及两种数据更新模式（事件回调和 R3 响应式）。"
type: docs
weight: 35
prev: concepts
next: advanced
---

Unity SDK (`cn.hetudb.clientsdk`) 是 Unity 游戏与 HeTu 服务器通信的方式。它封装了 WebSocket 协议、消息管道（MessagePack + zlib + 加密）以及订阅管理，全部通过一个 `HeTuClient.Instance` 实现。

本文假设你已能启动服务器并阅读过[概念](concepts.md) — `Components`、`Systems` 和 Subscriptions 会直接引用，不再重新解释。

## 安装

通过 UPM 添加包（Window → Package Manager → + → Add from git URL）：

```
https://github.com/Heerozh/HeTu.git?path=/ClientSDK/unity/cn.hetudb.clientsdk
```

导入后，打开 **HeTu → Setup Wizard...** 并按照它提供的三个步骤操作：

1. **NuGet 依赖** — MessagePack, BouncyCastle。
2. **UPM 依赖** — Unity 2022.3 上使用 `UniTask`，Unity 6000+ 内置 `Awaitable`。

首次导入时向导会自动弹出；这是预期行为。

## 连接、调用、断开

整个客户端是一个单例：`HeTuClient.Instance`。没有单独的“客户端构建器”对象 — 你直接在单例上配置回调并调用 `Connect`。

```csharp
using HeTu;
using UnityEngine;

public class NetBootstrap : MonoBehaviour
{
    public long SelfID = 1;

    async void Start()
    {
        // 在 Connect 之前挂接，以免错过握手完成事件。
        HeTuClient.Instance.OnConnected += () =>
        {
            // Forget() = 触发后忘记；入队并按顺序发送。
            HeTuClient.Instance.CallSystem("login", SelfID).Forget();
        };

        // Connect() 仅在连接结束时返回。
        // 使用 while 循环在临时断开时自动重连。
        while (true)
        {
            var err = await HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu/MyGame");
            if (err is null || err == "Canceled") break;     // 正常关闭或应用退出
            Debug.LogError($"Reconnecting after error: {err}");
            await Awaitable.WaitForSecondsAsync(1f);          // 2022.3 上使用 UniTask.Delay
        }
    }

    void OnDestroy() => HeTuClient.Instance.Close();
}
```

源码强制执行但片段中不易察觉的关键点：

- **URL 格式** — `ws://<host>:<port>/hetu/<instance>`（如果服务器/反向代理终止 TLS，则使用 `wss://...`）。`/hetu/` 前缀是必需的，`<instance>` 必须匹配服务器 `config.yml` 中的 `INSTANCES:` 条目之一（如果从 CLI 启动，则匹配 `--instance` 标志）。未知实例会在握手后被拒绝 — 这是设计使然，以防止端口扫描器枚举有效名称。
- **`Connect` 是长阻塞的。** 它会等待直到套接字关闭，正常关闭时返回 `null`，应用退出 / 调用 `Close()` 时返回 `"Canceled"`，否则返回错误字符串。不要在需要开始发送 RPC 的同一路径上 `await` 它 — 从 `OnConnected`（或单独的任务）中发起 `CallSystem`，而不是在 `await Connect` 之后。
- **`Connect(url, authKey)`** 是相同的调用，但使用预共享密钥对握手进行签名；如果你的服务器使用 `--authkey` 运行，请使用此方法。
- **每个 `Connect()` 对应一个 `Close()`。** `Close()` 会取消正在进行的 `CallSystem` / `Get` / `Range` 调用并销毁套接字 — 在 `OnDestroy` 中调用它，这样退出播放模式时不会泄漏工作任务。

## 调用 `Systems`

`CallSystem(name, args...)` 通过名称调用服务器端的 `System`。有两种使用方式：

```csharp
// 触发后忘记：立即返回，入队，按顺序发送。
HeTuClient.Instance.CallSystem("move_to", x, z).Forget();

// 等待：等待服务器回复（默认是 "ok"，或者是 System 中 ResponseToClient(...) 返回的任何内容）。
var resp = await HeTuClient.Instance.CallSystem("buy", itemId);
Debug.Log(resp.To<string>());
```

对于快速的输入流（如每帧移动），使用 `.Forget()`（或 `_ = CallSystem(...)`）；对于需要结果的 action，使用 `await`。

**本地预回调。** 你可以注册一个客户端钩子，每次调用指定名称的 `System` 时都会运行 — 适用于客户端预测：

```csharp
HeTuClient.Instance.SystemLocalCallbacks["move_to"] = args =>
{
    // 乐观的本地更新，在服务器往返之前执行
    transform.position = new Vector3((float)args[0], 0, (float)args[1]);
};
```

## 订阅：`Get` vs `Range`

两种订阅都是**实时的**：当底层行在 Redis 中发生变化时，服务器会推送增量更新。

| API                                                | 返回                                                             | 使用场景                                                                                         |
|----------------------------------------------------|------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `Get<T>(index, value)`                             | `RowSubscription<T>`（单行，如果没有匹配行则为 `null`）          | 你希望根据唯一键获取一行 — 自己的血量、自己的库存记录。                                         |
| `Range<T>(index, left, right, limit, desc, force)` | `IndexSubscription<T>`（一个字典，行数据保持同步）                | 你希望获取索引列上的一个窗口 — 附近的玩家、排行榜前N名、最近100条聊天消息。                     |

`T` 参数是你的强类型 `Component` 类（见下一节）。如果不指定，则使用 `DictComponent`，它是一个可用字符串键手动索引的 `Dictionary`。

`Range` 的 `force=true`（默认值）即使初始查询返回零行也会保持订阅活跃，因此新插入的行仍然会触发 `OnInsert` / `ObserveAdd`。如果你不希望订阅空查询，请设置 `force=false`。

## 类型化组件 vs `DictComponent`

服务器可以通过 `hetu build` 从你的 `Component` 定义生成匹配的 C# 类。结果实现了 `IBaseComponent`：

```csharp
public class Position : IBaseComponent
{
    public long ID { get; set; }   // ID 是必需的；匹配服务器上的 `id`
    public long owner;
    public float x;
    public float y;
}
```

使用类型化的 `T`，你可以直接读取字段：`sub.Data.x`。没有类型化时，你会得到一个 `DictComponent`（`Dictionary<string, object>`），需要通过 `Convert.ToSingle(sub.Data["x"])` 来读取 — 灵活但冗长，并且会丢失编译时的字段检查。

## 响应数据变化的两种方式

同一个订阅对象同时支持**事件 API** 和 **R3 响应式 API**。根据调用位置选择合适的方式 — 它们共存，基于相同的内部状态，可以在同一个代码库中混合使用。

### 模式 A — 事件回调

纯 C# 事件。无需额外依赖。

```csharp
async void SubscribeOthers()
{
    var players = await HeTuClient.Instance.Range<Position>(
        "owner", 1, 999, 100);
    players.AddTo(gameObject); // 当此 GameObject 销毁时释放

    // 初始行已填充：
    foreach (var p in players.Rows.Values)
        AddPlayer(p);

    // 服务器端在索引范围内 INSERT
    players.OnInsert += (sender, rowID) =>
        AddPlayer(sender.Rows[rowID]);

    // 服务器端 UPDATE 已在范围内的行
    players.OnUpdate += (sender, rowID) =>
    {
        var p = sender.Rows[rowID];
        MovePlayer(p.owner, new Vector3(p.x, 0.5f, p.y));
    };

    // 服务器端 DELETE，或行离开范围
    players.OnDelete += (sender, rowID) =>
        RemovePlayer(sender.Rows[rowID].owner);
}
```

对于单行 `RowSubscription<T>`，事件更简单：`OnUpdate(sender)` 和 `OnDelete(sender)`。

### 模式 B — R3 响应式流

同一个订阅暴露了 `Observable<T>` 流。当你在链式拼接操作符或绑定 UI 时，这种模式更有价值。

```csharp
async void SubscribeOthers()
{
    var players = await HeTuClient.Instance.Range<Position>(
        "owner", 1, 999, 100);
    players.AddTo(gameObject);

    // 添加流 — 先发出初始行，然后发出实时插入。
    players.ObserveAdd()
        .Subscribe(p => AddPlayer(p))
        .AddTo(ref players.DisposeBag);

    // 移除流 — 发出离开范围的行 ID。
    players.ObserveRemove()
        .Subscribe(rowID => RemovePlayer(rowID))
        .AddTo(ref players.DisposeBag);

    // 每行更新流 — 当行被移除时完成（OnCompleted）。
    foreach (var rowID in players.Rows.Keys)
        BindRow(players, rowID);
    players.ObserveAdd().Subscribe(p => BindRow(players, p.ID))
        .AddTo(ref players.DisposeBag);
}

void BindRow(IndexSubscription<Position> players, long rowID)
{
    players.ObserveRow(rowID)
        .Subscribe(p => MovePlayer(p.owner, new Vector3(p.x, 0.5f, p.y)))
        .AddTo(ref players.DisposeBag);
}
```

对于 `RowSubscription<T>`，使用 `sub.Subject` — 它首先发出当前行，然后每次更新，非常适合直接绑定 UI：

```csharp
var hp = await HeTuClient.Instance.Get<HP>("owner", SelfID);
hp.AddTo(gameObject);

hp.Subject
    .Select(x => x.ID != 0 ? $"HP: {x.value}" : "Dead")
    .SubscribeToText(hpLabel)        // R3 Unity 扩展
    .AddTo(ref hp.DisposeBag);
```

何时优先选择哪种方式：

- **推荐使用 `R3` 方式**，因为它更简洁清晰，代码量更少。
- **`事件`** 适用于几个简单的副作用（生成、移动、销毁）。

## 订阅生命周期（不要跳过此节）

每个订阅都持有服务器端资源。如果订阅在没有调用 `Dispose()` 的情况下被 GC，SDK 的终结器会记录错误 — 这是真正的泄漏，不是可忽略的警告。

三种正确的模式：

```csharp
// 1. 绑定到 GameObject — 在 Destroy 时释放。
sub.AddTo(gameObject);

// 2. 绑定到 DisposableBag（用于嵌套的 R3 订阅或分组）。
sub.AddTo(ref _bag);

// 3. 手动释放。
try { /* 使用 sub */ } finally { sub.Dispose(); }
```

`Dispose()` 做两件事：告诉服务器“停止为此查询推送变更”，并销毁从该订阅派生的所有 R3 流。释放后，`Subject` / `ObserveRow` 流将不再发出数据。

## Unity 版本说明

- **Unity 6000+** — `Connect`、`CallSystem`、`Get` 和 `Range` 返回 `Awaitable<T>`。使用 `await Awaitable.WaitForSecondsAsync(...)` 进行延迟。
- **Unity 2022.3** — 相同的 API 返回 `UniTask<T>`。通过设置向导安装 UniTask。使用 `await UniTask.Delay(ms)` 进行延迟。

两种代码路径编译在 `#if UNITY_6000_0_OR_NEWER` 后面，因此你的调用代码只需要选择一种延迟风格即可。

## 下一步

- **[高级](advanced.md)** — `System` 副本、计划中的将来调用、原始 `Endpoints`、自定义管道层以及当项目变得真实时你需要的引擎内部机制。
- **[概念](concepts.md)** — 现在你已经看到了客户端，重新阅读订阅部分；权限/RLS 会过滤 `Get` 和 `Range` 可以返回的内容。
- **[教程：聊天室](tutorial/chat-room.md)** — 一个使用上述模式的完整客户端-服务器示例。
- **[概念](concepts.md)** — 现在你已经看到了客户端，重新阅读订阅部分；权限/RLS 会过滤 `Get` 和 `Range` 可以返回的内容。
- **[教程：聊天室](tutorial/chat-room.md)** — 一个使用上述模式的完整客户端-服务器示例。
