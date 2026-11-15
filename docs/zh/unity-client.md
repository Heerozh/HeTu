---
title: "Unity 客户端 SDK"
description: "将 Unity 连接到 HeTu 服务器：`System` RPC、行订阅以及两种数据更新模式（事件回调和 R3 响应式）。"
type: docs
weight: 35
prev: concepts
next: advanced
---

Unity SDK (`cn.hetudb.clientsdk`) 是 Unity 游戏与 HeTu 服务器通信的方式。它封装了
WebSocket 协议、消息管道（MessagePack + zlib + 加密）以及订阅管理，分成两层入口：

- **`HeTuClient.Instance`** — 单例，对应一条物理 WebSocket。最底层：你自己调用 `Connect`、自己处理
  断线、自己重新鉴权。
- **`HeTuSessionClient.Instance`** — *游戏客户端推荐用这个。* 同样是单例，是建在 `HeTuClient` 之上的
  持久逻辑会话：自动重连、重跑你的 `bootstrap`（比如登录），并自动恢复所有还活着的订阅。

两者的报文协议、组件、`CallSystem` 以及 `WatchRow` / `WatchRange` 的用法完全一样 — 会话客户端只是在
断网时把它们续上。先读[连接、调用、断开](#连接调用断开)了解基本操作，再看
[逻辑会话](#逻辑会话hetusessionclient)里的生产用法。

本文假设你已能启动服务器并阅读过[概念](concepts.md) — `Components`、`Systems` 和
Subscriptions 会直接引用，不再重新解释。

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

整个客户端是一个单例：`HeTuClient.Instance`。没有单独的“客户端构建器”对象 — 你直接在单例上配置回调并调用
`Connect`。

```csharp
using HeTu;
using UnityEngine;

public class NetBootstrap : MonoBehaviour
{
    public long SelfID = 1;

    async void Start()
    {
        // Connect() 在握手完成后返回。
        await HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu/MyGame");

        // 连接已建立 — 直接调用。
        await HeTuClient.Instance.CallSystem("login", SelfID);

        // 可选：之后连接断开时收到通知。
        HeTuClient.Instance.OnClosed += err =>
            Debug.Log($"disconnected: {err ?? "normal close"}");
    }

    void OnDestroy() => HeTuClient.Instance.Close();
}
```

源码强制执行但片段中不易察觉的关键点：

- **URL 格式** — `ws://<host>:<port>/hetu/<instance>`（如果前面的反向代理（推荐 Caddy）终止了 TLS，
  则使用 `wss://...`；HeTu 自己不处理 TLS）。`/hetu/` 前缀是必需的，`<instance>` 必须匹配服务器 `config.yml` 中的
  `INSTANCES:` 条目之一（如果从 CLI 启动，则匹配 `--instance`
  标志）。未知实例会在握手后被拒绝 — 这是设计使然，以防止端口扫描器枚举有效名称。
- **`Connect` 在握手完成后返回。** await 成功就说明套接字已连上、加密 / 字典握手已完成，可以立刻调用
  `CallSystem` / `WatchRow` / `WatchRange`。要知道连接什么时候意外断开，订阅 `OnClosed` 事件 — 套接字
  断开时触发，服务器正常关闭时参数为 `null`，否则是错误字符串（你自己调用 `Close()` 不会触发）。
  （旧版 SDK 的 `Connect` 会一直阻塞到连接关闭；如果是升级，把 `while (true) { await Connect... }`
  这样的重连循环换成下面的 `HeTuSessionClient`。）
- **被顶号（账号在别处登录）。** 同一账号在另一条连接登录时，服务器会先发 WebSocket close 码
  `4001`（`HeTuCloseCode.Kicked`）、原因 `"kicked"`，再断开本连接。在 `OnClosed` 里判断
  `HeTuClient.Instance.LastCloseCode == HeTuCloseCode.Kicked`，提示玩家"账号已在别处登录"，
  并且不要自动重连 — 重连会重新登录，反过来把对方顶掉。
- **请求被拒。** release 模式下客户端越界（调了服务器上不存在的 System、无权限、参数数量不对、
  报文非法、超出收发频率上限）时，服务器发 close 码 `4002`（`HeTuCloseCode.Rejected`）、原因
  `"rejected"` 再断开，不回具体原因（看服务器日志）。最常见的是客户端比服务器新。同样不要自动
  重连 — 重连后再调一样会被断开，变成周期性掉线。4000-4999 段都是这类终态码
  （`HeTuCloseCode.IsTerminal(code)`），`HeTuSessionClient` 遇到都直接进入 `Faulted`（被顶号之外的
  异常为 `HeTuConnectionRejectedException`，`Code` 是 close 码）。服务端内部出错（后端连不上、
  System 抛异常）发 `1011`（`HeTuCloseCode.InternalError`），可以重连。
- **`Connect(url, authKey)`** 是相同的调用，但使用预共享密钥对握手进行签名；如果你的服务器使用
  `--authkey` 运行，请使用此方法。
- **每个 `Connect()` 对应一个 `Close()`。** `Close()` 会取消正在进行的 `CallSystem` /
  `WatchRow` / `WatchRange` 调用并销毁套接字 — 在 `OnDestroy` 中调用它，这样退出播放模式时不会泄漏工作任务。
- **这一层不会自动重连。** 套接字断开后，`HeTuClient` 会一直断着，直到你再次调用 `Connect`。需要一个能扛过
  短暂断线、并自动恢复订阅的会话，请用 [`HeTuSessionClient`](#逻辑会话hetusessionclient)。

## 调用 `Systems`

`CallSystem(name, args...)` 通过名称调用服务器端的 `System`。有两种使用方式：

```csharp
// 触发后忘记：立即返回，入队，按顺序发送。
HeTuClient.Instance.CallSystem("move_to", x, z).Forget();

// 等待：等待服务器回复（默认是 "ok"，或者是 System 中 ResponseToClient(...) 返回的任何内容）。
var resp = await HeTuClient.Instance.CallSystem("buy", itemId);
Debug.Log(resp.To<string>());
```

对于快速的输入流（如每帧移动），使用 `.Forget()`（或 `_ = CallSystem(...)`）；对于需要结果的
action，使用 `await`。

**等推送的调用。** `CallSystem` 的回复在服务端提交后立即发出，而这次写入引起的订阅推送要再晚约一个
合批间隔（默认 100ms）才到。所以调用一返回就按订阅数据刷新界面（比如恢复按钮：钱还够不够、库存还剩
几个），读到的还是旧值。这种场景改用 `CallSystemAwaitPush`：它等这次调用引起的推送都到了才返回，
返回时 `WatchRow` / `WatchRange` 拿到的订阅对象已经是新值。

```csharp
buyButton.interactable = false;
await HeTuClient.Instance.CallSystemAwaitPush("buy", itemId);
// 这里 gold 订阅已经是购买之后的数据
buyButton.interactable = gold.Data.value >= price;
```

`CallSystemAwaitPush` 比 `CallSystem` 约多等一个合批间隔，期间不耽误同一连接上别的调用和推送；移动这类
高频调用仍用 `CallSystem`。它和订阅本身一样是尽力而为：服务端负载过高时，推送仍可能晚于它返回。
`HeTuSessionClient` 和 headless 的 `HeadlessHeTuClient` 也有同名方法；已经收到服务端回复、还在等推送时
断线的，按成功返回（提交确定已经发生，重连后订阅会恢复）。

**本地预回调。** 你可以注册一个客户端钩子，每次调用指定名称的 `System` 时都会运行 —
适用于客户端预测：

```csharp
HeTuClient.Instance.SystemLocalCallbacks["move_to"] = args =>
{
    // 乐观的本地更新，在服务器往返之前执行
    transform.position = new Vector3((float)args[0], 0, (float)args[1]);
};
```

## 订阅：`WatchRow` vs `WatchRange` vs `WatchTable`

三种订阅都是**实时的**：当底层行在 Redis 中发生变化时，服务器会推送增量更新。

> **一致性说明**：订阅推送是尽力而为的最终一致。正常负载下约 99% 的情况，你收到的都是最新数据
> （通常在变更后 100～200ms 内）；服务端 Redis 压力过大时，可能残留旧数据，直到该行下次变更。
> 订阅数据适合用来显示，扣钱、发奖、校验这类需要强一致的判断请交给服务端 `System`。

| API                                                     | 返回                                        | 使用场景                                      |
|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|
| `WatchRow<T>(index, value)`                             | `RowSubscription<T>`（单行，如果没有匹配行则为 `null`） | 你希望根据唯一键获取一行 — 自己的血量、自己的库存记录。             |
| `WatchRange<T>(index, left, right, limit, desc, force)` | `IndexSubscription<T>`（一个字典，行数据保持同步）      | 你希望获取索引列上的一个窗口 — 附近的玩家、排行榜前N名、最近100条聊天消息。 |
| `WatchTable<T>()`                                       | `IndexSubscription<T>`（整张表，行数据保持同步）      | 你希望拿到"行多、行小、很少变"的整张表 — 所有玩家的名字、公会列表、公开配置。 |

`T` 参数是你的强类型 `Component` 类（见下一节）。如果不指定，则使用 `DictComponent`
，它是一个可用字符串键手动索引的 `Dictionary`。

`WatchRange` 的 `force=true`（默认值）即使初始查询返回零行也会保持订阅活跃，因此新插入的行仍然会触发
`OnInsert` / `ObserveAdd`。如果你不希望订阅空查询，请设置 `force=false`。

`WatchTable` 返回的对象和 `WatchRange` 是同一个 `IndexSubscription<T>`，事件与 R3 流用法完全一样，
区别在服务端：`WatchRange` 为结果集里的每一行各订阅一个频道，几千行就是几千个频道；`WatchTable`
不管多少行都只订一个表级频道，所以不计入行订阅配额。代价是该表**任何**写入都会触发一次
通知（服务端按权限过滤后再推），所以高频写入的表（位置、血量）请用 `WatchRange`。
服务端组件要声明 `table_sub=True`（见[概念 · 订阅](concepts.md#何时用整表订阅)），没声明的表、
以及行数超过 `MAX_TABLE_SUBSCRIPTION_ROWS`（默认 10 万）的表都会拒绝整表订阅，此时返回 `null`。

整表订阅在订阅生效后约 100ms（一个推送间隔）才读数据，确保读到的副本已经跟上。要订好几张表时，
先把请求都发出去再逐个 `await`，服务端会让它们一起等；一个一个 `await` 就是每张表各等一次：

```csharp
// 登录后订几张表：先都发出去，再 await
var namesTask = HeTuClient.Instance.WatchTable<PlayerNames>();
var guildsTask = HeTuClient.Instance.WatchTable<GuildList>();
var names = await namesTask;
var guilds = await guildsTask;
```

`WatchRange` 的点查询（`left == right`，如 `WatchRange<Item>("owner", myId, myId, 100)`）要高效，
服务端对应的索引要声明 `point_sub=True`，否则该索引上任何写入都会让服务端重跑一次比对。

```csharp
// 所有玩家的名字：几千行，一个订阅
var names = await HeTuClient.Instance.WatchTable<PlayerNames>();
names.OnInsert += (sub, id) => AddNameTag(sub.Rows[id]);
names.OnUpdate += (sub, id) => RefreshNameTag(sub.Rows[id]);
names.OnDelete += (sub, id) => RemoveNameTag(id);
```

### 按组订阅复合索引

服务端用 `property_field(groupby=...)` 声明的复合索引（见[概念](concepts.md)），订阅时用最后一个参数
`group` 给出全部分组列的值，只订这一组。`WatchRow` 和 `WatchRange` 都有这个参数：

```csharp
// level 按 zone 分组：订 zone=3 里 level 在 [10, 20] 的行，按 level 排序
var zone3 = await HeTuClient.Instance.WatchRange<Monster>(
    "level", 10, 20, 100,
    group: new Dictionary<string, object> { ["zone"] = 3 });

// 组内唯一的复合索引：按 (公会, 名字) 订一行
var member = await HeTuClient.Instance.WatchRow<GuildMember>(
    "name", "Ann", group: new Dictionary<string, object> { ["guild"] = guildId });
```

- **值的类型**：分组值只能是整数、bool、刚好是整数的浮点、`string`、`byte[]`。整数分组列也收数字字符串（如 `"3"`），和单列索引的查询值一样。
- **当场抛异常的情况**：值类型不合法，或者在 `id` 上带了 `group`，都当场抛 `ArgumentException`，请求不会发出去。
- **断开连接的情况**：分组列名写错、值和列类型不符（比如整数列给了不是数字的字符串、字符串列给了数字），客户端看不出来，服务器会以 4002 断开连接。会话随之进入终态，不会重连。
- **字典会被拷贝**：调用之后再改这个字典，不影响这次订阅；断线重连时按订阅时的 `group` 恢复。`WatchRow` 解析出行之后，照旧按行 `id` 恢复。
- **唤醒范围**：订阅只会被本组的写入叫醒，别的组的写入不会叫醒它；服务端不需要为此声明 `point_sub`。
- **服务器版本**：带 `group` 的订阅需要支持复合索引的服务器（v2025.11.1 之后的版本）。旧服务器不认识 `group`，会以 4002 断开连接。不带 `group` 的订阅发出的报文和以前完全一样。

### 在客户端消费提交序号列

服务端用 `property_field(commit_seq=True, groupby=...)` 声明的提交序号列（见[概念](concepts.md)），号在提交时
按组从 1 连续发。客户端可以订阅这一组、按号往后消费：

```csharp
// 订 owner 这一组里号大于游标的日志，窗口 100 行
var log = await HeTuClient.Instance.WatchRange<ChangeLog>(
    "seq", cursor + 1, long.MaxValue, 100,
    group: new Dictionary<string, object> { ["owner"] = myId });
```

- **游标只沿连续的号推进**：同一批推送里有多行时，按 `seq` 排好，从游标加一开始一个号接一个号地处理，处理到哪就把
  游标推到哪。遇到缺号就停下，后面的行先缓存起来：读写分离代理按命令分流、原生 cluster 下有多个副本时，服务器读回
  这一批时可能漏掉中间某一行，下一批推送会补上。不要直接把游标推到这一批里最大的号，那样漏掉的那一行就永远不会被
  处理了。
- **窗口满了要换订阅**：订阅只推窗口（`limit` 行）里的行。处理到窗口末尾后，用新的游标重新订阅，并退订旧的。
- **组里的每一行都要对这个客户端可见**：比如 `OWNER` 组件加 `groupby="owner"`。RLS 看不到的行照样占着窗口的名额、
  又不推送，客户端会永远停在它前面；做不到全部可见的，改在服务端消费（见[高级功能](advanced.md)的「binlog 式同步」）。
- **缺号一直不补**：客户端没法确认缺号是不是永久的（比如中间的行被删了），只能保证服务端不删中间的行，或者等一会儿
  还没补上时，调一个服务端 `System` 来确认、跳过缺口。
- **拿到刚插入那一行的号**：`System` 里 insert 的行，号要到提交之后才有，`System` 返回的行里是 0。最直接的做法是让
  `System` 提前提交、把号回给客户端：`seqs = await ctx.session_commit()`，再
  `return hetu.ResponseToClient({"seq": seqs[ChangeLog][row.id]})`（见[高级功能](advanced.md)的「binlog 式同步」），
  代价是提交之后的代码不在事务里。不想提前提交的话，订阅这一组，再用 `CallSystemAwaitPush` 调用，返回时推送已经到了，
  按 `System` 返回的行 id 找到推送来的那一行，就能拿到号。前提是新行落在订阅的窗口里：比如用 `desc: true` 订最新的
  几行，或者确认窗口还没满；窗口满了的话新行不会推送，`CallSystemAwaitPush` 照样返回，但拿不到号。

## 类型化组件 vs `DictComponent`

服务器可以通过 `hetu build` 从你的 `Component` 定义生成匹配的 C# 类。结果实现了
`IBaseComponent`：

```csharp
public class Position : IBaseComponent
{
    public long ID { get; set; }   // ID 是必需的；匹配服务器上的 `id`
    public long owner;
    public float x;
    public float y;
}
```

使用类型化的 `T`，你可以直接读取字段：`sub.Data.x`。没有类型化时，你会得到一个
`DictComponent`（`Dictionary<string, object>`），需要通过 `Convert.ToSingle(sub.Data["x"])`
来读取 — 灵活但冗长，并且会丢失编译时的字段检查。

## 响应数据变化的两种方式

同一个订阅对象同时支持**事件 API** 和 **R3 响应式 API**。根据调用位置选择合适的方式 —
它们共存，基于相同的内部状态，可以在同一个代码库中混合使用。

### 模式 A — 事件回调

纯 C# 事件。无需额外依赖。

```csharp
async void SubscribeOthers()
{
    var players = await HeTuClient.Instance.WatchRange<Position>(
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
    var players = await HeTuClient.Instance.WatchRange<Position>(
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

对于 `RowSubscription<T>`，使用 `sub.Subject` — 它首先发出当前行，然后每次更新，非常适合直接绑定
UI：

```csharp
var hp = await HeTuClient.Instance.WatchRow<HP>("owner", SelfID);
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

每个订阅都持有服务器端资源。如果订阅在没有调用 `Dispose()` 的情况下被 GC，SDK
的终结器会记录错误 — 这是真正的泄漏，不是可忽略的警告。

三种正确的模式：

```csharp
// 1. 绑定到 GameObject — 在 Destroy 时释放。
sub.AddTo(gameObject);

// 2. 绑定到 DisposableBag（用于嵌套的 R3 订阅或分组）。
sub.AddTo(ref _bag);

// 3. 手动释放。
try { /* 使用 sub */ } finally { sub.Dispose(); }
```

`Dispose()` 释放的是你手里这一个订阅：销毁从它派生的所有 R3 流，释放后它的 `Subject` /
`ObserveRow` 流不再发出数据、事件也不再触发。

同一行 / 同一范围可以在多处订阅：每次 `WatchRow` / `WatchRange` / `WatchTable` 都返回一个**自己的**
订阅对象，它们共用一个服务端订阅和同一份数据（`Data` / `Rows`）。各处按自己的生命周期 `Dispose`
即可，互不影响；最后一个 `Dispose` 时，SDK 才告诉服务器“停止为此查询推送变更”。

两点注意：

- 漏了 `Dispose` 的订阅会让服务端订阅一直留到断线，即使同一查询的其他订阅都已释放。SDK 的泄漏
  日志要等断线、这个订阅被回收时才会出现，别指望靠它及时发现泄漏。
- 退订可能稍晚才发出：有订阅请求还在等回复时，SDK 会等它们回复后再退订，以免把刚好落到同一个
  订阅上的请求一起退掉。持续不断地发订阅请求（比如快速翻页）时，旧订阅会一直留到请求停下来。

## 逻辑会话：`HeTuSessionClient`

裸的 `HeTuClient` 就是一条物理 WebSocket — 断了之后数据就不再更新，直到你重连、重新登录、重新发出每一个
`WatchRow` / `WatchRange`。`HeTuSessionClient` 替你做这些事。和 `HeTuClient` 一样，它也是单例
（`HeTuSessionClient.Instance`）— 它管理底层的 `HeTuClient.Instance`，自己重连，重跑你的 `bootstrap`
（通常是登录那一次调用），并按每个还活着的订阅的**原始订阅意图**（组件 + 索引 + 值 / 范围）重新订阅。

```csharp
var session = HeTuSessionClient.Instance;

await session.Connect(
    "ws://127.0.0.1:2466/hetu/MyGame",
    bootstrap: async client =>
    {
        // 'client' 就是 HeTuClient.Instance — 正在建立的那条物理连接。
        // 每次（重）连都要重新鉴权。
        await client.CallSystem("login", SelfID);
    });
// 这时 session.State 已经是 Ready。

using var hp = await session.WatchRow<HP>("owner", SelfID);
using var nearby = await session.WatchRange<Position>("zone", zone, zone, 50);

await session.CallSystem("move_to", 12f, 8f);
```

`Connect` 要等 bootstrap 和订阅恢复*都*完成之后才返回 — 所以一返回，实时数据就已经到位，你的代码和断线之前
处在同样的状态。

完整签名：

```csharp
Awaitable Connect(
    string url,
    string authKey = null,
    Func<HeTuClient, Awaitable> bootstrap = null,
    TimeSpan? reconnectDelay = null,        // 默认 1 秒
    TimeSpan? maxReconnectDelay = null,     // 默认 30 秒（指数退避的上限）
    int maxReconnectAttempts = 20,          // 0 = 不限次数
    TimeSpan? connectTimeout = null,        // 默认 30 秒
    TimeSpan? requestTimeout = null,        // 默认 30 秒
    double? reconnectJitter = null);        // 默认 0.5
```

这个 await 可能的结果：

- 会话到达 `Ready` 时**正常返回** — 正常路径。
- 一出错就抛出**底层的那个失败异常** — 会话在**第一次到达 `Ready` 之前不会重试**。首次连接中的任何失败
  （套接字关闭、bootstrap 抛异常、恢复订阅出错）都会在第一次尝试时进入 `Faulted` 终态，await 拿到的是原始
  异常（比如你的 `CallSystem("login", ...)` 抛的异常，或者
  `InvalidOperationException("Connection closed: <reason>")`）。用同样的 URL / 凭据重试也没用，所以 SDK 不会去试。
- 在 `connectTimeout`（默认 30 秒）内没到 `Ready` 就抛 **`TimeoutException`** — 主要是给传输层卡住、又一直
  不报关闭 / 错误的情况兜底。`Message` 和 `InnerException` 带着最后一次捕获到的错误（如果有的话）。抛出前会
  `Close()` 会话，之后可以再次 `Connect(...)`。
- 别的代码路径调用了 `Close()` 时，抛 **`OperationCanceledException`**。

会话已经在运行时再调用 `Connect` 会抛异常 — 真要换个 URL 重连，先调 `Close()`。`Close()` 之后（或者进入
`Faulted` 之后）可以再次 `Connect`，状态机会用一个全新的内核从头开始。

### 默认值 — 什么时候该改

- `reconnectDelay = 1s`、`maxReconnectDelay = 30s` — 从 1 秒开始指数退避，每次翻倍，最多 30 秒。短暂抖动和
  长时间中断都适用：上限避免一直冲击服务器，服务器恢复后也能及时连回来。
- `reconnectJitter = 0.5` — 每次等待的时长是当前退避延迟的 [0.5, 1] 倍之间的一个随机值（一开始是 0.5～1 秒，
  到上限时是 15～30 秒）；退避延迟本身照样按上面翻倍。不然同一时刻掉线的客户端（服务器重启、网关摘掉一个节点）
  会在同一瞬间一起重连，一轮接一轮，每一轮都是一次建连风暴。传 `0` 就是精确的延迟。
- `maxReconnectAttempts = 20` — **只管会话至少到达过一次 `Ready` 之后的重连**。按上面的指数退避，20 次大约
  要在游戏里重试 4～8 分钟（不算抖动是 1+2+4+8+16+30 × 15 ≈ 481 秒）才放弃、进入 `Faulted`。维护窗口很长的话
  传 `0`（不限次数），玩家开着应用，服务器恢复后就能连回来。**首次连接永远只试 1 次** — 凭据或 URL 可能就是
  错的，重试没有意义。
- `connectTimeout = 30s` — 只管**首次** `Connect`；`Ready` 之后的重连不受它限制。如果设了
  `maxReconnectAttempts > 0`，考虑调大或者关掉它 — 否则哪个预算先用完就按哪个算。

### 状态机

`session.State` 按顺序经过这些状态，每次转换都会触发 `StateChanged`：

| 状态                       | 含义                                                                                     |
|--------------------------|----------------------------------------------------------------------------------------|
| `Stopped`                | 初始状态，或者 `Close()` / `Dispose()` 之后。`Connect()` 让它往下走。                                |
| `Connecting`             | WebSocket 握手中。                                                                         |
| `Bootstrapping`          | 套接字已连上；你的 `bootstrap` 委托正在运行。                                                         |
| `RestoringSubscriptions` | bootstrap 完成；正在重新发出每个还活着的 `WatchRow` / `WatchRange`。                                  |
| `Ready`                  | 排队的调用已经发完；触发 `Ready` 事件。稳定状态。                                                         |
| `Reconnecting`           | 短暂断线；等完当前的退避延迟（带抖动）后回到 `Connecting`。                                                  |
| `Faulted`                | 终态 — 首次连接失败（总是终态）、`Ready` 之后重连次数用完（`maxReconnectAttempts`）、被顶号（`HeTuKickedException`，不会重试），或者其他终态 close 码比如请求被拒（`HeTuConnectionRejectedException`，不会重试）。`Faulted` 事件带着最后一个 `Exception`。 |

`Faulted` **事件**的范围比 `Faulted` **状态**大：每次尝试失败都会触发一次 — `Ready` 之前的失败触发一次后进入
终态；`Ready` 之后的短暂断线每次重试都会触发。在回调里看 `session.State` 就能分清是终态还是短暂的（`Ready`
之后重试期间状态停在 `Reconnecting`）。

被顶号是重试的例外：账号在别处登录时，服务器以 close 码 `4001` 关闭连接，会话直接进入 `Faulted`，异常为
`HeTuKickedException`，不会重连 — 重连会重跑你的登录 `bootstrap`、把另一台设备顶掉，两边就会一直互相顶号。
在 `Faulted` 回调里（或者 `await Connect` 外面）判断它，提示"账号已在别处登录"。其他终态 close 码（4000-4999，
比如 `4002` 请求被拒）同样直接进入 `Faulted`，异常为 `HeTuConnectionRejectedException`：同样的请求重试也只会
再被拒。可以提示"版本不匹配，请更新"之类。

### `CallSystem` 的语义

调用在断线那一刻处在*哪个阶段*，表现不同：

- **还没发出（在 `Ready` 之前排队的）** — 一直等到会话 `Ready`，再按顺序发出。从任何地方发起都是安全的，包括
  在 `Ready` 事件里面。
- **断线时正在路上的** — 以 `CallOutcomeUnknownException` 失败。会话**从不自动重试**这类调用，因为服务器可能
  已经执行了它的副作用。如果调用是幂等的，自己捕获后重试。

```csharp
try {
    await session.CallSystem("buy", itemId);
} catch (CallOutcomeUnknownException) {
    // 服务器可能扣了玩家的钱，也可能没扣。每个调用自己决定怎么处理。
}
```

### 订阅的存续

`session.WatchRow<T>` / `session.WatchRange<T>` 返回的就是上面见过的 `RowSubscription<T>` /
`IndexSubscription<T>`，会话额外填好三个钩子：

- **`sub.IsStale`** — 底层套接字断开期间为 `true`。`sub.Data` / `sub.Rows` 保留最后已知的值；会话再次
  `Ready` 之前不会有更新。
- **`sub.OnResynced`** — 每次重连、快照刷新完之后触发一次。断线期间发生的插入 / 更新 / 删除会先经过正常的
  `OnInsert` / `OnUpdate` / `OnDelete` 事件（以及对应的 R3 流）重放一遍，所以你的回调看到的是一份干净的差异，
  而不只是一个"全部重新加载了"的信号。用它来清掉 UI 上的"数据过期"提示。
- **去重** — 用同样的参数调用两次 `WatchRow` / `WatchRange`，只发一个服务端订阅，但每个调用方都拿到**自己的**
  订阅对象；*每一个*都 `Dispose` 之后才会远程退订，重连时这个查询也只为它们恢复一次。

`RowSubscription` 的恢复目标是**绑定到的那一行的 `ID`**，不是最初的索引 / 值：如果你调用的是
`session.WatchRow<Player>("owner", userId)`，匹配到的行主键是 `42`，重连时就按 `id=42` 重新订阅。你离线期间
不再满足原始条件的那一行**不会**被换掉 — 订阅会触发 `Update(null)`，并一直绑定在 `42` 上。

### 关闭

`HeTuSessionClient.Instance.Close()`（或者 `Dispose()` — 两者是同一个）以 `OperationCanceledException` 取消
正在进行的调用，释放它持有的每个订阅，并销毁套接字。单例本身还在 — 再调 `Connect(...)` 就能开始一个新会话。

```csharp
async void Start() {
    await HeTuSessionClient.Instance.Connect(url, bootstrap: Login);
    // ... 订阅 / 调用 ...
}

void OnDestroy() => HeTuSessionClient.Instance.Close();
```

## Unity 版本说明

- **Unity 6000+** — `Connect`、`CallSystem`、`WatchRow` 和 `WatchRange` 返回 `Awaitable<T>`。使用
  `await Awaitable.WaitForSecondsAsync(...)` 进行延迟。
- **Unity 2022.3** — 相同的 API 返回 `UniTask<T>`。通过设置向导安装 UniTask。使用
  `await UniTask.Delay(ms)` 进行延迟。

两种代码路径编译在 `#if UNITY_6000_0_OR_NEWER` 后面，因此你的调用代码只需要选择一种延迟风格即可。

## 下一步

- **[高级](advanced.md)** — `System` 副本、计划中的将来调用、原始 `Endpoints`
  、自定义管道层以及当项目变得真实时你需要的引擎内部机制。
- **[概念](concepts.md)** — 现在你已经看到了客户端，重新阅读订阅部分；权限/RLS 会过滤
  `WatchRow` 和 `WatchRange` 可以返回的内容。
- **[教程：聊天室](tutorial/chat-room.md)** — 一个使用上述模式的完整客户端-服务器示例。
