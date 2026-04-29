---
title: "高级功能"
description: "用于垂直分片的`System`副本、定时FutureCall、生命周期钩子、原始`Endpoints`、自定义管道层、范围查询的NumPy模式，以及项目进入实战阶段后你会用到的其他引擎内部机制。"
type: docs
weight: 38
prev: unity-client
next: operations
---

当你使用 Unity SDK 构建并交付了几个 `System` 之后，本页列出的特性就是你接下来会用到的东西。它们都不是入门必需的，但每一个都能解决项目离开原型阶段后出现的实际难题：

- **[`System` 副本](#system-副本通过标签后缀实现垂直分片)** —— 针对多个独立的表复用同一个 `System` 主体，这样中心 `Component` 就不会把所有依赖的 `System` 拉入同一个集群。
- **[未来调用](#未来调用定时和循环system调用)** —— 持久化的定时和循环服务器端任务。
- **[`call_lock`](#call_lock与幂等system执行)** —— 使 `System` 在重试时具备幂等性。
- **[生命周期钩子](#the-on_disconnect-hook)** —— `on_disconnect` 在套接字关闭时运行。
- **[原始 `Endpoints`](#原始endpoints多system或非数据库rpc)** —— 无事务的 RPC 处理器，用于非数据库工作或独立调用多个 `System`。
- **[每连接状态](#每连接状态user_data-group-和-limits)** —— `ctx.user_data`、通过 `ctx.group` 提升管理员权限，以及速率限制覆盖。
- **[提前 `session_commit` / `session_discard`](#提前-session_commit--session_discard)** —— 在 `System` 主体返回之前提交（或放弃）事务。
- **[用于范围查询的 NumPy 模式](#用于范围查询的-numpy-模式)** —— 广播、布尔掩码、聚合，以及将两个查询在内存中合并而非循环。
- **[多后端](#多后端per-component)** —— 通过 `backend=` 将选定的 `Components` 固定到单独的数据库。
- **[易失性组件](#易失性组件)** —— `volatile=True` 用于在模式维护时应被清除的状态。
- **[`core` 命名空间](#core-命名空间始终加载的组件)** —— 即使没有 `System` 引用也会加载的 `Components`。
- **[自定义管道层](#自定义消息管道层)** —— 使用你自己的压缩/加密/分帧扩展线路协议。
- **[重放与慢日志](#可操作性可见性重放与慢日志)** —— 重现 bug 和查找热点 `System`。
- **[SnowflakeID](#snowflakeid-与工作器-id)** —— 行 ID 生成器及其背后的工作器 ID 租约。

## `System` 副本（通过 `:tag` 后缀实现垂直分片）

当多个 `System` 都引用同一个 `Component` 时，集群构建器会将它们合并到一个**同址集群**中——这是为事务隔离性付出的代价。直到某个 `Component`（队列、计数器、排行榜）将数十个不相关的 `System` 拉入一个无法水平分片的大集群，这才成为一个问题。

解决方法是 **`System` 副本**：保留一个 `System` 定义，但通过向 `depends=` 追加 `:tag` 后缀使其针对*单独的*表进行实例化：

```python
@hetu.define_system(namespace="Loot", components=(Order,))
async def remove(ctx, order_id):
    if order := await ctx.repo[Order].get(id=order_id):
        ctx.repo[Order].delete(order.id)


@hetu.define_system(
    namespace="Loot", depends=("remove:ItemOrder",), permission=hetu.Permission.USER,
)
async def remove_item_order(ctx, order_id):
    return await ctx.depend["remove:ItemOrder"](ctx, order_id)
```

这个后缀实际做了什么：

- `Order` `Component` 会被复制到名为 `Order:ItemOrder` 的兄弟表中。它拥有相同的模式、索引和权限——它是一个真实的、独立的物理表。
- 也会注册一份 `remove` `System` 主体的副本，并绑定到新表。`ctx.depend["remove:ItemOrder"]` 调用的是*那个*副本。
- 集群计算针对副本进行：`:ItemOrder` 集群与原始 `Order` 集群没有重叠，因此它们永远不会共享一个分片，也永远不会互相读写。
- 所有继承仍然有效。如果 `remove` 本身也依赖其他 `System`，则整个依赖图都会在相同后缀下被复制。

当一个 `Component` 成为瓶颈（比如 `FutureCalls` 队列或全局 `Inventory`），并且你希望某些调用方在单独的物理副本上操作时，就可以使用此功能。**不要**将其用作通用的“命名空间”机制：每个副本都是一个真实的表，需要存储和隔离，而你可能实际上希望它们协同工作。

内置的 `create_future_call` `System` 就是典型例子；下面的[未来调用](#未来调用定时和循环system调用)部分使用了 `:scheduler` 后缀，以将未来调用队列排除在应用程序主集群之外。

## 未来调用（定时和循环 `System` 调用）

`create_future_call` 是一个内置的 `System`（位于 `global` 命名空间），它安排另一个 `System` 在**未来**某个时间运行，可选**重复**运行。这个调度在服务器重启后仍然有效，因为它是作为 `Component`（`hetu.system.future.FutureCalls`）存储的，而不是内存队列。

基本骨架：

```python
@hetu.define_system(
    namespace="MyGame",
    permission=None,  # 不可由客户端调用
    components=(SomeComponent,),
    call_lock=True,  # 未来调用必需（见下文）
)
async def reward_daily_bonus(ctx: hetu.SystemContext, user_id: int):
    async with ctx.repo[SomeComponent].upsert(owner=user_id) as row:
        row.bonus += 100


@hetu.define_system(
    namespace="MyGame",
    permission=hetu.Permission.USER,
    depends=("create_future_call:scheduler",),
)
async def schedule_my_bonus(ctx: hetu.SystemContext, delay_seconds: float):
    uuid = await ctx.depend["create_future_call:scheduler"](
        ctx, -delay_seconds, "reward_daily_bonus", ctx.caller,
        timeout=10, recurring=False,
    )
    return hetu.ResponseToClient({"future_call_id": int(uuid)})
```

关于 API 你需要知道的事情：

- **`at`** 为正数时是 POSIX 时间戳；**负数或零**表示“从现在起多少秒”。`at=-10` 表示十秒后运行。
- **`*args`** 必须可被 `repr()` 序列化，并且 `eval()` 后能恢复为等同的值——调度器将参数存储为字符串。坚持使用基本类型（`int`、`float`、`str`、`bool`、简单元组）。总长度必须 ≤ 1024 字符。
- **`timeout`（秒，默认 60，非零时最小 5）** 是重试时间窗口。如果调用在 `timeout` 内未提交，调度器会再次运行它。`timeout=0` 表示“发射后不管”——不重试，你需要接受进程在调用中途崩溃会丢失任务。
- **`recurring=True`** 将条目变为周期性作业。每次运行会在 `timeout` 秒后重新调度自身。要求 `timeout > 0`。
- **目标 `System` 必须声明 `call_lock=True`**——当 `timeout>0` 且不是 `recurring` 时。调度器使用调用的行 ID 作为 UUID 来去重重试；没有 `call_lock`，引擎会拒绝注册未来调用。
- **触发粒度约为 1 秒。** 每个工作器运行一个 `future_call_task` 后台协程，每秒轮询一次；不要将其用于亚秒级精度。
- **执行 `ctx` 没有用户身份。** 调度器作为内部流量运行——`ctx.caller` 是 `0`，`ctx.address` 是 `localhost`。如果工作需要用户 ID，请在 `args` 中显式传递。
- **权限警告。** 如果目标 `System` 的 `permission=USER`（或任何非 `ADMIN`/`None` 的值），`create_future_call` 会发出警告，因为同一个 `System` 现在也可能由客户端直接调用。最佳实践是未来调用目标使用 `permission=None`。

### 为什么 `create_future_call` 要使用 `:scheduler` 后缀

注意 `depends=("create_future_call:scheduler",)`——这是一个 [`System` 副本](#system-副本通过标签后缀实现垂直分片)。由于 `create_future_call` 可能被你的应用程序中的许多 `System` 引用，直接依赖它会把*所有*它们都拉入一个以 `FutureCalls` 为中心的集群。`:scheduler` 后缀为这个调用方提供了自己独立的队列副本，与同样使用未来调用的其他代码路径隔离。为每个逻辑组选择一个稳定的后缀（例如 `:scheduler`、`:rewards`）——每个不同的后缀都是一个真实的、独立的队列表。

既然表是分离的，`FutureCalls` 工作线程如何知道这些表呢？这是因为这些表会在一个重复列表中注册自己，而 `FutureCalls` 会逐一处理它们。

### 取消未来调用

未来调用行存在于 `FutureCalls`（或其 `:tag` 副本）中。通过 ID 删除即可取消。在模块加载时解析副本，然后将其绑定到取消 `System` 的 `components=`：

```python
from hetu.system.future import FutureCalls

# 与生产者使用的后缀相同：depends=("create_future_call:scheduler",)
_FCQueue = FutureCalls.duplicate("MyGame", "scheduler")


@hetu.define_system(
    namespace="MyGame", permission=hetu.Permission.USER,
    components=(_FCQueue,),
)
async def cancel_future(ctx, future_id):
    if row := await ctx.repo[_FCQueue].get(id=future_id):
        ctx.repo[_FCQueue].delete(row.id)
```

## `call_lock` 与幂等 `System` 执行

`call_lock=True` 使 `System` 参与 HeTu 的 UUID 键控去重。这是未来调用在至少一次传输语义上实现恰好一次执行的方式，你也可以直接使用它，当一个 `System` 必须在每个逻辑操作中最多执行一次时。

```python
@hetu.define_system(
    namespace="Shop", components=(Wallet,),
    permission=None, call_lock=True,
)
async def settle(ctx, user_id, amount):
    async with ctx.repo[Wallet].upsert(owner=user_id) as w:
        w.balance += amount


# 调用者传递一个稳定的 UUID。重新运行是空操作。
await ctx.systems.call("settle", user_id, amount, uuid=order_id_str)
```

机制：

- 启用 `call_lock=True` 会自动向 `System` 的集群附加一个重复的 `SystemLock` `Component`（使用与 `System` 副本相同的后缀技巧——每个上锁的 `System` 对应一个锁表）。
- 调用时，引擎会读取给定 `uuid` 的 `SystemLock`。如果存在行，则跳过 `System` 主体并返回 `None`。
- 提交时，引擎会在同一事务中将 `uuid` 行与你的数据一起写入。如果事务中止（`RaceCondition`），锁行也会随之中止——重试仍被允许，但仅限到成功一次为止。
- 锁行在 `SystemLock` 上无限期存在；在工作器启动时，引擎会清除超过 7 天的行。如果你想更早释放槽位，请使用 `SystemCaller.remove_call_lock(name, uuid)`。

`uuid=` 是 `ctx.systems.call(...)` 以及父 `Systems` 的 `ctx.depend[...](...)` 风格调用上的关键字参数。

## `on_disconnect` 钩子

定义一个名为 `on_disconnect` 的 `System`，当 websocket 关闭时引擎会运行它：

```python
@hetu.define_system(
    namespace="MyGame", components=(OnlineUser,),
    permission=None,  # 不可由客户端调用
)
async def on_disconnect(ctx: hetu.SystemContext):
    if not ctx.caller:
        return
    if row := await ctx.repo[OnlineUser].get(owner=ctx.caller):
        row.online = False
        await ctx.repo[OnlineUser].update(row)
```

注意：

- **`permission=None`** 以保证安全。任何其他值也会生成一个同名的客户端可调用 `Endpoint`，恶意客户端可以随意触发你的“断开连接”行为。引擎本身在套接字关闭调用此钩子时会忽略权限。
- **`ctx.caller`** 是用户 ID（如果连接已提升）或 `0`（如果它在登录前已断开连接）。请相应地进行防护。
- 钩子在正常事务中运行，因此失败会在 `RaceCondition` 时重试。不要在此处阻塞外部服务——连接已经不存在了。
- 触发 `on_disconnect` 是尽力而为的：如果工作器进程被杀死（`SIGKILL`、机器断电），钩子会被跳过。对于保证清理，请将此钩子与周期性 Future Call 配对，以回收过期的 `last_active` 连接。

## 原始 `Endpoints`（多 `System` 或非数据库 RPC）

大多数 RPC 处理器应该是 `Systems`。仅在以下情况之一为真时才使用 `@define_endpoint`：

- 处理器所做的操作**不**涉及任何 `Component`（验证、扇出到外部服务、返回派生数据）。
- 处理器必须调用**多个** `Systems`，并且你明确希望每个 `System` 独立提交——不希望或不可能在它们之间实现原子性（不同的后端、长时间运行的步骤）。

```python
@hetu.define_endpoint(namespace="MyGame", permission=hetu.Permission.USER)
async def buy_and_log(ctx: hetu.EndpointContext, item_id: int):
    # 第一个 System 自行提交。
    res = await ctx.systems.call("buy_item", item_id)
    # 第二个 System 打开一个新事务。
    await ctx.systems.call("log_purchase", item_id)
    return hetu.ResponseToClient(res)
```

与 `Systems` 的关键对比：

- **无自动事务。** 没有 `ctx.repo`，没有 `ctx.depend`。使用 `ctx.systems.call("name", *args)` 调用一个 `System`（每次调用都会打开自己的 Session 并独立提交）。
- **无自动重试。** 在 `System` 内部引发的 `RaceCondition` 仅在该调用内部重试。
- **相同的权限门控。** `permission=USER` 需要 `elevate`，`permission=ADMIN` 需要 `ctx.is_admin()`，等等。
- **相同的 `Context`。** `ctx.caller`、`ctx.user_data` 和速率限制字段的工作方式与 `Systems` 中相同。

## 每连接状态：`user_data`、`group` 和限制

每个连接都有一个 `Context`（`Systems` 使用 `SystemContext`，`Endpoints` 使用普通 `Context`），其生命周期与 websocket 相同。它有多个字段供应用程序代码使用：

```python
async def my_system(ctx: hetu.SystemContext, ...):
    ctx.user_data["last_seen_zone"] = zone_id  # 任意状态
    if ctx.is_admin():
        ...  # ctx.group 以 "admin" 开头
    ctx.client_limits = [[100, 1], [500, 60]]  # 放宽速率限制
    ctx.max_index_sub *= 4  # 允许更多并发范围
```

每个字段的用途：

- **`ctx.user_data: dict[str, Any]`** —— 每个连接的任意状态。用于缓存用户的主要 `OnlineUser` 行、当前区域等。*不*持久化；套接字关闭时消失。它也是 `rls_compare` 第三个元组元素的默认来源（当 `ctx` 本身未找到命名属性时）。
- **`ctx.group: str`** —— 连接的组标签。默认是 `"guest"`；引擎将以 `"admin"` 开头的任何值视为管理员（跳过 RLS 行过滤器，并允许 `Permission.ADMIN` 门控的调用）。从受信任的登录 `System` 设置 `ctx.group = "admin"` 是在 HeTu 中授予管理员权限的方式——没有单独基于令牌的管理员端点。
- **`ctx.client_limits` / `ctx.server_limits`** —— `[max_count, window_seconds]` 对的列表。一旦超出任何一对，引擎就会断开连接。`elevate()` 会自动将这些限制乘以 10 倍，因此登录后的用户获得匿名连接所没有的余量。如果需要为机器人账户等自定义预算，可以在自己的逻辑中覆盖每个连接的设置。
- **`ctx.max_row_sub` / `ctx.max_index_sub`** —— 活动 `Get` 和 `Range` 订阅数量的上限。`elevate()` 会将其乘以 50 倍。根据需要收紧或放宽。
- **`ctx.race_count`** —— 当前事务的重试次数。用于退避非幂等副作用：`if ctx.race_count == 0: send_email(...)` 仅在第一次尝试时发送电子邮件。

`ctx.timestamp` 在每次 `System`/`Endpoint` 调用开始时设置为 `time()`，因此可以安全地用作“现在”，无需重新读取时钟。

## 提前 `session_commit` / `session_discard`

`System` 通常在其主体返回时提交。`ctx` 上有两个可等待方法可以让你提前提交（或中止），从而允许仅在提交成功完成后才执行长时间运行或非幂等的操作。

```python
async def long_running(ctx: hetu.SystemContext, ...):
    async with ctx.repo[Order].upsert(id=order_id) as o:
        o.status = "processing"
    await ctx.session_commit()  # <-- 写入现在已持久化

    # ↓ 下方的慢工作；即使工作器死亡，"processing" 也已持久化。
    result = await call_external_payment_provider(...)
    ...
```

两个重要的注意事项：

- **在此调用之后，`ctx.repo` 和 `ctx.depend` 无法使用。** 主体后续的所有操作都运行在事务*外部*。无法“重新打开”会话。
- **`session_commit` 之后的操作不会在 `RaceCondition` 时重试。** 只有提交前的主体部分参与 HeTu 的乐观重试。如果提交后的工作失败，你需要自行恢复。

`session_discard()` 格式相同，但会丢弃所有内容。当 `System` 提前确定正确的答案是“什么也不做”并且希望完全跳过提交时，使用它。

## 用于范围查询的 NumPy 模式

`await ctx.repo[Comp].range(...)` 返回一个 NumPy **recarray**——一个类型化的 C 结构体数组，而不是 Python 列表。如果你从未使用过 NumPy，跳过以下特性，退回到 `for row in rows:` 是*正确的*，但会浪费 HeTu 的 NumPy 存储所设计实现的吞吐量优势。对于一个 1000 行的 recarray，通过向量化表达式运行的代价大约相当于一个 5 行的 Python `for` 循环。

你需要 `import numpy as np` 来使用辅助函数（`np.sqrt`、`np.percentile`、`np.argsort`、`np.intersect1d`、`np.isin` …）；列方法如 `.sum()` / `.mean()` 无需导入即可使用。

### 列访问与广播

`rows.field` 是该列值的一维数组。运算符**逐元素**应用，标量会广播到整个列：

```python
rows = await ctx.repo[Player].range("level", 1, 100, limit=1000)

# 每个行到原点的距离，在 C 中向量化。
d2 = rows.x ** 2 + rows.y ** 2

# 将所有行移动 (dx, dy)
shifted_x = rows.x + dx
shifted_y = rows.y + dy
```

对于两个等长的 recarray，运算符按元素对齐：

```python
dx = players.x - targets.x
dy = players.y - targets.y
distances = np.sqrt(dx * dx + dy * dy)
```

对于 1k 行查询，Python 等价写法（`for row in rows: d = row.x ** 2 + row.y ** 2`）会慢 10–100 倍，因为内部工作被解释执行而非 SIMD 向量化。

### 布尔掩码：无需重新查询的复合过滤

对列的比较会返回一个布尔数组；用该掩码索引 recarray 会返回匹配的行：

```python
hot = rows[rows.hp < 30]
mine = rows[rows.owner == ctx.caller]
```

使用 `&`、`|`、`~` 组合掩码。**括号是必须的**——`&` 的运算符优先级低于 `<`/`==`，因此不加括号会引发错误：

```python
critical = rows[(rows.hp < 30) & (rows.shield == 0)]
```

这是**复合查询**的推荐模式：使用一个索引列从数据库拉取一个小窗口，然后在内存中用 NumPy 进行细化。要求后端评估复合谓词会更慢，因为数据库解释查询而 NumPy 使用 SIMD。

```python
# 使用真实索引拉取一个小窗口，然后在内存中过滤。
items = await ctx.repo[Item].range(level=(10, 20), limit=200)
cheap_strong = items[(items.price < 100) & (items.attack > 50)]
```

### 聚合与统计

每个列都有内置的统计方法：

```python
total_damage = rows.damage.sum()
average_hp = rows.hp.mean()
max_score = rows.score.max()
hp_p95 = np.percentile(rows.hp, 95)
hp_std = rows.hp.std()
n_alive = (rows.hp > 0).sum()  # 通过布尔掩码计数
```

`(boolean_array).sum()` 统计 `True` 的数量——这是经典的“计数符合条件”的模式。使用它代替 `len([r for r in rows if r.hp > 0])`。

对于分组计数，`np.unique(arr, return_counts=True)` 是 `collections.Counter` 的一行等效写法：

```python
kinds, counts = np.unique(rows.kind, return_counts=True)
# kinds  = array(['chat', 'system'], dtype='<U16')
# counts = array([842, 18])
```

`len(rows)` 和 `rows.shape[0]` 都能获得行数。

### Top-N，argmin，argsort

对非索引列排序在内存中进行是可以接受的。`argsort` 返回排序后的*索引*——用这些索引来索引 recarray 以选择前 N 个，无需排序两次：

```python
# 按分数降序排名前三。
top3 = rows[np.argsort(rows.score)[-3:][::-1]]

# 离目标点最近的单行。
dx = rows.x - target_x
dy = rows.y - target_y
nearest = rows[np.argmin(dx * dx + dy * dy)]
```

在一维列上使用 `argmax`/`argmin` 会返回极值点的索引；用该索引索引 recarray 就可以得到整行。

### “连接”两个范围查询

一个常见的模式：分别查询两个索引 `Components`，然后通过 `owner`（或任何共享键）在进程内合并：

```python
positions = await ctx.repo[Position].range("zone", zone_id, zone_id, limit=500)
hps = await ctx.repo[HP].range("zone", zone_id, zone_id, limit=500)

# 两个结果集中都存在的所有者
both = np.intersect1d(positions.owner, hps.owner)
matched_positions = positions[np.isin(positions.owner, both)]
```

`np.intersect1d` 和 `np.isin` 是 `set` 交集和 `in` 成员检查的 SIMD 友好替代；它们都保持在 NumPy 域内，因此结果保持 recarray 类型，你可以继续在其上链接更多掩码。

### 何时 Python 循环才是正确答案

两种退回到 `for` 循环也没问题（或不可避免）的情况：

- **逐行数据库写入。** `ctx.repo[Comp].update(row)` 和 `upsert(...)` 是异步的，一次操作一行。显式遍历：`for r in rows: await ctx.repo[Comp].update(r)`。
- **真正异构的逐行工作。** 每行分支到不同的表或不同的外部服务无法向量化；循环比扭曲的 NumPy 更清晰。

对于其他所有情况——过滤、算术、统计、排序、连接——都保持在 NumPy 中。

## 每 `Component` 多后端

每个 `@define_component` 都接受 `backend="<name>"`，其中名称匹配配置中 `BACKENDS:` 块的一个键。不同的 `Components` 可以位于不同的物理数据库上：

```python
@hetu.define_component(namespace="Game", backend="hot")
class Position(hetu.BaseComponent):
    ...


@hetu.define_component(namespace="Game", backend="cold")
class GameLog(hetu.BaseComponent):
    ...
```

硬约束：**一个 `System` 引用的每个 `Component` 必须共享同一个后端**。集群构建器在启动时会拒绝混合后端的集群。因此实际上，“多后端”是指“不同组的关联 `Components` 位于不同的数据库上”。目前仅支持 Redis；其用途在现阶段有限，它是为未来扩展而设计的。

后端名称默认为 `"default"`。如果不存在名为 `"default"` 的键，则 `config.yaml` 的 `BACKENDS:` 中列出的第一个后端也会被视为默认后端。

## 易失性组件

`@define_component(volatile=True, ...)` 将一个 `Component` 标记为**易失性**：其行应在模式维护期间被清除，并且它们可以用于 `direct_set` 低级写入（引擎使用它进行快速非事务性更新，例如内置 `Connection` `Component` 上的 `last_active`）。

在以下情况使用易失性：

- `Component` 表示瞬态运行时状态——连接、会话、租约、临时排行榜——你在全新服务器启动时也会重建它们。
- 你存储的行的真实数据源在其他地方（外部服务、另一个 `Component`），而易失性副本只是缓存。

不要对玩家数据、货币或任何你在部署之间关心的数据使用易失性——`hetu upgrade` 被允许清除这些表。

## `core` 命名空间：无需 `System` 引用即可加载

通常，一个 `Component` 只有在至少一个 `System` 通过 `components=(...)` 引用它时，才会注册到 `ComponentTableManager`（即它会获得一个可以进行查询的真实后端表）。如果你 `@define_component` 了某个东西但从未将其连接到 `System` 中，引擎会将其视为死代码并跳过。

`namespace="core"` 是唯一的例外：在启动时，集群构建器通过合成一个引用它的空全局 `System` 自动固定每个核心 `Component`。然后无论用户代码做什么，该 `Component` 都会出现在每个命名空间的表管理器中。

为什么引擎需要这样：HeTu 自己的基础设施表——`Connection`（每个 websocket 的存在记录）和 `WorkerLease`（SnowflakeID 工作器 ID 池）——是通过 `tbl_mgr.get_table(Connection)` 从引擎内部直接访问的。它们从未被用户定义的 `Systems` 引用。如果没有 `core`，这些表就不会被创建，引擎也就无法跟踪连接或分配工作器 ID。

对于应用程序代码，这主要是一个实现细节。合法的用户端使用场景很窄：你想让某个 `Component` 存在于数据库中，但只能通过直接后端调用（`direct_set`/`direct_get`）进行读写。如果某个常规 `System` 会触及它，那就给它一个普通的命名空间——它会被自动加载。

## 自定义消息管道层

线路协议是一个 `MessageProcessLayer` 对象栈：每一层为外发数据编码并解码传入数据。`CONFIG_TEMPLATE.yml` 中的默认栈是 `jsonb → zlib → crypto`。你可以通过子类化来替换或添加层：

```python
# myproto.py
from hetu.server.pipeline import MessageProcessLayer


class FramingLayer(MessageProcessLayer, alias="framing"):
    def is_handshake_required(self) -> bool:
        return False

    def encode(self, layer_ctx, message):
        return b"\x01" + message  # 此处的 message 是 bytes

    def decode(self, layer_ctx, message):
        assert message[:1] == b"\x01"
        return message[1:]
```

在 `config.yml` 中：

```yaml
PACKET_LAYERS:
  - type: jsonb
  - type: zlib
    level: 1
  - type: framing       # ← 你的层，由 `alias` 标识
  - type: crypto
    auth_key: ...
```

需要记住的三条规则：

- 子类会在其 `alias`（在类头中声明）下自动注册。`MessageProcessLayerFactory.create(type=...)` 通过别名解析它。
- **顺序很重要，并且必须与客户端匹配。** 编码在外发时从上到下运行；解码在传入时从下到上运行。如果客户端以不同的顺序运行这些层，它将无法与你的服务器通信。
- 是否需要握手指定（`is_handshake_required()`）决定它是否在 websocket 打开期间有机会协商参数。大多数层（压缩、分帧）返回 `False`。默认的 `crypto` 层返回 `True` 以执行 ECDH 密钥交换。

Unity SDK 附带了匹配的默认值。如果你添加了自定义层，你需要一个匹配的客户端实现——线路上没有自动发现机制。

## 可操作性可见性：重放与慢日志

HeTu 暴露了两个容易被忽略的专用日志记录器：

- **`HeTu.replay`** —— 每个连接事件（握手、RPC 调用、非法请求、websocket 关闭）都会以 `INFO` 级别发送到此日志记录器。默认配置将其写入循环的 `replay.log`。逐行重放该日志可以重现服务器看到的精确顺序——对于诊断“我无法重现”的 bug 非常宝贵。将级别设置为 `ERROR` 可完全禁用（引擎会快速路径处理字符串格式化开销）。
- **`HeTu.root` 慢日志** —— 引擎会测量每个 `System` 调用的挂钟时间和 `RaceCondition` 重试次数。当单个调用超过约 1 秒或 5 次重试时，它会记录一条警告，其中包含该工作器上最慢/争用最激烈的 `Systems` 的前 20 个表。每个工作器随机选择一个 60–600 秒的抑制间隔，以避免所有副本同时打印相同的警告。

两者都在 `config.yml` 的 `LOGGING:` 部分（标准 `dictConfig`）中配置。你会在 `CONFIG_TEMPLATE.yml` 中通过处理器名称看到它们。

## SnowflakeID 与工作器 ID

HeTu 中的每个行 ID（`row.id`）都是一个 64 位 Snowflake：

```
1 符号位 | 41 时间戳(毫秒) | 10 工作器 ID | 12 序列号
```

你实际关心的数字：

- 每个工作器每毫秒 4096 个 ID。
- 整个集群最多 1024 个工作器（租约池）。
- 从纪元（`2025-12-18` UTC+8）起有 69 年的余量。

工作器 ID 由 `WorkerKeeper` 自动租用，它将这些 ID 存储在 `WorkerLease` `Component`（一个 `core`/易失性表）中，并且每 5 秒续租一次。如果进程在未释放租约的情况下死亡，该槽位会在租约过期后被回收。启动时，引擎会恢复*上次持久化的时间戳*，如果系统时钟回退，会等待一个短暂的宽限期——这是 HeTu 在主机重启后 NTP 调整时防止重复 ID 的防御机制。

对于你的代码，实际影响很短：

- `id` 是为你生成的；切勿自己赋值。
- `BaseComponent.new_row()` 在底层调用 `SnowflakeID().next_id()`。如果你批量插入，首选 `new_rows(N)` 以便所有 ID 来自同一个单调突发。
- 时钟回滚保护意味着**时钟严重错误的服务器将拒绝发出 ID 并停止工作**。运行 NTP。如果你必须修复回滚，请重新启动受影响的工作器——管理者会自动重新播种时间戳。

## 接下来去哪儿

- **[运维](operations.md)** —— 生产部署、Redis 拓扑、`hetu` CLI 以及 `config.yml` 的其余部分。
- **[API 参考](api/)** —— 本页引用的所有公共符号。参阅 `new_rows(N)` 以便所有 ID 来自同一个单调突发。
- 时钟回滚保护意味着**时钟严重错误的服务器将拒绝发出 ID 并停止工作**。运行 NTP。如果你必须修复回滚，请重新启动受影响的工作器——管理者会自动重新播种时间戳。
