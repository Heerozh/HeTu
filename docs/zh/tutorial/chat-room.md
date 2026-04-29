---
title: "教程：聊天室"
description: "逐步构建一个使用 HeTu 的多用户聊天应用。"
type: docs
weight: 10
prev: /tutorial
next: ../concepts
---

在本教程中，你将构建一个多用户聊天室。完成后，你将实践 HeTu 的每一个主要概念：类型化组件、服务端系统、基于订阅的实时更新、权限检查和生命周期钩子。

完整的参考实现位于：
[
`examples/chat/server/src/app.py`](https://github.com/Heerozh/HeTu/blob/main/examples/chat/server/src/app.py)。

## 你将构建什么

- 一个在线状态组件（谁在线）
- 一个聊天历史组件
- 登录、发送消息和退出的 RPC
- 一个将新消息实时推送给每个客户端的订阅
- 一个清理在线状态的断连钩子

## 第 1 步 — 定义 `ChatMessage`

组件是类型化的表。将以下内容添加到 `src/app.py`：

```python
import time
import numpy as np
import hetu


@hetu.define_component(namespace="Chat", permission=hetu.Permission.EVERYBODY)
class ChatMessage(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, index=True)
    name: str = hetu.property_field("", dtype="U32")
    text: str = hetu.property_field("", dtype="U256")
    kind: str = hetu.property_field("chat", dtype="U16")
    created_at_ms: np.int64 = hetu.property_field(0, index=True)
```

关于此声明的一些说明：

- `permission=Permission.EVERYBODY` 允许未认证的客户端**读取**此表。（写入仍然通过系统进行，系统可以有更严格的权限。）
- `owner` 和 `created_at_ms` 上的 `index=True` 构建了排序索引，客户端可以通过 `range` 查询。
- `dtype="U256"` 声明了一个固定宽度的 256 字符 UTF-32 列。字符串存储在 NumPy 结构化数组中，这就是为什么需要显式指定宽度。

## 第 2 步 — 定义 `OnlineUser`（在线状态）

为当前在线的用户添加第二个组件：

```python
@hetu.define_component(namespace="Chat", permission=hetu.Permission.EVERYBODY)
class OnlineUser(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)
    name: str = hetu.property_field("", unique=True, dtype="U32")
    online: bool = hetu.property_field(False)
    last_seen_ms: np.int64 = hetu.property_field(0)
```

`owner` 和 `name` 上的 `unique=True` 在插入时强制唯一性，同时也创建了一个快速的点查找索引。

## 第 3 步 — `user_login` 系统

系统是在事务中运行的异步函数。它们接收一个 `SystemContext`（通常是 `ctx`）加上你的 RPC 参数。

```python
def _now_ms() -> int:
    return int(time.time() * 1000)


async def _insert_message(ctx, owner, name, text, kind):
    row = ChatMessage.new_row()
    row.owner = owner
    row.name = name
    row.text = text
    row.kind = kind
    row.created_at_ms = _now_ms()
    await ctx.repo[ChatMessage].insert(row)


@hetu.define_system(
    namespace="Chat",
    components=(OnlineUser, ChatMessage),
    permission=hetu.Permission.EVERYBODY,
)
async def user_login(ctx: hetu.SystemContext, user_id: int, name: str):
    await hetu.elevate(ctx, int(user_id), kick_logged_in=True)
    async with ctx.repo[OnlineUser].upsert(owner=ctx.caller) as row:
        row.name = name
        row.online = True
        row.last_seen_ms = _now_ms()
        ctx.user_data["me"] = row

    await _insert_message(
        ctx, owner=ctx.caller, name=name,
        text=f"{name} 加入了聊天", kind="system",
    )
```

有两件事值得指出：

- **`hetu.elevate(ctx, user_id)`** 将此连接从匿名提升为用户认证。之后在同一个连接上的一切操作都以 `ctx.caller == user_id` 运行，并通过 `Permission.USER` 检查。（实际应用应在调用 `elevate` 之前，根据外部认证提供者验证 `user_id`。）
- **`ctx.user_data`** 是一个按连接存储的字典，可在 RPC 调用间持久存在。我们将用户的 `OnlineUser` 行存储起来，这样后面的系统就不必重新查询了。

## 第 4 步 — `user_chat` 系统

实际的“发送消息”RPC：

```python
@hetu.define_system(
    namespace="Chat", components=(ChatMessage,),
    permission=hetu.Permission.USER,
)
async def user_chat(ctx: hetu.SystemContext, text: str):
    me = ctx.user_data["me"]
    assert me and me.online, "请先调用 user_login"
    await _insert_message(
        ctx, owner=ctx.caller, name=me.name, text=text, kind="chat",
    )
```

`permission=Permission.USER` 意味着只有经过 `elevate` 的连接才能调用它——匿名客户端在函数体运行之前就会收到错误。

## 第 5 步 — `user_quit` 和 `on_disconnect`

干净地将用户标记为离线：

```python
@hetu.define_system(
    namespace="Chat", components=(OnlineUser, ChatMessage),
    permission=hetu.Permission.USER,
)
async def user_quit(ctx: hetu.SystemContext):
    if row := await ctx.repo[OnlineUser].get(owner=ctx.caller):
        row.online = False
        row.last_seen_ms = _now_ms()
        await ctx.repo[OnlineUser].update(row)
        await _insert_message(
            ctx, owner=ctx.caller, name=row.name,
            text=f"{row.name} 离开了聊天", kind="system",
        )


@hetu.define_system(
    namespace="Chat", components=(OnlineUser,),
    depends=("user_quit",), permission=None,
)
async def on_disconnect(ctx: hetu.SystemContext):
    await ctx.depend["user_quit"](ctx)
```

`on_disconnect` 是特殊的：

- `permission=None` 意味着**客户端不能直接调用它。**
- HeTu 在 WebSocket 连接关闭时自动触发它。
- `depends=("user_quit",)` 让我们通过 `ctx.depend["user_quit"](ctx)` 复用 `user_quit` 的实现。

## 第 6 步 — 运行它

保存 `src/app.py` 并启动服务器（本地开发使用 SQLite）：

```bash
uv run hetu start \
  --app-file=./src/app.py \
  --db=sqlite:///./chat.db \
  --namespace=Chat \
  --instance=dev
```

提供的示例还附带了一个 `config.yml`，你可以使用它：

```bash
cd examples/chat/server
uv run hetu start --config=./config.yml
```

## 第 7 步 — 从客户端订阅

在 Unity 中，订阅聊天历史并对新消息做出响应：

```csharp
// Fire and forget connect
// 实际使用中，应将其包装在异步方法中，并通过循环控制自动重连。
HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu/Chat"); 
// 会自动等待连接建立后再发送。
await HeTuClient.Instance.CallSystem("user_login", 1001, "Alice");

var messages = await HeTuClient.Instance.Range<ChatMessage>(
    "created_at_ms", 0, long.MaxValue, 1024);

messages.addTo(gameObject);
messages.ObserveAdd()
    .Subscribe(msg => Debug.Log($"{msg.name}: {msg.text}"))
    .AddTo(ref messages.DisposeBag);

await HeTuClient.Instance.CallSystem("user_chat", "Hello, world!");
```

订阅是响应式的：由**任何**客户端（不仅仅是你的）插入的任何新消息，都会在毫秒内流入 `ObserveAdd()`，无需轮询。

## 你学到了什么

- **组件**是存储在 Redis（或开发中的 SQLite/Postgres）中的类型化表。
- **系统**是在事务内读写组件的异步函数。它们的 `permission=` 控制谁可以调用它们。
- **`elevate()`** 将连接提升为已认证。
- **订阅**将行级更改推送到客户端，无需轮询。
- **生命周期系统**（`on_disconnect`、定期的 `FutureCalls` 等）让引擎按自己的调度调用你的代码。

## 下一步

- **[概念](../concepts.md)** — 深入了解底层的 ECS 模型、事务保证和权限系统。
- **[API 参考](../api/)** — 所有公共符号，包含签名和示例。
- **[运维](../operations.md)** — Docker、Redis 拓扑和生产环境负载均衡。
