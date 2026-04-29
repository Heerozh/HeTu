---
title: "Tutorial: Chat Room"
description: "Build a multi-user chat application with HeTu, step by step."
type: docs
weight: 10
prev: /tutorial
next: ../concepts
---

In this tutorial you'll build a multi-user chat room. By the end you'll have
exercised every major HeTu concept: typed components, server-side systems,
subscription-based realtime updates, permission checks, and lifecycle hooks.

The complete reference implementation is at
[
`examples/chat/server/src/app.py`](https://github.com/Heerozh/HeTu/blob/main/examples/chat/server/src/app.py).

## What you'll build

- A presence component (who's online)
- A chat-history component
- Login, send-message, and quit RPCs
- A subscription that pushes new messages to every client in real time
- A disconnect hook that cleans up presence

## Step 1 — Define `ChatMessage`

`Components` are typed tables. Add this to `src/app.py`:

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

Notes on this declaration:

- `permission=Permission.EVERYBODY` lets unauthenticated clients **read** this
  table. (Writes still go through `Systems`, which can have stricter permissions.)
- `index=True` on `owner` and `created_at_ms` builds sorted indexes that
  clients can `range`-query.
- `dtype="U256"` declares a fixed-width 256-character UTF-32 column. Strings
  are stored in NumPy structured arrays, which is why widths are explicit.

## Step 2 — Define `OnlineUser` (presence)

Add a second component for who's currently connected:

```python
@hetu.define_component(namespace="Chat", permission=hetu.Permission.EVERYBODY)
class OnlineUser(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)
    name: str = hetu.property_field("", unique=True, dtype="U32")
    online: bool = hetu.property_field(False)
    last_seen_ms: np.int64 = hetu.property_field(0)
```

`unique=True` on `owner` and `name` enforces uniqueness at insert time and
also creates a fast point-lookup index.

## Step 3 — `user_login` System

`Systems` are async functions that run inside a transaction. They receive a
`SystemContext` (typically `ctx`) plus your RPC arguments.

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
        text=f"{name} joined the chat", kind="system",
    )
```

Two things worth pointing out:

- **`hetu.elevate(ctx, user_id)`** promotes this connection from anonymous to
  user-authenticated. Everything after it on the same connection runs with
  `ctx.caller == user_id` and passes `Permission.USER` checks. (Real
  applications will validate `user_id` against an external auth provider
  before calling `elevate`.)
- **`ctx.user_data`** is a per-connection dictionary that survives across RPC
  calls. We stash the user's `OnlineUser` row so later systems don't have to
  re-query it.

## Step 4 — `user_chat` System

The actual "send a message" RPC:

```python
@hetu.define_system(
    namespace="Chat", components=(ChatMessage,),
    permission=hetu.Permission.USER,
)
async def user_chat(ctx: hetu.SystemContext, text: str):
    me = ctx.user_data["me"]
    assert me and me.online, "call user_login first"
    await _insert_message(
        ctx, owner=ctx.caller, name=me.name, text=text, kind="chat",
    )
```

`permission=Permission.USER` means only connections that have been through
`elevate` can call this — anonymous clients get an error before the function
body runs.

## Step 5 — `user_quit` and `on_disconnect`

Cleanly mark a user offline:

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
            text=f"{row.name} left the chat", kind="system",
        )


@hetu.define_system(
    namespace="Chat", components=(OnlineUser,),
    depends=("user_quit",), permission=None,
)
async def on_disconnect(ctx: hetu.SystemContext):
    await ctx.depend["user_quit"](ctx)
```

`on_disconnect` is special:

- `permission=None` means **the client cannot call it directly.**
- HeTu fires it automatically when a websocket connection closes.
- `depends=("user_quit",)` lets us reuse `user_quit`'s implementation
  via `ctx.depend["user_quit"](ctx)`.

## Step 6 — Run it

Save `src/app.py` and start the server (SQLite for local dev):

```bash
uv run hetu start \
  --app-file=./src/app.py \
  --db=sqlite:///./chat.db \
  --namespace=Chat \
  --instance=dev
```

The provided example also ships a `config.yml` you can use instead:

```bash
cd examples/chat/server
uv run hetu start --config=./config.yml
```

## Step 7 — Subscribe from a client

In Unity, subscribe to the chat history and react to new messages:

```csharp
// Fire and forget connect
// In practice, this should be wrapped within an asynchronous method, 
// with a loop controlling the automatic reconnection.
HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu/Chat"); 
// will automatically wait for the connection to be established before sending.
await HeTuClient.Instance.CallSystem("user_login", 1001, "Alice");

var messages = await HeTuClient.Instance.Range<ChatMessage>(
    "created_at_ms", 0, long.MaxValue, 1024);

messages.addTo(gameObject);
messages.ObserveAdd()
    .Subscribe(msg => Debug.Log($"{msg.name}: {msg.text}"))
    .AddTo(ref messages.DisposeBag);

await HeTuClient.Instance.CallSystem("user_chat", "Hello, world!");
```

The subscription is reactive: any new message inserted by **any** client (not
just yours) flows into `ObserveAdd()` within milliseconds, no polling.

## What you've learned

- **Components** are typed tables stored in Redis (or SQLite/Postgres in dev).
- **Systems** are async functions that read/write components inside a
  transaction. Their `permission=` controls who can call them.
- **`elevate()`** promotes a connection to authenticated.
- **Subscriptions** push row-level changes to clients without polling.
- **Lifecycle systems** (`on_disconnect`, periodic `FutureCalls`, etc.) let
  the engine call your code on its own schedule.

## Where to next

- **[Concepts](../concepts.md)** — the underlying ECS model, transaction
  guarantees, and permission system in depth.
- **[API Reference](../api/)** — every public symbol, with signatures and
  examples.
- **[Operations](../operations.md)** — Docker, Redis topology, and load
  balancing for production.
