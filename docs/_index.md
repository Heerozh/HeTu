---
title: "HeTu Documentation"
description: "A high-performance, distributed game-server engine built on ECS and Redis."
type: docs
next: getting-started
sidebar:
  open: true
---

# HeTu

HeTu (河图) is a high-performance, multi-process, distributed game-server
engine. It uses the **Entity-Component-System (ECS)** pattern, stores state in
**Redis**, and exposes that state to game clients over **WebSocket** as a
subscribable, row-level-permissioned database.

## Why HeTu?

- **2-Tier development model.** Write game logic directly against typed
  components — no separate database layer, no ORM, no transaction plumbing.
- **Stateful long-lived connections.** Unlike Backend-as-a-Service products
  that focus on stateless CRUD, HeTu is built for in-memory game state with
  millisecond-scale push updates.
- **Redis throughput.** Roughly 10x the write throughput of typical
  BaaS-on-Postgres stacks; see the benchmarks in the project README.
- **Reactive Unity client.** The C# SDK ships with subscription objects that
  drive UI updates without polling.

## A 30-second example

Server (Python):

```python
import hetu
import numpy as np


@hetu.define_component(namespace="Chat", permission=hetu.Permission.EVERYBODY)
class ChatMessage(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, index=True)
    text: str = hetu.property_field("", dtype="U256")


@hetu.define_system(
    namespace="Chat", components=(ChatMessage,), permission=hetu.Permission.USER
)
async def user_chat(ctx: hetu.SystemContext, text: str):
    row = ChatMessage.new_row()
    row.owner = ctx.caller
    row.text = text
    await ctx.repo[ChatMessage].insert(row)
```

Client (Unity / C#):

```csharp
await HeTuClient.Instance.CallSystem("user_chat", "Hello world");

var sub = await HeTuClient.Instance.Range<ChatMessage>("id", 0, long.MaxValue, 1024);
sub.AddTo(gameObject); 
sub.ObserveAdd().Subscribe(msg => Render(msg));
```

That's the entire wire: a typed table, an RPC entry point, and a reactive
subscription. No schema migrations, no API gateway, no message broker.

## Where to next

- **[Getting Started](getting-started.md)** — install HeTu and run your first
  server in under 10 minutes.
- **[Tutorial: Chat Room](tutorial/chat-room.md)** — build the example above
  end-to-end.
- **[Concepts](concepts.md)** — the ECS model, subscriptions, permissions, and
  the transaction guarantees you get.
- **[Operations](operations.md)** — production deployment, Redis topology,
  and load balancing.
- **[API Reference](api/)** — auto-generated from source docstrings.

## Status

HeTu is currently in **closed beta** (内测中) and developed primarily inside
its sponsoring company. The public API is stable enough to build against, but
expect occasional breaking changes until the 1.0 release.
