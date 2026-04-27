---
title: "Concepts"
description: "HeTu's ECS model, subscription system, permissions, and transaction guarantees."
type: docs
weight: 30
prev: tutorial/chat-room
next: operations
---

# Concepts

This page explains the mental model behind HeTu so you can reason about
performance, transactions, and security yourself instead of guessing.

## ECS in one paragraph

HeTu uses the Entity-Component-System pattern in its original game-engine
form, not the data-mapper sense the term has acquired in some web frameworks.
**Entities** are implicit — every row carries an int64 `id` and that is the
entity. **Components** are typed tables (one per logical kind of data).
**Systems** are async functions that operate on those tables inside a
transaction. There is no inheritance, no per-row methods, and no central
"world" object. State lives in Redis (or SQL); systems are stateless.

## Components

A Component is a typed table backed by NumPy structured arrays. You declare
it with `@define_component` and one `property_field()` per column:

```python
@hetu.define_component(namespace="Chat", permission=hetu.Permission.EVERYBODY)
class ChatMessage(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, index=True)
    text: str = hetu.property_field("", dtype="U256")
```

A few invariants that surprise new users:

- **Strings are fixed-width.** `dtype="U256"` is a 256-character UTF-32 column;
  longer values are truncated. This is the cost of NumPy storage and the
  source of HeTu's read throughput.
- **No nulls.** Every column has a default; you cannot tell whether a value
  was "set" or "still default". If you need optional data, split it into a
  separate Component and join via `owner`.
- **One index type, two flavors.** Indexes are always sorted sets supporting
  `range()` queries and subscriptions. `unique=True` is the same sorted index
  plus a uniqueness check on insert, and it implicitly turns on `index=True`.
- **`namespace=` is just a label.** Any string works. A running server binds
  to exactly one namespace at startup (`--namespace`) and only its Systems
  and Endpoints are loaded; Components from any namespace come along for the
  ride if those Systems reference them. To host multiple namespaces, start
  multiple servers.

## Systems

A System is an async function decorated with `@define_system`. It declares
which Components it touches via `components=(...)`, and runs inside a
transaction:

```python
@hetu.define_system(
    namespace="Chat", components=(ChatMessage,), permission=hetu.Permission.USER
)
async def user_chat(ctx: hetu.SystemContext, text: str):
    row = ChatMessage.new_row()
    row.owner = ctx.caller
    row.text = text
    await ctx.repo[ChatMessage].insert(row)
```

The `ctx` argument carries the transaction (`ctx.repo[Component]`), the
caller's id (`ctx.caller`), per-connection state (`ctx.user_data`), and a
handle for calling other systems (`ctx.depend["other_system"]`).

### System Clusters — the unit of isolation

The `components=` declaration is not just a hint; the engine groups Systems
into **co-location clusters** based on overlap. Two Systems whose
`components=` sets share at least one Component live in the same cluster.
Systems in different clusters never conflict and run in parallel; Systems
in the same cluster contend for the same set of rows.

Practical consequence: declare `components=` accurately. Leaving out a
Component "for performance" hides a real conflict and corrupts data.
Including extra Components serializes work that didn't need to be serialized.

### `RaceCondition` and automatic retry

HeTu uses optimistic concurrency. Every Session keeps an `IdentityMap` of the
rows it read or wrote. On commit, the engine checks each row's version against
Redis. If anything changed underneath, the commit aborts with `RaceCondition`
and the engine **automatically re-runs the System from the top**, up to
`retry=` times (default 9999).

Two implications:

- Your System body must be **safe to re-run**. Don't send an HTTP request
  from inside a System unless that request is idempotent.
- Long-running Systems are more likely to lose the race. Keep them short;
  push slow work into a separate Endpoint that doesn't hold the transaction.

## Endpoints (advanced)

Endpoints are the underlying RPC primitive; Systems are Endpoints with a
transactional body. You only need to write a raw Endpoint when:

- You want to call **multiple** Systems from a single client RPC, with each
  System committing independently.
- You want to do work that doesn't touch Components at all (validation,
  fan-out to external services).

```python
@hetu.define_endpoint(namespace="Chat", permission=hetu.Permission.USER)
async def whoami(ctx: hetu.EndpointContext):
    return hetu.ResponseToClient({"id": ctx.caller})
```

**Caveat:** Systems called through an Endpoint do **not** share a transaction.
Each commits on its own. If you need atomicity across multiple Components,
declare a single System that lists all of them in `components=` instead.

## Subscriptions

Clients ask the server for live row data with two operations:

- **`select(Component, key=value)`** — one row, looked up by a unique key.
- **`range(Component, index, low, high, limit)`** — a sorted slice over an
  index, refreshed on every change.

Behind the scenes the `SubscriptionBroker` watches Redis pub/sub for row
changes, filters them by the client's permission level, and pushes deltas
back over the websocket. Latency is dominated by Redis round-trip — typically
under one millisecond on the same VPC.

Subscriptions are checked against the same permission system as Systems, so a
client cannot subscribe to data it isn't allowed to see.

## Permissions

Every Component and every System carries a `permission=` level. The four
useful levels:

| Level       | Meaning                                                                                                                                                                                                                           |
|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `EVERYBODY` | Any websocket connection, including pre-`elevate`. Useful for chat history, lobby lists, anything public.                                                                                                                         |
| `USER`      | Connection must have called `elevate(ctx, user_id)` first. Standard "logged in" gate.                                                                                                                                             |
| `OWNER`     | Same as USER plus an automatic row filter `row.owner == ctx.caller`. Use for personal inventory, private messages.                                                                                                                |
| `RLS`       | Same as OWNER but with a custom comparator. Declare `rls_compare=(operator, component_field, context_field)` on the Component to use a non-`owner` filter (for example, "rows whose `guild_id` matches the caller's `guild_id`"). |
| `ADMIN`     | Server-internal calls only; not exposed over the RPC wire.                                                                                                                                                                        |

OWNER and RLS are enforced inside `SessionRepository`, not just at the call
boundary. A System with `permission=USER` that reads an OWNER Component still
only sees rows the caller owns — there is no way to "leak" through a more
privileged caller.

## Transactions

Every System call opens a `Session`. A Session holds an `IdentityMap` (the set
of rows touched), routes reads and writes through `SessionRepository` per
Component, and commits all writes atomically at the end. If two Sessions
conflict on a row, the second to commit raises `RaceCondition` and the engine
retries.

There is no manual `BEGIN` / `COMMIT` — Systems are the transaction boundary.
If you need multiple steps that share a transaction, put them in one System.
If you need steps that should commit independently, use an
[Endpoint](#endpoints-advanced) and call multiple Systems from it.

## Where to next

- **[Operations](operations.md)** — production deployment, Redis topology,
  load balancing.
- **[API Reference](api/)** — generated reference for every public symbol.
