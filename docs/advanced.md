---
title: "Advanced"
description: "`System` copies for vertical sharding, scheduled FutureCalls, lifecycle hooks, raw `Endpoints`, custom pipeline layers, NumPy patterns for range queries, and other engine internals you'll reach for once a project gets real."
type: docs
weight: 38
prev: unity-client
next: operations
---

Once you've built a few `Systems` and shipped them through the Unity SDK, the
features on this page are what you'll reach for next. None of them are
required to get started, but each one solves a real problem that comes up
once a project leaves the prototype stage:

- **[`System` copies](#system-copies-vertical-sharding-via-tag-suffixes)** —
  reuse one `System` body against multiple, isolated tables so a hub
  `Component` doesn't pull every dependent `System` into one cluster.
- **[Future calls](#future-calls-scheduled-and-recurring-system-invocations)** —
  durable scheduled and recurring server-side tasks.
- **[`call_lock`](#call_lock-and-idempotent-system-execution)** — make a
  `System` idempotent under retries.
- **[Lifecycle hooks](#the-on_disconnect-hook)** — `on_disconnect` runs on
  socket close.
- **[Raw `Endpoints`](#raw-endpoints-multi-system-or-non-database-rpcs)** —
  RPC handlers without a transaction, for non-database work or to call
  multiple `Systems` independently.
- **[Per-connection state](#per-connection-state-user_data-group-and-limits)** —
  `ctx.user_data`, admin elevation via `ctx.group`, and rate-limit
  overrides.
- **[Early `session_commit` / `session_discard`](#early-session_commit--session_discard)
  ** —
  commit (or abort) the transaction before the `System` body returns.
- **[NumPy patterns for range queries](#numpy-patterns-for-range-queries)** —
  broadcasting, boolean masks, aggregations, and joining two queries in
  memory instead of looping.
- **[Multiple backends](#multiple-backends-per-component)** — pin selected
  `Components` to a separate database via `backend=`.
- **[Volatile components](#volatile-components)** — `volatile=True` for
  state that should be cleared on schema-maintenance.
- **[The `core` namespace](#the-core-namespace-always-loaded-components)** —
  `Components` that load even when no `System` references them.
- **[Custom pipeline layers](#custom-message-pipeline-layers)** — extend
  the wire protocol with your own compression / encryption / framing.
- **[Replay & slow logs](#operational-visibility-replay-and-slow-logs)** —
  reproducing a bug and finding hot `Systems`.
- **[SnowflakeID](#snowflakeid-and-worker-ids)** — the row-id generator and
  the worker-ID lease behind it.

## `System` copies (vertical sharding via `:tag` suffixes)

When several `Systems` all reference the same `Component`, the cluster builder
unions them all into one **co-location cluster** — the price you pay for
transactional isolation. That's fine until a single `Component` (a queue, a
counter, a leaderboard) pulls dozens of unrelated `Systems` into one big
cluster you can no longer shard horizontally.

The escape hatch is a **`System` copy**: you keep one `System` definition but
materialize it against a *separate* table, by appending a `:tag` suffix in
`depends=`:

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

What that one suffix actually does:

- The `Order` `Component` is duplicated into a sibling table named
  `Order:ItemOrder`. It carries the same schema, indexes, and permissions —
  it's a real, separate physical table.
- A copy of the `remove` `System` body is registered too, bound to the new
  table. `ctx.depend["remove:ItemOrder"]` calls *that* copy.
- Cluster computation runs against the copies: the `:ItemOrder` cluster
  has no overlap with the original `Order` cluster, so they never share a
  shard and never serialize across each other.
- All inheritance still works. If `remove` itself depended on other
  `Systems`, the whole graph is duplicated under the same suffix.

Use this when one `Component` sits at a bottleneck — say, a `FutureCalls`
queue or a global `Inventory` — and you want some callers to operate on a
separate physical copy. **Don't** use it as a generic "namespacing"
mechanism: every copy is a real table that costs storage and breaks any
cross-copy queries you might have wanted.

The built-in `create_future_call` `System` is the canonical example; the
[Future calls](#future-calls-scheduled-and-recurring-system-invocations)
section below uses a `:scheduler` suffix to keep the future-call queue
out of your application's main cluster.

## Future calls (scheduled and recurring `System` invocations)

`create_future_call` is a built-in `System` (in the `global` namespace) that
schedules another `System` to run **at** a future time, optionally
**repeatedly**. The schedule survives server restarts because it's stored
as a `Component` (`hetu.system.future.FutureCalls`), not a memory queue.

Skeleton:

```python
@hetu.define_system(
    namespace="MyGame",
    permission=None,  # not callable from clients
    components=(SomeComponent,),
    call_lock=True,  # required by future calls (see below)
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

Things to know about the API:

- **`at`** is a POSIX timestamp when positive; **negative or zero** means
  "this many seconds from now". `at=-10` runs ten seconds in the future.
- **`*args`** must be `repr()`-able and `eval()`-back-equivalent — the
  scheduler stores arguments as a string. Stick to primitives (`int`,
  `float`, `str`, `bool`, simple tuples). The total length must be ≤ 1024
  characters.
- **`timeout` (seconds, default 60, min 5 when non-zero)** is the
  retry-after window. If the call doesn't commit within `timeout`, the
  scheduler runs it again. `timeout=0` means "fire-and-forget" — no
  retries, and you accept that a process crash mid-call drops the task.
- **`recurring=True`** turns the entry into a periodic job. Each run
  reschedules itself `timeout` seconds later. Requires `timeout > 0`.
- **The target `System` must declare `call_lock=True`** when `timeout>0` and
  not `recurring`. The scheduler uses the call's row id as a UUID to
  deduplicate retries; without `call_lock`, the engine refuses to register
  the future call.
- **Trigger granularity is ~1 second.** Each worker runs a
  `future_call_task` background coroutine that polls every second; don't
  use this for sub-second precision.
- **The execution `ctx` has no user identity.** The scheduler runs as
  internal traffic — `ctx.caller` is `0`, `ctx.address` is `localhost`. If
  the work needs a user id, pass it explicitly in `args`.
- **Permission warnings.** If the target `System` has `permission=USER` (or
  any non-`ADMIN`/`None` value), `create_future_call` will warn you, since
  the same `System` could now also be invoked directly by clients. Best
  practice is `permission=None` for future-call targets.

### Why the `:scheduler` suffix on `create_future_call`

Notice the `depends=("create_future_call:scheduler",)` — that's a
[`System` copy](#system-copies-vertical-sharding-via-tag-suffixes). Because
`create_future_call` could be referenced from many of your application's
`Systems`, depending on it directly would pull *all* of them into a single
cluster centered on `FutureCalls`. The `:scheduler` suffix gives this
caller its own copy of the queue, isolated from any other code path that
also uses futures. Pick a stable suffix per logical group (e.g.
`:scheduler`, `:rewards`) — every distinct suffix is a real, separate
queue table the future-call worker will sweep.

### Cancelling a future call

Future-call rows live in `FutureCalls` (or its `:tag` copy). Delete by id
to cancel. Resolve the duplicate at module load time, then bind it to the
cancel `System`'s `components=`:

```python
from hetu.system.future import FutureCalls

# Same suffix the producer used in `depends=("create_future_call:scheduler",)`
_FCQueue = FutureCalls.duplicate("MyGame", "scheduler")


@hetu.define_system(
    namespace="MyGame", permission=hetu.Permission.USER,
    components=(_FCQueue,),
)
async def cancel_future(ctx, future_id):
    if row := await ctx.repo[_FCQueue].get(id=future_id):
        ctx.repo[_FCQueue].delete(row.id)
```

## `call_lock` and idempotent `System` execution

`call_lock=True` makes a `System` participate in HeTu's UUID-keyed
deduplication. It's how future calls implement exactly-once semantics on
top of an at-least-once queue, and you can use it directly when a `System`
must execute at most once per logical operation.

```python
@hetu.define_system(
    namespace="Shop", components=(Wallet,),
    permission=None, call_lock=True,
)
async def settle(ctx, user_id, amount):
    async with ctx.repo[Wallet].upsert(owner=user_id) as w:
        w.balance += amount


# Caller passes a stable UUID. Re-runs are no-ops.
await ctx.systems.call("settle", user_id, amount, uuid=order_id_str)
```

Mechanics:

- Enabling `call_lock=True` automatically attaches a duplicate
  `SystemLock` `Component` to the `System`'s cluster (using the same suffix
  trick as `System` copies — one lock table per locked `System`).
- On call, the engine reads `SystemLock` for the given `uuid`. If a row
  exists, the `System`'s body is skipped and `None` is returned.
- On commit, the engine writes the `uuid` row alongside your data, in the
  same transaction. If the transaction aborts (`RaceCondition`), the lock
  row aborts with it — retries are still allowed, but only until one
  succeeds.
- Lock rows live on `SystemLock` indefinitely; on worker startup the
  engine sweeps rows older than 7 days. If you want to free a slot
  earlier, use `SystemCaller.remove_call_lock(name, uuid)`.

`uuid=` is a keyword-only argument on `ctx.systems.call(...)` and on
parent-`Systems`' `ctx.depend[...](...)`-style invocations.

## The `on_disconnect` hook

Define a `System` named exactly `on_disconnect` and the engine will run it
when the websocket closes:

```python
@hetu.define_system(
    namespace="MyGame", components=(OnlineUser,),
    permission=None,  # not callable by clients
)
async def on_disconnect(ctx: hetu.SystemContext):
    if not ctx.caller:
        return
    if row := await ctx.repo[OnlineUser].get(owner=ctx.caller):
        row.online = False
        await ctx.repo[OnlineUser].update(row)
```

Notes:

- **`permission=None`** is strongly recommended. Any other value also
  generates a client-callable `Endpoint` with the same name, and a
  malicious client could fire your "disconnect" behavior at will. The
  engine itself ignores permission when invoking the hook on socket
  close, so `None` costs you nothing.
- **`ctx.caller`** is the user id if the connection was elevated, or `0`
  if it disconnected before login. Guard accordingly.
- The hook runs in a normal transaction, so failures retry on
  `RaceCondition`. Do not block on external services here — the
  connection is already gone.
- Triggering `on_disconnect` is best-effort: if the worker process is
  killed (`SIGKILL`, machine power-loss), the hook is skipped. For
  guaranteed cleanup, pair this hook with a periodic Future Call that
  reaps stale `last_active` connections.

## Raw `Endpoints` (multi-`System` or non-database RPCs)

Most RPC handlers should be `Systems`. Reach for `@define_endpoint` only
when one of the following is true:

- The handler does work that does **not** touch any `Component`
  (validation, fan-out to an external service, returning derived data).
- The handler must invoke **multiple** `Systems` and you explicitly want
  each one to commit independently — atomicity *across* them is
  undesirable or impossible (different backends, long-running steps).

```python
@hetu.define_endpoint(namespace="MyGame", permission=hetu.Permission.USER)
async def buy_and_log(ctx: hetu.EndpointContext, item_id: int):
    # First System commits on its own.
    res = await ctx.systems.call("buy_item", item_id)
    # Second System opens a fresh transaction.
    await ctx.systems.call("log_purchase", item_id)
    return hetu.ResponseToClient(res)
```

Key contrasts with `Systems`:

- **No automatic transaction.** No `ctx.repo`, no `ctx.depend`. Use
  `ctx.systems.call("name", *args)` to invoke a `System` (each call opens
  its own Session and commits independently).
- **No automatic retry.** A `RaceCondition` raised inside a `System` call
  is retried inside that call only.
- **Same permission gate.** `permission=USER` requires `elevate`,
  `permission=ADMIN` requires `ctx.is_admin()`, etc.
- **Same `Context`.** `ctx.caller`, `ctx.user_data`, and the rate-limit
  fields work the same way as in `Systems`.

## Per-connection state: `user_data`, `group`, and limits

Every connection has a single `Context` (`SystemContext` for `Systems`,
plain `Context` for `Endpoints`) that lives for the lifetime of the
websocket. Several fields on it are intended for application code:

```python
async def my_system(ctx: hetu.SystemContext, ...):
    ctx.user_data["last_seen_zone"] = zone_id  # arbitrary state
    if ctx.is_admin():
        ...  # ctx.group startswith "admin"
    ctx.client_limits = [[100, 1], [500, 60]]  # widen rate limit
    ctx.max_index_sub *= 4  # allow more concurrent ranges
```

What each field is for:

- **`ctx.user_data: dict[str, Any]`** — arbitrary per-connection state.
  Use it to cache a user's primary `OnlineUser` row, current zone, etc.
  *Not* persisted; gone when the socket closes. It is also the default
  source for `rls_compare`'s third tuple element when the named
  attribute isn't found on `ctx` itself.
- **`ctx.group: str`** — the connection's group label. The default is
  `"guest"`; the engine treats any value starting with `"admin"` as an
  administrator (skipping RLS row filters and allowing
  `Permission.ADMIN`-gated calls). Setting `ctx.group = "admin"` from a
  trusted login `System` is how you grant admin in HeTu — there is no
  separate token-based admin endpoint.
- **`ctx.client_limits` / `ctx.server_limits`** — list of `[max_count,
  window_seconds]` pairs. The engine tears the connection down once any
  pair is exceeded. `elevate()` automatically multiplies these limits by
  10×, so post-login users get the headroom that anonymous connections
  don't. Override per-connection in your own logic if you need a custom
  budget for, say, a bot account.
- **`ctx.max_row_sub` / `ctx.max_index_sub`** — caps on the number of
  active `Get` and `Range` subscriptions. `elevate()` multiplies by 50×.
  Tighten or widen as needed.
- **`ctx.race_count`** — current retry count for this transaction. Useful
  for backing off non-idempotent side-effects: `if ctx.race_count == 0:
  send_email(...)` runs the email only on the first attempt.

`ctx.timestamp` is set to `time()` at the start of every `System`/`Endpoint`
call, so it's safe to use as "now" without re-reading the clock.

## Early `session_commit` / `session_discard`

A `System` normally commits when its body returns. Two awaitable methods on
`ctx` let you commit (or abort) earlier:

```python
async def long_running(ctx: hetu.SystemContext, ...):
    async with ctx.repo[Order].upsert(id=order_id) as o:
        o.status = "processing"
    await ctx.session_commit()  # <-- writes are durable now

    # ↓ Slow work below; even if the worker dies, "processing" is persisted.
    result = await call_external_payment_provider(...)
    ...
```

Two important caveats:

- **`ctx.repo` and `ctx.depend` are unusable after either call.** Anything
  the body still does runs *outside* the transaction. There is no way to
  "reopen" the session.
- **Anything after `session_commit` is not retried on `RaceCondition`.**
  Only the pre-commit half of the body participates in HeTu's optimistic
  retry. If the post-commit work fails, you own the recovery.

`session_discard()` is the same shape but throws everything away. Use it
when the `System` has decided early that the right answer is "do nothing"
and you want to skip the commit entirely.

## NumPy patterns for range queries

`await ctx.repo[Comp].range(...)` returns a NumPy **recarray** — a
typed C-struct array, not a Python list. If you've never used NumPy,
skipping the features below and falling back to `for row in rows:` is
*correct* but throws away the throughput advantage HeTu's NumPy storage
was designed for. A 1000-row recarray run through a vectorized
expression is roughly the cost of a 5-row Python `for` loop.

You'll need `import numpy as np` for the helpers (`np.sqrt`,
`np.percentile`, `np.argsort`, `np.intersect1d`, `np.isin` …); column
methods like `.sum()` / `.mean()` are available without it.

### Column access and broadcasting

`rows.field` is a 1-D array of that column's values. Operators apply
**element-wise**, with scalars broadcast across the whole column:

```python
rows = await ctx.repo[Player].range("level", 1, 100, limit=1000)

# Distance from origin for every row, vectorized in C.
d2 = rows.x ** 2 + rows.y ** 2

# Shift everyone by (dx, dy)
shifted_x = rows.x + dx
shifted_y = rows.y + dy
```

For two equal-length recarrays, operators line up element-by-element:

```python
dx = players.x - targets.x
dy = players.y - targets.y
distances = np.sqrt(dx * dx + dy * dy)
```

The Python equivalent (`for row in rows: d = row.x ** 2 + row.y ** 2`)
is 10–100× slower on a 1k-row query because the inner work is
interpreted instead of SIMD-vectorized.

### Boolean masks: compound filtering without a re-query

A comparison on a column returns a boolean array; indexing a recarray
with that mask returns the matching rows:

```python
hot = rows[rows.hp < 30]
mine = rows[rows.owner == ctx.caller]
```

Combine masks with `&`, `|`, `~`. **Parentheses are mandatory** —
operator precedence on `&` is lower than `<`/`==`, so the unparenthesized
version raises:

```python
critical = rows[(rows.hp < 30) & (rows.shield == 0)]
```

This is the recommended pattern for **compound queries**: pull a small
window from the database with one indexed column, refine in memory with
NumPy. Asking the backend to evaluate compound predicates is slower
because the database interprets a query while NumPy uses SIMD.

```python
# Pull a small window with a real index, then filter in memory.
items = await ctx.repo[Item].range(level=(10, 20), limit=200)
cheap_strong = items[(items.price < 100) & (items.attack > 50)]
```

### Aggregations and statistics

Every column has built-in statistics:

```python
total_damage = rows.damage.sum()
average_hp = rows.hp.mean()
max_score = rows.score.max()
hp_p95 = np.percentile(rows.hp, 95)
hp_std = rows.hp.std()
n_alive = (rows.hp > 0).sum()  # counting via boolean mask
```

`(boolean_array).sum()` counts `True`s — the canonical "count where"
pattern. Use it instead of `len([r for r in rows if r.hp > 0])`.

For grouped counts, `np.unique(arr, return_counts=True)` is a one-liner
equivalent of `collections.Counter`:

```python
kinds, counts = np.unique(rows.kind, return_counts=True)
# kinds  = array(['chat', 'system'], dtype='<U16')
# counts = array([842, 18])
```

`len(rows)` and `rows.shape[0]` both give the row count.

### Top-N, argmin, argsort

Sorting by a non-indexed column is fine in memory. `argsort` returns
the *indices* you'd reorder by — index the recarray with those to pick
top-N without sorting twice:

```python
# Top-3 by score, descending order.
top3 = rows[np.argsort(rows.score)[-3:][::-1]]

# The single closest row to a target point.
dx = rows.x - target_x
dy = rows.y - target_y
nearest = rows[np.argmin(dx * dx + dy * dy)]
```

`argmax`/`argmin` on a 1-D column returns the index of the extremum;
indexing the recarray with that gives you the whole row.

### "Joining" two range queries

A common pattern: query two indexed `Components` separately, then combine
in-process by `owner` (or any shared key):

```python
positions = await ctx.repo[Position].range("zone", zone_id, zone_id, limit=500)
hps = await ctx.repo[HP].range("zone", zone_id, zone_id, limit=500)

# Owners present in both result sets
both = np.intersect1d(positions.owner, hps.owner)
matched_positions = positions[np.isin(positions.owner, both)]
```

`np.intersect1d` and `np.isin` are the SIMD-friendly equivalents of
`set` intersection and `in` membership; both stay in NumPy land, so the
result keeps the recarray type and you can chain more masks onto it.

### When a Python loop is the right answer

Two cases where falling back to a `for` loop is fine (or unavoidable):

- **Per-row database writes.** `ctx.repo[Comp].update(row)` and
  `upsert(...)` are async and operate one row at a time. Iterate
  explicitly: `for r in rows: await ctx.repo[Comp].update(r)`.
- **Genuinely heterogeneous per-row work.** Branching to different
  tables or different external services per row can't be vectorized; a
  loop is clearer than contorted NumPy.

For everything else — filtering, arithmetic, statistics, sorting,
joining — stay in NumPy.

## Multiple backends per `Component`

Every `@define_component` accepts `backend="<name>"`, where the name
matches a key in the `BACKENDS:` block of your config. Different
`Components` can live on different physical databases:

```python
@hetu.define_component(namespace="Game", backend="hot")
class Position(hetu.BaseComponent):
    ...


@hetu.define_component(namespace="Game", backend="cold")
class GameLog(hetu.BaseComponent):
    ...
```

The hard constraint: **every `Component` referenced by one `System` must
share the same backend**. The cluster builder rejects mixed-backend
clusters at startup. So in practice "multiple backends" is "different
groups of related `Components` on different databases" — typically a fast
in-memory Redis for hot game state and a cheaper SQL or larger Redis for
logs / analytics.

Backend names default to `"default"`. The first backend listed in
`BACKENDS:` is also treated as the default if no key named `"default"`
exists.

## Volatile components

`@define_component(volatile=True, ...)` marks a `Component` as **volatile**:
its rows are intended to be cleared during schema maintenance, and they
are eligible for `direct_set` low-level writes (used by the engine for
fast non-transactional updates like `last_active` on the built-in
`Connection` `Component`).

Use volatile when:

- The `Component` represents transient runtime state — connections,
  sessions, leases, ephemeral leaderboards — that you'd rebuild on a
  fresh server start anyway.
- You're storing a row whose canonical source of truth is somewhere else
  (an external service, another `Component`) and the volatile copy is
  only a cache.

Don't use volatile for player data, currency, or anything you care about
across deploys — `hetu upgrade` is allowed to wipe these tables.

## The `core` namespace: load without a `System` reference

Normally a `Component` is registered in `ComponentTableManager` (i.e. it
gets a real backend table you can query) **only if at least one `System`
references it** via `components=(...)`. If you `@define_component`
something but never wire it into a `System`, the engine treats it as dead
code and skips it.

`namespace="core"` is the one exception: at startup the cluster builder
auto-pins every core `Component` by synthesizing an empty global `System`
that references it. The `Component` then shows up in every namespace's
table manager regardless of what user code does.

Why the engine needs this: HeTu's own infrastructure tables —
`Connection` (the per-websocket presence record) and `WorkerLease` (the
SnowflakeID worker-id pool) — are accessed directly through
`tbl_mgr.get_table(Connection)` from inside the engine. They are never
referenced by user-defined `Systems`. Without `core`, those tables
wouldn't be created and the engine couldn't track connections or assign
worker ids.

For application code this is mostly an implementation detail. The
legitimate user-side use case is narrow: a `Component` that you want to
exist in the database but read/write only via direct backend calls
(maintenance scripts, custom CLI tools, an audit table populated by a
trigger). If a regular `System` touches it, just give it a normal
namespace — it'll be loaded automatically.

## Custom message pipeline layers

The wire protocol is a stack of `MessageProcessLayer` objects: each layer
encodes outgoing data and decodes incoming data. The default stack from
`CONFIG_TEMPLATE.yml` is `jsonb → zlib → crypto`. You can substitute or
add layers by subclassing:

```python
# myproto.py
from hetu.server.pipeline import MessageProcessLayer


class FramingLayer(MessageProcessLayer, alias="framing"):
    def is_handshake_required(self) -> bool:
        return False

    def encode(self, layer_ctx, message):
        return b"\x01" + message  # message is bytes here

    def decode(self, layer_ctx, message):
        assert message[:1] == b"\x01"
        return message[1:]
```

In `config.yml`:

```yaml
PACKET_LAYERS:
  - type: jsonb
  - type: zlib
    level: 1
  - type: framing       # ← your layer, identified by `alias`
  - type: crypto
    auth_key: ...
```

Three rules to keep in mind:

- The subclass auto-registers under its `alias` (declared in the class
  header). `MessageProcessLayerFactory.create(type=...)` resolves it by
  alias.
- **Order matters and must match the client.** Encoding runs top-to-
  bottom on the way out; decoding runs bottom-to-top on the way in. A
  client that runs the layers in a different order won't talk to your
  server.
- Whether the layer needs a handshake (`is_handshake_required()`) decides
  whether it gets a chance to negotiate parameters during the websocket
  open. Most layers (compression, framing) return `False`. The default
  `crypto` layer returns `True` to do an ECDH key exchange.

The Unity SDK ships with the matching defaults. If you add a custom
layer, you'll need a matching client implementation — there's no
auto-discovery on the wire.

## Operational visibility: replay and slow logs

HeTu exposes two specialized loggers that are easy to overlook:

- **`HeTu.replay`** — every connection event (handshake, RPC call,
  illegal request, websocket close) is emitted on this logger at `INFO`
  level. The default config writes it to a rotating `replay.log`.
  Replaying that log line-by-line reproduces the exact sequence the
  server saw — invaluable for diagnosing "I can't repro" bugs. Set the
  level to `ERROR` to disable entirely (the engine fast-paths the
  string-formatting cost).
- **`HeTu.root` slow log** — the engine measures every `System` call's
  wall-clock and `RaceCondition` retry count. When a single call exceeds
  ~1 second or 5 retries, it logs a warning that includes a top-20 table
  of the slowest / most-contended `Systems` for the worker. Each worker
  picks a random 60–600s suppression interval to avoid every replica
  printing the same warning at once.

Both are configured in the `LOGGING:` section of `config.yml` (standard
`dictConfig`). You'll see them mentioned by handler name in
`CONFIG_TEMPLATE.yml`.

## SnowflakeID and worker IDs

Every row id (`row.id`) in HeTu is a 64-bit Snowflake:

```
1 sign | 41 timestamp(ms) | 10 worker_id | 12 sequence
```

The numbers you actually care about:

- 4096 ids per millisecond per worker.
- Up to 1024 workers across the cluster (the lease pool).
- 69 years of headroom from the epoch (`2025-12-18` UTC+8).

Worker IDs are leased automatically by `WorkerKeeper`, which stores
them in the `WorkerLease` `Component` (a `core`/volatile table) and renews
the lease every 5 seconds. If a process dies without releasing its
lease, the slot is reclaimed after the lease expires. On startup, the
engine restores the *last persisted timestamp* and waits a short grace
period if the system clock has gone backwards — that's HeTu's defense
against duplicate ids when a host's NTP slews after a reboot.

For your code, the practical implications are short:

- `id` is generated for you; never assign one yourself.
- `BaseComponent.new_row()` calls `SnowflakeID().next_id()` under the
  hood. If you batch-insert, prefer `new_rows(N)` so all ids come from
  the same monotonic burst.
- The clock-rollback guard means **a server with a wildly wrong clock
  will refuse to issue ids and stall**. Run NTP. If you must fix a
  rollback, restart the affected workers — the keeper will reseed the
  timestamps automatically.

## Where to next

- **[Operations](operations.md)** — production deployment, Redis topology,
  the `hetu` CLI, and the rest of `config.yml`.
- **[API Reference](api/)** — every public symbol referenced on this page.
refer `new_rows(N)` so all ids come from
  the same monotonic burst.
- The clock-rollback guard means **a server with a wildly wrong clock
  will refuse to issue ids and stall**. Run NTP. If you must fix a
  rollback, restart the affected workers — the keeper will reseed the
  timestamps automatically.

## Where to next

- **[Operations](operations.md)** — production deployment, Redis topology,
  the `hetu` CLI, and the rest of `config.yml`.
- **[API Reference](api/)** — every public symbol referenced on this page.
