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
  `ctx.user_data`, admin / GM groups via `ctx.group`, and rate-limit
  overrides.
- **[Early `session_commit` / `session_discard`](#early-session_commit--session_discard)
  ** —
  commit (or abort) the transaction before the `System` body returns.
- **[Insert if missing](#insert-if-missing-two-ways)** — the range check
  on `range` (phantom protection), and the cheaper unique-anchor + `upsert`
  pattern for hot paths.
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
    namespace="Loot",
    depends=("remove:ItemOrder",),
    permission=hetu.Permission.USER,
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
  shard and never read/write across each other.
- All inheritance still works. If `remove` itself depended on other
  `Systems`, the whole graph is duplicated under the same suffix. A
  dependency in that graph that names its own suffix (say
  `depends=("audit:Log",)`), and a duplicate listed explicitly in
  `components=`, keep their own suffix.
- The same `System` can't appear in one dependency graph under two
  different suffixes (or both with and without one) — say, depending on
  both `remove` and `remove:ItemOrder`. `ctx.repo[Order]` can only point at
  one table, so startup fails with an error.

Use this when one `Component` sits at a bottleneck — say, a `FutureCalls`
queue or a global `Inventory` — and you want some callers to operate on a
separate physical copy. **Don't** use it as a generic "namespacing"
mechanism: every copy is a real table that costs storage and isolated,
whereas you might actually want them to function cooperatively.

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
        ctx,
        -delay_seconds,
        "reward_daily_bonus",
        ctx.caller,
        timeout=10,
        recurring=False,
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
  For one-shot (non-`recurring`) calls, `timeout` must not exceed half the
  call-lock retention (config `CALL_LOCK_RETENTION`, default 30 minutes),
  otherwise `create_future_call` raises `ValueError`: when the call is
  re-run after `timeout`, its lock must still be there to tell that it
  already ran.
- **`recurring=True`** turns the entry into a periodic job. Each run
  reschedules itself `timeout` seconds later. Requires `timeout > 0`.
- **The target `System` must declare `call_lock=True`** when `timeout>0` and
  not `recurring`. The scheduler uses the call's row id as a UUID to
  deduplicate retries; without `call_lock`, the engine refuses to register
  the future call.
- **Trigger granularity is ~1 second.** Each `worker` runs a
  `future_call_task` background coroutine. Each pass scans every future-call
  table (the main one and every copy) and runs the due calls; when nothing
  is due it sleeps until the earliest next call, at most 1 second, so a new
  call is picked up within about a second. Don't use this for sub-second
  precision. A call may run late but never before `at`: the `ctx.timestamp`
  the target `System` sees is never less than `at`, so checking "is it time
  yet" inside it (e.g. a cast finishing) is safe.
- **A call that keeps colliding doesn't hold up the scheduler.** Each
  `worker` runs future calls one at a time. When the target `System` hits
  `RaceCondition`, it is retried in place for about 2 seconds; if it still
  collides, the scheduler gives up on this round, moves on to the next call,
  and logs a rate-limited warning with the conflict reason. The call is
  re-run within about 5 seconds (or `timeout`, if shorter), so one-shot calls
  are still guaranteed to succeed. A call that always collides never
  succeeds: shrink what it reads and writes, e.g. pass `phantom_check=False`
  to a `range` that doesn't depend on the interval. `timeout=0` calls have no
  such limit because giving up would lose them: they retry up to the
  `System`'s `retry=` limit, holding this `worker`'s loop while later calls
  wait.
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
queue table.

Since table are separated, how exactly do the `FutureCalls` worker threads become
aware of these tables? This is because these duplicated tables are enumerable
(by `base_component.get_duplicates()`), and `FutureCalls` processes them all, one by
one.

### Cancelling a future call

Future-call rows live in `FutureCalls` (or its `:tag` copy). Delete by id
to cancel. Resolve the duplicate at module load time, then bind it to the
cancel `System`'s `components=`:

```python
from hetu.system.future import FutureCalls

# Same suffix the producer used in `depends=("create_future_call:scheduler",)`
_FCQueue = FutureCalls.duplicate("MyGame", "scheduler")


@hetu.define_system(
    namespace="MyGame",
    permission=hetu.Permission.USER,
    components=(_FCQueue,),
)
async def cancel_future(ctx, future_id):
    if row := await ctx.repo[_FCQueue].get(id=future_id):
        ctx.repo[_FCQueue].delete(row.id)
```

## `call_lock` and idempotent `System` execution

`call_lock=True` makes a `System` participate in UUID-keyed deduplication.
It's how future calls implement exactly-once semantics, on top of the existing "at least
once" guarantee in the "FutureCalls" queue.
And you can use it directly when calling `system`, by passing a UUID, you can invoke it
repeatedly, yet it will ultimately execute only once.

```python
@hetu.define_system(
    namespace="Shop",
    components=(Wallet,),
    permission=None,
    call_lock=True,
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
- The lock table isn't inherited through `depends=`: when a parent
  `System` calls a locked `System` via `ctx.depend[...]`, it neither reads
  nor writes that lock table. Only the `System` you call directly with
  `uuid=` deduplicates, against its own lock table — so a `System` that
  needs `uuid=` deduplication (including a future-call target) must
  declare `call_lock=True` itself.
- On call, the engine reads `SystemLock` for the given `uuid`. If a row
  exists, the `System`'s body is skipped and `None` is returned.
- On commit, the engine writes the `uuid` row alongside your data, in the
  same transaction. If the transaction aborts (`RaceCondition`), the lock
  row aborts with it — retries are still allowed, but only until one
  succeeds.
- Lock rows are kept for `CALL_LOCK_RETENTION` seconds (config, default
  30 minutes) and then swept by every worker periodically, so **uuid
  deduplication only holds within the retention window**. If you need a
  longer window (for example payment callbacks that may be resent hours
  later), record the processed order ids in your own data. Locks of
  `on_start` `System`s are exempt (they back "run once per boot") and are
  only swept on worker startup once older than 7 days. If you want to free
  a slot earlier, use `SystemCaller.remove_call_lock(name, uuid)`.
  `SystemLock` is volatile: `hetu upgrade` clears it.

`uuid=` is a keyword-only argument on `ctx.systems.call(...)`, at most 64
characters long (`str(uuid.uuid4())` is 36); a longer one raises `ValueError`.
`ctx.depend[...](...)` is a plain function call inside the same
transaction: it takes no `uuid=` and does no deduplication.

## The `on_disconnect` hook

Define a `System` named exactly `on_disconnect` and the engine will run it
when the websocket closes:

```python
@hetu.define_system(
    namespace="MyGame",
    components=(OnlineUser,),
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

- **`permission=None`** for safety. Any other value also
  generates a client-callable `Endpoint` with the same name, and a
  malicious client could fire your "disconnect" behavior at will. The
  engine itself ignores permission when invoking the hook on socket
  close.
- **`ctx.caller`** is the user id if the connection was elevated, or `0`
  if it disconnected before login. Guard accordingly.
- **A kicked connection runs the hook with `ctx.caller == 0`.** When the
  same account logs in on another connection
  (`elevate(..., kick_logged_in=True)`), the old connection is closed and
  its `on_disconnect` runs as an anonymous connection: the account now
  lives on the new connection, whose login logic may already have run
  (e.g. marked the user online), so the old connection must not mark it
  offline. In the rare case where the old connection drops on its own at
  the same moment the account logs in elsewhere, its hook may still run as
  the user; if that matters, store `ctx.connection_id` on the user row at
  login and only change the row in the hook when it still matches.
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
  attribute isn't found on `ctx` itself; if it's found in neither, no
  row of that component is visible to the connection.
- **`ctx.group: str`** — the connection's group label. The default is
  `"guest"`; the engine treats any value starting with `"admin"` as an
  administrator (skipping RLS row filters and allowing
  `Permission.ADMIN`-gated calls). Setting `ctx.group = "admin"` from a
  trusted login `System` is how you grant admin in HeTu — there is no
  separate token-based admin endpoint. Admin is root-level and meant for
  back-office tools only: never give it to a game-client connection (GM
  accounts included); in-game management always uses GM. A value starting
  with `"gm"` marks a GM: once logged in, it may call `Permission.GM`-gated
  `Systems`/`Endpoints`, and otherwise reads data like an ordinary player
  (RLS still filters its subscriptions, no `ADMIN` `Components`). Set
  `ctx.group = "gm"` in the login `System` after `elevate`.
- **`ctx.client_limits` / `ctx.server_limits`** — list of `[max_count,
  window_seconds]` pairs. The engine tears the connection down once any
  pair is exceeded. `elevate()` automatically multiplies these limits by
  10×, so post-login users get the headroom that anonymous connections
  don't. Override per-connection in your own logic if you need a custom
  budget for, say, a bot account.
- **`ctx.max_row_sub` / `ctx.max_index_sub` / `ctx.max_table_sub`** — caps on
  the number of active `Get`, `Range` and `Table` (whole-table) subscriptions.
  `elevate()` multiplies by 50×. Tighten or widen as needed. The per-table row
  cap for table subscriptions is the global `MAX_TABLE_SUBSCRIPTION_ROWS`
  setting, not a `ctx` field.
- **`ctx.race_count`** — current retry count for this transaction. Useful
  for backing off non-idempotent side-effects: `if ctx.race_count == 0:
  send_email(...)` runs the email only on the first attempt.

`ctx.timestamp` is set to `time()` at the start of every `System`/`Endpoint`
call, so it's safe to use as "now" without re-reading the clock.

## Early `session_commit` / `session_discard`

A `System` normally commits when its body returns. Two awaitable methods on
`ctx` let you commit (or abort) earlier, allows you to execute long-running or
non-idempotent operations only after the commit has successfully completed.

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

`session_commit()` returns the numbers that commit handed out to commit
sequence columns (`commit_seq`); see "Binlog-style syncing" on this page.

`session_discard()` is the same shape but throws everything away. Use it
when the `System` has decided early that the right answer is "do nothing"
and you want to skip the commit entirely.

## Insert if missing: two ways

"One row per player per item type: insert it if missing, otherwise add to
the quantity" is about the most common thing to write. There are two correct
ways to do it in HeTu.

### The plain way: decide after a `range`

```python
import hetu
import numpy as np


@hetu.define_component(namespace="game", permission=hetu.Permission.OWNER)
class Item(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, index=True)
    template: np.int32 = hetu.property_field(0)
    qty: np.int32 = hetu.property_field(0)


@hetu.define_system(
    namespace="game", components=(Item,), permission=hetu.Permission.USER
)
async def add_item(ctx: hetu.SystemContext, tpl: int, n: int):
    items = await ctx.repo[Item].range(owner=(ctx.caller, ctx.caller), limit=-1)
    hit = items[items.template == tpl]
    if len(hit) == 0:
        row = Item.new_row()
        row.owner, row.template, row.qty = ctx.caller, tpl, n
        await ctx.repo[Item].insert(row)
    else:
        row = hit[0]
        row.qty += n
        await ctx.repo[Item].update(row)
```

This is safe: the range read by `range` is checked at commit. If another
transaction inserts a row for the same player in the meantime, or this
`range` read from a replica that hadn't caught up yet, the commit is treated
as a race; the retried `System` reads that row and takes the `update` branch.

The same goes for `get`: inserting after a `get` on a non-unique field came
back empty is checked at commit too. When `get` finds a row, only that
returned row is guarded; rows with the same value inserted afterwards don't
count as a conflict.

Things to watch:

- **Read it all.** A truncated read (one that returned `limit` rows) only
  guards the first `limit` rows it saw; rows it didn't read are treated as
  missing. When checking whether something exists, use `limit=-1`. Rows this
  transaction deleted or moved out are left out of the result and don't take
  up `limit` slots, so getting fewer than `limit` rows back means you read it
  all.
- **Cost grows with the rows read.** Every row read gets a version check on
  the master, so a player with many items pays for the whole inventory on
  every item added.
- **In `range`, a leading `(` / `[` on a string bound marks an open / closed
  end.** When you `range` over a string that comes from a player (a name like
  "[GM]Bob"), put one more `[` in front, or a different value gets queried.
  To look up a single value, use `get` / `upsert`: they compare literally.

### Hot paths: unique within a group + `upsert`

Give the compound key a composite index that is unique within a group
(`unique=True, groupby=...`), and anchor an `upsert` on it by giving both the
field and the group-by columns; the transaction touches a single row:

```python
@hetu.define_component(namespace="game", permission=hetu.Permission.OWNER)
class Item(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, index=True)
    # one row per item kind per player: grouped by owner, unique within it
    template: np.int32 = hetu.property_field(0, unique=True, groupby="owner")
    qty: np.int32 = hetu.property_field(0)


@hetu.define_system(
    namespace="game", components=(Item,), permission=hetu.Permission.USER
)
async def add_item(ctx: hetu.SystemContext, tpl: int, n: int):
    async with ctx.repo[Item].upsert(template=tpl, owner=ctx.caller) as item:
        item.qty += n  # a new row already has template and owner filled in
```

When two transactions both reach the insert branch, the later commit hits
the unique-within-group constraint and is treated as a race; on retry the
`upsert` finds the other row and updates it instead. The price: one more
composite index, and adding it to an existing component needs `hetu upgrade`
to rebuild the index (duplicates within a group in existing data raise an
error). The old workaround — a concatenated string such as
`f"{owner}:{template}"` as the unique key — is no longer needed: a composite
index stores no extra column, and nothing has to be kept in sync when an item
changes owner.

Moving unique values between rows within one transaction (refilling a slot
after moving an item away, swapping two rows through a temporary value)
commits fine: a taken value is not a conflict if this commit moves it off its
current row.

### Turning the range check off

For busy ranges where the logic doesn't depend on "nothing else is in the
range" — say, reading the latest N messages before inserting one — every new
message makes concurrent transactions race and retry for nothing. Pass
`phantom_check=False` to such a `range`: the rows it returns still take part
in the version check, but rows added to the range are ignored. The
`[RaceCondition]` lines in `replay.log` carry the reason (e.g.
`Range changed Item.owner`), which helps find the ranges that conflict a lot.

## Binlog-style syncing: commit sequence columns

Writers append rows to a log table; a consumer remembers "consumed up to
number N" and reads onward by number. If numbers are handed out before the
commit (a counter outside the transaction, or reading and bumping a counter row
inside it), then with several processes number 2 may commit before number 1 and
the consumer has to stop and wait; a transaction that took a number and then
conflicted, retried or failed leaves that number empty forever; and a counter
row makes every write to the same group conflict and retry. With a commit
sequence column the number is handed out at commit:

```python
@hetu.define_component(namespace="ssw", permission=hetu.Permission.OWNER)
class ChangeLog(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0)
    # numbered from 1 per owner; without groupby the whole table is one sequence
    seq: np.int64 = hetu.property_field(0, commit_seq=True, groupby="owner")
    kind: np.int16 = hetu.property_field(0)
```

Writers insert as usual and leave `seq` at 0. The number is handed out at
commit, so the transaction can't see it. To learn the number your row got (to
send it back to the client, say), commit early in the System:
`ctx.session_commit()` returns the numbers handed out by that commit,
`{component class: {row id: number}}`:

```python
@hetu.define_system(
    namespace="ssw", components=(ChangeLog,), permission=hetu.Permission.USER
)
async def submit(ctx: hetu.SystemContext, kind: int):
    row = ChangeLog.new_row()
    row.owner, row.kind = ctx.caller, kind
    await ctx.repo[ChangeLog].insert(row)
    # on RaceCondition the whole System reruns; the failed attempt uses no number
    seqs = await ctx.session_commit()
    return hetu.ResponseToClient({"seq": seqs[ChangeLog][row.id]})
```

- Keys are the same as `ctx.repo`'s: the master component class even when the
  System references a duplicate. Only rows inserted by this commit into
  components with a commit sequence column are included; with none, it is an
  empty dict.
- The number is only in the return value: after the commit, the row object you
  hold still has `seq` 0.
- The automatic commit after the System returns doesn't return the numbers;
  commit early to get them (code after the commit runs outside the
  transaction and isn't retried, see "Early `session_commit`").
- Writers that use a session directly, such as headless clients, call
  `await session.commit()` explicitly; the result has the same shape, keyed by
  the component class the repository was opened with (`session.using(...)` /
  `s[...]`). For a component a headless client declared by name, the key is
  `client.table("ChangeLog").comp_cls`, not the string.
- `hetu call --dry-run` commits nothing and hands out no numbers: rows that
  would have got one map to 0 (0 means "no number" in a commit sequence column).

A server-side consumer looks like this (one consumer per cursor):

```python
@hetu.define_component(namespace="ssw")
class LogCursor(hetu.BaseComponent):  # the consumer's own cursor per owner
    owner: np.int64 = hetu.property_field(0, unique=True)
    last: np.int64 = hetu.property_field(0)


@hetu.define_system(namespace="ssw", components=(ChangeLog, LogCursor))
async def consume(ctx: hetu.SystemContext, owner: int):
    async with ctx.repo[LogCursor].upsert(owner=owner) as cursor:
        log = ctx.repo[ChangeLog]
        rows = await log.range(
            seq=(cursor.last + 1, np.inf), owner=owner, limit=100, phantom_check=False
        )
        expected = cursor.last + 1
        for row in rows:
            if expected < row.seq:  # numbers are missing: confirm the whole gap in one read
                for missing in await log.range(
                    seq=(expected, row.seq - 1), owner=owner, limit=-1
                ):
                    handle(missing)  # this read missed it: rows came from a lagging replica
            handle(row)
            expected = row.seq + 1
        cursor.last = expected - 1
```

- **`phantom_check=False`**: when fewer than `limit` rows come back, the range
  read stays open up to `inf`, so every new log row in the group between the
  read and the commit would make the consumer race and retry. The consumer
  doesn't depend on "nothing else is in the range", so turn the check off; the
  rows it read are still version-checked. A cursor belongs to one logical
  consumer, and several instances of that consumer may share it: the cursor
  row's version check keeps them from advancing it at once and handling rows
  twice.
- **Confirm missing numbers before skipping them**: numbers are normally
  consecutive. Numbers can be missing in two ways: behind a read/write
  splitting proxy that routes per command, or on native cluster with several
  replicas, one read may fetch rows from replicas at different replication
  points and miss a row in the middle (temporary); or the rows were deleted
  (permanent — in rare cases a commit script that fails halfway on Redis leaves
  such a gap too). When numbers are missing, read that span once with a closed
  `range`: rows it finds are handled; the read keeps the range check (the
  default), so the commit confirms on the master that the span has no rows. The
  span ends below a number that has already been committed, so new writes
  can't land in it and won't make the read race for nothing; numbers are
  handed out in commit order, so the span will never get rows later and can be
  skipped. If the empty read came from a lagging replica while the master does
  have the rows, the commit races and retries. Each gap costs one read however
  long it is: after a long head has been trimmed, a new consumer starting from
  0 only reads once more. Don't just step over missing numbers: with
  `phantom_check=False` rows that fail to fetch are skipped silently.
- **Cost**: every row read into the transaction gets a version check on the
  master at commit, about 1µs of master time per consumed row. Where the log can
  be consumed by a client through a subscription, prefer that: it doesn't touch
  the master.
- **External side effects in `handle`** (granting rewards, calling other
  services) run again when the transaction retries; make them idempotent.
- **Trimming**: only delete the head that every consumer has passed. Deleting a
  row in the middle leaves a hole: server-side consumers confirm and skip it as
  above, client-side consumers stop in front of it. A consumer outside the
  database also needs its cursor row kept; see the note in the next subsection.
- **Volatile tables**: numbering starts over from 1 after every maintenance;
  reset consumer cursors along with it.
- **Consumers outside the database** (cursors kept outside the database, e.g.
  reading a command table through headless, or syncing the log to an external
  system): they are not in a transaction; use `servant_seq_read`, see the next
  subsection.
- **Client-side consumers**: subscribe with
  `WatchRange("seq", cursor + 1, inf, limit, group: …)`, see
  [Unity Client SDK](unity-client.md).

### Consumers outside the database: `servant_seq_read`

A consumer whose cursor lives outside the database (e.g. a process that reads a
command table through headless and keeps its cursor in its own snapshot file,
see "Reading and writing tables from a non-server process" on this page) is not
in a transaction and can't get the commit-time confirmation used above. `Table`
has three non-transactional reads for it; they all go to replicas and never
read the master:

- `servant_seq_head(group=...)` / `servant_seq_head_many(groups)`: the latest
  number the group has issued, i.e. how far numbering has got; 0 if none yet.
  The batch form takes one round trip and answers per group: a group whose
  counter is corrupted gets None (see below). Use it only to notice new
  numbers, monitor the backlog and raise alerts: it is not in the same snapshot
  as the row reads, so it **must not** be used to move the cursor past numbers
  whose rows you haven't seen.
- `servant_seq_read(after, limit, group=..., after_id=...)`: reads the rows
  after the cursor and returns `(rows, blocked)`. It reads the group's index
  with a single command, then fetches the rows. Numbers are handed out in commit
  order, so once a node shows number n, any smaller number it doesn't show is
  gone, and is skipped; gaps are confirmed by that one index read alone. The
  rows may be fetched from a different node, and a row that is in the index but
  can't be fetched is **not** skipped: the read stops in front of it, reports it
  as `blocked`, and the next read tries again. `after_id` is the
  snowflake id of the row at the cursor; when given, the read checks the
  sequence: if the cursor's number now holds another row (the number was
  reused), the cursor's row is gone while larger numbers exist, or the group
  has duplicate numbers, it raises `CommitSeqDiverged`.

```python
import time

from hetu.data.backend import CommitSeqDiverged

tbl = client.table(ChangeLog)
cursors = load_snapshot()             # {owner: (seq, row_id)}, in the same snapshot as the consumer's state
stopped: set[int] = set()             # groups stopped because the sequence no longer matches
behind_since: dict[int, float] = {}   # when the counter first dropped below the cursor


async def poll(owner: int) -> None:
    seq, row_id = cursors.get(owner, (0, None))
    rows, blocked = await tbl.servant_seq_read(
        seq, limit=1000, group={"owner": owner}, after_id=row_id
    )
    for row in rows:
        handle(row)  # numbers that didn't show up in between are confirmed gone
    if len(rows):
        cursors[owner] = (int(rows[-1].seq), int(rows[-1].id))
    watch_blocked(owner, blocked)  # alert only if the same (seq, id) blocks for long, see below


async def poll_all(owners: list[int]) -> None:
    heads = await tbl.servant_seq_head_many([{"owner": o} for o in owners])
    now = time.monotonic()
    for owner, head in zip(owners, heads):
        if owner in stopped:
            continue
        seq = cursors.get(owner, (0, None))[0]
        if head is not None and head < seq:  # this replica lags or is resyncing, the cluster changed, or the data rolled back
            if now - behind_since.setdefault(owner, now) > 30:
                await alarm_behind(owner, head, seq)  # call client.check_schema() first, see "Cluster changes"
            continue
        behind_since.pop(owner, None)
        if head is None:  # corrupted counter: alert and read anyway, row reads don't use it (see below)
            await alarm_counter(owner)
        elif head == seq:
            continue  # nothing new
        try:
            await poll(owner)
        except CommitSeqDiverged as exc:
            stopped.add(owner)  # stop only this group; the others go on
            await alarm_diverged(owner, exc)
```

**Note: trim only rows numbered strictly below every consumer's persisted
cursor, and check before deleting.**

- When `servant_seq_read` sees a number with no row while a larger number
  exists, it takes that number as gone and skips it, without an error. There is
  no telling whether the row was deleted after being handled or before.
- Consumers restore from snapshot files. If trimming follows a consumer's
  in-memory progress and the snapshot is older than that progress, then after a
  crash and restart the commands between the snapshot and that progress are
  already deleted: they are skipped as gaps, and the consumer's state silently
  diverges. So trimming must go by cursors that have **already been persisted**.
- The trimmer (a server-side System or an ops script) can't read the consumers'
  local snapshots: after each snapshot is persisted, the consumer writes the
  snapshot's `(seq, row id)` back to a cursor table in the database (an upsert
  in a headless session, one commit per snapshot), and the trimmer takes the
  minimum from that table.
- Delete only rows numbered **strictly below** the minimum cursor, keeping each
  consumer's cursor row. It is never deleted in normal operation, so if it is
  gone something is wrong, and `servant_seq_read` raises `CommitSeqDiverged`.
- Check before deleting: the minimum cursor's number must still hold the row
  recorded in the cursor table (in a System, `range(seq=(s, s), ...)`; through
  headless, `servant_range(..., row_format=RowFormat.ID_LIST)`). If it doesn't,
  don't delete; raise an alert. After a rollback, the new rows that reuse
  numbers all sit at or below the old cursor; trimming without the check would
  delete these never-consumed rows, and the rollback would go unnoticed.
- With `after_id`, don't delete each row as soon as it is consumed; keep at
  least the last handled row. To consume-and-delete, don't pass `after_id`,
  which means number reuse can't be detected. Game logic must not delete
  commands that haven't been consumed; to cancel one, write a separate "cancel"
  command.

| Case | Meaning | What to do |
|---|---|---|
| The cursor's number still holds the same row | Normal | Read on (`servant_seq_read` handles it) |
| The cursor's number holds another row, the group has duplicate numbers, or the cursor's row is gone while larger numbers exist | The sequence no longer matches: the data rolled back (a failover that lost the last commits, an AOF / RDB restart, a full restore, clearing a volatile table), or the counter fell behind the data (counter deleted by hand, evicted, rows imported alone) | `CommitSeqDiverged` is raised, its `reason` says which: stop this group and raise an alert |
| Nothing was read | No new commands; or this replica lags; or the data rolled back to before the cursor and the numbers haven't been reused yet | Wait. Alert only if the counter stays below the cursor (see below) |

- **Counter staying below the cursor**: alert only when every counter read
  within a window (e.g. 30 seconds, longer than replication lag) is below the
  cursor. The alert should name the possible causes: a replica far behind or
  in a full resync, a cluster change, or a rollback.
- **Blocked**: a non-None `blocked` only means this round was blocked by that
  row: a fetch that hit a lagging replica, or a row deleted after the index was
  read, both do it, and the next round usually goes through. Alert only when
  the same `(seq, id)` blocks for a long time: if `servant_get` never finds the
  row, an index entry was left behind (e.g. a maintenance script deleted only
  the row), so stop the servers and run `hetu upgrade` to rebuild indexes; if
  the row can be read, most likely a read-only node whose replication is broken
  is still answering reads (a read-write splitting proxy and a native cluster
  both send reads to it): fix that replica, or take it out. Until then the
  consumer keeps stopping in front of that row; it never skips it.
- **Caught up?** Look at whether `rows` is empty and `blocked` is None, not at
  whether the cursor equals the counter. If the newest rows were deleted before
  being consumed, the cursor stays at the last row seen until a newer row
  appears; meanwhile the counter minus the cursor is positive, but there is
  nothing to consume.
- **Snapshots**: store the cursor (number and snowflake id) in the same snapshot
  as the consumer's own state. Restoring from a snapshot after a crash replays
  the commands after it: `handle` must be idempotent, or the state and the
  cursor must be restored together.
- **Cluster changes**: counters and data move with a cluster change; a consumer
  that hasn't reconnected reads all counters as 0 and finds nothing. Before
  alerting, call `client.check_schema()`; on `ClusterChanged`, reconnect and keep
  the cursors as they are.
- **Many groups**: each group costs one key computation per round (about 3µs):
  100k groups take about 0.3 s of CPU per round, and the replica looks up 100k
  keys per round. With many groups, poll less often or only poll active
  groups; the consumer keeps the group list itself (e.g. from the game's room
  table). The larger `limit` is, the longer one read holds the replica;
  a few hundred to a few thousand rows is typical.
- **Corrupted counters** (a value that isn't a non-negative integer, usually
  from a manual edit): `servant_seq_head_many` returns None for that group and
  the other groups' values as usual; `servant_seq_head` raises RuntimeError
  naming the key and the value. Commits to that group fail, but its rows can
  still be read, so the example alerts and keeps reading the group. While the
  group still has rows, `hetu upgrade` recalibrates it to the group's largest
  number when rebuilding indexes; if the group has no rows (e.g. rows are
  deleted once consumed), upgrade leaves it alone: set it by hand to a value no
  smaller than any consumer's cursor (e.g. the largest one in the cursor
  table). Don't just delete it: numbering would restart from 1 and reuse
  numbers. On Redis, a counter key overwritten with another type reads as 0
  (commits then fail with WRONGTYPE as well).
- **Rows left by a failed commit**: on Redis, a runtime error halfway through the
  commit script can leave a row without its index entry; it is skipped as a gap.
  A later index rebuild puts it back at its number, and consumers already past
  it won't handle it.
- **Server-side consumers** keep their cursor and the log in the same
  transaction, so they roll back together and don't need these checks.

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
d2 = rows.x**2 + rows.y**2

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

When the condition is "a range of one field within a group" (levels 10–20 in
one zone, a player's latest N entries), declare a composite index
(`property_field(groupby=...)`, see [Concepts](concepts.md)) so the database
reads only that group. For other **compound queries** the recommended pattern
is: pull a small window from the database with one indexed column, refine in
memory with NumPy. Asking the backend to evaluate compound predicates is slower
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
class Position(hetu.BaseComponent): ...


@hetu.define_component(namespace="Game", backend="cold")
class GameLog(hetu.BaseComponent): ...
```

The hard constraint: **every `Component` referenced by one `System` must
share the same backend**. The cluster builder rejects mixed-backend
clusters at startup. So in practice "multiple backends" is "different
groups of related `Components` on different databases". Currently, only Redis is
supported; its utility is limited at this stage, as it is intended for future expansion.

Backend names default to `"default"`. The first backend listed in `config.yaml`'s
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
(`direct_set`/`direct_get`). If a regular `System` touches it, just give it a normal
namespace — it'll be loaded automatically.

## Custom message pipeline layers

The wire protocol is a stack of `MessageProcessLayer` objects: each layer
encodes outgoing data and decodes incoming data. The default stack from
`hetu/CONFIG_TEMPLATE.yml` is `jsonb → zlib → crypto`. You can substitute or
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

## Custom HTTP endpoints

HeTu's built-in web server is Sanic, and the client SDK's websocket entry is
just one route on it. You can mount your own plain HTTP routes next to it —
file downloads, an admin page, a health check — with `@hetu.define_route`:

```python
import hetu
from sanic import Request, response


@hetu.define_route("/download/<name:str>")
async def download(request: Request, name: str):
    return await response.file(f"/data/{name}")
```

The decorator has to run while the engine loads your app file, i.e. at module
top level, the same as `@define_system`. Everything after the uri is passed
straight through to Sanic's `Blueprint.route`, so `methods=["POST"]`,
`stream=True` and friends behave exactly as Sanic documents them.

These are plain web endpoints and nothing more. They do not go through HeTu's
`Permission` system, they get no `SystemContext`, and they have no database
access — authentication, rate limiting and input validation are yours to
write. Game logic that the client SDK calls belongs in `define_system` /
`define_endpoint` instead.

For anything a single route can't express — a static directory, middleware, an
error handler, your own Blueprint with a `url_prefix` — use
`@hetu.on_server_setup`, which hands you the Sanic app right after it is
created:

```python
@hetu.on_server_setup
def setup(app):
    app.static("/download", "/data/files")
```

Three things to know:

- `/hetu/...` is reserved for the websocket entry, and `define_route` refuses
  it. `/` is *not* reserved: register it and your page quietly replaces
  HeTu's default welcome text.
- Both hooks run once in the manager process and once per worker, before any
  backend is connected. Do registration only; anything that needs per-worker
  initialization should be hung off `app.before_server_start(...)` from
  inside the setup callback.
- Your routes share the event loop with websocket game traffic, and every
  worker serves them. Keep handlers stateless, and in production put large or
  slow downloads behind nginx or a CDN instead of through the game workers.

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
`hetu/CONFIG_TEMPLATE.yml`.

## SnowflakeID and worker IDs

Every row id (`row.id`) in HeTu is a 64-bit Snowflake:

```
1 sign | 41 timestamp(ms) | 10 worker_id | 12 sequence
```

The numbers you actually care about:

- 4096 ids per millisecond per worker.
- Up to 1024 workers across the cluster (the lease pool).
- 69 years of headroom from the epoch (`2025-12-18` UTC+8).

Worker IDs are leased automatically by `WorkerKeeper`: on the Redis
backend each lease is a Redis key renewed every 5 seconds, and if a
process dies without releasing its lease, the slot is reclaimed after the
lease expires; the SQLite backend (development only) simply uses the
process's index on the host. On startup, the engine restores the *last
persisted timestamp* from the `WorkerLease` `Component` (a `core`/volatile
table) and waits a short grace period if the system clock has gone
backwards — that's HeTu's defense against duplicate ids when a host's NTP
slews after a reboot.

For your code, the practical implications are short:

- `id` is generated for you; never assign one yourself.
- `BaseComponent.new_row()` calls `SnowflakeID().next_id()` under the
  hood. If you batch-insert, prefer `new_rows(N)` so all ids come from
  the same monotonic burst.
- The clock-rollback guard means **a server with a wildly wrong clock
  will refuse to issue ids and stall**. Run NTP. If you must fix a
  rollback, restart the affected workers — the keeper will reseed the
  timestamps automatically.

## Reading and writing tables from a non-server process (`hetu.headless`)

Some processes must share a backend with a HeTu server without *being* a HeTu app: a standalone battle simulation, say — it runs no Sanic, defines no `System`, accepts no client connections, and only wants to read a command table, write a report table, and maintain a lease table together with the server. `hetu.headless` is the table client for such **trusted internal processes**.

```python
import hetu.headless

client = await hetu.headless.connect(
    backend_config,                      # the BACKENDS[x] dict from config.yml; Redis and SQLite both work
    instance="my-region",                # an instance name from INSTANCES; tables are per instance
    components=[BattleCommand, "BattleReport", BattleSim],  # classes or names, mixed freely
)
```

It has exactly one principle: **writes go through the very same `Session.commit()` path a `System` uses**. Table-level PUBLISH, keyspace notifications, optimistic version checks and unique validation all live in that layer, so a client's `WatchRow` / `WatchRange` / table subscriptions — and the server's own subscriptions and FutureCalls — cannot tell whether a row was written by a `System` or by a headless process. What it deliberately does *not* do is just as clear: no sanic import, no app file, no cluster building, no table creation or migration, no snowflake ids, and **no permission / RLS checks** — treat it as a trusted internal database connection for your own processes only.

### Resolving tables: cluster ids and schemas come from the server's meta

When the server creates a table it stores the component's schema and `cluster_id` in the table meta. `connect()` only reads that meta; it never recomputes clusters locally:

- Table missing → `TableNotFound`. Creating and migrating tables is the server's job; run `hetu start` once, then connect.
- Passing a **component class**: the local definition is compared with the meta on **data layout** — `namespace`, column names, dtype, unique, index, groupby, commit_seq. Any difference raises `SchemaMismatch` whose message lists each difference (direction "server -> local"); a column property in the meta that this version doesn't know (added by a newer HeTu) also counts as a difference, so upgrade headless clients together with the server. Fields irrelevant to headless such as `permission` / `rls_compare` / `volatile` are ignored; a differing `default` only logs a warning.
- Passing a **component name**: a class is generated from the schema in the meta, so the process shares no code at all; when you need the class, take `client.table("BattleReport").comp_cls` and use `new_row(id_=...)` as usual.

### Reading: `Table.servant_*`

`client.table(comp)` returns the engine's own `Table`. Non-transactional reads use its existing `servant_get` / `servant_range`, plus the batch `servant_get_many(ids)`, and `servant_seq_head` / `servant_seq_head_many` / `servant_seq_read` for commit sequence columns. Polling a command queue looks like this:

```python
cmd_tbl = client.table(BattleCommand)
rows = await cmd_tbl.servant_range("created_at", watermark - 2.0, float("inf"), limit=4096)
for row in rows:
    if row.seq > cursor[row.system_id]:   # de-duplicate by per-system seq
        ...
```

Note that `right` cannot be omitted — omitting it means "exactly equal to `left`", not `>=`. `float("inf")` works on every backend. `servant_*` reads go to read replicas and are allowed to lag; a watermark that looks back a little plus seq de-duplication covers that. Re-read specific rows in bulk with `servant_get_many`. Do not use `Table.direct_set`: it bypasses the transaction and does not guarantee consistent notifications.

For a command table that must be consumed in order without gaps, give it a commit sequence column (`commit_seq`) and consume it with `servant_seq_read`; see the consumers-outside-the-database subsection of "Binlog-style syncing" on this page. Don't treat gaps in `servant_range` results as real gaps: it reads the index first and then fetches the rows one by one, and rows that a lagging replica can't return are silently skipped.

### Writing: `client.session(*comps)`

```python
async with client.session(BattleReport, BattleSim) as s:
    await s[BattleReport].insert(BattleReport.new_row(id_=-report_key(r)))
    async with s[BattleSim].upsert(system_id=7) as sim:
        sim.epoch += 1
# leaving the block commits; RaceCondition propagates

async for attempt in client.session(BattleSim).retry(5):   # or let it retry
    async with attempt as s:
        ...
```

`s[Comp]` is the engine's `SessionRepository` (`insert` / `update` / `upsert` / `delete` / `get` / `range`); there is no second API. The rules:

- **Same cluster**: every component in one transaction must belong to the same cluster, otherwise `ValueError` — it is never silently split into two transactions. Clusters are decided by the `System` references on the server side: if several tables must be writable in one transaction, give the server a `System` that references all of them; tables referenced by no `System` must be defined with `namespace="core"` (see the previous section).
- **No id minting**: a headless process holds no worker-id lease and `connect()` does not initialise `SnowflakeID`. So inserted rows must carry an explicit non-zero id via `new_row(id_=...)` (`id == 0` raises); `upsert` may create a row only when anchored on `id=<explicit value>`, while a miss on any other unique field raises `LookupError` — such rows are pre-created by a server `System` and headless only updates them. Use **negative ids** by convention: snowflake ids are always positive, the negative range is reserved for keyed rows (the same convention as `ensure_future_call`), e.g. `id=-report_key`.
- **Idempotent resend**: with deterministic ids in a report table, a process that crashes and resends the same batch should use `upsert(id=-key)` — a hit becomes an update, and nothing is written if no field changed; `insert` would hit `UniqueViolation` and fail the whole batch.
- Reads inside a transaction go to the master by default (`only_master=True`): a headless process writes little, reading replicas saves nothing, and replication lag would only turn into `RaceCondition` retries.

### Migrations and `check_schema()`

When the server's `hetu upgrade` reshuffles clusters it renames **every** key under the old cluster prefix, including rows headless wrote earlier; but a headless process that was not restarted keeps writing under the old prefix, producing orphan data the server never sees. The operational rule is **restart headless processes after a cluster reshuffle or schema change**; add a safety net in code as well:

```python
async def lease_loop():
    while True:
        await client.check_schema()  # TableNotFound / ClusterChanged / SchemaMismatch → exit and let the supervisor restart
        async with client.session(BattleSim) as s:
            async with s[BattleSim].upsert(system_id=sid) as row:
                row.owner_host, row.lease_until = me, now + 30
        await asyncio.sleep(10)
```

`check_schema()` only raises; it never swaps `cluster_id` behind your back — the `Table` objects you hold are immutable, and silently replacing them would leave in-flight transactions and cached tables inconsistent.

### Threads and event loops

Headless can be created and used on an event loop in a non-main thread and does not depend on Sanic; the typical deployment runs the sim thread as a synchronous numpy loop with HeTu on a loop in a side thread. The one constraint: **`connect`, use and `close` must happen on the same loop** (Redis async connections are bound to a loop) — one client per loop. `await client.close()` only closes connections; there is no lease and no message queue, so a killed process leaves no cross-process state behind.

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

