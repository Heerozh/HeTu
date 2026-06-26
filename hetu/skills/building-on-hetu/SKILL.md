---
name: building-on-hetu
description: Use when building or extending a game server / realtime backend on the HeTu (河图) engine — writing Components, Systems or Endpoints in app.py, wiring a Unity/C# client's RPC and subscriptions, configuring backends, or deploying with the hetu CLI. An index into HeTu's model, public API and source; follow the links for specifics.
---

# Building on HeTu

HeTu (河图) is an ECS game-server engine: state lives in **Components** (typed
tables, backed by NumPy structured arrays, stored in Redis), logic lives in
**Systems** (async functions that run inside an optimistic transaction), and
clients talk to it over **WebSocket** — calling Systems by RPC and subscribing
to row/index changes that are pushed back live. Permissions are row-level.

This skill is an **index**: it lists what exists and where to look. Read the
linked docs and the installed `hetu` source for details — when unsure of an
API, open the file rather than guessing.

## Public API (`import hetu`)

Everything an app touches is exported at the top level:

| Symbol | Role |
|--------|------|
| `@define_component(namespace=, permission=, backend=, volatile=)` | Declare a typed table |
| `property_field(default, dtype=, index=, unique=)` | Declare a column |
| `BaseComponent` | Base class for a component; `.new_row()`, `.new_rows(n)` |
| `@define_system(namespace=, components=, permission=, depends=, retry=, call_lock=)` | Declare a transactional RPC |
| `@define_endpoint(namespace=, permission=)` | Declare a raw (non-transactional) RPC |
| `SystemContext` / `EndpointContext` | The `ctx` passed into your function |
| `Permission` | `EVERYBODY` / `USER` / `OWNER` / `RLS` / `ADMIN` |
| `elevate(ctx, user_id, ...)` | Promote a connection to authenticated |
| `ResponseToClient(data)` | Wrap a return value to send back to the client |

## Minimal shape

```python
import hetu
import numpy as np

@hetu.define_component(namespace="Game", permission=hetu.Permission.EVERYBODY)
class Player(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)
    name: str = hetu.property_field("", dtype="U32")   # strings are fixed-width!

@hetu.define_system(namespace="Game", components=(Player,),
                    permission=hetu.Permission.USER)
async def rename(ctx: hetu.SystemContext, name: str):
    async with ctx.repo[Player].upsert(owner=ctx.caller) as row:
        row.name = name
```

`hetu init` scaffolds this layout (`src/app.py` plus a package whose
`component/` and `system/` modules auto-register). The generated files are
working examples — read them first.

## Concepts, one line each

- **Component** — a typed table. Strings are **fixed-width** (`dtype="U256"`)
  and truncate; there are **no nulls** (every column has a default). `index=True`
  builds a sorted index for `range()`; `unique=True` adds a uniqueness check.
- **Volatile components** — `@define_component(volatile=True)` marks a table as
  transient: `hetu upgrade` may **wipe** it, and it permits fast non-transactional
  `direct_set` writes. Use for runtime-only state (connections, sessions, leases,
  ephemeral leaderboards) or caches whose source of truth is elsewhere. **Never**
  for player data, currency, or anything that must survive a deploy. (→ `advanced.md`)
- **System** — an async function in a transaction. Declare *every* table it
  touches in `components=`; Systems sharing a Component land in the same
  **co-location cluster** (the unit of isolation and sharding — avoid "hub"
  tables touched by everything). CRUD through `ctx.repo[Component]`: `get`,
  `range`, `upsert`, `insert`, `update`, `delete`. Compose other Systems in the
  **same** transaction via `depends=` + `ctx.depend["name"](ctx, ...)`.
- **System copies / component duplicates (`:tag` sharding)** — the escape hatch
  for a hub Component that drags everything into one cluster. Append a `:tag` in
  `depends=` (`depends=("remove:ItemOrder",)`, invoked via
  `ctx.depend["remove:ItemOrder"]`): the engine duplicates the Component into a
  **separate physical sibling table** (`Order:ItemOrder`, same schema / index /
  permission) and registers a copy of the System bound to it, in its own isolated
  cluster — not generic namespacing, each copy is a real table. Resolve a copy at
  load time with `Component.duplicate(namespace, tag)`, enumerate with
  `get_duplicates()`. Built-in `create_future_call:scheduler` is the canonical
  example. (→ `advanced.md`)
- **RaceCondition / retry** — commit uses optimistic version checks; on conflict
  the engine re-runs the System from the top (`retry=`, default high). So System
  bodies must be **safe to re-run**; do external / non-idempotent side effects
  only *after* commit (or after `await ctx.session_commit()`).
- **Endpoint** — a raw RPC with **no** transaction. Use only for non-DB work, or
  to call several Systems that each commit independently (`ctx.systems.call(...)`).
- **Permissions** — enforced at the Component level too: a `USER` System reading
  an `OWNER` Component still sees only the caller's rows. `RLS` needs
  `rls_compare=` on the Component. `elevate(ctx, user_id)` is how a login System
  authenticates a connection.
- **Subscriptions** — clients `select` one row (by unique key) or `range` over an
  indexed column; the server pushes deltas via Redis pub/sub, permission-filtered.
  No polling.
- **`ctx`** — `ctx.caller` (user id, `0` if anonymous), `ctx.user_data` (per-
  connection dict), `ctx.timestamp`, `ctx.race_count`. See `system/context.py`.
- **Row ids** — every row has an int64 `id` from SnowflakeID; never assign it
  yourself. Use `new_row()` / `new_rows(n)`.

## Testing Systems & Endpoints

`hetu.testing.Sandbox` runs your app **in-process on a temp SQLite file** — unit-test
Systems and Endpoints with no Docker/Redis needed. (→ `testing/__init__.py`)

```python
import app                              # your module with the @define_* decorators
from hetu.testing import Sandbox, sandbox_fixture, CallRejected, ConnectionClosed

sandbox = sandbox_fixture("Game", app)             # a pytest fixture (put in conftest.py)

async def test_rename(sandbox):
    await sandbox.call("rename", "Alice", caller=1001)     # full Endpoint path, as user 1001
    assert (await sandbox.get("Player", owner=1001)).name == "Alice"
```

Two ways to invoke your code:

- **`call(name, *args, caller=0, user_data=None)`** — the **client path**. Runs the full
  Endpoint pipeline: connection → permission/arg checks → guards → your `@define_endpoint`
  (or a System's auto-generated endpoint). `caller != 0` first `elevate`s a fresh
  connection (simulates a logged-in client). Returns what the client SDK actually receives
  (msgpack round-tripped). A guard soft-reject raises **`CallRejected`** (assert
  `.code`); an illegal call (bad permission/args, unknown endpoint, or the endpoint
  raised) raises **`ConnectionClosed`**. Each call is a fresh connection (no cross-call
  guard/rate-limit state — `@rate_limit` counting is HeTu's job, test it in integration).
- **`call_system(name, *args, caller=0, raw=False)`** — **bypasses the Endpoint layer**,
  runs a System directly (trusted internal call, no permission/guard/login) in a real
  transaction (auto-retries on `RaceCondition`). Default returns the client payload (same
  msgpack round-trip — a raw `np.int64` raises here as on the wire, `int()` it first; a
  `tuple` comes back as a `list`). Pass **`raw=True`** for the System's untouched return
  value (assert internal / nested-call results).
- **`get(Comp, **key)`** / **`range(Comp, index, lo, hi, limit=)`** read rows back
  directly (no RLS filtering); **`insert`/`upsert`** seed rows; **`flush()`** wipes all
  tables between tests.
- One app/namespace per process; a `call_system` target must reference ≥1 Component.

## Client (Unity / C#)

`hetu build` generates typed C# component classes from your definitions. The
client uses `HeTuClient.Instance` (one raw socket) or, preferred,
`HeTuSessionClient.Instance` (auto-reconnect + subscription replay):
`CallSystem(name, args...)`, `WatchRow<T>(index, value)`,
`WatchRange<T>(index, lo, hi, limit)`. Subscriptions hold a server resource and
**must be disposed** (`sub.AddTo(gameObject)`). See `unity-client.md`.

## CLI

`hetu init` (scaffold) · `hetu start --config=config.yml` (run) · `hetu upgrade`
(migrate schema — run before deploying any Component change) · `hetu build`
(regenerate the client SDK after any Component change).

## Where to read more

- **Docs**: https://github.com/Heerozh/HeTu/tree/main/docs/en —
  `getting-started.md`, `tutorial/chat-room.md`, `concepts.md`,
  `unity-client.md`, `advanced.md`, `operations.md` (or the site
  https://hetudb.vercel.app/).
- **`hetu/llms.txt`** — a link index shipped inside the installed package.
- **Engine source** — open it to read an API instead of guessing. Locate with
  `python -c "import hetu,pathlib;print(pathlib.Path(hetu.__file__).parent)"`,
  then read: `data/component.py` (components & fields), `system/definer.py`
  (Systems & clusters), `system/context.py` (`ctx` + repo), `endpoint/`
  (Endpoints, `elevate`), `data/backend/` (`SessionRepository` CRUD),
  `testing/__init__.py` (`Sandbox` unit-test helper), `CONFIG_TEMPLATE.yml`
  (every config key).
- **Advanced** (`advanced.md`): scheduled `FutureCalls`, `call_lock` idempotency,
  the `on_disconnect` hook, per-connection limits, NumPy patterns over `range()`
  results, custom pipeline layers.
