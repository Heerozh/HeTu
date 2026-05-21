---
title: "Unity Client SDK"
description: "Connecting Unity to a HeTu server: `System` RPC, row subscriptions, and the two data-update patterns (event callbacks and R3 reactive)."
type: docs
weight: 35
prev: concepts
next: advanced
---

The Unity SDK (`cn.hetudb.clientsdk`) is how a Unity game talks to a HeTu
server. It wraps the WebSocket protocol, the message pipeline (MessagePack +
zlib + crypto), and the subscription bookkeeping behind two layered entry
points:

- **`HeTuClient.Instance`** — singleton over one physical WebSocket. Bare
  metal: you call `Connect`, you handle drops, you re-auth yourself.
- **`HeTuSessionClient.Instance`** — *recommended for game clients.*
  Singleton too. A persistent logical session built on top of `HeTuClient`:
  auto-reconnect, re-runs your `bootstrap` (e.g. login), and restores every
  live subscription on its own.

The wire protocol, components, `CallSystem`, and `WatchRow` / `WatchRange`
shapes are identical between the two — the session client just keeps them
alive across network drops. Read [Connect, call, disconnect](#connect-call-disconnect)
first to understand the primitives, then jump to
[Logical session](#logical-session-hetusessionclient) for the production
pattern.

This page assumes you can already start a server and have read
[Concepts](concepts.md) — `Components`, `Systems`, and Subscriptions are
mentioned without re-explanation.

## Install

Add the package via UPM (Window → Package Manager → + → Add from git URL):

```
https://github.com/Heerozh/HeTu.git?path=/ClientSDK/unity/cn.hetudb.clientsdk
```

After import, open **HeTu → Setup Wizard...** and walk through the three
steps it offers:

1. **NuGet dependencies** — MessagePack, BouncyCastle.
2. **UPM dependencies** — `UniTask` on Unity 2022.3, `Awaitable` is built in
   on Unity 6000+.

The wizard pops automatically on first import; that's expected.

## Connect, call, disconnect

The whole client is a singleton: `HeTuClient.Instance`. There is no separate
"client builder" object — you configure callbacks on the singleton and call
`Connect`.

```csharp
using HeTu;
using UnityEngine;

public class NetBootstrap : MonoBehaviour
{
    public long SelfID = 1;

    async void Start()
    {
        // Connect() returns once the handshake is done.
        await HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu/MyGame");

        // The socket is up — call straight away.
        await HeTuClient.Instance.CallSystem("login", SelfID);

        // Optional: be notified when the socket later drops.
        var err = await HeTuClient.Instance.WaitClosedAsync();
        Debug.Log($"disconnected: {err ?? "normal close"}");
    }

    void OnDestroy() => HeTuClient.Instance.Close();
}
```

Key points the source enforces but isn't always obvious from a snippet:

- **URL format** — `ws://<host>:<port>/hetu/<instance>` (or `wss://...`
  when the server / reverse-proxy terminates TLS). The `/hetu/` prefix is
  required, and `<instance>` must match one of the `INSTANCES:` entries
  in your server's `config.yml` (or the `--instance` flag if it was
  started from CLI). An unknown instance is rejected after the handshake
  — by design, so port scanners can't enumerate valid names.
- **`Connect` returns once the handshake is complete.** A successful await
  means the socket is up, the encryption / dictionary handshake finished,
  and you can call `CallSystem` / `WatchRow` / `WatchRange` immediately. To
  observe the eventual disconnect, await `WaitClosedAsync()` — it returns
  `null` on a clean close, `"Canceled"` on app-exit / `Close()`, or the
  error string otherwise. (Older versions of the SDK had `Connect` block
  until close; if you're upgrading, replace the `while (true) { await
  Connect... }` reconnect loop with `HeTuSessionClient` below.)
- **`Connect(url, authKey)`** is the same call but signs the handshake with
  a pre-shared key; use this if your server runs with `--authkey`.
- **One `Close()` per `Connect()`.** `Close()` cancels in-flight `CallSystem`
  / `WatchRow` / `WatchRange` calls and tears the socket down — call it from
  `OnDestroy` so quitting Play Mode doesn't leak a worker task.
- **No automatic reconnect at this layer.** When the socket drops,
  `HeTuClient` stays down until you call `Connect` again. If you need a
  session that survives transient drops and replays its subscriptions, use
  [`HeTuSessionClient`](#logical-session-hetusessionclient).

## Calling `Systems`

`CallSystem(name, args...)` invokes a server-side `System` by name. You have
two ways to use it:

```csharp
// Fire-and-forget: returns immediately, queued, sent in order.
HeTuClient.Instance.CallSystem("move_to", x, z).Forget();

// Await: waits for the server's reply (default "ok", or whatever
// ResponseToClient(...) returned from the System).
var resp = await HeTuClient.Instance.CallSystem("buy", itemId);
Debug.Log(resp.To<string>());
```

Use `.Forget()` (or `_ = CallSystem(...)`) for fast input streams like per-
frame movement; `await` for actions whose result you actually need.

**Local pre-callbacks.** You can register a client-side hook that runs every
time you call a `System` with that name — useful for client-side prediction:

```csharp
HeTuClient.Instance.SystemLocalCallbacks["move_to"] = args =>
{
    // optimistic local update before the server round-trip
    transform.position = new Vector3((float)args[0], 0, (float)args[1]);
};
```

## Subscriptions: `WatchRow` vs `WatchRange`

Both subscriptions are **live**: the server pushes deltas as the underlying
rows change in Redis.

| API                                                     | Returns                                                     | Use it when                                                                                               |
|---------------------------------------------------------|-------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `WatchRow<T>(index, value)`                             | `RowSubscription<T>` (one row, or `null` if no row matched) | You want exactly one row by a unique key — your own HP, your own inventory record.                        |
| `WatchRange<T>(index, left, right, limit, desc, force)` | `IndexSubscription<T>` (a dictionary of rows, kept in sync) | You want a window over an indexed column — nearby players, top-N leaderboard, the last 100 chat messages. |

The `T` parameter is your strongly-typed `Component` class (see next section).
Drop it for `DictComponent`, a string-keyed `Dictionary` you index manually.

`WatchRange`'s `force=true` (default) keeps the subscription alive even if the
initial query returns zero rows, so newly-inserted rows still trigger
`OnInsert` / `ObserveAdd`. Set `force=false` if you not want subscript an empty query.

## Typed components vs `DictComponent`

The server can generate matching C# classes from your `Component` definitions
via `hetu build`. The result implements `IBaseComponent`:

```csharp
public class Position : IBaseComponent
{
    public long ID { get; set; }   // ID is mandatory; matches `id` on the server
    public long owner;
    public float x;
    public float y;
}
```

With a typed `T`, you read fields directly: `sub.Data.x`. Without it, you
get a `DictComponent` (a `Dictionary<string, object>`), and you read with
`Convert.ToSingle(sub.Data["x"])` — flexible but verbose, and you lose
compile-time field checks.

## Two ways to react to data changes

The same subscription object exposes **both** an event-based API and an R3
reactive API. Pick whichever fits the call site — they coexist, they're
backed by the same internal state, and you can mix them in the same
codebase.

### Pattern A — event callbacks

Plain C# events. No extra dependencies.

```csharp
async void SubscribeOthers()
{
    var players = await HeTuClient.Instance.WatchRange<Position>(
        "owner", 1, 999, 100);
    players.AddTo(gameObject); // dispose when this GameObject is destroyed

    // Initial rows are already populated:
    foreach (var p in players.Rows.Values)
        AddPlayer(p);

    // Server-side INSERT into the index range
    players.OnInsert += (sender, rowID) =>
        AddPlayer(sender.Rows[rowID]);

    // Server-side UPDATE on a row already in the range
    players.OnUpdate += (sender, rowID) =>
    {
        var p = sender.Rows[rowID];
        MovePlayer(p.owner, new Vector3(p.x, 0.5f, p.y));
    };

    // Server-side DELETE, or row leaving the range
    players.OnDelete += (sender, rowID) =>
        RemovePlayer(sender.Rows[rowID].owner);
}
```

For a single-row `RowSubscription<T>` the events are simpler:
`OnUpdate(sender)` and `OnDelete(sender)`.

### Pattern B — R3 reactive streams

The same subscription exposes
`Observable<T>` streams. This pays off when you're chaining operators or
binding to UI.

```csharp
async void SubscribeOthers()
{
    var players = await HeTuClient.Instance.WatchRange<Position>(
        "owner", 1, 999, 100);
    players.AddTo(gameObject);

    // Add stream — initial rows are emitted first, then live inserts.
    players.ObserveAdd()
        .Subscribe(p => AddPlayer(p))
        .AddTo(ref players.DisposeBag);

    // Remove stream — emits the row ID that left the range.
    players.ObserveRemove()
        .Subscribe(rowID => RemovePlayer(rowID))
        .AddTo(ref players.DisposeBag);

    // Per-row update stream — completes (OnCompleted) when the row is removed.
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

For `RowSubscription<T>`, use `sub.Subject` — it emits the current row first,
then every update, and is ideal for direct UI binding:

```csharp
var hp = await HeTuClient.Instance.WatchRow<HP>("owner", SelfID);
hp.AddTo(gameObject);

hp.Subject
    .Select(x => x.ID != 0 ? $"HP: {x.value}" : "Dead")
    .SubscribeToText(hpLabel)        // R3 Unity extension
    .AddTo(ref hp.DisposeBag);
```

When to prefer which:

- **`R3`** approach is recommended, as it is more concise and clear, and involves less
  code.
- **`Events`** for a couple of simple side-effects (spawn, move, despawn).

## Subscription lifecycle (don't skip this)

Every subscription holds a server-side resource. The SDK's finalizer logs an
error if a subscription is GC'd without `Dispose()` — that is a real leak,
not a warning to ignore.

Three correct patterns:

```csharp
// 1. Tie to a GameObject — disposes on Destroy.
sub.AddTo(gameObject);

// 2. Tie to a DisposableBag (for nested R3 subscriptions, or grouping).
sub.AddTo(ref _bag);

// 3. Manual.
try { /* use sub */ } finally { sub.Dispose(); }
```

`Dispose()` does two things: tells the server "stop pushing me changes for
this query" and tears down all R3 streams chained off the subscription.
After dispose, the `Subject` / `ObserveRow` streams will not emit further.

## Logical session: `HeTuSessionClient`

A bare `HeTuClient` is one physical WebSocket — if it drops, your data goes
silent until you reconnect, re-login, and re-issue every `WatchRow` /
`WatchRange`. `HeTuSessionClient` does that bookkeeping for you. Like
`HeTuClient`, it's a singleton (`HeTuSessionClient.Instance`) — it owns the
underlying `HeTuClient.Instance`, reconnects on its own, replays your
`bootstrap` (typically the login round-trip), and re-issues every still-alive
subscription using its **original watch intent** (component + index + value /
range).

```csharp
var session = HeTuSessionClient.Instance;

await session.Connect(
    "ws://127.0.0.1:2466/hetu/MyGame",
    bootstrap: async client =>
    {
        // 'client' is HeTuClient.Instance — the physical connection that's
        // being brought up. Re-auth on every (re)connect.
        await client.CallSystem("login", SelfID);
    });
// session.State is now Ready.

using var hp = await session.WatchRow<HP>("owner", SelfID);
using var nearby = await session.WatchRange<Position>("zone", zone, zone, 50);

await session.CallSystem("move_to", 12f, 8f);
```

`Connect` only resolves *after* bootstrap and subscription restore have both
completed — so once it returns, your live data is hot and your code is on
the same footing it would have been before the drop.

The full signature:

```csharp
Awaitable Connect(
    string url,
    string authKey = null,
    Func<HeTuClient, Awaitable> bootstrap = null,
    TimeSpan? reconnectDelay = null,        // default: 1s
    TimeSpan? maxReconnectDelay = null,     // default: 30s (exponential cap)
    int maxReconnectAttempts = 20,          // 0 = unlimited
    TimeSpan? connectTimeout = null);       // default: 30s
```

What the await can do:

- **resolve** when the session reaches `Ready` — normal path.
- throw **the underlying failure exception** as soon as it happens — the
  session does **not retry before first Ready**. Anything that fails the
  initial connect (socket close, bootstrap throw, restore error) lands in
  `Faulted` terminal state on the first attempt, and the awaiter sees the
  original exception (e.g. your `CallSystem("login", ...)` exception, or
  `InvalidOperationException("Connection closed: <reason>")`). Retries with
  the same URL / credentials wouldn't help, so the SDK doesn't try.
- throw **`TimeoutException`** if `Ready` isn't reached within
  `connectTimeout` (default 30 s) — primarily a safety net for a transport
  that hangs without ever raising close/error. The `Message` and
  `InnerException` carry the last captured fault (if any). The session is
  `Close()`'d on the way out; you can `Connect(...)` again afterward.
- throw **`OperationCanceledException`** if someone called `Close()` from
  another code path.

Calling `Connect` while the session is already running throws — call
`Close()` first if you really want to reconnect with a different URL. After
`Close()` (or `Faulted`), you can `Connect` again and the state machine
starts from scratch with a fresh core.

### Defaults — when to override

- `reconnectDelay = 1s`, `maxReconnectDelay = 30s` — exponential back-off
  from 1 s, doubling each attempt, capped at 30 s. Reasonable for both
  transient hiccups and longer outages; the cap stops you from hammering
  the server while still waking up promptly when it recovers.
- `maxReconnectAttempts = 20` — **only governs reconnects after the session
  has reached `Ready` at least once**. With the exponential schedule above,
  20 attempts spans roughly 8 minutes (1+2+4+8+16+30 × 15 ≈ 481 s) of
  in-game retry before surrendering to `Faulted`. Long maintenance windows:
  pass `0` (unlimited) so a player can leave the app open and rejoin when
  the server returns. **Initial connect always uses 1 attempt** — there's no
  retry value when the credentials or URL might be wrong.
- `connectTimeout = 30s` — guards the **initial** `Connect` only; reconnects
  after `Ready` aren't subject to it. If you set `maxReconnectAttempts > 0`,
  consider raising or disabling this — otherwise whichever budget runs out
  first wins.

### State machine

`session.State` walks these in order, and `StateChanged` fires on every
transition:

| State                    | Meaning                                                                                            |
|--------------------------|----------------------------------------------------------------------------------------------------|
| `Stopped`                | Initial, or after `Close()` / `Dispose()`. `Connect()` moves it forward.                           |
| `Connecting`             | WebSocket handshake in progress.                                                                   |
| `Bootstrapping`          | Socket up; your `bootstrap` delegate is running.                                                   |
| `RestoringSubscriptions` | Bootstrap done; re-issuing each live `WatchRow` / `WatchRange`.                                    |
| `Ready`                  | Queued calls have been flushed; the `Ready` event fires. Steady state.                             |
| `Reconnecting`           | Transient drop; waiting `reconnectDelay`, then back to `Connecting`.                               |
| `Faulted`                | Terminal — either an initial-connect failure (always terminal), or post-Ready `maxReconnectAttempts` reached. `Faulted` event carries the last `Exception`. |

The `Faulted` **event** is broader than the `Faulted` **state**: it fires
once per failed attempt — pre-Ready failures fire it once then go terminal;
post-Ready transient drops fire it on every retry. Check `session.State`
from inside the handler to tell terminal from transient (post-Ready retry
keeps state in `Reconnecting`).

### `CallSystem` semantics

Calls behave differently depending on *when* a drop hits them:

- **Not yet sent (queued before `Ready`)** — held until the session is
  `Ready`, then dispatched in order. Safe to issue from anywhere, including
  from inside the `Ready` event.
- **In-flight when the socket dropped** — fail with
  `CallOutcomeUnknownException`. The session **never auto-retries** these,
  because the server may already have applied the call's side-effects. If
  the call is idempotent, catch and retry it yourself.

```csharp
try {
    await session.CallSystem("buy", itemId);
} catch (CallOutcomeUnknownException) {
    // Server may or may not have charged the player. Decide per-call.
}
```

### Subscription survival

`session.WatchRow<T>` / `session.WatchRange<T>` return the same
`RowSubscription<T>` / `IndexSubscription<T>` you've seen above, with three
extra hooks the session populates:

- **`sub.IsStale`** — `true` while the underlying socket is down. `sub.Data`
  / `sub.Rows` keep their last-known values; no updates are coming until
  the session is `Ready` again.
- **`sub.OnResynced`** — fires once after each reconnect, after the snapshot
  has been refreshed. Any inserts / updates / deletes that happened during
  the drop are replayed first through the normal `OnInsert` / `OnUpdate` /
  `OnDelete` events (and the matching R3 streams), so your handlers see a
  clean diff — not just a "everything reloaded" signal. Use it to clear
  any "stale" indicators in your UI.
- **De-duplication** — calling `WatchRow` / `WatchRange` twice with the
  same arguments hands you the **same** subscription object back; the
  remote unsubscribe only happens when *every* handle is disposed.

`RowSubscription`'s restore target is the **bound row's `ID`**, not the
original index/value: if your call was `session.WatchRow<Player>("owner",
userId)` and the matched row's primary key is `42`, reconnects re-subscribe
by `id=42`. A row that stops matching the original predicate while you were
offline is **not** swapped out — the subscription fires `Update(null)` and
stays bound to `42`.

### Shutdown

`HeTuSessionClient.Instance.Close()` (or `Dispose()` — they're aliases)
cancels in-flight calls with `OperationCanceledException`, disposes every
subscription it owns, and tears the socket down. The singleton itself
survives — call `Connect(...)` again to start a fresh session.

```csharp
async void Start() {
    await HeTuSessionClient.Instance.Connect(url, bootstrap: Login);
    // ... watch / call ...
}

void OnDestroy() => HeTuSessionClient.Instance.Close();
```

## Unity version notes

- **Unity 6000+** — `Connect`, `CallSystem`, `WatchRow`, and `WatchRange` return
  `Awaitable<T>`. Use `await Awaitable.WaitForSecondsAsync(...)` for delays.
- **Unity 2022.3** — same APIs return `UniTask<T>`. Install UniTask through
  the Setup Wizard. Use `await UniTask.Delay(ms)` for delays.

Both code paths are compiled behind `#if UNITY_6000_0_OR_NEWER`, so your
calling code only needs to choose one delay style.

## Where to next

- **[Advanced](advanced.md)** — `System` copies, scheduled future calls,
  raw `Endpoints`, custom pipeline layers, and the engine internals you'll
  reach for once a project gets real.
- **[Concepts](concepts.md)** — re-read the Subscriptions section now that
  you've seen the client side; permissions / RLS filter what `WatchRow` and
  `WatchRange` can return.
- **[Tutorial: Chat Room](tutorial/chat-room.md)** — a complete client-and-
  server example using the patterns above.
