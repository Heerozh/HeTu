---
title: "Unity Client SDK"
description: "Connecting Unity to a HeTu server: System RPC, row subscriptions, and the two data-update patterns (event callbacks and R3 reactive)."
type: docs
weight: 35
prev: concepts
next: operations
---

# Unity Client SDK

The Unity SDK (`cn.hetudb.clientsdk`) is how a Unity game talks to a HeTu
server. It wraps the WebSocket protocol, the message pipeline (MessagePack +
zlib + crypto), and the subscription bookkeeping behind a single
`HeTuClient.Instance`.

This page assumes you can already start a server and have read
[Concepts](concepts.md) — Components, Systems, and Subscriptions are
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
3. **Optional dependencies** — install **R3** if you want the reactive APIs
   shown later on this page. The event-based APIs work without it.

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
        // Hook BEFORE Connect so the handshake-complete event isn't missed.
        HeTuClient.Instance.OnConnected += () =>
        {
            // Forget() = fire-and-forget; queued and sent in order.
            HeTuClient.Instance.CallSystem("login", SelfID).Forget();
        };

        // Connect() returns only when the connection ends.
        // Use a while loop for auto-reconnect on transient disconnects.
        while (true)
        {
            var err = await HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu/MyGame");
            if (err is null || err == "Canceled") break;     // normal close or app exit
            Debug.LogError($"Reconnecting after error: {err}");
            await Awaitable.WaitForSecondsAsync(1f);          // UniTask.Delay on 2022.3
        }
    }

    void OnDestroy() => HeTuClient.Instance.Close();
}
```

Key points the source enforces but isn't always obvious from a snippet:

- **`Connect` is long-lived.** It awaits until the socket closes, returning
  `null` on a clean close, `"Canceled"` on app-exit / `Close()`, or an error
  string otherwise. Don't `await` it on the same path that needs to start
  sending RPCs — kick off `CallSystem` from `OnConnected` (or after a
  separate task awaits the handshake), not from below the `await Connect`.
- **`Connect(url, authKey)`** is the same call but signs the handshake with
  a pre-shared key; use this if your server runs with `--authkey`.
- **One `Close()` per `Connect()`.** `Close()` cancels in-flight `CallSystem`
  / `Get` / `Range` calls and tears the socket down — call it from
  `OnDestroy` so quitting Play Mode doesn't leak a worker task.

## Calling Systems

`CallSystem(name, args...)` invokes a server-side System by name. You have
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
time you call a System with that name — useful for client-side prediction:

```csharp
HeTuClient.Instance.SystemLocalCallbacks["move_to"] = args =>
{
    // optimistic local update before the server round-trip
    transform.position = new Vector3((float)args[0], 0, (float)args[1]);
};
```

## Subscriptions: `Get` vs `Range`

Both subscriptions are **live**: the server pushes deltas as the underlying
rows change in Redis.

| API                                                | Returns                                                     | Use it when                                                                                               |
|----------------------------------------------------|-------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `Get<T>(index, value)`                             | `RowSubscription<T>` (one row, or `null` if no row matched) | You want exactly one row by a unique key — your own HP, your own inventory record.                        |
| `Range<T>(index, left, right, limit, desc, force)` | `IndexSubscription<T>` (a dictionary of rows, kept in sync) | You want a window over an indexed column — nearby players, top-N leaderboard, the last 100 chat messages. |

The `T` parameter is your strongly-typed Component class (see next section).
Drop it for `DictComponent`, a string-keyed `Dictionary` you index manually.

`Range`'s `force=true` (default) keeps the subscription alive even if the
initial query returns zero rows, so newly-inserted rows still trigger
`OnInsert` / `ObserveAdd`. Set `force=false` if you only care when there is
already data.

## Typed components vs `DictComponent`

The server can generate matching C# classes from your Component definitions
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
    var players = await HeTuClient.Instance.Range<Position>(
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
    var players = await HeTuClient.Instance.Range<Position>(
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
var hp = await HeTuClient.Instance.Get<HP>("owner", SelfID);
hp.AddTo(gameObject);

hp.Subject
    .Select(x => x.ID != 0 ? $"HP: {x.value}" : "Dead")
    .SubscribeToText(hpLabel)        // R3 Unity extension
    .AddTo(ref hp.DisposeBag);
```

When to prefer which:

- **Events** for a couple of simple side-effects (spawn, move, despawn).
- **R3** when you want to filter, throttle, combine, or feed UI bindings —
  and when chaining is clearer than nested handlers.

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

## Unity version notes

- **Unity 6000+** — `Connect`, `CallSystem`, `Get`, and `Range` return
  `Awaitable<T>`. Use `await Awaitable.WaitForSecondsAsync(...)` for delays.
- **Unity 2022.3** — same APIs return `UniTask<T>`. Install UniTask through
  the Setup Wizard. Use `await UniTask.Delay(ms)` for delays.

Both code paths are compiled behind `#if UNITY_6000_0_OR_NEWER`, so your
calling code only needs to choose one delay style.

## Where to next

- **[Concepts](concepts.md)** — re-read the Subscriptions section now that
  you've seen the client side; permissions / RLS filter what `Get` and
  `Range` can return.
- **[Tutorial: Chat Room](tutorial/chat-room.md)** — a complete client-and-
  server example using the patterns above.
