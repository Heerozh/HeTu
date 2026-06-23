---
name: hetu-csharp-client
description: Use when writing the Unity/C# client of a HeTu (河图) game — connecting / logging in with HeTuSessionClient, RPC via CallSystem, or live data via WatchRow / WatchRange (R3 subscriptions); also when subscribed rows show only their initial value and never update live, or a reconnect overlay / session-state handling is needed.
---

# HeTu C# Client (Unity)

The client talks to a HeTu server over one WebSocket: call Systems by RPC, subscribe to
rows/ranges that the server pushes back live. Prefer **`HeTuSessionClient.Instance`**
(auto-reconnect + subscription replay) over the raw `HeTuClient.Instance`. Subscriptions
are **R3** (`Observable<T>`) hot sources, each holds a server resource and **must be
disposed**. For the server side (Components/Systems/permissions) see [[building-on-hetu]].

When unsure of an API, read the SDK source under
`/HeTu/ClientSDK/unity/cn.hetudb.clientsdk/HeTu/` (`SessionClient.cs`, `Subscription.cs`).

## Session lifecycle

`HeTuSessionClient.Instance` is a process-wide singleton. `Connect` runs your `bootstrap`
after **every** (re)handshake, replays live subscriptions, and returns once
`State == Ready`.

```csharp
await HeTuSessionClient.Instance.Connect(
    url,                                   // ws://host:2466/hetu/MyGame
    authKey,                               // optional pre-shared key (server --authkey)
    async client => {                      // bootstrap: runs on first connect AND every reconnect
        var resp = await client.CallSystem("login", clientId, token);
        // …read identity from resp, stash it…
    },
    connectTimeout: TimeSpan.FromSeconds(10),
    maxReconnectAttempts: 20);
```

- **bootstrap re-runs on every reconnect** — the server forgets the connection on drop, so
  re-establish identity here; keep it idempotent. `SetBootstrap(...)` sets it after a first
  anonymous `Connect`.
- `maxReconnectAttempts` governs reconnects **only after the first `Ready`**. Any failure
  *before* the first Ready → `Faulted` (retrying the same creds/URL is pointless). `0` =
  unlimited post-Ready retries.
- `Connect` throws if already running — `Close()` first. Editor tip: with Domain Reload off,
  the static client survives Play; call `Close()` in your login scene's `Start` before
  connecting. (See `Login.cs`.)

### React to reconnects: `StateChanged`

`Stopped → Connecting → Bootstrapping → RestoringSubscriptions → Ready`; on drop,
`Reconnecting` (loops) → `Ready`, or `Faulted` (terminal). Wire a global overlay **once** on
the facade event — it survives internal core rebuilds, so subscribe at startup and never
re-attach:

```csharp
HeTuSessionClient.Instance.StateChanged += state => {
    if (state == HeTuSessionState.Reconnecting) ShowOverlay();
    else if (state == HeTuSessionState.Ready)   HideOverlay();
    else if (state == HeTuSessionState.Faulted) { HideOverlay(); ReturnToLogin(); }
};
```

(See `ReconnectOverlay.cs`, installed once from `GameBootstrap.cs`.)

## RPC

```csharp
JsonObject resp = await HeTuSessionClient.Instance.CallSystem("rename", "Alice");
var d = resp?.ToDict<string, object>();          // also .ToUntyped()
```

Wrap calls that may race a reconnect in `try/catch (CallOutcomeUnknownException)`: the result
is genuinely unknown mid-reconnect — don't auto-retry, let the subscription reflect the truth.

## Subscriptions — the part everyone gets wrong

`WatchRow<T>` watches **one** row (by unique index); `WatchRange<T>` watches a **set** over an
indexed column. Both are awaitable and return a disposable — `.AddTo(gameObject)` so it
unsubscribes on destroy. `force: true` (default) keeps the subscription alive even if the
range is currently empty, so you still receive later inserts.

```csharp
var sub = await client.WatchRange<Hp>("owner", 0, long.MaxValue, 100);
sub.AddTo(gameObject);
```

### THE BUG: `ObserveAdd()` / `ObserveRemove()` are membership, NOT field updates

`ObserveAdd()` fires **once per row** — it replays the current rows on subscribe, then fires
once per future insert. `ObserveRemove()` fires once per delete. **Neither re-fires when a
row's fields change.** Wire only these two and every label shows its *initial* value forever
(and reopening a view shows stale data).

For **live field updates** you must subscribe `ObserveRow(row.ID)` **inside** the `ObserveAdd`
handler:

```csharp
var ui = new Dictionary<long, GameObject>();

sub.ObserveAdd().Subscribe(row => {              // initial rows + future inserts
    var go = CreateLabel(row.ID);                 // (requirement: appear)
    ui[row.ID] = go;
    var text = go.GetComponent<Text>();
    text.text = $"{row.Value}";                    // seed with current value
    // ✅ live field updates — WITHOUT this line the label never changes again:
    sub.ObserveRow(row.ID).Subscribe(r => text.text = $"{r.Value}");
}).AddTo(ref sub.DisposeBag);

sub.ObserveRemove().Subscribe(id => {            // (requirement: disappear)
    if (ui.Remove(id, out var go)) Object.Destroy(go);
}).AddTo(ref sub.DisposeBag);
```

| You want… | You need |
|---|---|
| a row appears / disappears | `ObserveAdd()` + `ObserveRemove()` |
| a row's **fields** change live | `ObserveRow(id)` **inside** the add handler |
| all current rows right now | `sub.Rows` (`Dictionary<long,T>`) |

### Gotcha: don't leak the per-row subscription

Do **not** `.AddTo(ref sub.DisposeBag)` (or any long-lived bag) the inner `ObserveRow`
subscription: `ObserveAdd` is unbounded, so a bag you append to per-add grows forever → leak.
No need to anyway — `ObserveRow(id)` **completes when its row is deleted** (R3 auto-disposes),
and `IndexSubscription.Dispose` drops every per-row subject. For explicit cleanup, bind it to a
**per-row lifetime**: `.AddTo(go)` on the row's *own* GameObject, never the shared bag.

### Single row

```csharp
var hp = await client.WatchRow<Hp>("owner", myId);
hp.Subject.Subscribe(r => text.text = $"{r.Value}").AddTo(ref hp.DisposeBag); // .Subject prepends current
hp.AddTo(gameObject);
```

## Quick reference

| API | Purpose |
|---|---|
| `HeTuSessionClient.Instance` | process-wide auto-reconnect client |
| `.Connect(url, authKey, bootstrap, …, maxReconnectAttempts, connectTimeout)` | start session; awaits to `Ready` |
| `.StateChanged` / `.State` | session-state events (reconnect UI) · `HeTuSessionState` |
| `.CallSystem(name, args…)` → `JsonObject` | RPC a System/Endpoint |
| `.WatchRow<T>(index, value)` → `RowSubscription<T>` | one row; `.Subject`, `.Data` |
| `.WatchRange<T>(index, left, right, limit, force:)` → `IndexSubscription<T>` | a set; `.Rows`, `.ObserveAdd/Remove/Row` |
| `.Close()` | end session (call before re-`Connect`) |

In-repo reference code: `Login.cs` (Connect + login bootstrap), `ReconnectOverlay.cs` /
`GameBootstrap.cs` (StateChanged overlay, installed once), `LobbyMatchSection.cs` (range +
per-row `ObserveRow`).
