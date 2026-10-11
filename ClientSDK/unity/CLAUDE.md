# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This directory hosts the **HeTu Unity Client SDK** (`cn.hetudb.clientsdk`), a Unity Package Manager (UPM) package that lets Unity clients talk to a HeTu (河图) game server over WebSocket. The server project lives at `/workspace-HeTu` (see top-level `CLAUDE.md`); this SDK is shipped to game developers as a standalone package and must keep its wire protocol in lock-step with the server's `hetu/server/pipeline/` layers and message conventions in `hetu/server/receiver.py`.

Repository layout:

- `cn.hetudb.clientsdk/HeTu/` — runtime assembly (`HeTu.ClientSDK`)
- `cn.hetudb.clientsdk/Editor/` — Editor-only tooling: `Setup/` (dependency wizard), `Inspector/HeTuInspectorWindow.cs` (live request/response viewer fed by `InspectorTrace`), `PostStation/HeTuPostStationWindow.cs` (manual `CallSystem` / `WatchRange` console)
- `cn.hetudb.clientsdk/Tests/HeTu/` — PlayMode tests. Only `HeTuClientTest.cs` needs a running server; `SessionClientFacadeTest.cs` and `InspectorTraceTest.cs` use fakes and are server-less (they live here because they need the Unity engine / Awaitable bridge)
- `cn.hetudb.clientsdk/Tests/Editor/` — EditMode tests (pure-C# unit tests, no server / no Unity engine required)
- `cn.hetudb.clientsdk/UnityWebSocket/` — vendored `com.psygames.unitywebsocket` (do not modify; upstream copy)

开发时请使用TDD，先写RED测试，再修改。

## Architecture

### Layered client design

There are **two client layers**, used together or separately:

1. **`HeTuClientBase`** (`HeTu/ClientBase.cs`) — transport-agnostic core. Owns the message `MessagePipeline`, `ResponseManager` (FIFO request/response queue), and `SubscriptionManager` (sub_id → shared subscription core, strong refs). It exposes synchronous `*Sync` methods that take callbacks; it never touches Unity APIs.
2. **`HeTuClient : HeTuClientBase`** (`HeTu/UnityClient.cs`) — singleton `HeTuClient.Instance`. Implements the abstract `ConnectCore`/`CloseCore`/`SendCore` using `UnityWebSocket`, and wraps the callback-based base methods into Unity-friendly async ones (`Awaitable` on Unity 6000+, `UniTask` on 2022.3). All public APIs in this layer use compile-time `#if UNITY_6000_0_OR_NEWER` to switch between the two.

On top of those, **`HeTuSessionClient`** (`HeTu/SessionClient.cs`) is an optional logical-session layer. Its awaitable-free core lives in `HeTu/SessionClientBase.cs` as `internal sealed class HeTuSessionClientBase`, which takes an injected `IHeTuSessionConnection` and a scheduler — the Unity-facing `HeTuSessionClient` just bridges that core into `Awaitable`/`UniTask`, which is why the session core is unit-testable with fakes without booting the engine.

- Manages a persistent logical session rather than a single physical WebSocket.
- Auto-reconnects, re-runs the user-supplied `bootstrapAsync` (e.g. login), then restores still-alive subscriptions using the **original watch intent** (component + index + value/range), not the prior remote `sub_id`.
- `CallSystem` semantics: pending (not-yet-sent) calls wait for `Ready`; calls that were already sent when the connection drops throw `CallOutcomeUnknownException` — they are **never retried automatically** to avoid duplicating side effects.
- Restores per shared core (`ManagedRowCore<T>` / `ManagedIndexCore<T>`, see Subscriptions below): one restore per sub_id however many handles exist, and every handle gets `OnResynced`.
- Decoupled from Unity via `IHeTuSessionConnection`; `UnityHeTuSessionConnection` adapts `HeTuClient`. Tests provide their own fake connection.

### Wire protocol

Outbound and inbound messages flow through a configurable `MessagePipeline` of `MessageProcessLayer`s. The default stack matches the server's default and is set in `HeTuClientBase`'s constructor:

```
SetupPipeline([JsonbLayer, ZlibLayer, CryptoLayer])
```

- **`JsonbLayer`** — MessagePack serialization. Decode is lazy for payload data: complex bodies stay as `JsonObject` (a raw byte-slice wrapper) until the caller asks for `To<T>()`/`ToList<T>()`/`ToDict<TKey,TValue>()`. Only the standard envelopes `rsp`/`sub`/`updt`/`rej`/`err`/`sync` are parsed eagerly.
- **`ZlibLayer`** — stream zlib via `Unity.SharpZipLib`, with an optional preset dictionary negotiated during handshake.
- **`CryptoLayer`** — X25519 ECDH key agreement + ChaCha20-Poly1305 (BouncyCastle). Optional `SetAuthKey(...)` toggles a signed-hello variant (magic bytes `H2A1`).

If you change pipeline behavior **the server's matching layer in `hetu/server/pipeline/` must change in the same commit** — they handshake byte-for-byte.

Replies to `rpc` / `sub` carry no request id: `ResponseManager` matches them FIFO. The one exception is `CallSystemAwaitPush`: it sends `["rpcs", sync_id, system, ...]`, gets its `rsp` FIFO as usual, and completes only when the out-of-order `["sync", sync_id]` frame arrives (the server sends it after the subscription pushes the call caused). `HeTuClientBase` keeps the pending map; on disconnect (after `OnClosed`; Unity's `HeTuClient` does it in `CloseCore` before cancelling its waiters) / `Close` / reconnect / `Dispose`, calls that already got their `rsp` complete successfully. `HeTuSessionClientBase` does not rely on that timing: an internal `onAnswered` callback marks the `PendingCall` when its `rsp` arrives, and connection loss / Faulted / `Close` complete those calls successfully after the session state has changed (so continuations queue for the reconnect). Keep in lock-step with `rpcs` in `hetu/server/receiver.py`.

### Subscriptions

Two call shapes, each with a typed generic and a `DictComponent` (dynamic dictionary) overload:

- `WatchRow<T>(index, value, componentName = null)` — find the first row matching `index == value` and persistently watch *that row* by its primary key. If the row stops matching the original predicate, the subscription does **not** switch to a new match. After the first resolution, Session reconnects restore by row id, not by re-running the first-match query. The "by primary key" case is just `WatchRow("id", rowId)`. If **no row matches at subscribe time**, the call resolves to `null` (no subscription) and will **not** auto-attach when a matching row is created later — the watch binds to a concrete row id only at first resolution. To watch a row that may not exist yet (e.g. a per-player row created lazily on first write), either seed the row up-front, or use `WatchRange` over its key with `force: true` (see below).
- `WatchRange<T>(index, left, right, limit, desc = false, force = true, componentName = null)` — index range. `force = true` (the default) **keeps the subscription alive even when the range currently has no matching rows**, and delivers an `ObserveAdd` when a row later enters the range. That makes it the idiom for a lazily-created own-row: watch the single-key range (`left == right == myOwnerId`) and you receive the row the moment it is first written, no pre-seeding needed.

Returned `RowSubscription<T>` / `IndexSubscription<T>` are **`IDisposable` and MUST be disposed** — otherwise the server keeps streaming forever. The base class's finalizer logs a leak warning; the `StackTrace` shown is captured at creation only in `DEBUG` builds (release builds log a null trace). With R3 installed, `subscription.AddTo(gameObject)` ties the lifetime to a Unity `GameObject`. During a Session reconnect each subscription is marked `IsStale = true` and emits `OnResynced` after restore.

Subscriptions are split into a shared core and per-caller handles (`HeTu/Subscription.cs`). `ManagedRowCore<T>` / `ManagedIndexCore<T>` (internal) own the server subscription, the data (`Data` / `Rows`) and the restore state; `RowSubscription<T>` / `IndexSubscription<T>` are handles with their own events, `Subject`s, per-row subjects and `DisposeBag`. Every `Watch*` call — on `HeTuClientBase` and on the session — returns a **new handle**; a second watch of the same `subID` sends no new `sub` and attaches to the existing core (the caller's type generic is verified — mismatched `T` throws `InvalidCastException`). Disposing a handle only releases that handle; the `unsub` is sent when the last handle of a core is disposed, and only if that core is still the one registered for the `subID` (a core left over from a previous connection never unsubscribes a newer one). The server does not count duplicate subscriptions per connection — one `unsub` drops it — so the counting must stay client-side. For the same reason an `unsub` is never sent while any `sub` request is awaiting its reply (a non-id `WatchRow` can land on the subscription being dropped): it is parked until no `sub` is in flight, then dropped if the `subID` got registered again meanwhile, otherwise sent; parked ones are discarded when the connection closes. Pushes fan out to every live handle; a user callback that throws is logged and does not block the other handles. Session transports (`IHeTuSessionTransport`) deal in cores via `HeTuClientBase.Watch*CoreSync`; the session hands out the handles.

### Inspector trace

`HeTu/InspectorTrace.cs` is a built-in observability hook. `HeTuClientBase` owns an `InspectorTraceCollector` that records every request/subscription/response as an `InspectorTraceEvent` (target, status, size, duration, payload, response). Consumers register via `ConfigureInspector(bool)` plus `AddInspectorDispatcher(IInspectorTraceDispatcher)` — `LoggerInspectorTraceDispatcher` dumps to `Logger`, and `Editor/Inspector/HeTuInspectorWindow.cs` is the in-Editor live viewer. Two completion modes exist (`OnResponse` for RPC, `AfterSend` for fire-and-forget) — preserve this distinction if you add new code paths that produce trace events.

### Optional / conditional dependencies

The runtime asmdef (`HeTu.ClientSDK.asmdef`) is gated on `MSGPACK_INSTALLED` and uses `versionDefines` to set `R3_UNITY_INSTALLED` and `MSGPACK_INSTALLED` from package presence. The Editor Setup Wizard (`Editor/Setup/Wizard.cs`) walks users through installing:

1. **NuGet** (via NuGetForUnity): `MessagePack`, `BouncyCastle.Cryptography`, optionally `R3`.
2. **UPM**: `com.github.messagepack-csharp`, `com.cysharp.unitask` (only on Unity < 6000), optionally `com.cysharp.r3`.

Code that depends on R3 must be wrapped in `#if R3_UNITY_INSTALLED` (e.g. `BaseSubscription.AddTo(GameObject)` in `Subscription.cs`). New optional integrations should follow this pattern, not hard-add dependencies to `package.json`.

## Common commands

This package has no shell-driven build/test loop — everything runs inside the Unity Editor:

- **Open the test project**: open a Unity project (Unity 2022.3 or Unity 6000+) that has `cn.hetudb.clientsdk` added via UPM, then `Window > General > Test Runner`.
- **EditMode tests** (`Tests/Editor/`): run inside the Editor with no server. `ConnectionSemanticsTest.cs` exercises `HeTuClientBase` via an in-file `TestClient` subclass; `SessionClientBaseTest.cs` exercises `HeTuSessionClientBase` via `FakeTransport` + `FakeScheduler`; `SubscriptionTest.cs` exercises subscription rebinding via a `StubClient`; `SubscriptionHandleTest.cs` exercises shared cores / per-caller handles end to end through `HeTuClientBase` frames. Prefer adding new tests here whenever the behavior doesn't actually need a WebSocket.
- **PlayMode tests** (`Tests/HeTu/`): only `HeTuClientTest.cs` requires the HeTu server's pytest harness running at `ws://127.0.0.1:2466/hetu/pytest` (start it via `tests/app.py` in the server repo). `SessionClientFacadeTest.cs` uses fakes; `InspectorTraceTest.cs` is pure unit tests — both run without a server. Per the file comment in `HeTuClient.cs` (`CloseCore`), server-backed PlayMode tests **must be run in Player mode, not Editor mode** — otherwise the socket close path doesn't run a Unity main loop and tasks hang.
- **Run a single test**: in Test Runner, right-click the test name → "Run Selected". The `[Order(N)]` attribute on `TestRowSubscribe`/`TestSystemCall` in `HeTuClientTest.cs` matters — `TestRowSubscribe` must run before any successful `login` call (see `// 必须未调用过login测试才会通过` comment).
- **Manual end-to-end**: see the snippet in `cn.hetudb.clientsdk/README.md`.

## Conventions

- **Style** is enforced by the package's `.editorconfig`: 4-space indent, max line length **90** for `.cs`, Allman braces (newline before `{`), no implicit `this.`, `_camelCase` for private fields, `s_` prefix for private static fields. Run Rider's "Reformat & Cleanup" (or `dotnet format` if you have the SDK) before submitting.
- **Async API**: all public async methods come in pairs guarded by `#if UNITY_6000_0_OR_NEWER` — `Awaitable<T>` for Unity 6, `UniTask<T>` otherwise. When adding new public async APIs, follow this pattern; do not introduce a hard `UniTask` dependency on Unity 6.
- **Public APIs are bilingual** — XML docs are written in mixed Chinese + English. Keep new doc-comments in the same style; the CLI tooling at `hetu/sourcegen/` consumes these for client-side stubs.
- **Commit prefixes** (shared with the server repo): `ENH:` (feature), `BUG:` (fix), `MAINT:` (chore), `TST:` (tests). See `git log` for examples.
- **Logger**: never call `UnityEngine.Debug.Log` directly from the runtime assembly — go through `Logger.Instance`. The Unity client wires `Logger` to `Debug.Log` once when the singleton is constructed; the base class stays Unity-free.
- **Annotations**: `[MustDisposeResource]` and `[HandlesResourceDisposal]` from JetBrains.Annotations are used to silence false-positive leak warnings in Rider — re-use them rather than suppressing per-call.
- **Do not use .Net Threading.** for WebGL support purpose. Specifically, not use `Task` async return value, use Unity's `Awaitable`, or `UniTask` (For older Unity support) instead, and also TCS use `AwaitableCompletionSource`/`UniTaskCompletionSource`.

## Things to watch out for

- Bumping `package.json` `version` requires a matching tag on the server release; the encoded protocol must stay compatible or both ends bump.
- `UnityWebSocket/` is a vendored third-party plugin (MIT). Do not edit; if it needs a fix, file upstream and re-vendor.
- The `Setup Wizard` auto-pops every Editor open if dependencies are missing (`Editor/Setup/Wizard.cs : AutoPromptOncePerSession`). If you change which deps are mandatory vs. optional, update both `NuGetDependenciesInstaller.s_dependencies` and `UPMDependenciesInstaller.s_dependencies`.
- `HeTuClient` is a singleton (`HeTuClient.Instance`) — there is no per-instance Unity client. `HeTuSessionClient` is the per-session abstraction; create one per logical session if you need multiple.
