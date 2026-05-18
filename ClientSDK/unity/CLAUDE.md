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

## Architecture

### Layered client design

There are **two client layers**, used together or separately:

1. **`HeTuClientBase`** (`HeTu/ClientBase.cs`) — transport-agnostic core. Owns the message `MessagePipeline`, `ResponseManager` (FIFO request/response queue), and `SubscriptionManager` (weak-ref cache of live subscriptions). It exposes synchronous `*Sync` methods that take callbacks; it never touches Unity APIs.
2. **`HeTuClient : HeTuClientBase`** (`HeTu/UnityClient.cs`) — singleton `HeTuClient.Instance`. Implements the abstract `ConnectCore`/`CloseCore`/`SendCore` using `UnityWebSocket`, and wraps the callback-based base methods into Unity-friendly async ones (`Awaitable` on Unity 6000+, `UniTask` on 2022.3). All public APIs in this layer use compile-time `#if UNITY_6000_0_OR_NEWER` to switch between the two.

On top of those, **`HeTuSessionClient`** (`HeTu/SessionClient.cs`) is an optional logical-session layer. Its awaitable-free core lives in `HeTu/SessionClientBase.cs` as `internal sealed class HeTuSessionClientBase`, which takes an injected `IHeTuSessionConnection` and a scheduler — the Unity-facing `HeTuSessionClient` just bridges that core into `Awaitable`/`UniTask`, which is why the session core is unit-testable with fakes without booting the engine.

- Manages a persistent logical session rather than a single physical WebSocket.
- Auto-reconnects, re-runs the user-supplied `bootstrapAsync` (e.g. login), then restores still-alive subscriptions using the **original watch intent** (component + index + value/range), not the prior remote `sub_id`.
- `CallSystem` semantics: pending (not-yet-sent) calls wait for `Ready`; calls that were already sent when the connection drops throw `CallOutcomeUnknownException` — they are **never retried automatically** to avoid duplicating side effects.
- Subscriptions are reference-counted through `ManagedRowCore<T>` / `ManagedIndexCore<T>`; duplicate `WatchRowById/WatchFirst/WatchRange` calls for the same intent share a single remote subscription, and the remote unsub happens when the last handle disposes.
- Decoupled from Unity via `IHeTuSessionConnection`; `UnityHeTuSessionConnection` adapts `HeTuClient`. Tests provide their own fake connection.

### Wire protocol

Outbound and inbound messages flow through a configurable `MessagePipeline` of `MessageProcessLayer`s. The default stack matches the server's default and is set in `HeTuClientBase`'s constructor:

```
SetupPipeline([JsonbLayer, ZlibLayer, CryptoLayer])
```

- **`JsonbLayer`** — MessagePack serialization. Decode is lazy for payload data: complex bodies stay as `JsonObject` (a raw byte-slice wrapper) until the caller asks for `To<T>()`/`ToList<T>()`/`ToDict<TKey,TValue>()`. Only the standard envelopes `rsp`/`sub`/`updt` are parsed eagerly.
- **`ZlibLayer`** — stream zlib via `Unity.SharpZipLib`, with an optional preset dictionary negotiated during handshake.
- **`CryptoLayer`** — X25519 ECDH key agreement + ChaCha20-Poly1305 (BouncyCastle). Optional `SetAuthKey(...)` toggles a signed-hello variant (magic bytes `H2A1`).

If you change pipeline behavior **the server's matching layer in `hetu/server/pipeline/` must change in the same commit** — they handshake byte-for-byte.

### Subscriptions

Two call shapes, each with a typed generic and a `DictComponent` (dynamic dictionary) overload:

- `WatchRow<T>(index, value, componentName = null)` — find the first row matching `index == value` and persistently watch *that row* by its primary key. If the row stops matching the original predicate, the subscription does **not** switch to a new match. After the first resolution, Session reconnects restore by row id, not by re-running the first-match query. The "by primary key" case is just `WatchRow("id", rowId)`.
- `WatchRange<T>(index, left, right, limit, desc = false, force = true, componentName = null)` — index range.

Returned `RowSubscription<T>` / `IndexSubscription<T>` are **`IDisposable` and MUST be disposed** — otherwise the server keeps streaming forever. The base class's finalizer logs a leak warning; the `StackTrace` shown is captured at creation only in `DEBUG` builds (release builds log a null trace). With R3 installed, `subscription.AddTo(gameObject)` ties the lifetime to a Unity `GameObject`. During a Session reconnect each subscription is marked `IsStale = true` and emits `OnResynced` after restore.

`SubscriptionManager` deduplicates: when the same `subID` is requested twice the existing object is returned and the caller's type generic is verified — mismatched `T` for an already-subscribed row throws `InvalidCastException`.

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
- **EditMode tests** (`Tests/Editor/`): run inside the Editor with no server. `ConnectionSemanticsTest.cs` exercises `HeTuClientBase` via an in-file `TestClient` subclass; `SessionClientBaseTest.cs` exercises `HeTuSessionClientBase` via `FakeTransport` + `FakeScheduler`; `SubscriptionTest.cs` exercises subscription deduping/rebinding via a `StubClient`. Prefer adding new tests here whenever the behavior doesn't actually need a WebSocket.
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
