# SessionClient 重构：用 Future<T> 替代 callback 串

**Branch**: `bug/client-sdk-connect-fail-throw`
**Date**: 2026-05-18
**Goal**: 消除 `HeTuSessionClientBase` / `HeTuSessionClient` 里"一堆 callback"导致的可读性问题。

## 背景与约束

`SessionClientBase.cs`（933 行）与 `SessionClient.cs`（556 行）当前通过 callback 串状态机：
`Start → ConnectNewTransport → HandleTransportConnected → RestoreSubscriptions → BecomeReady`
六个 handler 跳来跳去；`HandleTransportClosed` 与 `HandleRecoverableFailure` 有大段重复；
`PendingRowWatch<T>` 和 `PendingRangeWatch<T>` 几乎是 copy-paste。Facade 端的 `Connect`
则有 `HandleConnectTimeout` / `AwaitConnectAsync` / `ScheduleConnectTimeout` /
`RunConnectTimeoutAsync` 一连串 TCS + 三 event handler + timeout 的拼装。

**硬约束**：

- 基类必须保持无 `Task` / `Awaitable` / `UniTask` 依赖（WebGL 安全；EditMode 单测
  可在无 Unity 引擎下用 fake 跑通）。
- `IHeTuSessionTransport` 接口必须保留 callback 形式——`FakeTransport` 和 26 个
  `SessionClientBaseTest` 用例直接依赖它。
- 公共 API（`CallSystem` / `WatchRow` / `WatchRange` / `Connect` / `Close` / 事件）
  签名不变；只是内部走线变化。

## 架构

新增一层内部 `Future` / `Promise` 抽象（无外部依赖），让 Base 状态流可线性阅读；
`IHeTuSessionTransport` 接口保持 callback 不变，在 Base 内部用 helper 把 callback
adapt 成 `Future`。

```
HeTu/
├── Future.cs                       新增 ~180 行
├── SessionClientBase.cs            933 → ~600
├── SessionClientBase.Pending.cs    新增 ~100（拆出 PendingCall + 合并的 PendingWatch<TSub>）
├── SessionClient.cs                556 → ~280
```

## §1 Future / Promise 契约

`internal sealed`，纯 callback 聚合器；不依赖 `Task` / `Awaitable` / `UniTask`。

```csharp
internal sealed class Future {
    public static readonly Future Completed;
    public static Future Failed(Exception ex);

    public bool IsCompleted { get; }
    public bool IsFailed    { get; }
    public Exception Exception { get; }

    public Future Then(Action next);
    public Future Then(Func<Future> next);
    public Future Catch(Action<Exception> handler);
    public Future Finally(Action handler);
}

internal sealed class Future<T> {
    public bool IsCompleted { get; }
    public bool IsFailed    { get; }
    public T Value { get; }
    public Exception Exception { get; }

    public Future Then(Action<T> next);
    public Future<U> Then<U>(Func<T, Future<U>> next);
    public Future Then(Func<T, Future> next);
    public Future<T> Catch(Action<Exception> handler);
    public Future<T> Finally(Action handler);
}

internal sealed class Promise {
    public Future Future { get; }
    public bool TryComplete();
    public bool TryFail(Exception ex);
}
internal sealed class Promise<T> {
    public Future<T> Future { get; }
    public bool TryComplete(T value);
    public bool TryFail(Exception ex);
}
```

关键语义：

- **一次性完成**：`TryComplete` / `TryFail` 只生效一次，重复返回 `false`。天然取代
  原 `_inFlightCalls.Remove(pending)` 的去重防护。
- **完成即同步触发**：与原 callback 行为一致；已完成的 future 添加 continuation 时
  **立即同步执行**，不引入调度。
- **continuation 异常隔离**：所有 `Then` / `Catch` 内部统一用 `SafeInvoke` 包装，
  单个 handler 抛异常不会卡死后续——把原文件里散落的 try/catch 收成一处。
- **不取消、只失败**：会话关闭一律 `TryFail(new OperationCanceledException(...))`。
  少一个状态。
- **不依赖 `Task` / `Awaitable` / `UniTask`**：WebGL 安全。Facade 端用一对
  `FutureToAwaitable` / `AwaitableToFuture`（或 UniTask 对应版本）桥接，单点。

## §2 Base 状态流改造

主流程从散在 6 个 handler 改为单一可读链：

```csharp
private void Start() {
    if (_closed || State != HeTuSessionState.Stopped) return;
    RunSessionLifecycle();
}

private void RunSessionLifecycle() {
    if (_closed) return;
    ConnectTransport()
        .Then(RunBootstrap)
        .Then(RestoreAllSubscriptions)
        .Then(MarkReady)
        .Catch(HandleSessionFailure);
}

private Future ConnectTransport() {
    var p = new Promise();
    _transportClosedItself = false;
    CleanupTransport(false);
    _transport = _transportFactory();
    _transport.Closed += reason => {
        _transportClosedItself = true;
        p.TryFail(new InvalidOperationException(
            $"Connection closed: {reason ?? "(unknown)"}"));
    };
    _transport.Connected += () => p.TryComplete();
    SetState(HeTuSessionState.Connecting);
    _transport.Connect();
    return p.Future;
}

private Future RunBootstrap() {
    if (_closed) return Future.Completed;
    SetState(HeTuSessionState.Bootstrapping);
    return _bootstrap == null ? Future.Completed : _bootstrap(_transport);
}

private Future RestoreAllSubscriptions() {
    if (_closed) return Future.Completed;
    SetState(HeTuSessionState.RestoringSubscriptions);
    var head = Future.Completed;
    foreach (var sub in _subscriptions.Values.ToArray())
        head = head.Then(() => RestoreOne(sub));
    return head;
}
```

变化要点：

- **失败合一**：原 `HandleTransportClosed` / `HandleRecoverableFailure` 重复路径
  收敛成 `HandleSessionFailure(Exception)`，由 `.Catch(...)` 调用；区分"对端关 vs
  本端关"用 `_transportClosedItself` 标志位。
- **递归消除**：`RestoreSubscriptionAt` 的同步递归改为 `head = head.Then(...)` fold；
  栈深恒为 1。
- **`HeTuSessionBootstrap` delegate 重塑**：`(tx, succeed, fail) → void` → `tx → Future`。
  Facade 适配。
- **取消保护**：每步入口 `if (_closed) return Future.Completed;`——与原代码 `if
  (_closed) return;` 哨兵等价。

## §3 PendingWatch 合并

`PendingRowWatch<T>` + `PendingRangeWatch<T>`（各 80 行）→ 统一 `PendingWatch<TSub>`：

```csharp
private sealed class PendingWatch<TSub> where TSub : BaseSubscription {
    private readonly string _key;                                  // 可空
    private readonly Func<IHeTuSessionTransport, Promise<TSub>, Action> _dispatch;
    private readonly List<(Action<TSub> ok, Action<Exception> fail)> _waiters = new();
    private bool _inFlight;

    public string Key => _key;
    public bool IsInFlight => _inFlight;
    public void AddWaiter(Action<TSub> ok, Action<Exception> fail);
    public void MarkRetryable() => _inFlight = false;

    public Future<TSub> Dispatch(IHeTuSessionTransport transport);
    public void Complete(TSub sub);
    public void Fail(Exception ex);
}
```

Row 与 Range 的差异通过传入的 `_dispatch` lambda 体现。

**保留的关键分支**（不要"统一" Row 和 Range 的 dedup 路径）：

- `WatchRange`：subId 由 `(component, index, left, right, limit, desc)` 完全决定，
  **总是**查 `_subscriptions` 和 `_pendingWatchesByKey`。
- `WatchRow`：仅当 `index == HeTuClientBase.IndexId` 时 subId 可本地预测；其他
  index（如 `email`）必须等服务器返回 rowId，**仅此分支**才查缓存。其他情况
  传 `null` key 走纯排队路径。`AddPendingWatch` / `RemovePendingWatch` 对 null
  key 已经短路 `_pendingWatchesByKey` 写入。

## §4 WaitForReady（Base 新增）

Facade Connect 的"等 Ready / Faulted / timeout"逻辑下沉到 Base：

```csharp
public Future WaitForReady(TimeSpan timeout) {
    if (_closed)
        return Future.Failed(new ObjectDisposedException(nameof(HeTuSessionClientBase)));
    if (State == HeTuSessionState.Ready) return Future.Completed;
    if (State == HeTuSessionState.Faulted)
        return Future.Failed(_lastFault ?? new InvalidOperationException("..."));

    var p = new Promise();
    Action onReady = null;
    Action<Exception> onFaulted = null;
    Action<HeTuSessionState> onState = null;
    IDisposable timer = null;

    void Unwire() {
        Ready -= onReady;
        Faulted -= onFaulted;
        StateChanged -= onState;
        timer?.Dispose();
    }

    onReady   = ()  => { Unwire(); p.TryComplete(); };
    onFaulted = ex  => _lastFault = ex;
    onState   = st  => {
        if (st == HeTuSessionState.Faulted) {
            Unwire();
            p.TryFail(_lastFault ?? new InvalidOperationException("..."));
        } else if (st == HeTuSessionState.Stopped) {
            Unwire();
            p.TryFail(new OperationCanceledException("Session closed."));
        }
    };

    Ready        += onReady;
    Faulted      += onFaulted;
    StateChanged += onState;

    if (timeout > TimeSpan.Zero) {
        timer = _scheduler.Schedule(timeout, () => {
            if (HasBeenReady) return;
            Unwire();
            Close();
            var msg = _lastFault != null
                ? $"Connect timed out after {timeout.TotalSeconds:F0}s: {_lastFault.Message}"
                : $"Connect timed out after {timeout.TotalSeconds:F0}s.";
            p.TryFail(new TimeoutException(msg, _lastFault));
        });
    }
    return p.Future;
}
```

新增字段 `private Exception _lastFault;` 替代原 Facade closure 里的 `capturedFault`。

## §5 Facade 缩减

```csharp
public Awaitable Connect(string url, string authKey = null, ...) {
    if (_core != null
        && _core.State != HeTuSessionState.Stopped
        && _core.State != HeTuSessionState.Faulted)
        throw new InvalidOperationException("HeTuSessionClient 已经在运行中...");

    var client = HeTuClient.Instance;
    _core = new HeTuSessionClientBase(
        () => new UnityHeTuSessionTransport(client, url, authKey),
        new UnityHeTuSessionScheduler(),
        bootstrap == null ? null : tx => AwaitableToFuture(bootstrap(client)),
        reconnectDelay ?? DefaultReconnectDelay,
        maxReconnectDelay ?? DefaultMaxReconnectDelay,
        maxReconnectAttempts);
    WireFacadeEvents(_core);
    _core.Start();
    return FutureToAwaitable(_core.WaitForReady(connectTimeout ?? DefaultConnectTimeout));
}
```

**整段消失**：`HandleConnectTimeout`（41 行）、`AwaitConnectAsync`（24 行）、
`ScheduleConnectTimeout` + `RunConnectTimeoutAsync`（双 `#if` ~20 行）、`ConnectCore`
里 closure 捕获 fault + 三个 event handler 拼装（~30 行）。

**新增桥接 helper**（单点）：

```csharp
#if UNITY_6000_0_OR_NEWER
private static Awaitable FutureToAwaitable(Future f);
private static Future AwaitableToFuture(Awaitable a);
#else
private static UniTask FutureToUniTask(Future f);
private static Future UniTaskToFuture(UniTask t);
#endif
```

## §6 重连循环

`HandleSessionFailure` 决策树**与现有语义完全一致**：

1. 区分对端关 vs 本端关，调 `MarkConnectionLost(closeTransport)`
2. 触发 `Faulted` 事件，捕获 `_lastFault`
3. 若用户在 Faulted 回调里 Close 了，走清理路径
4. `!_hasBeenReady` → `EnterFaulted`（首次 Ready 前不重试）
5. `ExhaustedRetries()` → `EnterFaulted`
6. 否则 `ScheduleReconnect`，`_scheduler` 到点重新调 `RunSessionLifecycle()`

`MarkConnectionLost` 简化：直接遍历 `_inFlightCalls` 的 `Promise<JsonObject>` 全部
`TryFail(new CallOutcomeUnknownException(...))`；后到的 transport callback 的
`TryComplete` 自动 no-op。

## §7 测试影响

**EditMode（`Tests/Editor/SessionClientBaseTest.cs`）**：

- `FakeTransport` / `FakeScheduler` 接口不变；26 个现有 `[Test]` 逻辑不改。
- 新增用例：
  - `Future_Then_Synchronously_Invokes_When_Already_Completed`
  - `Future_TryComplete_Twice_NoOp`
  - `Future_Catch_Inside_Then_Chain`
  - `WaitForReady_Completes_When_Already_Ready`
  - `WaitForReady_Fails_With_Captured_Fault`
  - `WaitForReady_Stopped_Yields_OperationCanceled`
  - `WaitForReady_TimeoutFires_TransportNeverConnects`
  - `WaitForReady_TimeoutIgnored_AfterReady`

**PlayMode（`Tests/HeTu/SessionClientFacadeTest.cs`）**：

- 通过 `internal HeTuSessionClient(HeTuSessionClientBase core)` 与 `Connect(TimeSpan)`
  进入；行为不变。

**`HeTuClientTest.cs`**（PlayMode + 真实 server）：完全不动。

## 关键不变量回归

| 用例 | 原行为 | 新行为 |
|---|---|---|
| `SentCallThenDisconnect_FailsAsUnknownOutcome_WithoutRetry` | 在飞调用收 `CallOutcomeUnknownException`，不重发 | `_inFlightCalls.TryFail(...)` + 队列不复活 |
| Bootstrap fails before Ready → Faulted | `!_hasBeenReady` 直接终态 | 同 `HandleSessionFailure` 分支 4 |
| `Reconnect_AfterReady_ExpBackoff` | Ready 后断线按 1/2/4/8/16/30s | `ScheduleReconnect` 数学不动 |
| `MaxAttempts_Exhausted → Faulted` | 连续失败上限转终态 | `ExhaustedRetries` 逻辑不动 |
| `Connect_TimesOut_When_TransportNeverConnects` | timeout 触发 Close + TimeoutException | `WaitForReady` 内部 scheduler 触发 |

## 估算

| 部分 | Before | After |
|---|---|---|
| `Future.cs` | — | ~180 |
| `SessionClientBase.cs` | 933 | ~600 |
| `SessionClientBase.Pending.cs` | — | ~100 |
| `SessionClient.cs` | 556 | ~280 |

## TDD 顺序

1. RED→GREEN：`Future` / `Promise`（含 `Future<T>` / `Promise<T>`）单元
2. RED→GREEN：`HeTuSessionClientBase.WaitForReady`（保留现有 callback API 不动，
   先把 WaitForReady 加进来）
3. 灰盒迁移 Base 内部状态流到 Future 链；每步跑现有 26 个用例保绿
4. 合并 `PendingRowWatch` / `PendingRangeWatch` → `PendingWatch<TSub>`；保绿
5. Facade 切换到 `WaitForReady`；删除老 Connect 拼装；保绿
