# System 速率限制 / 通用调用前守卫（Guard）机制 — 设计稿

- 日期：2026-06-08
- 状态：已与用户确认设计，待写实现计划
- 影响范围：服务端 `hetu/endpoint/`、`hetu/system/`、`hetu/server/receiver.py`；客户端 `ClientSDK/unity`（同时覆盖 headless `ClientSDK/csharp`，因其编译共享 `HeTu/*.cs`）

## 1. 背景与目标

需要给 System 增加一个通用的速率限制能力（类似 FastAPI 的 slowapi 装饰器），用于
限制客户端 `callSystem` 的频率：

1. 客户端快速连发两个 System 请求时，第二个返回一个通用的"被拒绝"信号。
2. 客户端 SDK 能自动察觉该信号，并通过一个**通用回调**对外暴露。
3. 用户在该回调里自行决定处理（弹对话框 / pop-up 等）；框架只给信号，不替用户决定 UI。

不通过 IP 判断（长连接场景），像权限检查一样限制 `callSystem` 行为即可。

### 关键架构发现（决定了实现路线）

slowapi 那种"装饰器包住函数、在函数入口拦截"的模型在河图里对客户端调用**天然失效**：

- 客户端 RPC 调用 System 时，引擎是从注册表取 `sys.func`（**原始函数**）直接 dispatch
  的（`hetu/system/caller.py` 中 `await sys.func(context, *args)`），**不会经过外层叠加
  的装饰器包装**。
- 因此无论装饰器放哪都不行：
  - 放 `@define_system` **下面** → 包装改了 `func.__code__`，破坏 `define_system` 读
    `co_argcount` 的参数个数校验（用户最初撞到的冲突）。
  - 放 `@define_system` **上面** → `define_system` 注册的是原始 `func`，客户端调用
    dispatch 到原始 `func`，外层装饰器被完全绕过、根本不执行。

**正确模型**：装饰器/参数只负责**附加元数据**，由引擎网关（`EndpointExecutor.execute`）
执行——这正是 `permission` 现在的工作方式（`permission` 不是包装，而是"元数据 + 网关判断"）。

### 已确认的设计决策

- **API 形态**：通用「调用前守卫（guard）」机制。`@rate_limit` 是第一个内置 guard；
  `@guard(check)` 让用户挂任意自定义守卫；`ClientReject` 是通用软拒绝原语。装饰器都是
  **标记式**（marker，只附加元数据、返回原函数）。
- **客户端行为**：被限流时触发**全局事件** `OnCallRejected(systemName, code)`（用户的通用
  回调挂这里），**同时**该次 `await CallSystem` 抛 `HeTuCallRejectedException`（发起方也能
  局部 try/catch）。
- **限流粒度**：每「连接 × system」，计数存在该连接的 `Context.guard_state`（纯内存），
  多进程零额外存储；重连即重置（对防连点足够）。
- **算法**：固定窗口计数，与现有 `ConnectionFloodChecker` 一致；命中拒绝当次、**不开事务、
  不断连接**（区别于 flood checker 命中即断连）。
- **作用面**：`define_system` 自动生成的 endpoint 与手写 `@define_endpoint` 都支持
  （guard 元数据统一挂到 `EndpointDefine` 上执行）。

## 2. 服务端设计

### 2.1 新原语（放在 `hetu/endpoint/`）

System 是 Endpoint 的特化，原语挂在 endpoint 层对两者通用。

```python
class ClientReject(Exception):
    """guard 抛出它 = 软拒绝当次调用：不开事务、不断连接、回 rej 帧给客户端。
    code 用于客户端通用回调里区分原因（如 'RATE_LIMITED'）；reason 可选、仅放进客户端异常对象。
    """
    def __init__(self, code: str = "REJECTED", reason: str | None = None):
        self.code = code
        self.reason = reason
        super().__init__(reason or code)


def guard(check):
    """通用自定义守卫装饰器（标记式）。
    check 签名 (ctx, *args) -> None，可同步或 async；想拒绝就 raise ClientReject(...)。
    用法：放在 @define_system / @define_endpoint 下面。
    """


def rate_limit(times: int, per: float):
    """内置 guard：每「连接 × system」固定窗口限流。
    per 秒窗口内最多允许 times 次，超出 raise ClientReject('RATE_LIMITED')。
    状态存 ctx.guard_state，以本 guard 的唯一 key 索引 {window_start, count}。
    """
```

`hetu/__init__.py` 导出 `rate_limit`、`guard`、`ClientReject`。

### 2.2 标记装饰器语义

- `@rate_limit(...)` / `@guard(...)` 把一个 guard 可调用对象记到 `func.__hetu_guards__`
  （list）并返回原函数（不改 `__code__`）。多个 marker 叠加时按**源码自上而下**的顺序
  执行；由于装饰器自底向上应用，实现上 marker 用 `list.insert(0, g)`（而非 append）写入，
  使最终列表顺序 == 源码自上而下顺序。
- **防呆**：marker 必须放在 `@define_system` / `@define_endpoint` **下面**。若误放上面，
  会作用到定义产物（wrapper）上而静默失效——因此 `define_system` 返回的
  `warp_direct_system_call` 标记 `__hetu_defined__ = True`，marker 检测到目标已带此标记
  时 **raise** 一条清晰错误，提示调换装饰器顺序。

### 2.3 元数据收集与执行路径统一

- `EndpointDefine` 增加字段 `guards: list = []`（默认空）。
- `define_endpoint`：从 `func.__hetu_guards__` 收集，存入 `EndpointDefine.guards`。
- `define_system`：从 `func.__hetu_guards__` 收集，存入 `SystemDefine`（同样新增 `guards`
  字段）；`SystemClusters.build_endpoints` 在创建自动 endpoint 时，把 `SystemDefine.guards`
  拷贝到对应 `EndpointDefine.guards`。
- 结果：手写 endpoint 与 system 自动 endpoint **走同一条 guard 执行路径**。

### 2.4 执行点（`EndpointExecutor.execute`）

执行顺序（在 `hetu/endpoint/executor.py`）：

1. `execute_check`（权限 + 参数个数）——不通过仍返回 `(False, None)` → 断连（原行为不变）。
2. alive 检查（`is_illegal`）——不通过返回 `(False, None)` → 断连（原行为不变）。
3. **新增：依次 `await guard(ctx, *args)`**（guard 可同步可 async，统一 `await` 兼容）。
   - 捕获 `ClientReject` → 返回 `(True, RejectResponse(code, reason))`。
     - `ok=True` 表示**不断连**；由返回对象类型驱动后续 rej 帧。
   - 其他异常按原有逻辑（视为内部错误，记录并断连）。
4. `execute_`（开事务、跑逻辑）——原行为不变。

新增内部响应类型（`hetu/endpoint/response.py`）：

```python
class RejectResponse(EndpointResponse):
    """软拒绝响应，承载 code/reason，由 receiver 转成 rej 帧。不发给普通 rsp 路径。"""
    def __init__(self, code: str, reason: str | None = None):
        self.code = code
        self.reason = reason
```

`execute` 的返回类型从 `tuple[bool, ResponseToClient | None]` 放宽为
`tuple[bool, ResponseToClient | RejectResponse | None]`。

### 2.5 每连接 guard 状态

- `Context`（`hetu/endpoint/context.py`）增加字段
  `guard_state: dict[str, Any] = field(default_factory=dict)`。每连接一个、纯内存。
- `rate_limit` guard 在装饰时生成一个唯一 key（保证同一函数多次 `@rate_limit` 或不同
  函数互不串扰），运行时读写 `ctx.guard_state[key]`：

  ```
  st = ctx.guard_state.get(key)
  now = time.time()
  if st is None or now - st["window_start"] > per:
      ctx.guard_state[key] = {"window_start": now, "count": 1}
      return                      # 放行
  st["count"] += 1
  if st["count"] > times:
      raise ClientReject("RATE_LIMITED")
  ```

  （固定窗口；与 `ConnectionFloodChecker` 算法一致，存在窗口边界突发的已知局限，可接受。）

## 3. 线协议：新增 `rej` 帧

- `hetu/server/receiver.py` 的 `rpc()`：当 `execute()` 返回的 `res` 是 `RejectResponse` 时，
  `await push_queue.put(["rej", system_name, code])`（可带可选 reason），**不关连接**；
  普通失败（`ok=False`）仍走原有断连逻辑。
- `rej` 与 `rsp` 一样是一次 round-trip 应答，走同一 FIFO 应答顺序，因此能精准对应到被拒的
  那次请求。

帧格式：`["rej", <system_name:str>, <code:str>]`（reason 暂不入帧，仅客户端异常对象用；
若后续需要可扩展为第 4 元素）。

## 4. 客户端设计（C# / Unity + headless + 会话层）

`ClientSDK/csharp` 编译共享 `ClientSDK/unity/.../HeTu/*.cs`，故核心改动落在共享文件。
`ClientSDK/js` 仅有 `todo.md`（未实现），无需改动。

### 4.1 `HeTuClientBase`（共享，`HeTu/ClientBase.cs`）

- 新增全局事件：`public event Action<string, string> OnCallRejected;`（systemName, code）。
- `OnReceived` 的 `switch` 增加 `case "rej":`，与 `rsp`/`sub` 一样调用
  `ResponseQueue.CompleteNext(structuredMsg)`（消费对应 pending 回调，维持 FIFO 对齐）。
- `CallSystemSync` 内部回调契约扩展：由 `(JsonObject, bool cancel)` 改为携带结果枚举
  `internal enum CallOutcome { Completed, Canceled, Rejected }`（外加 reject reason/code
  字符串）。`SendRequest` 回调里检测 `response[0] == "rej"`：
  1. `OnCallRejected?.Invoke(systemName, code);`
  2. 以 `Rejected` + code 通知上层 `onResponse`。

### 4.2 新异常（共享）

```csharp
public sealed class HeTuCallRejectedException : Exception
{
    public string SystemName { get; }
    public string Code { get; }   // 如 "RATE_LIMITED"
    // 构造与消息略
}
```

### 4.3 三处 await 包装各映射一次 `Rejected → 抛异常`

- `UnityClient.CallSystem`（`HeTu/UnityClient.cs`，Awaitable/UniTask 双版本）：
  `Completed→SetResult`、`Canceled→SetCanceled`、`Rejected→SetException(HeTuCallRejectedException)`。
- `HeadlessHeTuClient.CallSystem`（`ClientSDK/csharp/.../HeadlessHeTuClient.cs`）：同上
  （`tcs.TrySetException(...)`）。
- 会话层 `HeTuSessionClientBase`（`HeTu/SessionClientBase.cs`）：把 `Rejected` 视为
  **确定性失败**，以 `HeTuCallRejectedException` 失败对应 pending call，并**不自动重试**
  （区别于连接中断的 `CallOutcomeUnknownException`）。`IHeTuSessionConnection.CallSystem`
  / `UnityHeTuSessionConnection` 的回调签名随 `CallSystemSync` 契约同步调整（`SessionClient.cs`
  里目前只是转发 `onResponse`，改动点集中）。

## 5. 测试计划

### 5.1 服务端（pytest，`tests/`）

- `@rate_limit(times=1, per=...)`：窗口内第二次调用被拒（返回 rej，**连接保持**），窗口
  过后恢复放行。
- `@rate_limit` 命中时**不开事务**：用引用组件验证被拒调用没有任何写入。
- `@guard(custom)`：自定义守卫 `raise ClientReject` 生效；不拒绝时正常执行。
- marker 放错位置（放在 `@define_system` 上面）→ 定义期 raise 清晰错误。
- `@define_endpoint` 上 `@rate_limit` / `@guard` 同样生效（验证执行路径统一）。
- guard 粒度：不同 system / 不同连接（Context）计数互不串扰。

### 5.2 客户端 EditMode（无需服务器，`Tests/Editor/`，`ConnectionSemanticsTest` 风格）

- 喂入 `["rej", sys, code]` 帧：触发 `OnCallRejected(sys, code)`，且对应 `await CallSystem`
  抛 `HeTuCallRejectedException`（含 SystemName/Code）。
- FIFO 不错位：rej 帧只消费它对应的那次 pending 回调，后续 rsp 仍正确对齐。
- 会话层：收到 Rejected 不触发自动重试。

### 5.3 客户端集成 / PlayMode（有网 / Unity 机器）

- 真服务器跑一个带 `@rate_limit` 的 system，连点验证软拒绝 + 通用回调（沿用 headless
  客户端那条待验证流程；与项目记忆中的集成/Unity 回归一起做）。

## 6. 取舍与边界（YAGNI）

- 限流**每连接**、重连重置——对防连点足够；跨用户/跨进程的全局配额需要 Redis，本期不做。
- 不做全局配置默认值（如 `CLIENT_SEND_LIMITS` 那种）；限流参数写在装饰器里。
- 不处理 `SystemLocalCallbacks` 乐观预测的回滚（被拒后客户端预测的本地效果不自动撤销）；
  本期只给信号，回滚留作未来。
- `OnCallRejected` 事件签名保持轻量 `(systemName, code)`；`reason` 细节只放进异常对象。

## 7. 主要改动文件清单

服务端：
- `hetu/endpoint/__init__.py` / 新文件：`ClientReject`、`guard`、`rate_limit`、marker 收集
- `hetu/endpoint/response.py`：`RejectResponse`
- `hetu/endpoint/definer.py`：`EndpointDefine.guards`、`define_endpoint` 收集、防呆
- `hetu/endpoint/executor.py`：`execute` 中 guard 执行与 `ClientReject` 捕获
- `hetu/endpoint/context.py`：`Context.guard_state`
- `hetu/system/definer.py`：`SystemDefine.guards`、`define_system` 收集、防呆、`build_endpoints` 拷贝
- `hetu/server/receiver.py`：`rpc()` 输出 `rej` 帧
- `hetu/__init__.py`：导出新公共 API

客户端（共享 `ClientSDK/unity/.../HeTu/`，headless 自动覆盖）：
- `ClientBase.cs`：`OnCallRejected`、`rej` 分支、`CallSystemSync` 契约、`CallOutcome`
- 新增 `HeTuCallRejectedException`
- `UnityClient.cs`、`HeadlessHeTuClient.cs`、`SessionClientBase.cs`（+ `SessionClient.cs`
  转发处）：`Rejected` 映射为抛异常 / 失败不重试
