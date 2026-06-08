# System 速率限制 / 通用调用前守卫（Guard）机制 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 System/Endpoint 增加一个通用「调用前守卫(guard)」机制，内置 `@rate_limit` 限流；客户端被拒时触发全局 `OnCallRejected` 回调并让 `await CallSystem` 抛 `HeTuCallRejectedException`，不断连接。

**Architecture:** 标记式装饰器只往函数附加 guard 元数据（不包装、不改 `__code__`），由 `EndpointExecutor.execute` 网关在权限/参数校验之后、开事务之前执行；guard `raise ClientReject` → 返回 `RejectResponse` → `receiver.rpc` 发 `["rej", name, code]` 帧、连接保持。客户端 `CallSystemSync` 检测 rej 帧 → 触发全局事件 + 以 `Rejected` 结果让上层抛异常。

**Tech Stack:** Python 3.14 / pytest（后端测试需 `HETU_TEST_BACKENDS=redis` + Docker）；C# (.NET 8 headless `HeTu.Client.Tests` 可 `dotnet test`，Unity EditMode 镜像测试)。

设计依据：`docs/superpowers/specs/2026-06-08-system-rate-limit-guards-design.md`。

---

## 文件结构

新增：
- `hetu/endpoint/guard.py` — `ClientReject` 异常、`guard()` / `rate_limit()` 标记装饰器、元数据收集辅助
- `tests/test_system_guards.py` — 服务端测试（纯单元 + executor 级）
- `ClientSDK/csharp/HeTu.Client.Tests/CallRejectTests.cs` — headless 可运行的 rej 帧测试
- `ClientSDK/unity/cn.hetudb.clientsdk/Tests/Editor/CallRejectTest.cs` — Unity EditMode 镜像测试

修改（服务端）：
- `hetu/endpoint/response.py` — `RejectResponse`
- `hetu/endpoint/context.py` — `Context.guard_state` 字段
- `hetu/endpoint/definer.py` — `EndpointDefine.guards`、`define_endpoint` 收集 guard + 防呆
- `hetu/endpoint/executor.py` — `execute` 跑 guard、捕获 `ClientReject`
- `hetu/system/definer.py` — `SystemDefine.guards`、`define_system` 收集 + 防呆、`add()` 透传、`build_endpoints` 拷贝
- `hetu/server/receiver.py` — `rpc()` 输出 `rej` 帧
- `hetu/endpoint/__init__.py` + `hetu/__init__.py` — 导出 `rate_limit` / `guard` / `ClientReject`
- `tests/app.py` — 新增 `rate_limited_add` / `guarded_add` 系统供测试

修改（客户端，共享源码同时覆盖 unity + headless）：
- `ClientSDK/unity/cn.hetudb.clientsdk/HeTu/ClientBase.cs` — `CallOutcome` 枚举、`HeTuCallRejectedException`、`OnCallRejected` 事件、`rej` 分支、`CallSystemSync` 契约
- `HeTu/UnityClient.cs`、`ClientSDK/csharp/HeTu.Client/HeadlessHeTuClient.cs`、`HeTu/SessionClient.cs`、`HeTu/SessionClientBase.cs` — 适配新契约
- 三处测试 fake/harness：`Tests/Editor/ConnectionSemanticsTest.cs`、`Tests/Editor/SessionClientBaseTest.cs`、`Tests/HeTu/SessionClientFacadeTest.cs`

---

## Phase A — 服务端

### Task 1: `ClientReject` 异常 + `RejectResponse`

**Files:**
- Create: `hetu/endpoint/guard.py`
- Modify: `hetu/endpoint/response.py`
- Test: `tests/test_system_guards.py`

- [ ] **Step 1: 写失败测试**

Create `tests/test_system_guards.py`:

```python
import pytest

import hetu
from hetu.endpoint.guard import ClientReject
from hetu.endpoint.response import RejectResponse


def test_client_reject_carries_code_and_reason():
    e = ClientReject("RATE_LIMITED", "太快了")
    assert e.code == "RATE_LIMITED"
    assert e.reason == "太快了"
    assert isinstance(e, Exception)


def test_client_reject_defaults():
    e = ClientReject()
    assert e.code == "REJECTED"
    assert e.reason is None


def test_reject_response_carries_code():
    r = RejectResponse("RATE_LIMITED")
    assert r.code == "RATE_LIMITED"
    assert r.reason is None
```

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run pytest tests/test_system_guards.py -k "client_reject or reject_response" -v`
Expected: FAIL（`ModuleNotFoundError: hetu.endpoint.guard` / `ImportError: RejectResponse`）

- [ ] **Step 3: 实现 `ClientReject`**

Create `hetu/endpoint/guard.py`:

```python
"""通用调用前守卫(guard)机制：标记式装饰器只附加元数据，由网关执行。"""

import time

from ..i18n import _

# 函数上存放 guard 列表的属性名
_GUARD_ATTR = "__hetu_guards__"
# 定义产物(define_system/define_endpoint 的返回)标记，用于防呆装饰器顺序
_DEFINED_ATTR = "__hetu_defined__"


class ClientReject(Exception):
    """guard 抛出它 = 软拒绝当次调用：不开事务、不断连接、回 rej 帧给客户端。

    code 用于客户端通用回调区分原因（如 ``RATE_LIMITED``）；
    reason 可选，仅放进客户端异常对象。
    """

    def __init__(self, code: str = "REJECTED", reason: str | None = None):
        self.code = code
        self.reason = reason
        super().__init__(reason or code)
```

- [ ] **Step 4: 实现 `RejectResponse`**

Modify `hetu/endpoint/response.py`，在 `ResponseToClient` 之后追加：

```python
class RejectResponse(EndpointResponse):
    """软拒绝响应，承载 code/reason，由 receiver 转成 rej 帧；不走普通 rsp 路径。"""

    def __init__(self, code: str, reason: str | None = None):
        self.code = code
        self.reason = reason

    def __repr__(self):
        return f"RejectResponse({self.code!r})"
```

- [ ] **Step 5: 跑测试确认通过**

Run: `uv run pytest tests/test_system_guards.py -k "client_reject or reject_response" -v`
Expected: PASS（3 passed）

- [ ] **Step 6: 提交**

```bash
git add hetu/endpoint/guard.py hetu/endpoint/response.py tests/test_system_guards.py
git commit -m "ENH: add ClientReject and RejectResponse primitives"
```

---

### Task 2: `guard()` / `rate_limit()` 标记装饰器

**Files:**
- Modify: `hetu/endpoint/guard.py`
- Test: `tests/test_system_guards.py`

- [ ] **Step 1: 写失败测试**

追加到 `tests/test_system_guards.py`:

```python
def test_guard_marker_attaches_in_source_order():
    from hetu.endpoint.guard import guard

    async def chk_a(ctx, x):
        pass

    async def chk_b(ctx, x):
        pass

    @guard(chk_a)
    @guard(chk_b)
    async def f(ctx, x):
        pass

    # 源码自上而下：chk_a 在前
    assert getattr(f, "__hetu_guards__") == [chk_a, chk_b]


async def test_rate_limit_guard_logic_window():
    from hetu.endpoint.guard import rate_limit

    @rate_limit(times=2, per=100)
    async def f(ctx, x):
        pass

    g = f.__hetu_guards__[0]

    class FakeCtx:
        guard_state: dict = {}

    ctx = FakeCtx()
    await g(ctx, 1)  # 第1次放行
    await g(ctx, 1)  # 第2次放行
    with pytest.raises(ClientReject) as ei:
        await g(ctx, 1)  # 第3次拒绝
    assert ei.value.code == "RATE_LIMITED"


async def test_rate_limit_window_resets(monkeypatch):
    import hetu.endpoint.guard as guard_mod
    from hetu.endpoint.guard import rate_limit

    base = 1000.0
    monkeypatch.setattr(guard_mod.time, "time", lambda: base)

    @rate_limit(times=1, per=10)
    async def f(ctx, x):
        pass

    g = f.__hetu_guards__[0]

    class FakeCtx:
        guard_state: dict = {}

    ctx = FakeCtx()
    await g(ctx, 1)  # 放行
    with pytest.raises(ClientReject):
        await g(ctx, 1)  # 同窗口拒绝

    monkeypatch.setattr(guard_mod.time, "time", lambda: base + 11)
    await g(ctx, 1)  # 窗口过后重置，放行
```

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run pytest tests/test_system_guards.py -k "marker_attaches or rate_limit_guard or window_resets" -v`
Expected: FAIL（`ImportError: cannot import name 'guard'` / `'rate_limit'`）

- [ ] **Step 3: 实现装饰器**

追加到 `hetu/endpoint/guard.py`:

```python
def _attach_guard(func, g):
    """把 guard 可调用对象挂到 func 上。markers 必须在 define_* 下面。"""
    if getattr(func, _DEFINED_ATTR, False):
        raise TypeError(
            _("@rate_limit/@guard 必须放在 @define_system/@define_endpoint 的下面")
        )
    guards = func.__dict__.setdefault(_GUARD_ATTR, [])
    # 装饰器自底向上应用；insert(0) 让最终顺序 == 源码自上而下
    guards.insert(0, g)
    return func


def collect_guards(func) -> list:
    """供 define_system/define_endpoint 读取已附加的 guard 列表（拷贝一份）。"""
    return list(getattr(func, _GUARD_ATTR, []))


def mark_defined(obj):
    """供 define_system/define_endpoint 标记其产物，配合 _attach_guard 防呆。"""
    obj.__dict__[_DEFINED_ATTR] = True
    return obj


def guard(check):
    """通用自定义守卫装饰器（标记式）。

    check 签名 ``(ctx, *args) -> None``，可同步或 async；想拒绝就 raise ClientReject(...)。
    用法：放在 @define_system / @define_endpoint 下面。
    """

    def deco(func):
        return _attach_guard(func, check)

    return deco


def rate_limit(times: int, per: float):
    """内置 guard：每「连接 × system」固定窗口限流。

    per 秒窗口内最多允许 times 次，超出 raise ClientReject('RATE_LIMITED')。
    状态存 ctx.guard_state，以本装饰应用的唯一 key 索引 [window_start, count]。
    """

    def deco(func):
        key = f"ratelimit:{id(func)}:{times}:{per}"

        async def _rate_limit_guard(ctx, *args):
            now = time.time()
            st = ctx.guard_state.get(key)
            if st is None or now - st[0] > per:
                ctx.guard_state[key] = [now, 1]
                return
            st[1] += 1
            if st[1] > times:
                raise ClientReject("RATE_LIMITED")

        return _attach_guard(func, _rate_limit_guard)

    return deco
```

- [ ] **Step 4: 跑测试确认通过**

Run: `uv run pytest tests/test_system_guards.py -k "marker_attaches or rate_limit_guard or window_resets" -v`
Expected: PASS（3 passed）

- [ ] **Step 5: 提交**

```bash
git add hetu/endpoint/guard.py tests/test_system_guards.py
git commit -m "ENH: add guard/rate_limit marker decorators"
```

---

### Task 3: `Context.guard_state` 字段

**Files:**
- Modify: `hetu/endpoint/context.py:55`（在 `client_limits` 等 field 附近）
- Test: `tests/test_system_guards.py`

- [ ] **Step 1: 写失败测试**

追加到 `tests/test_system_guards.py`:

```python
def test_context_has_guard_state():
    from hetu.endpoint.context import Context

    ctx = Context(
        caller=0, connection_id=0, address="x", group="", user_data={},
        timestamp=0, request=None, systems=None,
    )
    assert ctx.guard_state == {}
    ctx.guard_state["k"] = 1
    # 不同实例不共享
    ctx2 = Context(
        caller=0, connection_id=0, address="x", group="", user_data={},
        timestamp=0, request=None, systems=None,
    )
    assert ctx2.guard_state == {}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run pytest tests/test_system_guards.py -k "guard_state" -v`
Expected: FAIL（`AttributeError: 'Context' object has no attribute 'guard_state'`）

- [ ] **Step 3: 加字段**

Modify `hetu/endpoint/context.py`，在 `client_limits` field 之前（紧接 `systems` field 之后）追加：

```python
    guard_state: dict[str, Any] = field(default_factory=dict)
    """每连接的 guard 私有状态（如 rate_limit 的窗口计数）；纯内存、不跨连接。"""
```

（`field` 与 `Any` 已在文件顶部 import。）

- [ ] **Step 4: 跑测试确认通过**

Run: `uv run pytest tests/test_system_guards.py -k "guard_state" -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add hetu/endpoint/context.py tests/test_system_guards.py
git commit -m "ENH: add Context.guard_state for per-connection guard state"
```

---

### Task 4: `define_endpoint` 收集 guard + `EndpointDefine.guards`

**Files:**
- Modify: `hetu/endpoint/definer.py`（`EndpointDefine` dataclass、`EndpointDefines.add`、`define_endpoint.warp`）
- Test: `tests/test_system_guards.py`

- [ ] **Step 1: 写失败测试**

追加到 `tests/test_system_guards.py`:

```python
def test_define_endpoint_collects_guards():
    from hetu.endpoint.definer import EndpointDefines

    async def chk(ctx, x):
        pass

    @hetu.define_endpoint(
        namespace="t_guard", permission=hetu.Permission.EVERYBODY, force=True
    )
    @hetu.guard(chk)
    async def my_ep(ctx, x):
        pass

    ep = EndpointDefines().get_endpoint("t_guard", "my_ep")
    assert ep is not None
    assert ep.guards == [chk]


def test_marker_above_definer_raises():
    with pytest.raises(TypeError, match="下面"):

        @hetu.rate_limit(times=1, per=1)
        @hetu.define_endpoint(
            namespace="t_guard", permission=hetu.Permission.EVERYBODY, force=True
        )
        async def bad(ctx, x):
            pass
```

注意：测试用到 `hetu.guard` / `hetu.rate_limit` 顶层导出，先在本 Task 不可用——本 Task 先用
`from hetu.endpoint.guard import guard, rate_limit` 替代顶层引用，顶层导出在 Task 8 加。
请把上面两测试里的 `hetu.guard` / `hetu.rate_limit` 暂写成局部 import 的 `guard` / `rate_limit`。

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run pytest tests/test_system_guards.py -k "define_endpoint_collects or marker_above" -v`
Expected: FAIL（`EndpointDefine` 无 `guards` 字段 / 顺序未报错）

- [ ] **Step 3: 给 `EndpointDefine` 加字段**

Modify `hetu/endpoint/definer.py`，`EndpointDefine` dataclass（约 23-28 行）改为：

```python
@dataclass
class EndpointDefine:
    func: AsyncHandler
    permission: Permission
    arg_count: int  # 全部参数个数（含默认参数）
    defaults_count: int  # 默认参数个数
    guards: list = field(default_factory=list)  # 调用前守卫链
```

在文件顶部 import 增加 `field`：

```python
from dataclasses import dataclass, field
```

- [ ] **Step 4: `EndpointDefines.add` 接收 guards**

Modify `EndpointDefines.add`（约 56-85 行），签名加 `guards=None`，构造 `EndpointDefine` 时传入：

```python
    def add(
        self,
        namespace,
        func: AsyncHandler,
        force,
        permission,
        arg_count=None,
        defaults_count=None,
        guards=None,
    ):
        sub_map = self._endpoint_map.setdefault(namespace, dict())

        if not force:
            assert func.__name__ not in sub_map, _("Endpoint重复定义：") + func.__name__

        assert isinstance(func, FunctionType)
        if not arg_count:
            arg_count = func.__code__.co_argcount
        if not defaults_count:
            defaults_count = len(func.__defaults__) if func.__defaults__ else 0

        sub_map[func.__name__] = EndpointDefine(
            func=func,
            permission=permission,
            arg_count=arg_count,
            defaults_count=defaults_count,
            guards=list(guards) if guards else [],
        )

        if namespace == "global":
            self._global_endpoint_map[func.__name__] = sub_map[func.__name__]
```

- [ ] **Step 5: `define_endpoint.warp` 收集 guard 并标记产物**

Modify `define_endpoint` 内 `warp(func)`（约 159-186 行），在文件顶部加 import：

```python
from .guard import collect_guards, mark_defined
```

在 `warp` 末尾、`return func` 之前，把 `EndpointDefines().add(...)` 改为携带 guards，并标记产物：

```python
        guards = collect_guards(func)
        EndpointDefines().add(namespace, func, force, permission, guards=guards)

        mark_defined(func)
        return func
```

- [ ] **Step 6: 跑测试确认通过**

Run: `uv run pytest tests/test_system_guards.py -k "define_endpoint_collects or marker_above" -v`
Expected: PASS（2 passed）

- [ ] **Step 7: 提交**

```bash
git add hetu/endpoint/definer.py tests/test_system_guards.py
git commit -m "ENH: collect guards in define_endpoint, EndpointDefine.guards"
```

---

### Task 5: `define_system` 收集 guard + `build_endpoints` 拷贝

**Files:**
- Modify: `hetu/system/definer.py`（`SystemDefine` dataclass、`SystemClusters.add`、`build_endpoints`、`define_system.warp`）
- Test: `tests/test_system_guards.py`

- [ ] **Step 1: 写失败测试**

追加到 `tests/test_system_guards.py`（用模块级 test app；不需要后端/Docker）：

```python
def test_system_guards_copied_to_endpoint(mod_test_app):
    from hetu.endpoint.definer import EndpointDefines

    ep = EndpointDefines().get_endpoint("pytest", "rate_limited_add")
    assert ep is not None
    assert len(ep.guards) == 1

    ep2 = EndpointDefines().get_endpoint("pytest", "guarded_add")
    assert ep2 is not None
    assert len(ep2.guards) == 1
```

同时在 `tests/app.py` 末尾追加被测系统（放在 `RLSComp` 定义之后任意位置即可，文件已 import hetu）：

```python
@hetu.define_system(
    namespace="pytest", components=(RLSComp,), permission=hetu.Permission.USER
)
@hetu.rate_limit(times=1, per=100)
async def rate_limited_add(ctx: hetu.SystemContext, value):
    async with ctx.repo[RLSComp].upsert(owner=ctx.caller) as row:
        row.value += value
    return row.value


async def _reject_when_negative(ctx, value):
    if value < 0:
        raise hetu.ClientReject("NEGATIVE")


@hetu.define_system(
    namespace="pytest", components=(RLSComp,), permission=hetu.Permission.USER
)
@hetu.guard(_reject_when_negative)
async def guarded_add(ctx: hetu.SystemContext, value):
    async with ctx.repo[RLSComp].upsert(owner=ctx.caller) as row:
        row.value += value
    return row.value
```

注意：`tests/app.py` 用到 `hetu.rate_limit` / `hetu.guard` / `hetu.ClientReject` 顶层导出——
这些在 Task 8 加。本 Task 先在 `tests/app.py` 顶部加临时 import：
`from hetu.endpoint.guard import rate_limit, guard, ClientReject`，并把 `hetu.rate_limit` →
`rate_limit`、`hetu.guard` → `guard`、`hetu.ClientReject` → `ClientReject`。Task 8 完成后可
改回 `hetu.` 前缀（可选）。

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run pytest tests/test_system_guards.py -k "copied_to_endpoint" -v`
Expected: FAIL（`SystemDefine` 无 `guards` / endpoint.guards 为空）

- [ ] **Step 3: 给 `SystemDefine` 加字段**

Modify `hetu/system/definer.py`，`SystemDefine` dataclass（约 31-39 行）末尾加：

```python
@dataclass
class SystemDefine(EndpointDefine):
    components: set[type[BaseComponent]]
    full_components: set[type[BaseComponent]]
    depends: set[str]
    full_depends: set[str]
    max_retry: int
    cluster_id: int
```

`SystemDefine` 继承自 `EndpointDefine`，已带 `guards` 字段（Task 4 加），**无需重复声明**。
但 `SystemClusters.add` 构造 `SystemDefine` 时要显式传 `guards`（见 Step 4）。

- [ ] **Step 4: `SystemClusters.add` 接收并存 guards**

Modify `SystemClusters.add`（约 274-300 行），签名加 `guards=None`，构造时传入：

```python
    def add(
        self, namespace, func, components, force, permission, depends, max_retry,
        guards=None,
    ):
        sub_map = self._system_map.setdefault(namespace, dict())

        if not force:
            assert func.__name__ not in sub_map, _("System重复定义：") + func.__name__
        if components is None:
            components = set()

        arg_count = func.__code__.co_argcount
        defaults_count = len(func.__defaults__) if func.__defaults__ else 0

        sub_map[func.__name__] = SystemDefine(
            func=func,
            components=components,
            depends=depends,
            max_retry=max_retry,
            arg_count=arg_count,
            defaults_count=defaults_count,
            cluster_id=-1,
            permission=permission,
            full_components=set(),
            full_depends=set(),
            guards=list(guards) if guards else [],
        )

        if namespace == "global":
            self._global_system_map[func.__name__] = sub_map[func.__name__]
```

- [ ] **Step 5: `build_endpoints` 拷贝 guards 到 EndpointDefine**

Modify `SystemClusters.build_endpoints`（约 255-272 行），`EndpointDefines().add(...)` 调用加 `guards=sys_def.guards`：

```python
                EndpointDefines().add(
                    namespace,
                    func,
                    force=True,
                    permission=sys_def.permission,
                    arg_count=sys_def.arg_count,
                    defaults_count=sys_def.defaults_count,
                    guards=sys_def.guards,
                )
```

- [ ] **Step 6: `define_system.warp` 收集 guard、透传、标记产物**

Modify `hetu/system/definer.py`：文件顶部 import 加：

```python
from ..endpoint.guard import collect_guards, mark_defined
```

`define_system` 内 `warp(func)`：在调用 `SystemClusters().add(...)`（约 502 行）之前收集 guards 并透传：

```python
        guards = collect_guards(func)

        SystemClusters().add(
            namespace, func, _components, force, permission, depend_names, retry,
            guards=guards,
        )
```

并在返回前标记产物——找到 `return warp_direct_system_call`（约 525 行），改为：

```python
        mark_defined(warp_direct_system_call)
        return warp_direct_system_call
```

- [ ] **Step 7: 跑测试确认通过**

Run: `uv run pytest tests/test_system_guards.py -k "copied_to_endpoint" -v`
Expected: PASS

- [ ] **Step 8: 跑既有 system/endpoint 定义测试确认无回归**

Run: `uv run pytest tests/test_system_define.py tests/test_component_define.py -q`
Expected: PASS（既有用例不受影响）

- [ ] **Step 9: 提交**

```bash
git add hetu/system/definer.py tests/app.py tests/test_system_guards.py
git commit -m "ENH: collect guards in define_system, copy to auto endpoint"
```

---

### Task 6: `EndpointExecutor.execute` 执行 guard、捕获 `ClientReject`

**Files:**
- Modify: `hetu/endpoint/executor.py`（`execute`，约 175-195 行）
- Test: `tests/test_system_guards.py`（需要 redis 后端）

- [ ] **Step 1: 写失败测试**

追加到 `tests/test_system_guards.py`:

```python
async def test_rate_limit_rejects_second_call(mod_test_app, executor):
    from hetu.endpoint.response import RejectResponse

    ok, _ = await executor.execute("login", 7001)
    assert ok

    ok, res = await executor.execute("rate_limited_add", 5)
    assert ok
    assert not isinstance(res, RejectResponse)

    # 同窗口第二次：被软拒绝，连接保持(ok=True)
    ok, res = await executor.execute("rate_limited_add", 5)
    assert ok
    assert isinstance(res, RejectResponse)
    assert res.code == "RATE_LIMITED"

    # 被拒调用没有写入：值仍为 100+5
    ok, _ = await executor.execute("test_rls_comp_value", 105)
    assert ok


async def test_custom_guard_rejects(mod_test_app, executor):
    from hetu.endpoint.response import RejectResponse

    ok, _ = await executor.execute("login", 7002)
    assert ok

    ok, res = await executor.execute("guarded_add", -1)
    assert ok
    assert isinstance(res, RejectResponse)
    assert res.code == "NEGATIVE"

    ok, res = await executor.execute("guarded_add", 3)
    assert ok
    assert not isinstance(res, RejectResponse)
```

- [ ] **Step 2: 跑测试确认失败**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_guards.py -k "rejects_second_call or custom_guard_rejects" -v`
Expected: FAIL（guard 未执行，第二次仍正常返回，非 `RejectResponse`）

- [ ] **Step 3: 在 `execute` 中执行 guard**

Modify `hetu/endpoint/executor.py`：顶部 import 加：

```python
import inspect
from .guard import ClientReject
from .response import RejectResponse
```

`execute`（约 175-195 行）在 alive 检查之后、`return await self.execute_(ep, *args)` 之前插入 guard 执行：

```python
    async def execute(
        self, endpoint: str, *args
    ) -> tuple[bool, ResponseToClient | None]:
        # 检查call参数和call权限
        ep = self.execute_check(endpoint, args)
        if ep is None:
            return False, None

        # 直接数据库检查connect数据是否是自己(可能被别人踢了)，以及要更新last activate
        illegal = await self.alive_checker.is_illegal(
            self.context, f"{self.namespace}.{endpoint}"
        )
        if illegal:
            return False, None

        # 调用前守卫(guard)：raise ClientReject 即软拒绝，不开事务、不断连接
        for g in ep.guards:
            try:
                r = g(self.context, *args)
                if inspect.isawaitable(r):
                    await r
            except ClientReject as e:
                replay.info(
                    f"[Rejected][{endpoint}] {e.code} {self.context}"
                )
                logger.info(
                    _("🚧 [📞Endpoint] {endpoint} 被守卫拒绝：{code}").format(
                        endpoint=endpoint, code=e.code
                    )
                )
                return True, RejectResponse(e.code, e.reason)

        # 开始调用
        return await self.execute_(ep, *args)
```

注意 `execute` / `execute_` 的返回类型注解可放宽为
`tuple[bool, ResponseToClient | RejectResponse | None]`（两处函数签名都改），保持 pyright 干净。

- [ ] **Step 4: 跑测试确认通过**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_guards.py -k "rejects_second_call or custom_guard_rejects" -v`
Expected: PASS（2 passed）

- [ ] **Step 5: 跑既有 executor 测试确认无回归**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_executor.py -q`
Expected: PASS

- [ ] **Step 6: 提交**

```bash
git add hetu/endpoint/executor.py tests/test_system_guards.py
git commit -m "ENH: run guards in EndpointExecutor.execute, soft-reject on ClientReject"
```

---

### Task 7: `receiver.rpc` 输出 `rej` 帧

**Files:**
- Modify: `hetu/server/receiver.py`（`rpc`，约 46-64 行）
- Test: `tests/test_system_guards.py`

- [ ] **Step 1: 写失败测试**

追加到 `tests/test_system_guards.py`（用真实 executor + 一个 asyncio.Queue 当 push_queue）：

```python
async def test_rpc_emits_rej_frame(mod_test_app, executor):
    import asyncio

    from hetu.server.receiver import rpc

    push_queue: asyncio.Queue = asyncio.Queue()

    # 登录
    cont = await rpc(["rpc", "login", 7003], executor, push_queue)
    assert cont is True
    await push_queue.get()  # 丢弃 login 的 rsp

    # 第一次正常
    cont = await rpc(["rpc", "rate_limited_add", 5], executor, push_queue)
    assert cont is True
    first = await push_queue.get()
    assert first[0] == "rsp"

    # 第二次被限流：发 rej 帧，且不关连接(cont=True)
    cont = await rpc(["rpc", "rate_limited_add", 5], executor, push_queue)
    assert cont is True
    rej = await push_queue.get()
    assert rej == ["rej", "rate_limited_add", "RATE_LIMITED"]
```

- [ ] **Step 2: 跑测试确认失败**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_guards.py -k "emits_rej_frame" -v`
Expected: FAIL（当前第二次会走 `["rsp", "ok"]` 而非 `["rej", ...]`）

- [ ] **Step 3: 在 `rpc` 中分发 rej 帧**

Modify `hetu/server/receiver.py`：顶部 import 加：

```python
from ..endpoint.response import RejectResponse
```

`rpc`（约 46-64 行）改为：

```python
async def rpc(data: list, executor: EndpointExecutor, push_queue: asyncio.Queue):
    """处理Client SDK调用Endpoint的命令"""
    check_length("rpc", data, 2, 100)
    ok, res = await executor.execute(data[1], *data[2:])
    if replay.level < logging.ERROR:
        replay.info(f"[EndpointResult][{data[1]}]({ok}, {str(res)})")

    if not ok:
        # 关闭连接
        return False

    if isinstance(res, RejectResponse):
        # 软拒绝：发 rej 帧，连接保持
        await push_queue.put(["rej", data[1], res.code])
    elif isinstance(res, ResponseToClient):
        await push_queue.put(["rsp", res.message])
    else:
        # 无视返回值，直接返回ok，如果不返回，Request无法对应
        await push_queue.put(["rsp", "ok"])
    return True
```

- [ ] **Step 4: 跑测试确认通过**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_guards.py -k "emits_rej_frame" -v`
Expected: PASS

- [ ] **Step 5: 跑 websocket 相关测试确认无回归**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_websocket.py -q`
Expected: PASS

- [ ] **Step 6: 提交**

```bash
git add hetu/server/receiver.py tests/test_system_guards.py
git commit -m "ENH: emit rej frame from receiver.rpc on RejectResponse"
```

---

### Task 8: 顶层导出 `rate_limit` / `guard` / `ClientReject`

**Files:**
- Modify: `hetu/endpoint/__init__.py`、`hetu/__init__.py`
- Test: `tests/test_system_guards.py`

- [ ] **Step 1: 写失败测试**

追加到 `tests/test_system_guards.py`:

```python
def test_top_level_exports():
    import hetu

    assert hasattr(hetu, "rate_limit")
    assert hasattr(hetu, "guard")
    assert hasattr(hetu, "ClientReject")
    assert hetu.ClientReject("X").code == "X"
```

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run pytest tests/test_system_guards.py -k "top_level_exports" -v`
Expected: FAIL（`AttributeError: module 'hetu' has no attribute 'rate_limit'`）

- [ ] **Step 3: endpoint 包导出**

Modify `hetu/endpoint/__init__.py`:

```python
from .context import Context
from .definer import define_endpoint
from .response import ResponseToClient
from .connection import elevate
from .guard import guard, rate_limit, ClientReject


__all__ = [
    "define_endpoint", "Context", "ResponseToClient", "elevate",
    "guard", "rate_limit", "ClientReject",
]
```

- [ ] **Step 4: 顶层导出**

Modify `hetu/__init__.py`：在 `EndpointContext = endpoint.Context` 之后加：

```python
guard = endpoint.guard
rate_limit = endpoint.rate_limit
ClientReject = endpoint.ClientReject
```

并在 `__all__` 中加入 `"guard"`, `"rate_limit"`, `"ClientReject"`。

- [ ] **Step 5: 跑测试确认通过**

Run: `uv run pytest tests/test_system_guards.py -k "top_level_exports" -v`
Expected: PASS

- [ ] **Step 6: 把 tests/app.py 临时 import 改回顶层前缀（可选清理）**

把 Task 5 在 `tests/app.py` 加的 `from hetu.endpoint.guard import ...` 删除，把 `rate_limited_add` /
`guarded_add` / `_reject_when_negative` 内的 `rate_limit`/`guard`/`ClientReject` 改回 `hetu.rate_limit`/
`hetu.guard`/`hetu.ClientReject`。test_system_guards.py 里 Task 4 的两处局部 import 同理可改回
`hetu.guard`/`hetu.rate_limit`。

- [ ] **Step 7: 跑全部 guard 测试确认通过**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_guards.py -v`
Expected: PASS（全部）

- [ ] **Step 8: Lint / 类型检查（只查改动文件）**

Run: `uv run ruff check hetu/endpoint/guard.py hetu/endpoint/executor.py hetu/endpoint/definer.py hetu/system/definer.py hetu/server/receiver.py tests/test_system_guards.py`
Run: `uv run basedpyright hetu/endpoint/guard.py hetu/endpoint/executor.py`
Expected: 无新增错误（既有存量问题忽略）

- [ ] **Step 9: 提交**

```bash
git add hetu/__init__.py hetu/endpoint/__init__.py tests/app.py tests/test_system_guards.py
git commit -m "ENH: export rate_limit/guard/ClientReject at top level"
```

---

## Phase B — 客户端（C#）

> 说明：headless `HeTu.Client` 编译共享 `ClientSDK/unity/.../HeTu/*.cs`，因此核心改在共享文件，
> headless 与 Unity 同时生效。可运行的回归测试放 headless `HeTu.Client.Tests`（`dotnet test`）；
> Unity EditMode 加一份镜像测试。若本机无 .NET SDK，按计划记录命令，由有 SDK 的环境执行。

### Task 9: `ClientBase` 新增 reject 协议与全局事件

**Files:**
- Modify: `ClientSDK/unity/cn.hetudb.clientsdk/HeTu/ClientBase.cs`
- Create: `ClientSDK/csharp/HeTu.Client.Tests/CallRejectTests.cs`

- [ ] **Step 1: 写失败测试（headless，可 dotnet test 运行）**

Create `ClientSDK/csharp/HeTu.Client.Tests/CallRejectTests.cs`:

```csharp
using System;
using System.Collections.Generic;
using HeTu;
using NUnit.Framework;

namespace HeTu.Client.Tests
{
    public class CallRejectTests
    {
        [Test]
        public void RejFrame_FiresOnCallRejected_AndRejectedOutcome()
        {
            var client = new RejectTestClient();
            Logger.Instance.SetLogger(_ => { }, _ => { }, _ => { });
            client.ForceConnected();

            string evtSys = null, evtCode = null;
            client.OnCallRejected += (sys, code) => { evtSys = sys; evtCode = code; };

            CallOutcome outcome = CallOutcome.Completed;
            string rejectCode = null;
            client.CallSystem("attack", new object[] { 1 },
                (_, oc, code) => { outcome = oc; rejectCode = code; });

            // 模拟服务器回 rej 帧
            client.Receive(new object[] { "rej", "attack", "RATE_LIMITED" });

            Assert.AreEqual("attack", evtSys);
            Assert.AreEqual("RATE_LIMITED", evtCode);
            Assert.AreEqual(CallOutcome.Rejected, outcome);
            Assert.AreEqual("RATE_LIMITED", rejectCode);
        }

        [Test]
        public void NormalRsp_AfterReject_StaysFifoAligned()
        {
            var client = new RejectTestClient();
            Logger.Instance.SetLogger(_ => { }, _ => { }, _ => { });
            client.ForceConnected();

            var outcomes = new List<CallOutcome>();
            client.CallSystem("a", Array.Empty<object>(),
                (_, oc, _2) => outcomes.Add(oc));
            client.CallSystem("b", Array.Empty<object>(),
                (_, oc, _2) => outcomes.Add(oc));

            client.Receive(new object[] { "rej", "a", "RATE_LIMITED" });
            client.Receive(new object[] { "rsp", "ok" });

            Assert.AreEqual(CallOutcome.Rejected, outcomes[0]);
            Assert.AreEqual(CallOutcome.Completed, outcomes[1]);
        }

        private sealed class RejectTestClient : HeTuClientBase
        {
            public RejectTestClient() =>
                SetupPipeline(new List<MessageProcessLayer> { new JsonbLayer() });

            public void ForceConnected() => State = ConnectionState.Connected;

            public void CallSystem(string systemName, object[] args,
                Action<JsonObject, CallOutcome, string> onResponse) =>
                CallSystemSync(systemName, args, onResponse);

            // 把帧编码后回灌进 OnReceived（JsonbLayer 可 Encode/Decode 往返）
            public void Receive(object[] frame)
            {
                var bytes = Pipeline.Encode(frame, out _);
                OnReceived(bytes);
            }

            protected override void ConnectCore(string url, Action onConnected,
                Action<byte[]> onMessage, Action<string> onClose, Action<string> onError)
            {
            }

            protected override void CloseCore()
            {
            }

            protected override void SendCore(byte[] data)
            {
            }
        }
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `dotnet test ClientSDK/csharp/HeTu.Client.Tests --filter CallRejectTests`
Expected: 编译失败 / FAIL（`CallOutcome` 未定义、`OnCallRejected` 不存在、`CallSystemSync` 仍是 2 参回调）

- [ ] **Step 3: 在 `ClientBase.cs` 加枚举、异常、事件**

Modify `ClientSDK/unity/cn.hetudb.clientsdk/HeTu/ClientBase.cs`：在 `namespace HeTu {` 内、`HeTuClientBase` 类之前加：

```csharp
    /// <summary>
    ///     System 调用结果。
    /// </summary>
    public enum CallOutcome
    {
        /// <summary>正常完成。</summary>
        Completed,

        /// <summary>因连接取消/重连而取消。</summary>
        Canceled,

        /// <summary>被服务端守卫拒绝（如限流）。</summary>
        Rejected
    }

    /// <summary>
    ///     System 调用被服务端守卫拒绝（如限流）时抛出。
    /// </summary>
    public sealed class HeTuCallRejectedException : Exception
    {
        public string SystemName { get; }
        public string Code { get; }

        public HeTuCallRejectedException(string systemName, string code)
            : base($"System '{systemName}' rejected by server: {code}")
        {
            SystemName = systemName;
            Code = code;
        }
    }
```

并在 `HeTuClientBase` 类内、`OnClosed` 事件附近加全局事件：

```csharp
        /// <summary>
        ///     System 调用被服务端守卫拒绝（如限流）时触发的通用回调。
        ///     参数为 (systemName, code)。用户可在此统一弹窗/提示。
        /// </summary>
        public event Action<string, string> OnCallRejected;
```

并加一个消息类型常量（在 `MessageUpdate` 常量附近）：

```csharp
        internal const string MessageReject = "rej";
```

- [ ] **Step 4: `CallSystemSync` 契约改为带 `CallOutcome`**

Modify `CallSystemSync`（约 259-290 行）：签名与内部回调改为：

```csharp
        internal protected void CallSystemSync(string systemName, object[] args,
            Action<JsonObject, CallOutcome, string> onResponse)
        {
            if (!EnsureConnected("CallSystem"))
            {
                onResponse(null, CallOutcome.Canceled, null);
                return;
            }

            var payload = new object[] { CommandRpc, systemName }.Concat(args).ToArray();
            var traceId = InspectorCollector.InterceptRequest("callsystem", systemName,
                payload);
            SendRequest(payload, (response, cancel) =>
            {
                if (cancel)
                {
                    InspectorCollector.CompleteRequest(traceId, "canceled");
                    onResponse(null, CallOutcome.Canceled, null);
                    return;
                }

                // 服务端守卫拒绝：response = ["rej", systemName, code]
                if (response != null && response.Length > 0 &&
                    response[0] as string == MessageReject)
                {
                    var code = response.Length > 2 ? response[2] as string : "REJECTED";
                    InspectorCollector.CompleteRequest(traceId, "rejected", code);
                    OnCallRejected?.Invoke(systemName, code);
                    onResponse(null, CallOutcome.Rejected, code);
                    return;
                }

                var responsePayload = response != null && response.Length > 1
                    ? response[1]
                    : null;
                InspectorCollector.CompleteRequest(traceId, "completed",
                    responsePayload);
                onResponse((JsonObject)responsePayload, CallOutcome.Completed, null);
            }, traceId);
            SystemLocalCallbacks.TryGetValue(systemName, out var callbacks);
            callbacks?.Invoke(args);
        }
```

- [ ] **Step 5: `OnReceived` 路由 `rej` 帧**

Modify `OnReceived`（约 583-605 行）的 `switch`，把 `case MessageResponse: case MessageSubed:` 扩为也含 reject：

```csharp
            switch (messageType)
            {
                case MessageResponse:
                case MessageSubed:
                case MessageReject:
                    // 这些都是 round trip 响应，有对应的请求等待队列
                    ResponseQueue.CompleteNext(structuredMsg);
                    break;
                case MessageUpdate:
                    // ……（原样保留）
```

- [ ] **Step 6: 跑测试**

此时仍会因其它调用点（Task 10 未改）编译失败。先只验证 `ClientBase.cs` 语法，把完整运行
推迟到 Task 10 之后。可执行：
Run: `dotnet build ClientSDK/csharp/HeTu.Client`
Expected: 仍 FAIL（UnityClient/HeadlessHeTuClient/Session 等调用点未适配）——进入 Task 10 一并修复。

- [ ] **Step 7: 暂存（不单独提交，留待 Task 10 一起绿）**

不提交；进入 Task 10。

---

### Task 10: 适配所有 `CallSystemSync` / `IHeTuSessionConnection` 调用点并跑绿

**Files:**
- Modify: `HeTu/UnityClient.cs`、`ClientSDK/csharp/HeTu.Client/HeadlessHeTuClient.cs`、`HeTu/SessionClient.cs`、`HeTu/SessionClientBase.cs`
- Modify(tests): `Tests/Editor/ConnectionSemanticsTest.cs`、`Tests/Editor/SessionClientBaseTest.cs`、`Tests/HeTu/SessionClientFacadeTest.cs`
- Create: `ClientSDK/unity/cn.hetudb.clientsdk/Tests/Editor/CallRejectTest.cs`

- [ ] **Step 1: `HeadlessHeTuClient.CallSystem` 适配（map Rejected→异常）**

Modify `ClientSDK/csharp/HeTu.Client/HeadlessHeTuClient.cs:91-99`:

```csharp
        public Task<JsonObject> CallSystem(string systemName, params object[] args)
        {
            var tcs = new TaskCompletionSource<JsonObject>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            _pump.Post(() => CallSystemSync(systemName, args, (resp, outcome, code) =>
            {
                switch (outcome)
                {
                    case CallOutcome.Canceled:
                        tcs.TrySetCanceled();
                        break;
                    case CallOutcome.Rejected:
                        tcs.TrySetException(
                            new HeTuCallRejectedException(systemName, code));
                        break;
                    default:
                        tcs.TrySetResult(resp);
                        break;
                }
            }));
            return tcs.Task;
        }
```

- [ ] **Step 2: `UnityClient.CallSystem` 适配**

Modify `ClientSDK/unity/cn.hetudb.clientsdk/HeTu/UnityClient.cs:260-269` 的 `CallSystemSync(...)` 回调：

```csharp
            CallSystemSync(systemName, args, (response, outcome, code) =>
            {
                switch (outcome)
                {
                    case CallOutcome.Canceled:
                        Logger.Instance.Error("CallSystem过程中遇到取消信号");
                        tcs.TrySetCanceled();
                        break;
                    case CallOutcome.Rejected:
                        tcs.TrySetException(
                            new HeTuCallRejectedException(systemName, code));
                        break;
                    default:
                        tcs.TrySetResult(response);
                        break;
                }
            });
```

- [ ] **Step 3: 会话连接接口 + Unity 适配器**

Modify `ClientSDK/unity/cn.hetudb.clientsdk/HeTu/SessionClientBase.cs:48-51`（`IHeTuSessionConnection.CallSystem` 声明）：

```csharp
        void CallSystem(
            string systemName,
            object[] args,
            Action<JsonObject, CallOutcome, string> onResponse);
```

Modify `ClientSDK/unity/cn.hetudb.clientsdk/HeTu/SessionClient.cs:417-419`（`UnityHeTuSessionConnection.CallSystem`）：

```csharp
        public void CallSystem(string systemName, object[] args,
            Action<JsonObject, CallOutcome, string> onResponse) =>
            _client.CallSystemSync(systemName, args, onResponse);
```

- [ ] **Step 4: 会话 `DispatchCall` 映射 Rejected→失败不重试**

Modify `ClientSDK/unity/cn.hetudb.clientsdk/HeTu/SessionClientBase.cs:585-606`（`DispatchCall`）：

```csharp
        private void DispatchCall(PendingCall pending)
        {
            _inFlightCalls.Add(pending);
            _transport.CallSystem(
                pending.SystemName,
                pending.Args,
                (response, outcome, code) =>
                {
                    if (!_inFlightCalls.Remove(pending))
                        return;

                    switch (outcome)
                    {
                        case CallOutcome.Canceled:
                            pending.OnFailed(new OperationCanceledException(
                                $"System call '{pending.SystemName}' was canceled."));
                            break;
                        case CallOutcome.Rejected:
                            // 确定性失败：直接失败该次调用，会话保持 Ready，不自动重试
                            pending.OnFailed(
                                new HeTuCallRejectedException(pending.SystemName, code));
                            break;
                        default:
                            pending.OnCompleted(response);
                            break;
                    }
                });
        }
```

- [ ] **Step 5: 修三处既有测试 fake/harness 的签名**

Modify `ClientSDK/unity/cn.hetudb.clientsdk/Tests/Editor/ConnectionSemanticsTest.cs`:
- `TestClient.CallSystem`（37-39）签名改为 `Action<JsonObject, CallOutcome, string>`，体内 `CallSystemSync(systemName, args, onResponse);` 不变。
- 调用点（19-22）改为：

```csharp
            client.CallSystem("login", Array.Empty<object>(), (_, outcome, _2) =>
            {
                canceled = outcome == CallOutcome.Canceled;
            });
```

Modify `ClientSDK/unity/cn.hetudb.clientsdk/Tests/Editor/SessionClientBaseTest.cs:1477` 处 `FakeTransport.CallSystem` 的 `Action<JsonObject, bool> onResponse` → `Action<JsonObject, CallOutcome, string> onResponse`，并把其内部调用 `onResponse(x, true)` / `onResponse(x, false)` 等改为对应 `onResponse(x, CallOutcome.Canceled, null)` / `onResponse(x, CallOutcome.Completed, null)`（按该 fake 现有语义逐处替换；搜索该文件内所有 `onResponse(` 调用统一改）。

Modify `ClientSDK/unity/cn.hetudb.clientsdk/Tests/HeTu/SessionClientFacadeTest.cs:354` 同上方式修改其 fake 的 `CallSystem` 签名与内部 `onResponse(...)` 调用。

> 提示：先 `grep -n "onResponse(" <file>` 列出全部调用点，再逐一把布尔 `cancel` 形态翻译为
> `CallOutcome`：`true`→`Canceled`、`false`→`Completed`（这些 fake 不模拟 reject）。

- [ ] **Step 6: 加 Unity EditMode 镜像测试**

Create `ClientSDK/unity/cn.hetudb.clientsdk/Tests/Editor/CallRejectTest.cs`，内容与 Task 9 的
`CallRejectTests.cs` 同（命名空间 `Tests.HeTu`，类名 `CallRejectTest`，`RejectTestClient` 内部类
同实现）。这样 Unity Test Runner 也覆盖。

- [ ] **Step 7: 跑 headless 测试确认全绿**

Run: `dotnet test ClientSDK/csharp/HeTu.Client.Tests`
Expected: PASS（含新 `CallRejectTests` 两条 + 既有用例）

- [ ] **Step 8: 格式化（按包 .editorconfig）**

Run: `dotnet format ClientSDK/csharp/HeTu.Client` （若装了 dotnet format）
Expected: 无 diff 或仅格式微调

- [ ] **Step 9: 提交（含 Task 9 暂存的 ClientBase.cs）**

```bash
git add ClientSDK/unity/cn.hetudb.clientsdk/HeTu/ClientBase.cs \
        ClientSDK/unity/cn.hetudb.clientsdk/HeTu/UnityClient.cs \
        ClientSDK/csharp/HeTu.Client/HeadlessHeTuClient.cs \
        ClientSDK/unity/cn.hetudb.clientsdk/HeTu/SessionClient.cs \
        ClientSDK/unity/cn.hetudb.clientsdk/HeTu/SessionClientBase.cs \
        ClientSDK/csharp/HeTu.Client.Tests/CallRejectTests.cs \
        ClientSDK/unity/cn.hetudb.clientsdk/Tests/Editor/CallRejectTest.cs \
        ClientSDK/unity/cn.hetudb.clientsdk/Tests/Editor/ConnectionSemanticsTest.cs \
        ClientSDK/unity/cn.hetudb.clientsdk/Tests/Editor/SessionClientBaseTest.cs \
        ClientSDK/unity/cn.hetudb.clientsdk/Tests/HeTu/SessionClientFacadeTest.cs
git commit -m "ENH: client OnCallRejected event + HeTuCallRejectedException on rej frame"
```

---

## Phase C — 端到端验证（有网 / Unity 机器）

### Task 11: 集成回归

- [ ] **Step 1: 服务端全量 guard 测试**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_guards.py tests/test_system_executor.py tests/test_websocket.py -q`
Expected: PASS

- [ ] **Step 2: headless 端到端冒烟（若 `IntegrationSmokeTests` 涉及真服务器，需先起 `tests/app.py`）**

Run: `dotnet test ClientSDK/csharp/HeTu.Client.Tests`
Expected: PASS

- [ ] **Step 3: Unity PlayMode（有 Unity 的机器）**

在测试工程的 Test Runner 跑 EditMode（含 `CallRejectTest`）；如需真服务器，按
`ClientSDK/unity/CLAUDE.md` 启 `ws://127.0.0.1:2466/hetu/pytest`，并在带 `@rate_limit` 的 system
上连点验证 `OnCallRejected` + `HeTuCallRejectedException`。该步与项目记忆中"headless/Unity 回归
待在有网机器验证"一并完成。

- [ ] **Step 4: 完成分支收尾**

按 superpowers:finishing-a-development-branch 决定合并/PR；本分支预期合回 `dev`。

---

## Self-Review 检查记录

- **Spec 覆盖**：guard 机制(Task 2/4/5/6)、ClientReject(Task 1)、rate_limit(Task 2/6)、
  rej 帧协议(Task 7/9)、Context.guard_state(Task 3)、顶层导出(Task 8)、客户端
  OnCallRejected+异常+三处适配(Task 9/10)、会话层不重试(Task 10 Step 4)、测试(各 Task + Task 11)
  ——逐条对应，无遗漏。
- **占位符**：无 TBD/TODO；每个改动步骤含完整代码或精确替换。
- **类型一致**：服务端 `guards` 字段贯穿 `EndpointDefine`/`SystemDefine`/两处 `add`/`build_endpoints`；
  客户端 `CallOutcome`/`Action<JsonObject, CallOutcome, string>` 契约在 `CallSystemSync` 定义、
  Unity/headless/会话三处调用、三处测试 fake 全部统一更新；`RejectResponse` 在 response.py 定义、
  executor 产出、receiver 消费——签名一致。
- **顺序依赖**：`hetu.rate_limit`/`hetu.guard`/`hetu.ClientReject` 顶层导出在 Task 8 才就绪，
  Task 4/5 先用 `hetu.endpoint.guard` 局部 import，Task 8 Step 6 统一回收——已在计划中显式标注。
