# Idempotent Keyed Future Call (ensure / cancel) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给河图加一对等幂的 keyed 未来调用 API —— `ensure_future_call(ctx, key, ...)` 与 `cancel_future_call(ctx, key)` —— 让用户在 `on_start` System 里幂等地播种 / 停止"开机即起的全局循环后台任务"。

**Architecture:** 复用现有 `FutureCalls` 组件与 `future_call_task` 调度机制。幂等靠把 `key` 经稳定哈希映射到**负数确定性 id**（雪花 id 恒正，天然隔离），复用 FutureCalls 主键唯一性去重 —— 不加字段、不改 schema、无迁移。`ensure` 先 `get(det_id)`：存在则原样返回（commit 空转，安全），不存在则插入；并发同 key 多余插入撞主键 → `RaceCondition` → 自动重试 → get 命中 → skip。`create_future_call` 与 `ensure_future_call` 共用抽出来的校验/建行 helper `_build_future_row`。

**Tech Stack:** Python 3.14、NumPy structured arrays、Redis backend、pytest（`asyncio_mode=auto`，需 Docker Redis）、`hashlib.blake2b`。

**设计依据:** `docs/superpowers/specs/2026-06-14-ensure-future-call-design.md`

**全局约定（每个任务都适用）:**
- TDD 测试命令统一用 redis 后端加速：`HETU_TEST_BACKENDS=redis uv run pytest ...`（需本机 Docker Redis）。
- Lint/类型检查**只查自己改的文件**（全仓无干净基线）：`uv run ruff format <file>`、`uv run ruff check <file>`、`uv run basedpyright <file>`。
- commit 前缀沿用仓库惯例：`ENH:` / `MAINT:` / `TST:`。

---

## Task 1: 抽取共享 helper `_build_future_row`（纯重构，行为不变）

把 `create_future_call` 的"校验 + 组装行"逻辑抽成内部函数，供 `ensure_future_call` 复用。现有 `tests/test_system_future.py` 是回归安全网。

**Files:**
- Modify: `hetu/system/future.py`（`create_future_call`，约 49-175 行）
- Test（回归）: `tests/test_system_future.py`（不改，仅用于验证未回归）

- [ ] **Step 1: 先跑现有未来调用测试，确认基线为绿**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_future.py -v`
Expected: 全部 PASS（`test_future_call_create`、`test_sleep_for_upcoming`、`test_pop_upcoming_call`、`test_duplicate_bug`）。

- [ ] **Step 2: 在 `create_future_call` 之上新增 `_build_future_row`**

在 `hetu/system/future.py` 中，紧挨着 `create_future_call` 的 `@define_system` 装饰器**之前**插入下面的函数（即放在 `FutureCalls` 组件定义之后、`create_future_call` 之前）：

```python
def _build_future_row(
    ctx: "SystemContext",
    at: float,
    system: str,
    args: tuple,
    *,
    timeout: int = 60,
    recurring: bool = False,
    id_: int | None = None,
) -> np.record:
    """校验参数并组装一条 FutureCalls 行（不插入）。

    create_future_call / ensure_future_call 共用。``id_`` 为 None 时用雪花 id；
    给定时（ensure 的确定性 id）用显式 id。

    Validate args and build (not insert) a FutureCalls row, shared by
    create_future_call / ensure_future_call. ``id_`` None -> snowflake id; given ->
    explicit id (ensure's deterministic id).
    """
    # 参数检查
    timeout = max(timeout, 5) if timeout != 0 else 0
    at = time.time() + abs(at) if at <= 0 else at

    args_str = repr(args)
    if len(args_str) > 1024:
        raise ValueError(
            _("args长度超过1024字符: {length}").format(length=len(args_str))
        )

    try:
        revert = ast.literal_eval(args_str)
    except Exception as e:
        raise AssertionError(_("args无法通过eval还原")) from e
    assert revert == args, _("args通过eval还原丢失了信息")

    assert not recurring or timeout != 0, _("recurring=True时timeout不能为0")

    # 读取保存的system define，检查是否开了call lock
    sys = SYSTEM_CLUSTERS.get_system(system)
    if not sys:
        raise RuntimeError(
            _("⚠️ [⚙️Future] [致命错误] 不存在的System {system}").format(system=system)
        )
    lk = any(
        comp == SystemLock or comp.master_ == SystemLock for comp in sys.full_components
    )
    if not lk:
        raise RuntimeError(
            _("⚠️ [⚙️Future] [致命错误] System {system} 定义未开启 call_lock").format(
                system=system
            )
        )

    if sys.permission == Permission.USER:
        warnings.warn(
            _(
                "⚠️ [⚙️Future] [警告] 未来任务的目标 {system} 为{permission}权限，"
                "建议设为None防止客户端调用。"
                "且未来调用为后台任务，执行时Context无用户信息"
            ).format(system=system, permission=sys.permission.name)
        )
    elif sys.permission != Permission.ADMIN and sys.permission is not None:
        warnings.warn(
            _(
                "⚠️ [⚙️Future] [警告] 未来任务的目标 {system} 为{permission}权限，"
                "建议设为None防止客户端调用。"
            ).format(system=system, permission=sys.permission.name)
        )

    # 创建
    row = FutureCalls.new_row(id_=id_)
    row.owner = ctx.caller or -1
    row.system = system
    row.args = args_str
    row.recurring = recurring
    row.created = time.time()
    row.last_run = 0
    row.scheduled = at
    row.timeout = timeout
    return row
```

- [ ] **Step 3: 把 `create_future_call` 的函数体改为委托给 helper**

保留 `create_future_call` 的 `@define_system` 装饰器、签名、以及完整 docstring **不变**；只把 docstring 之后的全部实现代码（原"参数检查 … `await ctx.repo[FutureCalls].insert(row)` / `return row.id`"那段）替换为：

```python
    row = _build_future_row(
        ctx, at, system, args, timeout=timeout, recurring=recurring
    )
    await ctx.repo[FutureCalls].insert(row)
    return row.id
```

- [ ] **Step 4: 跑测试确认无回归**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_future.py -v`
Expected: 与 Step 1 相同，全部 PASS。

- [ ] **Step 5: 为抽出的 helper 补校验测试（锁定"校验沿用"）**

在 `tests/test_system_future.py` 追加（`pytest` 已在文件顶部 import）：

```python
async def test_build_future_row_validation(test_app, new_ctx):
    """_build_future_row 的参数校验：目标 System 不存在 / 未开 call_lock 均报错"""
    from hetu.system.future import _build_future_row

    ctx = new_ctx()
    # 不存在的 System
    with pytest.raises(RuntimeError):
        _build_future_row(ctx, -1, "no_such_system", (1,), timeout=10)
    # 未开 call_lock 的 System（test_rls_comp_value 未设 call_lock=True）
    with pytest.raises(RuntimeError):
        _build_future_row(ctx, -1, "test_rls_comp_value", (1,), timeout=10)
```

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_future.py::test_build_future_row_validation -v`
Expected: PASS（校验逻辑随 helper 一同保留）。

- [ ] **Step 6: Lint/format/typecheck（仅改动文件）**

Run:
```bash
uv run ruff format hetu/system/future.py tests/test_system_future.py
uv run ruff check hetu/system/future.py tests/test_system_future.py
uv run basedpyright hetu/system/future.py
```
Expected: ruff 无新增问题；basedpyright 不新增针对本文件改动的错误（存量问题忽略）。

- [ ] **Step 7: Commit**

```bash
git add hetu/system/future.py tests/test_system_future.py
git commit -m "MAINT: extract _build_future_row shared helper for future calls"
```

---

## Task 2: 确定性 id helper `_key_to_id`

`key` → 负数 int64 的稳定映射。不需要后端，纯单元测试。

**Files:**
- Modify: `hetu/system/future.py`（顶部 import 增加 `hashlib`；新增 `_key_to_id`）
- Test: `tests/test_system_future.py`

- [ ] **Step 1: 写失败测试**

在 `tests/test_system_future.py` 末尾追加：

```python
def test_key_to_id_properties():
    """确定性 id：稳定、恒负（与雪花正 id 隔离）、非 0、落在 int64 范围、不同 key 不同 id"""
    from hetu.system.future import _key_to_id

    a = _key_to_id("world_tick")
    b = _key_to_id("world_tick")
    c = _key_to_id("other_key")
    assert a == b  # 确定性（跨调用稳定）
    assert a < 0  # 恒负
    assert a != 0
    assert c < 0 and a != c  # 不同 key 不同 id
    assert -(2**63) <= a <= -1  # int64 负数范围内
```

- [ ] **Step 2: 跑测试确认失败**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_future.py::test_key_to_id_properties -v`
Expected: FAIL —— `ImportError`/`cannot import name '_key_to_id'`。

- [ ] **Step 3: 实现**

在 `hetu/system/future.py` 顶部 import 区加入 `import hashlib`（与现有 `import ast` 等并列，按字母序放在 `import asyncio` 之前或就近合适位置）。

在 `_build_future_row` 之前（或 `FutureCalls` 组件定义之后）新增：

```python
def _key_to_id(key: str) -> int:
    """把 ensure_future_call 的 key 稳定映射到负数 id，用作 FutureCalls 主键去重。

    必须跨进程/重启确定性（不能用内置 hash()，其按进程加盐）；雪花 id 恒正，
    负数区间专供 keyed 行，二者不会相撞。

    Stably map a key to a negative id for primary-key dedup of FutureCalls. Must be
    deterministic across processes/restarts (builtin hash() is per-process salted);
    snowflake ids are always positive, so the negative range is reserved for keyed rows.
    """
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    h = int.from_bytes(digest, "big")  # 0 .. 2**64 - 1
    return -(h >> 1) - 1  # -> [-(2**63), -1]：恒负、非 0、落在 int64 范围内
```

- [ ] **Step 4: 跑测试确认通过**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_future.py::test_key_to_id_properties -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add hetu/system/future.py tests/test_system_future.py
git commit -m "ENH: add _key_to_id deterministic id helper for keyed future calls"
```

---

## Task 3: `ensure_future_call`（等幂创建）

新增 System + 测试用包装 System，验证同 key 多次只产生一条。

**Files:**
- Modify: `hetu/system/future.py`（新增 `ensure_future_call`）
- Modify: `tests/app.py`（新增 `ensure_rls_comp_value_future` 包装 System）
- Test: `tests/test_system_future.py`

- [ ] **Step 1: 在 `tests/app.py` 新增包装 System**

在 `add_rls_comp_value_future`（约 68-76 行）之后插入：

```python
@hetu.define_system(
    namespace="pytest",
    permission=hetu.Permission.EVERYBODY,
    depends=("ensure_future_call:copy1",),
)
async def ensure_rls_comp_value_future(ctx: hetu.SystemContext, key, value, recurring):
    return await ctx.depend["ensure_future_call:copy1"](
        ctx, key, -1, "add_rls_comp_value", value, timeout=10, recurring=recurring
    )
```

- [ ] **Step 2: 写失败测试**

在 `tests/test_system_future.py` 追加：

```python
async def test_ensure_future_call_idempotent(test_app, tbl_mgr, executor):
    """同 key 多次 ensure 只产生一条 FutureCalls 行，且返回同一确定性 id；不覆盖已有参数"""
    from hetu.system.future import FutureCalls, _key_to_id

    await executor.execute("login", 1020)
    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = tbl_mgr.get_table(FutureCallsTableCopy1)

    ok1, id1 = await executor.execute("ensure_rls_comp_value_future", "tick", 4, True)
    ok2, id2 = await executor.execute("ensure_rls_comp_value_future", "tick", 9, True)
    assert ok1 and ok2
    assert id1 == id2 == _key_to_id("tick")

    async with fc_tbl.session() as session:
        repo = session.using(FutureCallsTableCopy1)
        rows = await repo.range("scheduled", 0, time.time() + 100000, limit=100)
        assert rows.size == 1  # 只有一条
        assert rows[0].id == _key_to_id("tick")
        assert rows[0].system == "add_rls_comp_value"
        assert rows[0].recurring
        assert rows[0].timeout == 10
        # ensure-exists：第二次 value=9 未覆盖第一次 args=(4,)
        assert "4" in rows[0].args and "9" not in rows[0].args
```

- [ ] **Step 3: 跑测试确认失败**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_future.py::test_ensure_future_call_idempotent -v`
Expected: FAIL —— `ensure_future_call` 不存在（depends 解析失败 / 构建簇报错）。

- [ ] **Step 4: 实现 `ensure_future_call`**

在 `hetu/system/future.py` 的 `create_future_call` 之后新增：

```python
@define_system(namespace="global", permission=None, components=(FutureCalls,))
async def ensure_future_call(
    ctx: SystemContext,
    key: str,
    at: float,
    system: str,
    *args,
    timeout: int = 60,
    recurring: bool = False,
):
    """按 key 等幂地确保一个未来调用存在；已存在则原样保留（不更新参数），返回其 id。

    与 create_future_call 相同，但用 key 做幂等：同一 key 多次调用只会创建一条。
    适合在 on_start System 里播种"开机即起"的全局循环任务（recurring=True），
    服务器重启多次也不会重复堆积。

    幂等通过 key 的确定性 id 复用 FutureCalls 主键唯一性实现：已存在则直接返回（不写入，
    事务空提交），并发同 key 的多余插入会撞主键引发事务竞态并自动重试，最终只保留一条。

    Idempotently ensure a single future call exists, keyed by ``key``; if it already
    exists, keep it as-is (params are NOT updated) and return its id. Useful for seeding
    a server-wide recurring background task from an on_start system without piling up
    duplicates across restarts.

    Parameters
    ----------
    ctx: Context
        System默认变量
    key: str
        幂等键。同一 key 只会存在一条未来调用。
    at: float
        同 create_future_call：正数为绝对 POSIX 时间戳；负数为相对延后秒数。
    system: str
        未来调用的目标 system 名。
    *args
        目标 system 的参数（须能 repr 往返还原，如基础类型）。
    timeout: int
        再次调用时间（秒），含义同 create_future_call；recurring=True 时不能为 0。
    recurring: bool
        设置后永不删除，按 timeout 周期重复触发。

    Returns
    -------
    返回未来调用的 id: int（由 key 推导的确定性负数 id）

    Examples
    --------
    >>> import hetu
    >>> @hetu.define_system(namespace='game', permission=None, call_lock=True,
    ...                     components=(World,))
    ... async def world_tick(ctx): ...
    >>> @hetu.define_system(namespace='game', permission=None, on_start=True,
    ...                     depends=('ensure_future_call:game',))
    ... async def boot(ctx):
    ...     ctx.depend['ensure_future_call:game'](
    ...         ctx, 'world_tick', -30, 'world_tick', recurring=True, timeout=30)

    开服时 on_start 跑一次 → ensure 幂等 → 重启 N 次也只有一条 → 由 future_call_task
    每 ~1 秒轮询、全局只一个 worker 执行、timeout 重试、重启不丢。
    """
    fid = _key_to_id(key)
    if await ctx.repo[FutureCalls].get(id=fid):
        return fid  # 已存在 → no-op（无写入，commit 空转，安全）
    row = _build_future_row(
        ctx, at, system, args, timeout=timeout, recurring=recurring, id_=fid
    )
    await ctx.repo[FutureCalls].insert(row)
    return fid
```

- [ ] **Step 5: 跑测试确认通过**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_future.py::test_ensure_future_call_idempotent -v`
Expected: PASS

- [ ] **Step 6: Lint/format/typecheck（仅改动文件）**

Run:
```bash
uv run ruff format hetu/system/future.py tests/app.py tests/test_system_future.py
uv run ruff check hetu/system/future.py tests/app.py tests/test_system_future.py
uv run basedpyright hetu/system/future.py
```
Expected: 无新增问题。

- [ ] **Step 7: Commit**

```bash
git add hetu/system/future.py tests/app.py tests/test_system_future.py
git commit -m "ENH: add ensure_future_call (idempotent keyed future call)"
```

---

## Task 4: `cancel_future_call`（按 key 删除）

薄一层删除接口，让 recurring 任务可停止/重配。

**Files:**
- Modify: `hetu/system/future.py`（新增 `cancel_future_call`）
- Modify: `tests/app.py`（新增 `cancel_rls_comp_value_future` 包装 System）
- Test: `tests/test_system_future.py`

- [ ] **Step 1: 在 `tests/app.py` 新增包装 System**

在 `ensure_rls_comp_value_future` 之后插入：

```python
@hetu.define_system(
    namespace="pytest",
    permission=hetu.Permission.EVERYBODY,
    depends=("cancel_future_call:copy1",),
)
async def cancel_rls_comp_value_future(ctx: hetu.SystemContext, key):
    return await ctx.depend["cancel_future_call:copy1"](ctx, key)
```

- [ ] **Step 2: 写失败测试**

在 `tests/test_system_future.py` 追加：

```python
async def test_cancel_future_call(test_app, tbl_mgr, executor):
    """cancel 删除已存在 key（返回 True），不存在 key 返回 False，cancel 后可重新 ensure"""
    from hetu.system.future import FutureCalls, _key_to_id

    await executor.execute("login", 1020)
    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = tbl_mgr.get_table(FutureCallsTableCopy1)

    # 不存在 -> False
    ok, deleted = await executor.execute("cancel_rls_comp_value_future", "tick")
    assert ok and deleted is False

    # ensure 后存在
    await executor.execute("ensure_rls_comp_value_future", "tick", 4, True)
    async with fc_tbl.session() as session:
        repo = session.using(FutureCallsTableCopy1)
        assert await repo.get(id=_key_to_id("tick")) is not None

    # cancel -> True 且行被删
    ok, deleted = await executor.execute("cancel_rls_comp_value_future", "tick")
    assert ok and deleted is True
    async with fc_tbl.session() as session:
        repo = session.using(FutureCallsTableCopy1)
        assert await repo.get(id=_key_to_id("tick")) is None

    # cancel 后可重新 ensure（重配间隔的基础：cancel 再 ensure）
    ok, id2 = await executor.execute("ensure_rls_comp_value_future", "tick", 7, True)
    assert ok and id2 == _key_to_id("tick")
```

- [ ] **Step 3: 跑测试确认失败**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_future.py::test_cancel_future_call -v`
Expected: FAIL —— `cancel_future_call` 不存在。

- [ ] **Step 4: 实现 `cancel_future_call`**

在 `hetu/system/future.py` 的 `ensure_future_call` 之后新增：

```python
@define_system(namespace="global", permission=None, components=(FutureCalls,))
async def cancel_future_call(ctx: SystemContext, key: str) -> bool:
    """按 key 删除 ensure_future_call 创建的未来调用（停止 / 重配循环任务）。

    返回 True 表示存在并已删除，False 表示该 key 没有对应的未来调用。重配间隔等参数：
    先 cancel 再 ensure（ensure 是 ensure-exists，不会就地改参数）。

    Cancel a keyed future call created by ensure_future_call. Returns True if it existed
    and was deleted, False otherwise. To reconfigure (e.g. change interval): cancel then
    ensure again.

    Parameters
    ----------
    ctx: Context
        System默认变量
    key: str
        要删除的未来调用的幂等键，与 ensure_future_call 的 key 一致。

    Returns
    -------
    bool: 是否存在并删除
    """
    fid = _key_to_id(key)
    if not await ctx.repo[FutureCalls].get(id=fid):
        return False
    ctx.repo[FutureCalls].delete(fid)
    return True
```

- [ ] **Step 5: 跑测试确认通过**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_future.py::test_cancel_future_call -v`
Expected: PASS

- [ ] **Step 6: Lint/format/typecheck（仅改动文件）**

Run:
```bash
uv run ruff format hetu/system/future.py tests/app.py tests/test_system_future.py
uv run ruff check hetu/system/future.py tests/app.py tests/test_system_future.py
uv run basedpyright hetu/system/future.py
```
Expected: 无新增问题。

- [ ] **Step 7: Commit**

```bash
git add hetu/system/future.py tests/app.py tests/test_system_future.py
git commit -m "ENH: add cancel_future_call (remove keyed future call by key)"
```

---

## Task 5: 跨重启幂等测试（表里已有同 key 行 → ensure 跳过）

模拟"上次开服已播种、行持久化存活"，验证再 ensure 不新增、不报错、不覆盖。

> **并发竞态范围说明（有意识的取舍，非遗漏）:** 真正的并发 insert-insert 竞态由"确定性 id 复用主键唯一性"保证：多余插入会得到 `"RACE: Key already exists"` → `RaceCondition` → `SystemCaller` 自动重试 → get 命中 → skip。在单线程 pytest 里确定性地制造真并发不现实，故用下面这条"预置行 → ensure 跳过"测试覆盖现实中的跨重启幂等路径（get-skip 分支）；插入撞主键这一底层行为由后端既有的 duplicate-id RaceCondition 行为保证。

**Files:**
- Test: `tests/test_system_future.py`

- [ ] **Step 1: 写测试**

在 `tests/test_system_future.py` 追加：

```python
async def test_ensure_skips_preexisting_row(test_app, tbl_mgr, executor):
    """表里已有同 key 行时（代表上次开服播种的持久化行），再 ensure 不新增、不报错、返回同 id"""
    from hetu.system.future import FutureCalls, _key_to_id, _build_future_row

    await executor.execute("login", 1020)
    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = tbl_mgr.get_table(FutureCallsTableCopy1)

    fid = _key_to_id("tick")
    # 直接预置一行（代表上次开服播种、持久化存活的 recurring 行）
    async with fc_tbl.session() as session:
        repo = session.using(FutureCallsTableCopy1)
        row = _build_future_row(
            executor.context,
            -1,
            "add_rls_comp_value",
            (4,),
            timeout=10,
            recurring=True,
            id_=fid,
        )
        await repo.insert(row)

    # 再 ensure 同 key：命中 get-skip 分支（无写入，commit 空转），不报错
    ok, rid = await executor.execute("ensure_rls_comp_value_future", "tick", 999, True)
    assert ok and rid == fid

    async with fc_tbl.session() as session:
        repo = session.using(FutureCallsTableCopy1)
        rows = await repo.range("scheduled", 0, time.time() + 100000, limit=100)
        assert rows.size == 1  # 仍只有一条
        # 未被 999 覆盖（ensure-exists）
        assert "4" in rows[0].args and "999" not in rows[0].args
```

- [ ] **Step 2: 跑测试确认通过**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_future.py::test_ensure_skips_preexisting_row -v`
Expected: PASS（验证 `_build_future_row` 的 `id_=fid` 直插 + ensure 的 get-skip 协同正确）。

- [ ] **Step 3: 跑整组未来调用测试，确认全绿**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_future.py -v`
Expected: 全部 PASS（4 个原有 + `test_key_to_id_properties` + 3 个新增）。

- [ ] **Step 4: Commit**

```bash
git add tests/test_system_future.py
git commit -m "TST: ensure_future_call idempotent against pre-existing row"
```

---

## Task 6: ensure 的一次性调用真执行（负数 id 走完调度链路）

验证 ensure 产生的负数 id 行能被 `pop_upcoming_call` 取出、`exec_future_call` 执行（一次性 + timeout!=0 → 走 call_lock，`uuid=str(负数 id)`），执行后删除，且目标 System 真的生效。镜像现有 `test_pop_upcoming_call`，只是改用 ensure 创建。

> recurring 行的"周期重复触发"是 `future_call_task` / `pop_upcoming_call` 既有逻辑（不随本次改动而变，已被 create 相关测试覆盖）；本任务聚焦新代码路径 —— 负数 id 全程兼容。

**Files:**
- Test: `tests/test_system_future.py`

- [ ] **Step 1: 写测试**

在 `tests/test_system_future.py` 追加：

```python
async def test_ensure_one_shot_executes(monkeypatch, test_app, tbl_mgr, executor):
    """ensure 的一次性调用（负数 id）能被 pop + exec，执行后删除，目标 System 生效"""
    from hetu.system.future import (
        FutureCalls,
        _key_to_id,
        pop_upcoming_call,
        exec_future_call,
    )

    await executor.execute("login", 1020)
    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = tbl_mgr.get_table(FutureCallsTableCopy1)

    # ensure 一个一次性（recurring=False、timeout=10）调用：到点执行 add_rls_comp_value(4)
    ok, fid = await executor.execute("ensure_rls_comp_value_future", "once", 4, False)
    assert ok and fid == _key_to_id("once")

    # 让时间前进，pop 出到期任务（real time 捕获后再 monkeypatch）
    last_time = time.time() + 1
    monkeypatch.setattr(time, "time", lambda: last_time)
    call = await pop_upcoming_call(fc_tbl)
    assert call and call.id == fid  # 负数 id 正常 pop

    # 执行：一次性 + timeout!=0 → 走 call_lock，uuid=str(负数 id)
    ok = await exec_future_call(call, executor.context.systems, fc_tbl)
    assert ok

    # 执行成功后一次性任务被删除
    async with fc_tbl.session() as session:
        repo = session.using(FutureCallsTableCopy1)
        assert await repo.get(id=fid) is None

    # 目标 System 真的执行了：RLSComp.value = 100 + 4
    ok, _ = await executor.execute("test_rls_comp_value", 104)
    assert ok
```

- [ ] **Step 2: 跑测试确认通过**

Run: `HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_future.py::test_ensure_one_shot_executes -v`
Expected: PASS（负数 id 全程：pop → exec → call_lock → 删除 → 目标 System 生效）。

- [ ] **Step 3: Commit**

```bash
git add tests/test_system_future.py
git commit -m "TST: ensure_future_call one-shot executes through scheduler (negative id)"
```

---

## Task 7: 重新生成 API 文档 + 全量校验

`docs/api/` 由 `scripts/gen_api_docs.py` 自动生成（CI 会校验是否最新，stale 即失败）。新增的两个 System 带 docstring，需重新生成。

**Files:**
- Generated: `docs/api/system.md`、`docs/api/_index.md`（由脚本覆盖，勿手改）

- [ ] **Step 1: 重新生成 API 文档**

Run: `uv run python scripts/gen_api_docs.py`
Expected: 命令成功；`git status` 显示 `docs/api/system.md`（很可能还有 `docs/api/_index.md`、`docs/api/_coverage.md`）被修改。

- [ ] **Step 2: 核对生成结果**

Run: `git diff --stat docs/api/ && grep -n "ensure_future_call\|cancel_future_call" docs/api/system.md docs/api/_index.md`
Expected: 能看到 `ensure_future_call`、`cancel_future_call` 两个新条目出现在生成文档中。

- [ ] **Step 3: 全量未来调用测试 + 改动文件 lint 终检**

Run:
```bash
HETU_TEST_BACKENDS=redis uv run pytest tests/test_system_future.py -v
uv run ruff check hetu/system/future.py tests/app.py tests/test_system_future.py
uv run basedpyright hetu/system/future.py
```
Expected: 测试全绿；lint/类型检查无新增问题。

- [ ] **Step 4: Commit**

```bash
git add docs/api/
git commit -m "MAINT: regenerate API docs for ensure/cancel_future_call"
```

---

## 完成判据（DoD）

- `ensure_future_call(ctx, key, ...)` 同 key 多次调用只产生一条 FutureCalls 行，返回稳定的确定性负数 id；不覆盖已存在行的参数。
- `cancel_future_call(ctx, key)` 删除对应行并返回 True，无对应行返回 False；cancel 后可重新 ensure。
- `create_future_call` 行为不变（回归测试全绿）。
- 无 schema 变更 / 无迁移。
- `docs/api/` 已重新生成并含两个新 API，CI 文档校验可通过。
- 改动文件 ruff/basedpyright 无新增问题。
