import asyncio
import logging
import time

import numpy as np
import pytest

from hetu.common.snowflake_id import SnowflakeID
from hetu.endpoint.executor import EndpointExecutor

SnowflakeID().init(1, 0)


async def test_future_call_create(test_app, tbl_mgr, executor: EndpointExecutor):
    """测试创建未来调用任务是否正确"""
    time_time = time.time

    # 创建一个未来调用
    from hetu.system.future import FutureCalls

    await executor.execute("login", 1020)

    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = tbl_mgr.get_table(FutureCallsTableCopy1)

    ok, uuid = await executor.execute("add_rls_comp_value_future", 4, False)

    # 测试未来调用数据是否正确
    async with fc_tbl.session() as session:
        repo = session.using(FutureCallsTableCopy1)
        expire_time = time_time() + 1.1
        rows = await repo.range("scheduled", 0, expire_time, limit=1)
        assert rows[0].id == uuid
        assert rows[0].timeout == 10
        assert rows[0].system == "add_rls_comp_value"
        assert not rows[0].recurring
        assert rows[0].owner == 1020


def _task_app(tbl_mgr):
    """future_call_task 只用到 app.ctx.table_managers 与 app.config["NAMESPACE"]"""
    from types import SimpleNamespace

    return SimpleNamespace(
        ctx=SimpleNamespace(table_managers={"server1": tbl_mgr}),
        config={"NAMESPACE": "pytest"},
    )


def _task_callers(tbl_mgr):
    """同 future_call_task 里的执行器：内部服务身份（caller=0）"""
    from hetu.system import SystemContext
    from hetu.system.caller import SystemCaller

    context = SystemContext(
        caller=0,
        connection_id=0,
        address="localhost",
        group="guest",
        user_data={},
        timestamp=0,
        request=None,  # type: ignore
        systems=None,  # type: ignore
    )
    return {"server1": SystemCaller("pytest", tbl_mgr, context)}


async def _counter_value(tbl_mgr, test_app) -> int:
    """future_call_task 以 caller=0 执行 add_rls_comp_value：累加在 owner=0 那行（默认 100）"""
    tbl = tbl_mgr.get_table(test_app.RLSComp)
    async with tbl.session() as session:
        row = await session.using(test_app.RLSComp).get(owner=0)
    return 100 if row is None else int(row.value)


async def _insert_due_calls(fc_tbl, ctx, n, *, value=1, timeout=10, ago=5.0):
    """插入 n 条已到期的 add_rls_comp_value(value) 调用（按插入顺序依次到期），等副本同步"""
    from hetu.system.future import _build_future_row

    now = time.time()
    rows = []
    async with fc_tbl.session() as session:
        repo = session.using(fc_tbl.comp_cls)
        for i in range(n):
            row = _build_future_row(
                ctx,
                now - ago + i * 0.001,
                "add_rls_comp_value",
                (value,),
                timeout=timeout,
            )
            await repo.insert(row)
            rows.append(row)
    await fc_tbl.backend.wait_for_synced()
    return rows


async def _run_future_task_until(tbl_mgr, done, timeout: float) -> bool:
    """跑 future_call_task，直到 done() 为真或超时；返回 done() 的最终结果"""
    import contextlib

    from hetu.system import future

    task = asyncio.create_task(future.future_call_task(_task_app(tbl_mgr)))
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if await done():
                return True
            await asyncio.sleep(0.05)
        return await done()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def _take_after_index_read(monkeypatch, fc_tbl, victim_id, lease=600.0):
    """
    模拟另一个 worker 抢先取走 victim：本进程第一次读到 fc_tbl 的 scheduled 索引（ZRANGE）
    之后、读行之前，把 victim 取走（scheduled 顺延 lease 秒、版本 +1），并等副本同步完。
    索引和行是两次往返，这正是中间被别人取走的时机。
    """
    backend = fc_tbl.backend
    client_cls = type(backend.master)
    orig = client_cls.zrange_bylex_
    idx_key = client_cls.index_key(fc_tbl, "scheduled")
    state = {"taken": False}

    async def zrange_then_take(self, key, *args, **kwargs):
        members = await orig(self, key, *args, **kwargs)
        if key == idx_key and not state["taken"]:
            state["taken"] = True
            async with fc_tbl.session() as session:
                repo = session.using(fc_tbl.comp_cls)
                row = await repo.get(id=victim_id)
                assert row is not None
                row.scheduled = time.time() + lease
                row.last_run = time.time()
                await repo.update(row)
            await backend.wait_for_synced()
        return members

    monkeypatch.setattr(client_cls, "zrange_bylex_", zrange_then_take)
    return state


async def test_next_due_reads_scheduled_from_index(
    monkeypatch, test_app, tbl_mgr, new_ctx
):
    """next_due 返回 horizon 之前最早一条的 scheduled，只读索引（值从 member 解出），不读行：
    读行是第二次往返，中间那行可能已被别的 worker 取走、scheduled 改到 timeout 之后"""
    from hetu.system.future import FutureCalls, next_due

    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    assert await next_due(fc_tbl, time.time() + 1) is None

    (row,) = await _insert_due_calls(fc_tbl, new_ctx(), 1)
    scheduled = float(row.scheduled)

    def no_row_reads(*_args, **_kwargs):
        raise AssertionError("next_due 不该读行")

    backend = fc_tbl.backend
    for client in (backend.master, backend.servant):
        monkeypatch.setattr(type(client), "hgetall_many_", no_row_reads)
        monkeypatch.setattr(type(client), "get", no_row_reads)
    assert await next_due(fc_tbl, time.time() + 1) == scheduled
    assert await next_due(fc_tbl, scheduled - 1) is None


async def test_run_due_calls_wake_time(monkeypatch, test_app, tbl_mgr, new_ctx):
    """一轮扫描：执行所有表里到期的调用；返回距下次该扫的秒数——处理过调用就马上再扫（0），
    没有到期的就睡到最早那条，最多 1 秒"""
    from hetu.system.future import FutureCalls, _build_future_row, run_due_calls

    tables = [
        tbl_mgr.get_table(FutureCalls),
        tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1")),
    ]
    callers = _task_callers(tbl_mgr)

    delay = await run_due_calls(tables, callers)
    assert 0.9 <= delay <= 1.0

    await _insert_due_calls(tables[1], new_ctx(), 2)
    assert await run_due_calls(tables, callers) == 0
    assert await _counter_value(tbl_mgr, test_app) == 102

    # 等副本同步可能要 1 秒左右（副本约每秒回报一次复制进度），用冻结的时钟判定睡多久
    frozen = time.time() + 100
    async with tables[0].session() as session:
        row = _build_future_row(
            new_ctx(), frozen + 0.5, "add_rls_comp_value", (1,), timeout=10
        )
        await session.using(tables[0].comp_cls).insert(row)
    await tables[0].backend.wait_for_synced()
    monkeypatch.setattr(time, "time", lambda: frozen)
    assert await run_due_calls(tables, callers) == pytest.approx(0.5)


@pytest.mark.timeout(30)
async def test_future_call_task_drains_due_calls_across_tables(
    test_app, tbl_mgr, new_ctx
):
    """有多张 FutureCalls 表（主表 + 副本）时，某张表积压的到期调用要连续执行完，不能每处理
    一条就随机换一张表、碰上空表再睡 1 秒（K 张表时吞吐只有 worker 数 / (K-1) 条每秒）"""
    from hetu.system.future import FutureCalls

    assert (
        tbl_mgr.get_table(FutureCalls) is not None
    )  # 主表恒在：global 的 System 引用它
    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    n = 20
    await _insert_due_calls(fc_tbl, new_ctx(), n)

    async def all_done():
        return await _counter_value(tbl_mgr, test_app) >= 100 + n

    assert await _run_future_task_until(tbl_mgr, all_done, timeout=4)
    assert await _counter_value(tbl_mgr, test_app) == 100 + n


@pytest.mark.timeout(30)
async def test_future_call_task_not_stalled_by_call_taken_between_reads(
    monkeypatch, test_app, tbl_mgr, new_ctx
):
    """最早到期的调用在"读索引"和"读行"之间被别的 worker 取走（scheduled 已顺延 timeout）：
    不能按读到的新 scheduled 去睡（会睡 timeout 秒，长周期的 recurring 甚至一小时），
    后面到期的调用要照常执行"""
    from hetu.system.future import FutureCalls

    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    taken, _other = await _insert_due_calls(fc_tbl, new_ctx(), 2)
    state = _take_after_index_read(monkeypatch, fc_tbl, int(taken.id))

    async def other_done():
        return await _counter_value(tbl_mgr, test_app) >= 101

    assert await _run_future_task_until(tbl_mgr, other_done, timeout=4)
    assert state["taken"]
    # 被取走的那条归别人执行，本 worker 只执行了另一条
    assert await _counter_value(tbl_mgr, test_app) == 101


async def test_pop_upcoming_call(
    monkeypatch, test_app, tbl_mgr, executor: EndpointExecutor
):
    """测试pop_upcoming_call取出任务逻辑是否正确"""
    time_time = time.time

    # 创建一个未来调用
    from hetu.system.future import FutureCalls

    await executor.execute("login", 1020)

    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = tbl_mgr.get_table(FutureCallsTableCopy1)

    ok, uuid = await executor.execute("add_rls_comp_value_future", 4, False)

    # 测试pop_upcoming_call是否正常
    from hetu.system.future import pop_upcoming_call

    # 让时间延迟，才能pop出来
    last_time = time_time() + 1
    monkeypatch.setattr(time, "time", lambda: last_time)
    call = await pop_upcoming_call(fc_tbl)
    assert call
    assert call.id == uuid

    # 检测pop的task数据是否修改了
    async with fc_tbl.session() as session:
        repo = session.using(FutureCallsTableCopy1)
        row = await repo.get(id=uuid)
        assert row.last_run == last_time
        assert row.scheduled == last_time + 10

    # 测试exec_future_call调用是否正常
    last_time = time_time() + 2
    from hetu.system.future import exec_future_call

    # 此时future_call用的是已login的executor，实际运行future_call不可能有login的executor
    ok = await exec_future_call(call, executor.context.systems, fc_tbl)
    assert ok
    # 检测task是否删除
    async with fc_tbl.session() as session:
        repo = session.using(FutureCallsTableCopy1)
        row = await repo.get(id=uuid)
        assert row is None
    # 测试hp
    ok, _ = await executor.execute("test_rls_comp_value", 100 + 4)
    assert ok


async def test_pop_upcoming_call_ignores_new_due_calls(
    monkeypatch, test_app, tbl_mgr, executor: EndpointExecutor
):
    """取"最早到期的一条"不依赖区间里没有别的行：取出期间不断有更早到期的新调用插进来，
    也不能判竞态（pop 只重试 5 次，耗尽就是任务循环里的一条错误日志）"""
    from unittest.mock import patch

    from hetu.system.future import FutureCalls, pop_upcoming_call

    await executor.execute("login", 1020)
    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = tbl_mgr.get_table(FutureCallsTableCopy1)
    ok, uuid = await executor.execute("add_rls_comp_value_future", 4, False)
    assert ok

    master = fc_tbl.backend.master
    orig_commit = master.commit
    intruding = False
    intruders: list[int] = []

    async def commit_after_new_due_call(idmap):
        # 每次提交前都有一条更早到期的新调用插进来（一条比一条早）
        nonlocal intruding
        if not intruding:
            intruding = True
            try:
                async with fc_tbl.session() as session:
                    row = FutureCallsTableCopy1.new_row()
                    row.system = "nobody"
                    row.scheduled = 1.0 / (len(intruders) + 1)
                    await session.using(FutureCallsTableCopy1).insert(row)
                    intruders.append(int(row.id))
            finally:
                intruding = False
        return await orig_commit(idmap)

    last_time = time.time() + 1  # 让调用到期
    monkeypatch.setattr(time, "time", lambda: last_time)
    with patch.object(master, "commit", new=commit_after_new_due_call):
        call = await pop_upcoming_call(fc_tbl)
    assert call is not None and call.id == uuid

    async with fc_tbl.session() as session:
        repo = session.using(FutureCallsTableCopy1)
        for row_id in [uuid, *intruders]:
            if await repo.get(id=row_id):
                repo.delete(row_id)


def test_duplicate_bug(mod_auto_backend, new_clusters_env):
    """测试未来调用常用的duplicated的system，component是否会按namespace隔离"""
    from hetu.data.component import Permission
    from hetu.system import SystemContext, define_system

    # 定义2个不同的namespace的future call
    @define_system(
        namespace="ns1",
        permission=Permission.EVERYBODY,
        depends=("create_future_call:copy1",),
    )
    async def use_future_namespace1(ctx: SystemContext, value, recurring):
        return await ctx.depend["create_future_call:copy1"](
            ctx, -1, "any_other_system", value, timeout=10, recurring=recurring
        )

    @define_system(
        namespace="ns2",
        permission=Permission.EVERYBODY,
        depends=("create_future_call:copy1",),
    )
    async def use_future_namespace2(ctx: SystemContext, value, recurring):
        return await ctx.depend["create_future_call:copy1"](
            ctx, -1, "any_other_system", value, timeout=10, recurring=recurring
        )

    from hetu.system import SystemClusters

    SystemClusters().build_clusters("ns1")

    # 检查FutureCalls是否正确隔离
    from hetu.system.future import FutureCalls

    future_ns1 = list(FutureCalls.get_duplicates("ns1").values())
    future_ns2 = list(FutureCalls.get_duplicates("ns2").values())
    assert len(future_ns1) == len(future_ns2) == 1
    # Component的namespace并不会变
    assert future_ns1[0].namespace_ == future_ns2[0].namespace_ == "HeTu"
    assert future_ns1[0].name_ == future_ns2[0].name_

    # 检查component table manager是否正确隔离
    backend = mod_auto_backend()
    backends = {"default": backend}

    from hetu.manager import ComponentTableManager

    tbl_mgr = ComponentTableManager("ns1", "server1", backends)

    assert tbl_mgr.get_table(future_ns1[0]) is not None
    assert tbl_mgr.get_table(future_ns2[0]) is None


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


async def test_build_future_row_rejects_unstorable_args(test_app, new_ctx):
    """args 以 repr 存进 <U1024 字段、执行时 literal_eval 还原：超长的存不下，
    还原不回来的（如直接把组件字段的 numpy 标量当参数传）执行时必错，都要在创建时拒绝"""
    from hetu.system.future import _build_future_row

    ctx = new_ctx()
    with pytest.raises(ValueError, match="1024"):
        _build_future_row(ctx, -1, "add_rls_comp_value", ("x" * 1100,), timeout=10)
    # repr 为 np.int64(5)，literal_eval 不认
    with pytest.raises(AssertionError):
        _build_future_row(ctx, -1, "add_rls_comp_value", (np.int64(5),), timeout=10)
    with pytest.raises(AssertionError):
        _build_future_row(ctx, -1, "add_rls_comp_value", (object(),), timeout=10)


async def _insert_future_row(fc_tbl, row):
    async with fc_tbl.session() as session:
        await session.using(fc_tbl.comp_cls).insert(row)


async def _get_future_row(fc_tbl, row_id):
    async with fc_tbl.session() as session:
        return await session.using(fc_tbl.comp_cls).get(id=row_id)


async def test_timeout_zero_call_deleted_on_pop(
    monkeypatch, test_app, tbl_mgr, executor
):
    """timeout=0 的未来调用不保证成功：取出时就删掉，之后不会再被取出、重复执行"""
    from hetu.system.future import (
        FutureCalls,
        _build_future_row,
        exec_future_call,
        pop_upcoming_call,
    )

    await executor.execute("login", 1020)
    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    row = _build_future_row(executor.context, -1, "add_rls_comp_value", (4,), timeout=0)
    await _insert_future_row(fc_tbl, row)

    last_time = time.time() + 2
    monkeypatch.setattr(time, "time", lambda: last_time)
    call = await pop_upcoming_call(fc_tbl)
    assert call and call.id == row.id and call.timeout == 0
    assert await _get_future_row(fc_tbl, row.id) is None
    assert await pop_upcoming_call(fc_tbl) is None

    # 不走 call_lock 也能正常执行
    assert await exec_future_call(call, executor.context.systems, fc_tbl)
    ok, _ = await executor.execute("test_rls_comp_value", 104)
    assert ok


async def test_exec_future_call_missing_system(
    monkeypatch, caplog, test_app, tbl_mgr, executor
):
    """已持久化的未来调用，目标 System 后来被代码删掉了：记 error、返回 False，
    任务不丢（已按 timeout 顺延），代码修好后还会再触发"""
    from hetu.system.future import (
        FutureCalls,
        _build_future_row,
        exec_future_call,
        pop_upcoming_call,
    )

    await executor.execute("login", 1020)
    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    row = _build_future_row(
        executor.context, -1, "add_rls_comp_value", (4,), timeout=10
    )
    row.system = "removed_system"
    await _insert_future_row(fc_tbl, row)

    last_time = time.time() + 2
    monkeypatch.setattr(time, "time", lambda: last_time)
    call = await pop_upcoming_call(fc_tbl)
    assert call and call.id == row.id

    with caplog.at_level(logging.ERROR, logger="HeTu.root"):
        ok = await exec_future_call(call, executor.context.systems, fc_tbl)
    assert ok is False
    assert "removed_system" in caplog.text
    kept = await _get_future_row(fc_tbl, row.id)
    assert kept is not None and kept.scheduled == last_time + 10


async def test_exec_future_call_system_error_keeps_call(
    monkeypatch, caplog, test_app, tbl_mgr, executor
):
    """目标 System 执行抛异常：异常不外抛（不拖垮 future_call_task 的循环），记日志；
    任务不删除，按 timeout 顺延后重试"""
    from hetu.system.future import (
        FutureCalls,
        _build_future_row,
        exec_future_call,
        pop_upcoming_call,
    )

    await executor.execute("login", 1020)
    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    # add_rls_comp_value 里 row.value += "oops"：int32 加字符串，System 内抛 TypeError
    row = _build_future_row(
        executor.context, -1, "add_rls_comp_value", ("oops",), timeout=10
    )
    await _insert_future_row(fc_tbl, row)

    last_time = time.time() + 2
    monkeypatch.setattr(time, "time", lambda: last_time)
    call = await pop_upcoming_call(fc_tbl)
    assert call and call.id == row.id

    with caplog.at_level(logging.ERROR, logger="HeTu.root"):
        await exec_future_call(call, executor.context.systems, fc_tbl)
    assert "add_rls_comp_value('oops',)" in caplog.text
    kept = await _get_future_row(fc_tbl, row.id)
    assert kept is not None and kept.scheduled == last_time + 10


async def test_pop_upcoming_call_error_names_the_call(
    monkeypatch, test_app, tbl_mgr, executor
):
    """取出事务失败（竞态以外的错误，如后端断线）时，异常要带上正在取出的是哪条调用：
    call 出了 pop_upcoming_call 就没了，任务循环的错误日志里看不出是哪条"""
    import traceback
    from unittest.mock import patch

    from hetu.system.future import FutureCalls, _build_future_row, pop_upcoming_call

    await executor.execute("login", 1020)
    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    row = _build_future_row(
        executor.context, -1, "add_rls_comp_value", (4,), timeout=10
    )
    await _insert_future_row(fc_tbl, row)

    async def broken_commit(_idmap):
        raise ConnectionError("提交时后端断线")

    last_time = time.time() + 2
    monkeypatch.setattr(time, "time", lambda: last_time)
    with (
        patch.object(fc_tbl.backend.master, "commit", new=broken_commit),
        pytest.raises(ConnectionError) as exc_info,
    ):
        await pop_upcoming_call(fc_tbl)
    # 任务循环用 logger.exception 记日志，打出来的就是这段 traceback
    logged = "".join(traceback.format_exception(exc_info.value))
    assert "add_rls_comp_value(4,)" in logged
    assert str(row.id) in logged


@pytest.mark.timeout(20)
async def test_pop_upcoming_call_gives_up_on_race_without_error(
    test_app, tbl_mgr, new_ctx
):
    """取出时一直撞竞态（每次都被别的 worker 抢先）：放弃、返回 None，下一轮再取。多 worker 抢
    同一批到期调用时这是常态，不该变成任务循环里的一条错误日志外加 1 秒退避，也不该做
    0.1~1.6 秒的指数退避"""
    from unittest.mock import patch

    from hetu.data.backend.base import RaceCondition
    from hetu.system.future import FutureCalls, pop_upcoming_call

    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    await _insert_due_calls(fc_tbl, new_ctx(), 1)

    async def always_race(_idmap):
        raise RaceCondition("RACE: 每次都被别的 worker 抢先")

    started = time.monotonic()
    with patch.object(fc_tbl.backend.master, "commit", new=always_race):
        assert await pop_upcoming_call(fc_tbl) is None
    assert time.monotonic() - started < 0.5


async def test_pop_upcoming_call_skips_call_taken_by_other_worker(
    monkeypatch, test_app, tbl_mgr, new_ctx
):
    """读到索引之后、读行之前，这条已被别的 worker 取走（scheduled 已顺延、版本 +1）：读回的行
    已不到期，不能再取走它。提交时的版本校验拦不住（读回的就是新版本），只能核对读回的
    scheduled；否则两个 worker 都拿到这一条，各执行一遍"""
    from hetu.system.future import FutureCalls, pop_upcoming_call

    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    (taken,) = await _insert_due_calls(fc_tbl, new_ctx(), 1)
    state = _take_after_index_read(monkeypatch, fc_tbl, int(taken.id))

    assert await pop_upcoming_call(fc_tbl) is None
    assert state["taken"]
    # 还是别人的租约，没被改写
    row = await _get_future_row(fc_tbl, taken.id)
    assert row is not None and row.scheduled > time.time() + 500


async def test_exec_future_call_retries_delete_on_race(test_app, tbl_mgr, new_ctx):
    """执行成功后删行撞竞态（读到滞后副本上的旧版本，或这条刚被超时重投改了版本）：要重试删掉，
    不能把 RaceCondition 抛给任务循环（那会记一条错误、退避 1 秒，还跳过后面的收尾）"""
    from unittest.mock import patch

    from hetu.data.backend.base import RaceCondition
    from hetu.system.future import FutureCalls, exec_future_call, pop_upcoming_call

    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    (row,) = await _insert_due_calls(fc_tbl, new_ctx(), 1, value=4)
    call = await pop_upcoming_call(fc_tbl)
    assert call is not None and call.id == row.id

    master = fc_tbl.backend.master
    orig_commit = master.commit
    raced = []

    async def race_once_on_delete(idmap):
        ref = idmap.first_reference()
        if not raced and ref is not None and ref.comp_cls is fc_tbl.comp_cls:
            raced.append(ref)
            raise RaceCondition("RACE: Version mismatch（模拟读到滞后副本）")
        return await orig_commit(idmap)

    caller = _task_callers(tbl_mgr)["server1"]
    with patch.object(master, "commit", new=race_once_on_delete):
        assert await exec_future_call(call, caller, fc_tbl)
    assert raced
    assert await _get_future_row(fc_tbl, row.id) is None
    assert await _counter_value(tbl_mgr, test_app) == 104


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


def test_call_lock_uuid_tells_keyed_calls_apart():
    """一次性未来调用的 call lock uuid：雪花 id 不会复用，就是 id；按 key 的调用（负 id）同一个
    key 先后 ensure 的两条 id 相同，靠建行时间区分。最长也放得进 SystemLock.uuid"""
    from hetu.system.future import FutureCalls, _call_lock_uuid
    from hetu.system.lock import SystemLock

    row = FutureCalls.new_row()
    row.created = 1759400000.123
    assert _call_lock_uuid(row) == str(row.id)

    keyed = FutureCalls.new_row(id_=-(2**63))  # 最长的负 id
    keyed.created = 1759400000.123
    first = _call_lock_uuid(keyed)
    keyed.created += 0.002
    assert _call_lock_uuid(keyed) != first
    keyed.created = 16_000_000_000.0  # 2477 年
    width = SystemLock.dtype_map_["uuid"].itemsize // np.dtype("<U1").itemsize
    assert len(_call_lock_uuid(keyed)) <= width


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


async def test_ensure_skips_preexisting_row(test_app, tbl_mgr, executor):
    """表里已有同 key 行时（代表上次开服播种的持久化行），再 ensure 不新增、不报错、返回同 id"""
    from hetu.system.future import FutureCalls, _build_future_row, _key_to_id

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


async def test_ensure_one_shot_executes(monkeypatch, test_app, tbl_mgr, executor):
    """ensure 的一次性调用（负数 id）能被 pop + exec，执行后删除，目标 System 生效"""
    from hetu.system.future import (
        FutureCalls,
        _key_to_id,
        exec_future_call,
        pop_upcoming_call,
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
    # 注：测试复用已 login 的 executor（caller=1020）；生产中 future_call_task 的 caller 恒为 0
    ok = await exec_future_call(call, executor.context.systems, fc_tbl)
    assert ok

    # 执行成功后一次性任务被删除
    async with fc_tbl.session() as session:
        repo = session.using(FutureCallsTableCopy1)
        assert await repo.get(id=fid) is None

    # 目标 System 真的执行了：RLSComp.value = 100 + 4
    ok, _ = await executor.execute("test_rls_comp_value", 104)
    assert ok


async def test_exec_future_call_keeps_reensured_call(
    monkeypatch, test_app, tbl_mgr, executor
):
    """按 key 的一次性调用取出之后、执行完之前，这个 key 被 cancel 又重新 ensure（同一个 id 的
    新调用）：执行完收尾时只删自己取出的那一条，不能把新建的那条删掉"""
    from hetu.system.future import FutureCalls, exec_future_call, pop_upcoming_call

    await executor.execute("login", 1020)
    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    ok, fid = await executor.execute("ensure_rls_comp_value_future", "once", 4, False)
    assert ok

    last_time = time.time() + 1
    monkeypatch.setattr(time, "time", lambda: last_time)
    call = await pop_upcoming_call(fc_tbl)
    assert call and call.id == fid

    # 执行期间这个 key 被重配：cancel 掉旧的，ensure 一条新的
    ok, deleted = await executor.execute("cancel_rls_comp_value_future", "once")
    assert ok and deleted is True
    ok, again = await executor.execute("ensure_rls_comp_value_future", "once", 9, False)
    assert ok and again == fid

    assert await exec_future_call(call, executor.context.systems, fc_tbl)
    kept = await _get_future_row(fc_tbl, fid)
    assert kept is not None and "9" in kept.args


async def test_reensured_one_shot_call_runs_again(
    monkeypatch, test_app, tbl_mgr, executor
):
    """按 key 的一次性调用执行完以后，同一个 key 再 ensure 一条：新的这条也要执行"""
    from hetu.system.future import FutureCalls, exec_future_call, pop_upcoming_call

    await executor.execute("login", 1020)
    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    start = time.time()
    for round_, expect in ((1, 104), (2, 108)):
        ok, _ = await executor.execute("ensure_rls_comp_value_future", "once", 4, False)
        assert ok
        now = start + 2 * round_
        monkeypatch.setattr(time, "time", lambda now=now: now)
        call = await pop_upcoming_call(fc_tbl)
        assert call is not None
        assert await exec_future_call(call, executor.context.systems, fc_tbl)
        ok, _ = await executor.execute("test_rls_comp_value", expect)
        assert ok, round_


@pytest.mark.timeout(20)
async def test_future_call_task_backs_off_on_persistent_error(
    test_app, tbl_mgr, monkeypatch
):
    """后端已关闭（或任何持续报错）时，future_call_task 不能空转刷屏：
    出错后要退避再重试（否则同步抛出的异常让循环永不挂起，事件循环被饿死，
    Sanic 关服时连 CancelledError 都送不进去）。"""
    from hetu.data.backend import Backend
    from hetu.system import future

    calls = {"n": 0}

    def broken_servant(_self):
        # 主循环每轮先去 servant 上看有没有到期的：后端关了，这里同步抛出
        calls["n"] += 1
        raise ConnectionError("连接已关闭，已调用过close")

    monkeypatch.setattr(Backend, "servant", property(broken_servant))
    task = asyncio.create_task(future.future_call_task(_task_app(tbl_mgr)))
    await asyncio.sleep(2.5)  # 若循环不让出，这一句永远回不来（timeout 兜底）
    assert calls["n"] <= 4, calls  # 退避 1 s：2.5 s 内最多两三次
    task.cancel()
    try:  # 取消后要能及时退出（正常 break 返回，或抛 CancelledError 都算）
        await asyncio.wait_for(task, 3)
    except asyncio.CancelledError:
        pass
    assert task.done()


async def _lock_uuids(lock_tbl) -> list[str]:
    """这张 call lock 表里所有锁的 uuid（先等副本同步）"""
    await lock_tbl.backend.wait_for_synced()
    async with lock_tbl.session() as session:
        rows = await session.using(lock_tbl.comp_cls).range(
            called=(0, time.time() + 3600), limit=-1
        )
    return sorted(str(uuid) for uuid in rows.uuid)


async def _add_call_lock(lock_tbl, uuid: str, age: float) -> None:
    """直接插一行 call lock，called 为 age 秒前"""
    comp = lock_tbl.comp_cls
    async with lock_tbl.session() as session:
        row = comp.new_row()
        row.uuid, row.called = uuid, time.time() - age
        row.name = comp.name_.partition(":")[2]  # 副本后缀就是 System 名
        await session.using(comp).insert(row)


async def test_exec_future_call_keeps_call_lock(test_app, tbl_mgr, new_ctx):
    """执行成功后不立即删 call lock，留到保留期后由定期清理删：同一条调用还有别的执行在路上时
    （超时重投、卡住的 worker），它查锁才看得到已经执行过"""
    from hetu.system.future import FutureCalls, exec_future_call, pop_upcoming_call
    from hetu.system.lock import SystemLock

    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    await _insert_due_calls(fc_tbl, new_ctx(), 1)
    call = await pop_upcoming_call(fc_tbl)
    assert call is not None
    assert await exec_future_call(call, _task_callers(tbl_mgr)["server1"], fc_tbl)

    lock_tbl = tbl_mgr.get_table(SystemLock.duplicate("pytest", "add_rls_comp_value"))
    assert await _lock_uuids(lock_tbl) == [str(call.id)]


async def test_exec_future_call_twice_runs_system_once(test_app, tbl_mgr, new_ctx):
    """同一条调用执行了两次（第一份执行完以后，超时重投或卡住的另一份才去查锁）：目标 System
    只能生效一次。原来执行完立即删锁，后到的那份查不到锁就会再执行一遍"""
    from hetu.system.future import FutureCalls, exec_future_call, pop_upcoming_call

    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    await _insert_due_calls(fc_tbl, new_ctx(), 1, value=4)
    call = await pop_upcoming_call(fc_tbl)
    assert call is not None
    caller = _task_callers(tbl_mgr)["server1"]
    assert await exec_future_call(call, caller, fc_tbl)
    await fc_tbl.backend.wait_for_synced()
    assert await exec_future_call(call, caller, fc_tbl)
    assert await _counter_value(tbl_mgr, test_app) == 104


async def test_clean_expired_call_locks_retention_and_on_start(
    mod_auto_backend, new_component_env, new_clusters_env
):
    """清锁只清 called 早于保留期的；定期清理跳过 on_start 的锁表（"每次开服只跑一次"靠它，
    worker 在保留期之后崩溃重启也不能再跑一遍），启动时的兜底清理（7 天）照旧清它"""
    from hetu.data import BaseComponent, define_component, property_field
    from hetu.manager import ComponentTableManager
    from hetu.system import SystemClusters, define_system
    from hetu.system.lock import SystemLock, clean_expired_call_locks

    @define_component(namespace="pytest", force=True)
    class LockedComp(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)

    @define_system(namespace="pytest", components=(LockedComp,), call_lock=True)
    async def locked_sys(ctx):
        pass

    @define_system(namespace="pytest", components=(LockedComp,), on_start=True)
    async def boot_sys(ctx):
        pass

    SystemClusters().build_clusters("pytest")
    backend = mod_auto_backend()
    tbl_mgr = ComponentTableManager("pytest", "server1", {"default": backend})
    tbl_mgr._flush_all(force=True)

    retention = 600.0
    locked_tbl = tbl_mgr.get_table(SystemLock.duplicate("pytest", "locked_sys"))
    boot_tbl = tbl_mgr.get_table(SystemLock.duplicate("pytest", "boot_sys"))
    await _add_call_lock(locked_tbl, "old", retention * 2)
    await _add_call_lock(locked_tbl, "fresh", retention / 2)
    await _add_call_lock(boot_tbl, "boot-old", retention * 2)
    await backend.wait_for_synced()

    assert await clean_expired_call_locks(tbl_mgr, retention, skip_on_start=True) == 1
    assert await _lock_uuids(locked_tbl) == ["fresh"]
    assert await _lock_uuids(boot_tbl) == ["boot-old"]

    assert await clean_expired_call_locks(tbl_mgr, retention) == 1
    assert await _lock_uuids(boot_tbl) == []


async def test_build_future_row_timeout_within_lock_retention(
    monkeypatch, test_app, new_ctx
):
    """一次性调用（非 recurring、timeout 非 0）的 timeout 不能超过 call lock 保留期的一半：执行
    成功后 worker 没来得及删行就挂了，这条会在 timeout 后重投，那时锁必须还在"""
    from hetu.system import lock
    from hetu.system.future import _build_future_row

    monkeypatch.setattr(lock, "CALL_LOCK_RETENTION", 100, raising=False)
    ctx = new_ctx()
    with pytest.raises(ValueError, match="CALL_LOCK_RETENTION"):
        _build_future_row(ctx, -1, "add_rls_comp_value", (1,), timeout=51)
    _build_future_row(ctx, -1, "add_rls_comp_value", (1,), timeout=50)
    # recurring 与 timeout=0 的调用不加锁，不受限
    _build_future_row(ctx, -1, "add_rls_comp_value", (1,), timeout=3600, recurring=True)
    _build_future_row(ctx, -1, "add_rls_comp_value", (1,), timeout=0)


@pytest.mark.timeout(30)
async def test_future_call_task_cleans_expired_call_locks(
    monkeypatch, test_app, tbl_mgr
):
    """call lock 保留 CALL_LOCK_RETENTION 秒，由 future_call_task 定期清理（原来只在 worker
    启动时清 7 天前的）"""
    from hetu.system import future, lock
    from hetu.system.lock import SystemLock

    monkeypatch.setattr(lock, "CALL_LOCK_RETENTION", 5, raising=False)
    monkeypatch.setattr(future, "CALL_LOCK_CLEAN_INTERVAL", 0.2, raising=False)
    lock_tbl = tbl_mgr.get_table(SystemLock.duplicate("pytest", "add_rls_comp_value"))
    await _add_call_lock(lock_tbl, "expired", 60)
    await _add_call_lock(lock_tbl, "fresh", 0)

    async def expired_gone():
        return await _lock_uuids(lock_tbl) == ["fresh"]

    assert await _run_future_task_until(tbl_mgr, expired_gone, timeout=4)


async def test_clean_expired_call_locks_in_batches(monkeypatch, test_app, tbl_mgr):
    """过期的锁多于一批时连续分批清完"""
    from hetu.system import lock
    from hetu.system.lock import SystemLock

    monkeypatch.setattr(lock, "CLEAN_BATCH", 2)
    lock_tbl = tbl_mgr.get_table(SystemLock.duplicate("pytest", "add_rls_comp_value"))
    for i in range(9):
        await _add_call_lock(lock_tbl, f"old{i}", 3600)
    await _add_call_lock(lock_tbl, "fresh", 0)
    await lock_tbl.backend.wait_for_synced()

    assert await lock.clean_expired_call_locks(tbl_mgr, 60) == 9
    assert await _lock_uuids(lock_tbl) == ["fresh"]


async def test_clean_expired_call_locks_yields_on_race(test_app, tbl_mgr):
    """清锁撞竞态（别的 worker 也在清这张表）：让给别人清，不抛异常"""
    from unittest.mock import patch

    from hetu.data.backend.base import RaceCondition
    from hetu.system.lock import SystemLock, clean_expired_call_locks

    lock_tbl = tbl_mgr.get_table(SystemLock.duplicate("pytest", "add_rls_comp_value"))
    await _add_call_lock(lock_tbl, "old", 3600)
    await lock_tbl.backend.wait_for_synced()

    async def race(_idmap):
        raise RaceCondition("RACE: 别的 worker 也在清")

    with patch.object(lock_tbl.backend.master, "commit", new=race):
        assert await clean_expired_call_locks(tbl_mgr, 60) == 0
    assert await _lock_uuids(lock_tbl) == ["old"]


@pytest.mark.timeout(30)
async def test_future_call_task_startup_sweeps_week_old_locks(
    monkeypatch, test_app, tbl_mgr
):
    """worker 启动时兜底清一次 7 天前的 call lock（定期清理不碰的 on_start 锁靠它清）"""
    from hetu.system import future
    from hetu.system.lock import SystemLock

    monkeypatch.setattr(future, "CALL_LOCK_CLEAN_INTERVAL", 3600)  # 只看启动时那次
    lock_tbl = tbl_mgr.get_table(SystemLock.duplicate("pytest", "add_rls_comp_value"))
    await _add_call_lock(lock_tbl, "week-old", 8 * 24 * 3600)
    await _add_call_lock(lock_tbl, "fresh", 3600)

    async def swept():
        return await _lock_uuids(lock_tbl) == ["fresh"]

    assert await _run_future_task_until(tbl_mgr, swept, timeout=4)


async def test_run_due_calls_drain_limit_per_table(
    monkeypatch, test_app, tbl_mgr, new_ctx
):
    """一张表一轮最多连续执行 DRAIN_PER_TABLE 条就去看别的表，剩下的下一轮接着执行"""
    from hetu.system import future

    monkeypatch.setattr(future, "DRAIN_PER_TABLE", 2)
    fc_tbl = tbl_mgr.get_table(future.FutureCalls.duplicate("pytest", "copy1"))
    callers = _task_callers(tbl_mgr)
    await _insert_due_calls(fc_tbl, new_ctx(), 3)

    assert await future.run_due_calls([fc_tbl], callers) == 0
    assert await _counter_value(tbl_mgr, test_app) == 102
    await fc_tbl.backend.wait_for_synced()
    assert await future.run_due_calls([fc_tbl], callers) == 0
    assert await _counter_value(tbl_mgr, test_app) == 103


async def test_run_due_calls_retries_soon_when_due_call_taken(
    monkeypatch, test_app, tbl_mgr, new_ctx
):
    """看到有到期的却一条没取到（被别的 worker 抢先、副本还没同步）：隔
    CONTENDED_RETRY_DELAY 秒就再扫，不睡满 1 秒"""
    from hetu.system import future

    fc_tbl = tbl_mgr.get_table(future.FutureCalls.duplicate("pytest", "copy1"))
    await _insert_due_calls(fc_tbl, new_ctx(), 1)

    async def taken_by_others(_tbl):
        return None

    monkeypatch.setattr(future, "pop_upcoming_call", taken_by_others)
    frozen = time.time()
    monkeypatch.setattr(time, "time", lambda: frozen)
    delay = await future.run_due_calls([fc_tbl], _task_callers(tbl_mgr))
    assert delay == pytest.approx(future.CONTENDED_RETRY_DELAY)


async def test_pop_upcoming_call_error_before_row_read(
    monkeypatch, test_app, tbl_mgr, new_ctx
):
    """读行时就出错（如后端断线）：原样抛出；还没读到是哪条，不挂调用信息"""
    from hetu.system.future import FutureCalls, pop_upcoming_call

    fc_tbl = tbl_mgr.get_table(FutureCalls.duplicate("pytest", "copy1"))
    await _insert_due_calls(fc_tbl, new_ctx(), 1)

    def broken_get(*_args, **_kwargs):
        raise ConnectionError("读行时后端断线")

    backend = fc_tbl.backend
    for client in (backend.master, backend.servant):
        monkeypatch.setattr(type(client), "get", broken_get)
    with pytest.raises(ConnectionError) as exc_info:
        await pop_upcoming_call(fc_tbl)
    assert not getattr(exc_info.value, "__notes__", None)
