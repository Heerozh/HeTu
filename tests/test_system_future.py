import time
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


async def test_sleep_for_upcoming(test_app, tbl_mgr, executor: EndpointExecutor):
    """测试sleep_for_upcoming的等待逻辑是否正确"""
    time_time = time.time

    # 创建一个未来调用
    from hetu.system.future import FutureCalls

    await executor.execute("login", 1020)

    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = tbl_mgr.get_table(FutureCallsTableCopy1)

    ok, uuid = await executor.execute("add_rls_comp_value_future", 4, False)

    # 获取任务到期时间
    async with fc_tbl.session() as session:
        repo = session.using(FutureCallsTableCopy1)
        expire_time = time_time() + 1.1
        rows = await repo.range("scheduled", 0, expire_time, limit=1)
        expire_time = rows[0].scheduled

    # 测试sleep_for_upcoming(等待下一个到期任务)是否正常
    from hetu.system.future import sleep_for_upcoming

    have_task = await sleep_for_upcoming(fc_tbl)
    # 检测当前时间是否~>任务到期时间
    assert time_time() > expire_time
    assert expire_time, pytest.approx(time_time(), abs=0.1)
    assert have_task

    # 再调用应该只Sleep 0秒
    start = time_time()
    have_task = await sleep_for_upcoming(fc_tbl)
    assert time_time() - start < 0.1
    assert have_task

    # 删除未来任务
    from hetu.system.future import pop_upcoming_call

    call = await pop_upcoming_call(fc_tbl)
    assert call
    assert call.id == uuid

    # 再次调用sleep应该返回无任务False，并睡1秒
    start = time_time()
    have_task = await sleep_for_upcoming(fc_tbl)
    assert time_time() - start > 1
    assert not have_task


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


def test_duplicate_bug(mod_auto_backend, new_clusters_env):
    """测试未来调用常用的duplicated的system，component是否会按namespace隔离"""
    from hetu.system import define_system, SystemContext
    from hetu.data.component import Permission

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
