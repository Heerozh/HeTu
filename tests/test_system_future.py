import time
import pytest

from hetu.common.snowflake_id import SnowflakeID
from hetu.endpoint.executor import EndpointExecutor

SnowflakeID().init(1, 0)


async def test_future_call_create(test_app, comp_mgr, executor: EndpointExecutor):
    """测试创建未来调用任务是否正确"""
    time_time = time.time

    # 创建一个未来调用
    from hetu.system.future import FutureCalls

    await executor.execute("login", 1020)

    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = comp_mgr.get_table(FutureCallsTableCopy1)

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


async def test_sleep_for_upcoming(test_app, comp_mgr, executor: EndpointExecutor):
    """测试sleep_for_upcoming的等待逻辑是否正确"""
    time_time = time.time

    # 创建一个未来调用
    from hetu.system.future import FutureCalls

    await executor.execute("login", 1020)

    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = comp_mgr.get_table(FutureCallsTableCopy1)

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
    monkeypatch, test_app, comp_mgr, executor: EndpointExecutor
):
    """测试pop_upcoming_call取出任务逻辑是否正确"""
    time_time = time.time

    # 创建一个未来调用
    from hetu.system.future import FutureCalls

    await executor.execute("login", 1020)

    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = comp_mgr.get_table(FutureCallsTableCopy1)

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

    comp_mgr = ComponentTableManager("ns1", "server1", backends)

    assert comp_mgr.get_table(future_ns1[0]) is not None
    assert comp_mgr.get_table(future_ns2[0]) is None
