import time
import pytest


async def test_future_call_create(mod_test_app, comp_mgr, executor):
    time_time = time.time

    # 创建一个未来调用
    from hetu.system.future import FutureCalls

    await executor.exec("login", 1020)
    backend = comp_mgr.backends.get("default")

    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = comp_mgr.get_table(FutureCallsTableCopy1)

    ok, uuid = await executor.exec("add_rls_comp_value_future", 4, False)

    # 测试未来调用数据是否正确
    async with backend.transaction(fc_tbl.cluster_id) as session:
        fc_uow = fc_tbl.attach(session)
        expire_time = time_time() + 1.1
        rows = await fc_uow.query("scheduled", left=0, right=expire_time, limit=1)
        assert rows[0].id == uuid
        assert rows[0].timeout == 10
        assert rows[0].system == "add_rls_comp_value"
        assert rows[0].recurring == False
        assert rows[0].owner == 1020


async def test_sleep_for_upcoming(mod_test_app, comp_mgr, executor):
    time_time = time.time

    # 创建一个未来调用
    from hetu.system.future import FutureCalls

    await executor.exec("login", 1020)
    backend = comp_mgr.backends.get("default")

    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = comp_mgr.get_table(FutureCallsTableCopy1)

    ok, uuid = await executor.exec("add_rls_comp_value_future", 4, False)

    # 获取任务到期时间
    async with backend.transaction(fc_tbl.cluster_id) as session:
        fc_uow = fc_tbl.attach(session)
        expire_time = time_time() + 1.1
        rows = await fc_uow.query("scheduled", left=0, right=expire_time, limit=1)
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
    assert call.id == uuid

    # 再次调用sleep应该返回无任务False，并睡1秒
    start = time_time()
    have_task = await sleep_for_upcoming(fc_tbl)
    assert time_time() - start > 1
    assert not have_task


async def test_pop_upcoming_call(monkeypatch, mod_test_app, comp_mgr, executor):
    time_time = time.time

    # 创建一个未来调用
    from hetu.system.future import FutureCalls

    await executor.exec("login", 1020)
    backend = comp_mgr.backends.get("default")

    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = comp_mgr.get_table(FutureCallsTableCopy1)

    ok, uuid = await executor.exec("add_rls_comp_value_future", 4, False)

    # 测试pop_upcoming_call是否正常
    from hetu.system.future import pop_upcoming_call

    # 让时间延迟，才能pop出来
    last_time = time_time() + 1
    monkeypatch.setattr(time, "time", lambda: last_time)
    call = await pop_upcoming_call(fc_tbl)
    assert call.id == uuid

    # 检测pop的task数据是否修改了
    async with backend.transaction(fc_tbl.cluster_id) as session:
        fc_trx = fc_tbl.attach(session)
        row = await fc_trx.select(uuid, "uuid")
        assert row.last_run == last_time
        assert row.scheduled == last_time + 10

    # 测试exec_future_call调用是否正常
    last_time = time_time() + 2
    from hetu.system.future import exec_future_call

    # 此时future_call用的是已login的executor，实际运行future_call不可能有login的executor
    ok = await exec_future_call(call, executor, fc_tbl)
    assert ok
    # 检测task是否删除
    async with backend.transaction(fc_tbl.cluster_id) as session:
        fc_trx = fc_tbl.attach(session)
        row = await fc_trx.select(uuid, "uuid")
        assert row is None
    # 测试hp
    ok, _ = await executor.exec("test_rls_comp_value", 100 + 4)
    assert ok


def test_duplicate_bug(mod_auto_backend, new_clusters_env):
    """测试未来调用常用的duplicated的system，component是否会按namespace隔离"""
    from hetu.system import define_system, Context
    from hetu.data.component import Permission

    # 定义2个不同的namespace的future call
    @define_system(
        namespace="ns1",
        permission=Permission.EVERYBODY,
        subsystems=("create_future_call:copy1",),
    )
    async def use_future_namespace1(ctx: Context, value, recurring):
        return await ctx["create_future_call:copy1"](
            ctx, -1, "any_other_system", value, timeout=10, recurring=recurring
        )

    @define_system(
        namespace="ns2",
        permission=Permission.EVERYBODY,
        subsystems=("create_future_call:copy1",),
    )
    async def use_future_namespace2(ctx: Context, value, recurring):
        return await ctx["create_future_call:copy1"](
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
    assert future_ns1[0].component_name_ == future_ns2[0].component_name_

    # 检查component table manager是否正确隔离
    backend_component_table, get_or_create_backend = mod_auto_backend
    backend = get_or_create_backend()
    backends = {"default": backend}
    comp_tbl_classes = {"default": backend_component_table}

    from hetu import ComponentTableManager

    comp_mgr = ComponentTableManager("ns1", "server1", backends, comp_tbl_classes)

    assert comp_mgr.get_table(future_ns1[0]) is not None
    assert comp_mgr.get_table(future_ns2[0]) is None
