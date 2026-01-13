import logging
import time
from typing import cast

import pytest
import hetu
from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import RowFormat
from hetu.endpoint.executor import EndpointExecutor

SnowflakeID().init(1, 0)


async def test_call_not_exist(mod_test_app, mod_executor):
    # 测试不存在的endpoint call
    ok, _ = await mod_executor.execute("not_exist_system", "any_value")
    assert not ok

    # 测试不存在的system call
    with pytest.raises(ValueError, match="不存在"):
        await mod_executor.context.systems.call("not_exist_system", "any_value")


async def test_call_args_error(mod_test_app, mod_executor, caplog):
    # 测试参数少一个
    ok, _ = await mod_executor.execute("login")
    assert not ok

    # 测试参数多一个
    ok, _ = await mod_executor.execute("login", 1234, True, "extra_arg")
    assert not ok

    assert len(caplog.records) == 4  # 因为执行器会记录2份日志(加一份replay)
    assert "1-2" in caplog.records[0].msg
    assert "1-2" in caplog.records[2].msg


async def test_call_no_permission(mod_test_app, mod_executor):
    # 测试无权限call
    ok, _ = await mod_executor.execute("add_rls_comp_value", 9)
    assert not ok


async def test_call_none_permission(mod_test_app, mod_executor):
    # 测试无endpoint的system，即使admin也不能调用
    mod_executor.context.group = "admin"
    ok, _ = await mod_executor.execute("uncallable", 9)
    assert not ok


async def test_system_with_rls(mod_test_app, executor):
    # 测试登录
    ok, _ = await executor.execute("login", 1234)
    assert ok

    # 测试有权限call
    ok, _ = await executor.execute("add_rls_comp_value", 9)
    assert ok

    # 去数据库读取内容看是否正确
    ok, _ = await executor.execute("test_rls_comp_value", 100 + 9)
    assert ok

    # 故意传不正确的值
    ok, _ = await executor.execute("test_rls_comp_value", 100 + 10)
    assert not ok


@pytest.mark.timeout(20)
async def test_unique_violate_bug(mod_test_app, executor, caplog):
    # BUG: upsert时，unique违反不应该当成RaceCondition，因为这样会导致无限重试卡死
    # 只有upsert的where参数相关的Unique违反才能当成RaceCondition重试

    # 登录用户1234
    ok, _ = await executor.execute("login", 1234)
    assert ok

    ok, _ = await executor.execute("create_row", 20, 99, "zz")
    assert ok

    # unique违反 应该失败，而不是无限RaceCondition重试
    ok, _ = await executor.execute("create_row", 21, 99, "zz")
    assert not ok

    assert "RaceCondition" not in caplog.text
    assert "UniqueViolation" in caplog.text


@pytest.mark.timeout(20)
async def test_unique_violate_bug2(mod_test_app, executor, caplog):
    # BUG: upsert时，unique违反不应该当成RaceCondition，因为这样会导致无限重试卡死
    # 这里是用连续upsert 2次

    # 登录用户1234
    ok, _ = await executor.execute("login", 1234)
    assert ok

    ok, _ = await executor.execute("create_row_2_upsert", 99, "zz")
    assert ok

    ok, _ = await executor.execute("create_row_2_upsert", 18, "zz")
    assert not ok

    assert "RaceCondition" not in caplog.text
    assert "UniqueViolation" in caplog.text


async def test_slow_log(mod_test_app, executor, caplog):
    # 测试慢日志输出
    import hetu.common.slowlog
    from hetu.system.caller import SLOW_LOG

    hetu.common.slowlog.SLOW_LOG_TIME_THRESHOLD = 0.1
    SLOW_LOG.log_interval = 0  # 每次都打印

    # 登录用户1234
    ok, _ = await executor.execute("login", 1234)
    assert ok

    # 添加3行数据
    ok, _ = await executor.execute("create_row", executor.context.caller, 10, "b")
    assert ok
    ok, _ = await executor.execute("create_row", 3, 0, "a")  # b-d query不符合
    assert ok
    ok, _ = await executor.execute("create_row", 4, 19, "c")
    assert ok
    ok, _ = await executor.execute("create_row", 5, 20, "d")
    assert ok
    ok, _ = await executor.execute("create_row", 6, 21, "e")  # 0-20和b-d query都不符合
    assert ok

    with caplog.at_level(logging.INFO, logger="HeTu"):
        ok, _ = await executor.execute("composer_system")
        assert ok

    # 检查日志输出
    assert len(caplog.records) == 4
    assert "[User_b]" in caplog.text
    assert "[User_c]" in caplog.text
    assert "[User_d]" in caplog.text
    assert "慢日志" in caplog.text
    print(caplog.text)


async def test_get_race_condition(
    mod_test_app, comp_mgr, executor: EndpointExecutor, new_ctx
):
    # 测试race
    from hetu.system.caller import SystemCaller
    from hetu.system.context import SystemContext

    direct_caller = SystemCaller("pytest", comp_mgr, new_ctx())

    import asyncio

    # 必须差距大才能保证某个task先get
    await asyncio.gather(
        executor.execute("race_upsert", 0.4), direct_caller.call("race_upsert", 0.1)
    )

    assert cast(SystemContext, executor.context).race_count == 1
    assert direct_caller.context.race_count == 0


async def test_range_race_condition(mod_test_app, comp_mgr, executor, new_ctx):
    from hetu.system.caller import SystemCaller
    from hetu.system.context import SystemContext

    # 测试race
    direct_caller = SystemCaller("pytest", comp_mgr, new_ctx())

    # 登录用户1234
    ok, _ = await executor.execute("login", 1234)
    assert ok
    # 先添加一行
    ok, _ = await executor.execute("create_row", 3, 0, "a")
    assert ok

    import asyncio

    # 必须差距大才能保证某个task先range
    await asyncio.gather(
        executor.execute("race_range", 0.4), direct_caller.call("race_range", 0.1)
    )

    assert cast(SystemContext, executor.context).race_count == 1
    assert direct_caller.context.race_count == 0


async def test_execute_system_copy(mod_test_app, comp_mgr, executor):
    ok, _ = await executor.execute("login", 1001)
    assert ok
    # 使用copy的system，应该对应的储存空间也是copy的
    ok, _ = await executor.execute("add_rls_comp_value_copy", 9)
    assert ok
    ok, _ = await executor.execute("add_rls_comp_value", 1)
    assert ok

    # 去数据库读取内容看是否正确
    ok, _ = await executor.execute("test_rls_comp_value", 100 + 1)
    assert ok
    ok, _ = await executor.execute("test_rls_comp_value_copy", 100 + 9)
    assert ok

    # 直接通过Comp读取
    RLSComp = hetu.data.ComponentDefines().get_component("pytest", "RLSComp")
    RLSCompCopy = RLSComp.duplicate("pytest", "copy1")
    copied_tbl = comp_mgr.get_table(RLSCompCopy)
    async with copied_tbl.session() as session:
        repo = session.using(RLSCompCopy)
        row = await repo.get(owner=1001)
        assert row.value == 100 + 9


async def test_execute_system_call_lock(mod_test_app, executor: EndpointExecutor):
    ok, _ = await executor.execute("login", 1101)
    assert ok
    ok, _ = await executor.execute("add_rls_comp_value", 1)
    assert ok
    ok, _ = await executor.execute("add_rls_comp_value", 1)
    assert ok

    # 测试带uuid的call应该只执行1次
    systems = executor.context.systems
    await systems.call("add_rls_comp_value", 2, uuid="uuid1")
    await systems.call("add_rls_comp_value", 3, uuid="uuid1")

    # 去数据库读取内容看是否正确
    ok, _ = await executor.execute("test_rls_comp_value", 100 + 4)
    assert ok


async def test_clean_expired_call_locks(monkeypatch, mod_test_app, comp_mgr, executor):
    time_time = time.time

    # 测试lock数据过期清理是否正常

    await executor.execute("login", 1020)
    systems = executor.context.systems
    await systems.call("add_rls_comp_value", 2, uuid="test_uuid")
    from hetu.system.lock import SystemLock

    # call lock每次会按system名复制一份ExecutionLock表
    ExecutionLock_for_system = SystemLock.duplicate("pytest", "add_rls_comp_value")
    lock_tbl = comp_mgr.get_table(ExecutionLock_for_system)
    assert lock_tbl

    from hetu.system.lock import clean_expired_call_locks

    # 未清理
    await clean_expired_call_locks(comp_mgr)
    rows = await lock_tbl.servant_range(
        "called", left=0, right=time_time(), limit=1, row_format=RowFormat.RAW
    )
    assert len(rows) == 1

    # 清理
    import datetime

    monkeypatch.setattr(
        time, "time", lambda: time_time() + datetime.timedelta(days=8).total_seconds()
    )
    await clean_expired_call_locks(comp_mgr)
    rows = await lock_tbl.servant_range(
        "called", left=0, right=0xFFFFFFFF, limit=1, row_format=RowFormat.RAW
    )
    assert len(rows) == 0
