import logging

import pytest


async def test_call_not_exist(mod_test_app, mod_executor):
    # 测试不能存在的system call
    ok, _ = await mod_executor.exec("not_exist_system", "any_value")
    assert not ok


async def test_call_no_permission(mod_test_app, mod_executor):
    # 测试无权限call
    ok, _ = await mod_executor.exec("add_rls_comp_value", 9)
    assert not ok


async def test_system_with_rls(mod_test_app, executor):
    # 测试登录
    ok, _ = await executor.exec('login', 1234)
    assert ok

    # 测试有权限call
    ok, _ = await executor.exec('add_rls_comp_value', 9)
    assert ok

    # 去数据库读取内容看是否正确
    ok, _ = await executor.exec('test_rls_comp_value', 100 - 9)
    assert ok

    # 故意传不正确的值
    ok, _ = await executor.exec('test_rls_comp_value', 100 - 10)
    assert not ok


@pytest.mark.timeout(10)
async def test_unique_violate_bug(mod_test_app, executor, caplog):
    # BUG: upsert时，unique违反不应该当成RaceCondition，因为这样会导致无限重试卡死
    # 只有upsert的where参数相关的Unique违反才能当成RaceCondition重试

    # 登录用户1234
    ok, _ = await executor.exec('login', 1234)
    assert ok

    ok, _ = await executor.exec('create_row', 20, 99, "zz")
    assert ok

    # unique违反 应该失败，而不是无限RaceCondition重试
    ok, _ = await executor.exec('create_row', 21, 99, "zz")
    assert not ok

    assert "RaceCondition" not in caplog.text
    assert "UniqueViolation" in caplog.text


@pytest.mark.timeout(10)
async def test_unique_violate_bug2(mod_test_app, executor, caplog):
    # BUG: upsert时，unique违反不应该当成RaceCondition，因为这样会导致无限重试卡死
    # 这里是连续upsert 2次时出现

    # 登录用户1234
    ok, _ = await executor.exec('login', 1234)
    assert ok

    ok, _ = await executor.exec('create_row_2_upsert', 99, "zz")
    assert not ok

    assert "RaceCondition" not in caplog.text
    assert "UniqueViolation" in caplog.text


async def test_slow_log(mod_test_app, executor, caplog):
    # 测试慢日志输出
    import hetu.common.slowlog
    hetu.common.slowlog.SLOW_LOG_TIME_THRESHOLD = 0.1

    # 登录用户1234
    ok, _ = await executor.exec('login', 1234)
    assert ok

    # 添加3行数据
    ok, _ = await executor.exec('create_row', executor.context.caller, 10, "b")
    assert ok
    ok, _ = await executor.exec('create_row', 3, 0, "a")  # b-d query不符合
    assert ok
    ok, _ = await executor.exec('create_row', 4, 19, "c")
    assert ok
    ok, _ = await executor.exec('create_row', 5, 20, "d")
    assert ok
    ok, _ = await executor.exec('create_row', 6, 21, "e")  # 0-20和b-d query都不符合
    assert ok

    with caplog.at_level(logging.INFO, logger='HeTu'):
        ok, _ = await executor.exec('composer_system')
        assert ok

    # 检查日志输出
    assert len(caplog.records) == 4
    assert "[User_b]" in caplog.text
    assert "[User_c]" in caplog.text
    assert "[User_d]" in caplog.text
    assert '慢日志' in caplog.text
    print(caplog.text)


async def test_select_race_condition(mod_test_app, comp_mgr, executor):
    # 测试race
    import hetu
    executor2 = hetu.system.SystemExecutor("pytest", comp_mgr)
    await executor2.initialize("")

    import asyncio
    # 必须差距大才能保证某个task先select
    await asyncio.gather(
        executor.exec('race_select', 0.4),
        executor2.exec("race_select", 0.1)
    )

    assert executor.context.retry_count == 1
    assert executor2.context.retry_count == 0

    # 结束连接
    await executor2.terminate()


async def test_query_race_condition(mod_test_app, comp_mgr, executor):
    # 测试race
    import hetu
    executor2 = hetu.system.SystemExecutor("pytest", comp_mgr)
    await executor2.initialize("")

    # 登录用户1234
    ok, _ = await executor.exec('login', 1234)
    assert ok
    # 先添加一行
    ok, _ = await executor.exec('create_row', 3, 0, "a")
    assert ok

    import asyncio
    # 必须差距大才能保证某个task先query
    await asyncio.gather(
        executor.exec('race_query', 0.4),
        executor2.exec("race_query", 0.1)
    )

    assert executor.context.retry_count == 1
    assert executor2.context.retry_count == 0

    # 结束连接
    await executor2.terminate()
#
# async def test_execute_system_copy(self):
#     executor = hetu.system.SystemExecutor('ssw', self.comp_mgr)
#     await executor.initialize("")
#
#     ok, _ = await executor.exec('login', 1001)
#     assert ok
#     ok, _ = await executor.exec('use_hp_copy', 9)
#     assert ok
#     ok, _ = await executor.exec('use_hp', 1)
#     assert ok
#
#     # 去数据库读取内容看是否正确
#     ok, _ = await executor.exec('test_hp', 100-1)
#     assert ok
#
#     backend = self.backends['default']
#     HP = hetu.data.ComponentDefines().get_component('ssw', 'HP')
#     hp_copy = HP.duplicate('ssw', 'copy1')
#     hp_tbl = self.comp_mgr.get_table(hp_copy)
#     async with backend.transaction(hp_tbl.cluster_id) as trx:
#         hp_trx = hp_tbl.attach(trx)
#         row = await hp_trx.select(1001, 'owner')
#         assert row.value == 100-9
#
#     # 结束连接
#     await executor.terminate()
#
#
# async def test_execute_system_call_lock(self):
#     executor = hetu.system.SystemExecutor('ssw', self.comp_mgr)
#     await executor.initialize("")
#
#     ok, _ = await executor.exec('login', 1101)
#     assert ok
#     ok, _ = await executor.exec('use_hp', 1)
#     assert ok
#     ok, _ = await executor.exec('use_hp', 1)
#     assert ok
#
#     ok, _ = await executor.execute(SystemCall('use_hp', (2,), 'uuid1'))
#     assert ok
#     ok, _ = await executor.execute(SystemCall('use_hp', (3,), 'uuid1'))
#     assert ok
#
#     # 去数据库读取内容看是否正确
#     ok, _ = await executor.exec('test_hp', 100-4)
#     assert ok
#
#     # 结束连接
#     await executor.terminate()
#
#
# @mock.patch('time.time', mock_time)
# async def test_connect(self):
#     # 先登录几个连接
#     mock_time.return_value = time()
#     executor1 = hetu.system.SystemExecutor('ssw', self.comp_mgr)
#     await executor1.initialize("")
#     await executor1.exec('login', 1)
#
#     executor2 = hetu.system.SystemExecutor('ssw', self.comp_mgr)
#     await executor2.initialize("")
#     await executor2.exec('login', 2)
#
#     ok, _ = await executor1.exec('use_hp', 1)
#     assert ok
#     ok, _ = await executor2.exec('use_hp', 1)
#     assert ok
#
#     # 测试重复登录踢出已登录用户
#     executor3 = hetu.system.SystemExecutor('ssw', self.comp_mgr)
#     await executor3.initialize("")
#     await executor3.exec('login', 1)
#
#     # 测试运行第一个连接的system，然后看是否失败
#     ok, _ = await executor1.exec('test_hp', 99)
#     assert not ok
#     ok, _ = await executor3.exec('test_hp', 99)
#     assert ok
#
#     # 测试last active超时是否踢出用户
#     # 先测试不强制踢出是否生效
#     executor4 = hetu.system.SystemExecutor('ssw', self.comp_mgr)
#     await executor4.initialize("")
#     await executor4.exec('login', 1, False)
#     ok, _ = await executor3.exec('test_hp', 99)
#     assert ok
#     # 然后是不强制踢出，但是timeout应该生效
#     executor5 = hetu.system.SystemExecutor('ssw', self.comp_mgr)
#     mock_time.return_value = time() + executor5.context.idle_timeout
#     await executor5.initialize("")
#     await executor5.exec('login', 1, False)
#
#     ok, _ = await executor3.exec('test_hp', 99)
#     assert not ok
#
#     # 结束连接
#     await executor1.terminate()
#     await executor2.terminate()
#     await executor3.terminate()
#     await executor4.terminate()
#     await executor5.terminate()
#
# @mock.patch('time.time', mock_time)
# async def test_future_call(self):
#     mock_time.return_value = time()
#
#     executor1 = hetu.system.SystemExecutor('ssw', self.comp_mgr)
#     await executor1.initialize("")
#     await executor1.exec('login', 1020)
#
#     backend = self.backends['default']
#     from hetu.system.future import FutureCalls
#     FutureCallsCopy1 = FutureCalls.duplicate('ssw', 'copy1')
#     fc_tbl = self.comp_mgr.get_table(FutureCallsCopy1)
#
#     # 测试未来调用创建是否正常
#     ok, uuid = await executor1.exec('use_hp_future',1, False)
#     async with backend.transaction(fc_tbl.cluster_id) as trx:
#         fc_trx = fc_tbl.attach(trx)
#         expire_time = time() + 1.1
#         rows = await fc_trx.query('scheduled', left=0, right=expire_time, limit=1)
#         assert rows[0].uuid == uuid
#         assert rows[0].timeout == 10
#         assert rows[0].system == 'use_hp'
#         assert rows[0].recurring == False
#         assert rows[0].owner == 1020
#         expire_time = rows[0].scheduled
#
#     # 测试过期清理是否正常
#     await executor1.execute(SystemCall('use_hp', (2, ), 'test_uuid'))
#     from hetu.system.execution import ExecutionLock
#     ExecutionLock_use_hp = ExecutionLock.duplicate('ssw','use_hp')
#     lock_tbl = self.comp_mgr.get_table(ExecutionLock_use_hp)
#
#     from hetu.system.future import clean_expired_call_locks
#     # 未清理
#     await clean_expired_call_locks(self.comp_mgr)
#     rows = await lock_tbl.direct_query('called', left=0, right=time(), limit=1, row_format='raw')
#     assert len(rows) == 1
#
#     # 清理
#     mock_time.return_value = time() + datetime.timedelta(days=8).total_seconds()
#     await clean_expired_call_locks(self.comp_mgr)
#     rows =await lock_tbl.direct_query('called', left=0, right=0xFFFFFFFF, limit=1, row_format='raw')
#     assert len(rows) == 0
#
#     # 测试sleep_for_upcoming是否正常
#     mock_time.return_value = time()
#     from hetu.system.future import sleep_for_upcoming
#     have_task = await sleep_for_upcoming(fc_tbl)
#     # 检测当前时间是否~>=任务到期时间
#     self.assertGreater(time(), expire_time)
#     self.assertAlmostEqual(expire_time, time(), delta=0.1)
#     assert have_task
#     # 再调用应该只Sleep 0秒
#     mock_time.return_value = time()
#     start = time()
#     have_task = await sleep_for_upcoming(fc_tbl)
#     self.assertLess(time() - start, 0.1)
#
#     # 测试pop_upcoming_call是否正常
#     from hetu.system.future import pop_upcoming_call
#     call = await pop_upcoming_call(fc_tbl)
#     assert call.uuid == uuid
#     # 再次调用sleep应该返回False，并睡1秒
#     start = time()
#     have_task = await sleep_for_upcoming(fc_tbl)
#     self.assertGreater(time() - start, 1)
#     assert not have_task
#     # 检测pop的task数据是否修改了
#     async with backend.transaction(fc_tbl.cluster_id) as trx:
#         fc_trx = fc_tbl.attach(trx)
#         row = await fc_trx.select(uuid, 'uuid')
#         assert row.last_run == mock_time.return_value
#         assert row.scheduled == mock_time.return_value + 10
#
#     # 测试exec_future_call调用是否正常
#     mock_time.return_value = time()
#     from hetu.system.future import exec_future_call
#     # 此时future_call用的是已login的executor，实际运行future_call不可能有login的executor
#     ok = await exec_future_call(call, executor1, fc_tbl)
#     assert ok
#     # 检测task是否删除
#     async with backend.transaction(fc_tbl.cluster_id) as trx:
#         fc_trx = fc_tbl.attach(trx)
#         row = await fc_trx.select(uuid, 'uuid')
#         self.assertIs(row, None)
#     # 测试hp
#     ok, _ = await executor1.exec('test_hp', 100-3)
#     assert ok
#
#     await executor1.terminate()
#
