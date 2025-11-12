import asyncio
import logging
import sys
import unittest
import datetime
from time import time
from unittest import mock

import hetu
from backend_mgr import UnitTestBackends
from hetu.system import SystemCall

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('HeTu.root')
logger.setLevel(logging.DEBUG)
logging.lastResort.setLevel(logging.DEBUG)
mock_time = mock.Mock()


class TestExecutor(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        # 测试慢日志阈值
        import hetu.common.slowlog
        hetu.common.slowlog.SLOW_LOG_TIME_THRESHOLD = 0.1
        hetu.system.SystemClusters()._clear()

        # 加载玩家的app文件
        import app
        _ = app
        # 初始化SystemCluster
        hetu.system.SystemClusters().build_clusters('ssw')

        # 初始化redis backend docker
        cls.backend_mgr = UnitTestBackends()
        cls.table_constructor, cls.backend_constructor, cls.backend_args = (
            cls.backend_mgr.start_redis_server())

    @classmethod
    def tearDownClass(cls):
        cls.backend_mgr.teardown()

    def setUp(self):
        # 为每个test初始化comp_mgr，因为每个test的线程不同
        self.backends = {'default': self.backend_constructor(self.backend_args)}
        comp_tbl_cls = {'default': self.table_constructor}
        self.comp_mgr = hetu.ComponentTableManager(
            'ssw', 'server1', self.backends, comp_tbl_cls)

    async def asyncTearDown(self):
        for backend in self.backends.values():
            await backend.close()

    async def test_executor(self):
        executor = hetu.system.SystemExecutor('ssw', self.comp_mgr)
        await executor.initialize("")

        # 测试无权限call
        ok, _ = await executor.exec('use_hp', 9)
        self.assertFalse(ok)

        # 测试登录
        ok, _ = await executor.exec('login', 1234)
        self.assertTrue(ok)

        # 测试有权限call
        ok, _ = await executor.exec('use_hp', 9)
        self.assertTrue(ok)

        # 去数据库读取内容看是否正确
        ok, _ = await executor.exec('test_hp', 100 - 9)
        self.assertTrue(ok)


        # 测试magic方法，自身+-10距离内的位置都会被攻击到
        # 添加3个位置供magic查询
        ok, _ = await executor.exec('create_user', executor.context.caller, 10, 10)
        self.assertTrue(ok)
        ok, _ = await executor.exec('create_user', 3, 0, 0)
        self.assertTrue(ok)
        ok, _ = await executor.exec('create_user', 4, 10, -11)
        self.assertTrue(ok)

        with self.assertLogs('HeTu', level='INFO') as cm:
            ok, _ = await executor.exec('magic')
            self.assertTrue(ok)
            self.assertEqual(cm.output[:2], ['INFO:HeTu.root:User_123', 'INFO:HeTu.root:User_3'])
            self.assertIn('慢日志', cm.output[2])
            self.assertIn('magic', cm.output[2])

        # 测试race
        executor2 = hetu.system.SystemExecutor('ssw', self.comp_mgr)
        await executor2.initialize("")

        task1 = asyncio.create_task(executor.exec('race', 0.4))
        task2 = asyncio.create_task(executor2.exec('race', 0.1)) # 必须设高点才能让task1先执行到select
        await asyncio.gather(task2)
        await task1

        self.assertEqual(executor.context.retry_count, 1)
        self.assertEqual(executor2.context.retry_count, 0)

        # 结束连接
        await executor.terminate()

    async def test_execute_system_copy(self):
        executor = hetu.system.SystemExecutor('ssw', self.comp_mgr)
        await executor.initialize("")

        ok, _ = await executor.exec('login', 1001)
        self.assertTrue(ok)
        ok, _ = await executor.exec('use_hp_copy', 9)
        self.assertTrue(ok)
        ok, _ = await executor.exec('use_hp', 1)
        self.assertTrue(ok)

        # 去数据库读取内容看是否正确
        ok, _ = await executor.exec('test_hp', 100-1)
        self.assertTrue(ok)

        backend = self.backends['default']
        HP = hetu.data.ComponentDefines().get_component('ssw', 'HP')
        hp_copy = HP.duplicate('copy1')
        hp_tbl = self.comp_mgr.get_table(hp_copy)
        async with backend.transaction(hp_tbl.cluster_id) as trx:
            hp_trx = hp_tbl.attach(trx)
            row = await hp_trx.select(1001, 'owner')
            self.assertEqual(row.value, 100-9)

        # 结束连接
        await executor.terminate()


    async def test_execute_system_call_lock(self):
        executor = hetu.system.SystemExecutor('ssw', self.comp_mgr)
        await executor.initialize("")

        ok, _ = await executor.exec('login', 1101)
        self.assertTrue(ok)
        ok, _ = await executor.exec('use_hp', 1)
        self.assertTrue(ok)
        ok, _ = await executor.exec('use_hp', 1)
        self.assertTrue(ok)

        ok, _ = await executor.execute(SystemCall('use_hp', (2,), 'uuid1'))
        self.assertTrue(ok)
        ok, _ = await executor.execute(SystemCall('use_hp', (3,), 'uuid1'))
        self.assertTrue(ok)

        # 去数据库读取内容看是否正确
        ok, _ = await executor.exec('test_hp', 100-4)
        self.assertTrue(ok)

        # 结束连接
        await executor.terminate()


    @mock.patch('time.time', mock_time)
    async def test_connect(self):
        # 先登录几个连接
        mock_time.return_value = time()
        executor1 = hetu.system.SystemExecutor('ssw', self.comp_mgr)
        await executor1.initialize("")
        await executor1.exec('login', 1)

        executor2 = hetu.system.SystemExecutor('ssw', self.comp_mgr)
        await executor2.initialize("")
        await executor2.exec('login', 2)

        ok, _ = await executor1.exec('use_hp', 1)
        self.assertTrue(ok)
        ok, _ = await executor2.exec('use_hp', 1)
        self.assertTrue(ok)

        # 测试重复登录踢出已登录用户
        executor3 = hetu.system.SystemExecutor('ssw', self.comp_mgr)
        await executor3.initialize("")
        await executor3.exec('login', 1)

        # 测试运行第一个连接的system，然后看是否失败
        ok, _ = await executor1.exec('test_hp', 99)
        self.assertFalse(ok)
        ok, _ = await executor3.exec('test_hp', 99)
        self.assertTrue(ok)

        # 测试last active超时是否踢出用户
        # 先测试不强制踢出是否生效
        executor4 = hetu.system.SystemExecutor('ssw', self.comp_mgr)
        await executor4.initialize("")
        await executor4.exec('login', 1, False)
        ok, _ = await executor3.exec('test_hp', 99)
        self.assertTrue(ok)
        # 然后是不强制踢出，但是timeout应该生效
        executor5 = hetu.system.SystemExecutor('ssw', self.comp_mgr)
        mock_time.return_value = time() + executor5.context.idle_timeout
        await executor5.initialize("")
        await executor5.exec('login', 1, False)

        ok, _ = await executor3.exec('test_hp', 99)
        self.assertFalse(ok)

        # 结束连接
        await executor1.terminate()
        await executor2.terminate()
        await executor3.terminate()
        await executor4.terminate()
        await executor5.terminate()

    @mock.patch('time.time', mock_time)
    async def test_future_call(self):
        mock_time.return_value = time()

        executor1 = hetu.system.SystemExecutor('ssw', self.comp_mgr)
        await executor1.initialize("")
        await executor1.exec('login', 1020)

        backend = self.backends['default']
        from hetu.system.future import FutureCalls
        FutureCallsCopy1 = FutureCalls.duplicate('copy1')
        fc_tbl = self.comp_mgr.get_table(FutureCallsCopy1)

        # 测试未来调用创建是否正常
        ok, uuid = await executor1.exec('use_hp_future',1, False)
        async with backend.transaction(fc_tbl.cluster_id) as trx:
            fc_trx = fc_tbl.attach(trx)
            expire_time = time() + 1.1
            rows = await fc_trx.query('scheduled', left=0, right=expire_time, limit=1)
            self.assertEqual(rows[0].uuid, uuid)
            self.assertEqual(rows[0].timeout, 10)
            self.assertEqual(rows[0].system, 'use_hp')
            self.assertEqual(rows[0].recurring, False)
            self.assertEqual(rows[0].owner, 1020)
            expire_time = rows[0].scheduled

        # 测试过期清理是否正常
        await executor1.execute(SystemCall('use_hp', (2, ), 'test_uuid'))
        from hetu.system.execution import ExecutionLock
        ExecutionLock_use_hp = ExecutionLock.duplicate('use_hp')
        lock_tbl = self.comp_mgr.get_table(ExecutionLock_use_hp)

        from hetu.system.future import clean_expired_call_locks
        # 未清理
        await clean_expired_call_locks(self.comp_mgr)
        rows = await lock_tbl.direct_query('called', left=0, right=time(), limit=1, row_format='raw')
        self.assertEqual(len(rows), 1)

        # 清理
        mock_time.return_value = time() + datetime.timedelta(days=8).total_seconds()
        await clean_expired_call_locks(self.comp_mgr)
        rows =await lock_tbl.direct_query('called', left=0, right=0xFFFFFFFF, limit=1, row_format='raw')
        self.assertEqual(len(rows), 0)

        # 测试sleep_for_upcoming是否正常
        mock_time.return_value = time()
        from hetu.system.future import sleep_for_upcoming
        have_task = await sleep_for_upcoming(fc_tbl)
        # 检测当前时间是否~>=任务到期时间
        self.assertGreater(time(), expire_time)
        self.assertAlmostEqual(expire_time, time(), delta=0.1)
        self.assertTrue(have_task)
        # 再调用应该只Sleep 0秒
        mock_time.return_value = time()
        start = time()
        have_task = await sleep_for_upcoming(fc_tbl)
        self.assertLess(time() - start, 0.1)

        # 测试pop_upcoming_call是否正常
        from hetu.system.future import pop_upcoming_call
        call = await pop_upcoming_call(fc_tbl)
        self.assertEqual(call.uuid, uuid)
        # 再次调用sleep应该返回False，并睡1秒
        start = time()
        have_task = await sleep_for_upcoming(fc_tbl)
        self.assertGreater(time() - start, 1)
        self.assertFalse(have_task)
        # 检测pop的task数据是否修改了
        async with backend.transaction(fc_tbl.cluster_id) as trx:
            fc_trx = fc_tbl.attach(trx)
            row = await fc_trx.select(uuid, 'uuid')
            self.assertEqual(row.last_run, mock_time.return_value)
            self.assertEqual(row.scheduled, mock_time.return_value + 10)

        # 测试exec_future_call调用是否正常
        mock_time.return_value = time()
        from hetu.system.future import exec_future_call
        # 此时future_call用的是已login的executor，实际运行future_call不可能有login的executor
        ok = await exec_future_call(call, executor1, fc_tbl)
        self.assertTrue(ok)
        # 检测task是否删除
        async with backend.transaction(fc_tbl.cluster_id) as trx:
            fc_trx = fc_tbl.attach(trx)
            row = await fc_trx.select(uuid, 'uuid')
            self.assertIs(row, None)
        # 测试hp
        ok, _ = await executor1.exec('test_hp', 100-3)
        self.assertTrue(ok)

        await executor1.terminate()


if __name__ == '__main__':
    unittest.main()
