import asyncio
import logging
import sys
import unittest
from time import time
from unittest import mock

import hetu
from backend_mgr import UnitTestBackends

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('HeTu.root')
logger.setLevel(logging.DEBUG)
logging.lastResort.setLevel(logging.DEBUG)
mock_time = mock.Mock()


class TestExecutor(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
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
        # 先添加3个位置供magic查询
        ok, _ = await executor.exec('create_user', executor.context.caller, 10, 10)
        self.assertTrue(ok)
        ok, _ = await executor.exec('create_user', 3, 0, 0)
        self.assertTrue(ok)
        ok, _ = await executor.exec('create_user', 4, 10, -11)
        self.assertTrue(ok)

        with self.assertLogs('HeTu', level='INFO') as cm:
            ok, _ = await executor.exec('magic')
            self.assertTrue(ok)
            self.assertEqual(cm.output, ['INFO:HeTu.root:User_123', 'INFO:HeTu.root:User_3'])

        # 测试race
        executor2 = hetu.system.SystemExecutor('ssw', self.comp_mgr)
        await executor2.initialize("")

        task1 = asyncio.create_task(executor.exec('race', 0.3))
        task2 = asyncio.create_task(executor2.exec('race', 0.02))
        await asyncio.gather(task2)
        await task1

        self.assertEqual(executor.context.retry_count, 1)
        self.assertEqual(executor2.context.retry_count, 0)

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


if __name__ == '__main__':
    unittest.main()
