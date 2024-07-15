import unittest
import numpy as np
import docker
import importlib.util
import sys
import hetu
import asyncio
import logging
from time import time
from unittest import mock
logger = logging.getLogger('HeTu')
logger.setLevel(logging.DEBUG)
logging.lastResort.setLevel(logging.DEBUG)
mock_time = mock.Mock()


class TestExecutor(unittest.IsolatedAsyncioTestCase):
    def __init__(self, module_name='runTest'):
        super().__init__(module_name)
        # ComponentDefines().clear_()

        # 加载玩家的app文件
        spec = importlib.util.spec_from_file_location('HeTuApp', './app.py')
        module = importlib.util.module_from_spec(spec)
        sys.modules['HeTuApp'] = module
        spec.loader.exec_module(module)

        # docker启动对应的backend
        self.containers = []
        try:
            client = docker.from_env()
        except docker.errors.DockerException:
            raise unittest.SkipTest("请启动DockerDesktop或者Docker服务后再运行测试")
        # 先删除已启动的
        try:
            client.containers.get('hetu_test_redis').kill()
            client.containers.get('hetu_test_redis').remove()
        except (docker.errors.NotFound, docker.errors.APIError):
            pass
        # 启动服务器
        self.containers.append(
            client.containers.run("redis:latest", detach=True, ports={'6379/tcp': 23318},
                                  name='hetu_test_redis', auto_remove=True)
        )

        # 初始化SystemCluster
        hetu.system.SystemClusters().build_clusters('ssw')
        # 初始化所有ComponentTable
        backends = {'default': hetu.data.backend.RedisBackend(
            {"master": "redis://127.0.0.1:23318/0"})}
        comp_tbl_cls = {'default': hetu.data.backend.RedisComponentTable}
        hetu.ComponentTableManager().build(
            'ssw', 'server1', backends, comp_tbl_cls)

    def __del__(self):
        for container in self.containers:
            try:
                container.kill()
            except (docker.errors.NotFound, ImportError):
                pass
        pass

    async def test_executor(self):
        executor = hetu.system.SystemExecutor('ssw')
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
        ok, _ = await executor.exec('test_hp', 100-9)
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
            self.assertEqual(cm.output, ['INFO:HeTu:User_123', 'INFO:HeTu:User_3'])

        # 测试race
        executor2 = hetu.system.SystemExecutor('ssw')
        await executor2.initialize("")

        task1 = asyncio.create_task(executor.exec('race', 1))
        task2 = asyncio.create_task(executor2.exec('race', 0.2))
        await asyncio.gather(task2)
        await task1

        self.assertEqual(executor.context.retry_count, 1)
        self.assertEqual(executor2.context.retry_count, 0)

        # 结束连接
        await executor.terminate()
        pass

    @mock.patch('time.time', mock_time)
    async def test_connect(self):
        # 先登录几个连接
        mock_time.return_value = time()
        executor1 = hetu.system.SystemExecutor('ssw')
        await executor1.initialize("")
        await executor1.exec('login', 1)

        executor2 = hetu.system.SystemExecutor('ssw')
        await executor2.initialize("")
        await executor2.exec('login', 2)

        ok, _ = await executor1.exec('use_hp', 1)
        self.assertTrue(ok)
        ok, _ = await executor2.exec('use_hp', 1)
        self.assertTrue(ok)

        # 测试重复登录踢出已登录用户
        executor3 = hetu.system.SystemExecutor('ssw')
        await executor3.initialize("")
        await executor3.exec('login', 1)

        # 测试运行第一个连接的system，然后看是否失败
        ok, _ = await executor1.exec('test_hp', 99)
        self.assertFalse(ok)
        ok, _ = await executor3.exec('test_hp', 99)
        self.assertTrue(ok)

        # 测试last active超时是否踢出用户
        # 先测试不强制踢出是否生效
        executor4 = hetu.system.SystemExecutor('ssw')
        await executor4.initialize("")
        await executor4.exec('login', 1, False)
        ok, _ = await executor3.exec('test_hp', 99)
        self.assertTrue(ok)
        # 然后是不强制踢出，但是timeout应该生效
        from hetu.system.executor import SYSTEM_CALL_TIMEOUT
        mock_time.return_value = time() + SYSTEM_CALL_TIMEOUT
        executor5 = hetu.system.SystemExecutor('ssw')
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
