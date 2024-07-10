import unittest
import numpy as np
import docker
import importlib.util
import sys
import hetu
import asyncio
import logging
logger = logging.getLogger('HeTu')
logger.setLevel(logging.DEBUG)
logging.lastResort.setLevel(logging.DEBUG)


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
        ok, _ = await executor.run_('use_hp', 9)
        self.assertFalse(ok)

        # 测试登录
        ok, _ = await executor.run_('login', 1234)
        self.assertTrue(ok)

        # 测试有权限call
        ok, _ = await executor.run_('use_hp', 9)
        self.assertTrue(ok)

        # 去数据库读取内容看是否正确
        ok, _ = await executor.run_('test_hp', 100-9)
        self.assertTrue(ok)

        # 测试magic方法，自身+-10距离内的位置都会被攻击到
        # 先添加3个位置供magic查询
        ok, _ = await executor.run_('create_user', executor.context.caller, 10, 10)
        self.assertTrue(ok)
        ok, _ = await executor.run_('create_user', 3, 0, 0)
        self.assertTrue(ok)
        ok, _ = await executor.run_('create_user', 4, 10, -11)
        self.assertTrue(ok)

        with self.assertLogs('HeTu', level='INFO') as cm:
            ok, _ = await executor.run_('magic')
            self.assertTrue(ok)
            self.assertEqual(cm.output, ['INFO:HeTu:User_123', 'INFO:HeTu:User_3'])

        # 测试race
        executor2 = hetu.system.SystemExecutor('ssw')
        await executor2.initialize("")

        task1 = asyncio.create_task(executor.run_('race', 1))
        task2 = asyncio.create_task(executor2.run_('race', 0.2))
        await asyncio.gather(task2)
        await task1

        self.assertEqual(executor.context.retry_count, 1)
        self.assertEqual(executor2.context.retry_count, 0)

        # 结束连接
        await executor.terminate()
        pass


if __name__ == '__main__':
    unittest.main()
