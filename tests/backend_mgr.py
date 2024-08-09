import time

import docker
import unittest
from docker.errors import NotFound
from hetu.data.backend.redis import RedisTransaction


class UnitTestBackends:
    def __init__(self):
        self.containers = {}

    def teardown(self):
        print('ℹ️ 清理docker...')
        for container in self.containers.values():
            try:
                container.kill()
            except (NotFound, ImportError, docker.errors.APIError):
                pass
        # 因为服务器销毁了，清理下python中的全局lua缓存
        RedisTransaction.lua_check_unique = None
        RedisTransaction.lua_run_stacked = None

    def get_all_backends(self):
        # 启动所有start_*_server函数，并返回它们的连接配置
        rtn = []
        for func_name in dir(self):
            if func_name.startswith("start_") and func_name.endswith("_server"):
                rtn.append(getattr(self, func_name)())
        return rtn

    def start_redis_server(self):
        from hetu.data.backend import RedisComponentTable, RedisBackend
        import redis
        # 如果已启动则跳过
        if 'redis' not in self.containers:
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
            self.containers['redis'] = client.containers.run(
                "redis:latest", detach=True, ports={'6379/tcp': 23318}, name='hetu_test_redis',
                auto_remove=True)
            r = redis.Redis(host="127.0.0.1", port=23318)
            # 等待docker启动完毕
            while True:
                try:
                    time.sleep(1)
                    print("version:", r.info()['redis_version'])
                    break
                except Exception:
                    pass
            print('⚠️ 已启动redis docker.')
        return RedisComponentTable, RedisBackend, {"master": "redis://127.0.0.1:23318/0"}
