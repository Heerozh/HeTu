import time
import socket
import docker
import unittest
from docker.errors import NotFound
from hetu.data.backend.redis import RedisTransaction


class UnitTestBackends:
    def __init__(self):
        self.containers = {}
        self.network = None

    def teardown(self):
        print('ℹ️ 清理docker...')
        for container in self.containers.values():
            try:
                container.stop()
                container.wait()
            except (NotFound, ImportError, docker.errors.APIError):
                pass
        print('ℹ️ 清理交换机')
        try:
            self.network.remove()
        except (docker.errors.NotFound, docker.errors.APIError):
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

    def start_redis_server(self, port=23318):
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
            try:
                client.containers.get('hetu_test_redis_replica').kill()
                client.containers.get('hetu_test_redis_replica').remove()
            except (docker.errors.NotFound, docker.errors.APIError):
                pass
            try:
                client.networks.get('hetu_test_net').remove()
            except (docker.errors.NotFound, docker.errors.APIError):
                pass
            # 启动交换机
            self.network = client.networks.create("hetu_test_net", driver="bridge")
            # 启动服务器
            self.containers['redis'] = client.containers.run(
                "redis:latest", detach=True, ports={'6379/tcp': port}, name='hetu_test_redis',
                auto_remove=True, network="hetu_test_net", hostname="redis-master")
            self.containers['redis_replica'] = client.containers.run(
                "redis:latest", detach=True, ports={'6379/tcp': port+1},
                name='hetu_test_redis_replica', auto_remove=True, network="hetu_test_net",
                command=["redis-server", f"--replicaof redis-master 6379", "--replica-read-only yes"])
            r = redis.Redis(host="127.0.0.1", port=port)
            r_slave = redis.Redis(host="127.0.0.1", port=port+1)
            # 等待docker启动完毕
            while True:
                try:
                    time.sleep(1)
                    print("version:", r.info()['redis_version'], r.role(),
                          r.config_get('notify-keyspace-events'))
                    r.wait(1, 10000)
                    print("slave version:", r_slave.info()['redis_version'], r_slave.role(),
                          r_slave.config_get('notify-keyspace-events'))
                    break
                except Exception:
                    pass
            print('⚠️ 已启动redis docker.')
        # 先清除已加载lua的标记
        RedisBackend.lua_check_and_run = None
        # 返回backend
        return RedisComponentTable, RedisBackend, {
            "master": f"redis://127.0.0.1:{port}/0",
            "servants": [f"redis://127.0.0.1:{port+1}/0", ]
        }
