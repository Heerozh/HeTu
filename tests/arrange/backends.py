import time
import pytest

import docker
from docker.errors import NotFound


@pytest.fixture()
def redis_service():
    try:
        client = docker.from_env()
    except docker.errors.DockerException:
        return pytest.skip("请启动DockerDesktop或者Docker服务后再运行测试")

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
    network = client.networks.create("hetu_test_net", driver="bridge")

    # 启动服务器
    containers = {}
    port = 23318
    containers['redis'] = client.containers.run(
        "redis:latest", detach=True, ports={'6379/tcp': port},
        name='hetu_test_redis',
        auto_remove=True, network="hetu_test_net", hostname="redis-master")
    containers['redis_replica'] = client.containers.run(
        "redis:latest", detach=True, ports={'6379/tcp': port + 1},
        name='hetu_test_redis_replica', auto_remove=True,
        network="hetu_test_net",
        command=["redis-server", f"--replicaof redis-master 6379",
                 "--replica-read-only yes"])

    # 验证docker启动完毕
    import redis
    r = redis.Redis(host="127.0.0.1", port=port)
    r_slave = redis.Redis(host="127.0.0.1", port=port + 1)
    while True:
        try:
            time.sleep(1)
            print("version:", r.info()['redis_version'], r.role(),
                  r.config_get('notify-keyspace-events'))
            r.wait(1, 10000)
            print("slave version:", r_slave.info()['redis_version'],
                  r_slave.role(),
                  r_slave.config_get('notify-keyspace-events'))
            break
        except Exception:
            pass
    print('⚠️ 已启动redis docker.')

    # 返回redis地址
    yield f"redis://127.0.0.1:{port}/0", f"redis://127.0.0.1:{port + 1}/0"

    print('ℹ️ 清理docker...')
    for container in containers.values():
        try:
            container.stop()
            container.wait()
        except (NotFound, ImportError, docker.errors.APIError):
            pass
    print('ℹ️ 清理交换机')
    try:
        network.remove()
    except (docker.errors.NotFound, docker.errors.APIError):
        pass


def redis_backend(redis_service):
    from hetu.data.backend import RedisComponentTable, RedisBackend
    from hetu.data.backend.redis import RedisTransaction

    def _create_redis_backend(backend_cls, config):
        backend = backend_cls(config)
        backend.configure()
        if type(backend) is RedisBackend:
            assert(
                backend.io.config_get('notify-keyspace-events')[
                    "notify-keyspace-events"],
                "")

        item_data = table_cls(Item, 'test', 1, backend)
        item_data.flush(force=True)
        item_data.create_or_migrate()

    yield RedisComponentTable, RedisBackend, {
            "master": redis_service[0],
            "servants": [redis_service[1], ]
        }
    # 服务器销毁时，需要清理全局lua缓存，不然会认为lua脚本还在。这里强制清理下
    RedisTransaction.lua_check_unique = None
    RedisTransaction.lua_run_stacked = None


# 要测试新的backend，请添加backend到params中
@pytest.fixture(params=[redis_backend, ])
def backend(request):
    return request.param()