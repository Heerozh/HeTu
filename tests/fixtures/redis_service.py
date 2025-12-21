import time

import docker
import pytest
from docker.errors import NotFound


@pytest.fixture(scope="module")
def mod_redis_service():
    """
    启动redis docker服务，测试结束后销毁服务
    """
    from hetu.data.backend_old.redis import RedisBackend

    # redis服务器设置了不会保存lua脚本，redis服务销毁/重建时，需要清理全局lua缓存标记，
    # 此标记用来加速redis客户端请求速度，不然客户端每次都有逻辑需要保证脚本存在
    # 这里强制清理下
    RedisBackend.lua_check_and_run = None

    try:
        client = docker.from_env()
    except docker.errors.DockerException:
        return pytest.skip("请启动DockerDesktop或者Docker服务后再运行测试")

    # 先删除已启动的
    try:
        client.containers.get("hetu_test_redis").kill()
        client.containers.get("hetu_test_redis").remove()
    except (docker.errors.NotFound, docker.errors.APIError):
        pass
    try:
        client.containers.get("hetu_test_redis_replica").kill()
        client.containers.get("hetu_test_redis_replica").remove()
    except (docker.errors.NotFound, docker.errors.APIError):
        pass
    try:
        client.networks.get("hetu_test_net").remove()
    except (docker.errors.NotFound, docker.errors.APIError):
        pass

    # 启动交换机
    network = client.networks.create("hetu_test_net", driver="bridge")

    # 启动服务器
    containers = {}

    def run_redis_service(port=23318):
        if "redis" not in containers:
            containers["redis"] = client.containers.run(
                "redis:latest",
                detach=True,
                ports={"6379/tcp": port},
                name="hetu_test_redis",
                auto_remove=True,
                network="hetu_test_net",
                hostname="redis-master",
            )
            containers["redis_replica"] = client.containers.run(
                "redis:latest",
                detach=True,
                ports={"6379/tcp": port + 1},
                name="hetu_test_redis_replica",
                auto_remove=True,
                network="hetu_test_net",
                command=[
                    "redis-server",
                    "--replicaof redis-master 6379",
                    "--replica-read-only yes",
                ],
            )

        # 验证docker启动完毕
        import redis

        r = redis.Redis(host="127.0.0.1", port=port)
        r_slave = redis.Redis(host="127.0.0.1", port=port + 1)
        while True:
            try:
                time.sleep(1)
                print(
                    "version:",
                    r.info()["redis_version"],
                    r.role(),
                    r.config_get("notify-keyspace-events"),
                )
                r.wait(1, 10000)
                print(
                    "slave version:",
                    r_slave.info()["redis_version"],
                    r_slave.role(),
                    r_slave.config_get("notify-keyspace-events"),
                )
                break
            except Exception:
                pass
        print("⚠️ 已启动redis docker.")

        # 返回redis地址
        return f"redis://127.0.0.1:{port}/0", f"redis://127.0.0.1:{port + 1}/0"

    yield run_redis_service

    print("ℹ️ 清理docker...")
    for container in containers.values():
        try:
            container.stop()
            container.wait()
        except (NotFound, ImportError, docker.errors.APIError):
            pass
    print("ℹ️ 清理交换机")
    try:
        network.remove()
    except (docker.errors.NotFound, docker.errors.APIError):
        pass
