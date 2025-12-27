import time

import docker
import docker.errors
import pytest
from docker.errors import NotFound


@pytest.fixture(scope="module")
def mod_redis_service():
    """
    启动redis docker服务，测试结束后销毁服务
    """
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
        client.networks.get("hetu_test_redis_net").remove()
    except (docker.errors.NotFound, docker.errors.APIError):
        pass

    # 启动交换机
    network = client.networks.create("hetu_test_redis_net", driver="bridge")

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
                network="hetu_test_redis_net",
                hostname="redis-master",
            )
            containers["redis_replica"] = client.containers.run(
                "redis:latest",
                detach=True,
                ports={"6379/tcp": port + 1},
                name="hetu_test_redis_replica",
                auto_remove=True,
                network="hetu_test_redis_net",
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

    print("ℹ️ 清理redis docker...")
    for container in containers.values():
        try:
            container.stop()
            container.wait()
        except (NotFound, ImportError, docker.errors.APIError):
            pass
    print("ℹ️ 清理redis交换机")
    try:
        network.remove()
    except (docker.errors.NotFound, docker.errors.APIError):
        pass


@pytest.fixture(scope="module")
def mod_valkey_service():
    """
    启动valkey docker服务，测试结束后销毁服务
    """
    try:
        client = docker.from_env()
    except docker.errors.DockerException:
        return pytest.skip("请启动DockerDesktop或者Docker服务后再运行测试")

    # 先删除已启动的
    try:
        client.containers.get("hetu_test_valkey").kill()
        client.containers.get("hetu_test_valkey").remove()
    except (docker.errors.NotFound, docker.errors.APIError):
        pass
    try:
        client.containers.get("hetu_test_valkey_replica").kill()
        client.containers.get("hetu_test_valkey_replica").remove()
    except (docker.errors.NotFound, docker.errors.APIError):
        pass
    try:
        client.networks.get("hetu_test_valkey_net").remove()
    except (docker.errors.NotFound, docker.errors.APIError):
        pass

    # 启动交换机
    network = client.networks.create("hetu_test_valkey_net", driver="bridge")

    # 启动服务器
    containers = {}

    def run_valkey_service(port=23418):
        if "valkey" not in containers:
            containers["valkey"] = client.containers.run(
                "valkey/valkey:latest",
                detach=True,
                ports={"6379/tcp": port},
                name="hetu_test_valkey",
                auto_remove=True,
                network="hetu_test_valkey_net",
                hostname="valkey-master",
            )
            containers["valkey_replica"] = client.containers.run(
                "valkey/valkey:latest",
                detach=True,
                ports={"6379/tcp": port + 1},
                name="hetu_test_valkey_replica",
                auto_remove=True,
                network="hetu_test_valkey_net",
                command=[
                    "valkey-server",
                    "--replicaof valkey-master 6379",
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
        print("⚠️ 已启动valkey docker.")

        # 返回redis地址
        return f"redis://127.0.0.1:{port}/0", f"redis://127.0.0.1:{port + 1}/0"

    yield run_valkey_service

    print("ℹ️ 清理valkey docker...")
    for container in containers.values():
        try:
            container.stop()
            container.wait()
        except (NotFound, ImportError, docker.errors.APIError):
            pass
    print("ℹ️ 清理valkey交换机")
    try:
        network.remove()
    except (docker.errors.NotFound, docker.errors.APIError):
        pass
