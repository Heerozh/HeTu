import socket
import sys
import time

import docker
import docker.errors
import pytest
from docker.errors import NotFound


@pytest.fixture(scope="session")
def ses_redis_service():
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

    port = 23318
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
                r.info()["redis_version"],  # type: ignore
                r.role(),
                r.config_get("notify-keyspace-events"),
            )
            r.wait(1, 10000)
            print(
                "slave version:",
                r_slave.info()["redis_version"],  # type: ignore
                r_slave.role(),
                r_slave.config_get("notify-keyspace-events"),
            )
            break
        except Exception:
            pass
    print("⚠️ 已启动redis docker.")

    # 返回redis地址
    yield f"redis://127.0.0.1:{port}/0", f"redis://127.0.0.1:{port + 1}/0"

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


@pytest.fixture(scope="session")
def ses_valkey_service():
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

    port = 23418
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
                r.info()["redis_version"],  # type: ignore
                r.role(),
                r.config_get("notify-keyspace-events"),
            )
            r.wait(1, 10000)
            print(
                "slave version:",
                r_slave.info()["redis_version"],  # type: ignore
                r_slave.role(),
                r_slave.config_get("notify-keyspace-events"),
            )
            break
        except Exception:
            pass
    print("⚠️ 已启动valkey docker.")

    # 返回redis地址
    yield f"redis://127.0.0.1:{port}/0", f"redis://127.0.0.1:{port + 1}/0"

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


@pytest.fixture(scope="session")
def ses_redis_cluster_service():
    """
    启动redis cluster docker服务 (纯docker-py实现)，测试结束后销毁服务
    """
    # 在Linux下(如GitHub Actions)，host.docker.internal默认无法解析
    # 这里我们通过Monkey Patch让Python也能将其解析为127.0.0.1
    if sys.platform.startswith("linux"):
        _getaddrinfo = socket.getaddrinfo

        def new_getaddrinfo(*args, **kwargs):
            host = args[0]
            if host == "host.docker.internal":
                host = "127.0.0.1"
                l_args = list(args)
                l_args[0] = host
                return _getaddrinfo(*l_args, **kwargs)
            return _getaddrinfo(*args, **kwargs)

        socket.getaddrinfo = new_getaddrinfo
    try:
        client = docker.from_env()
    except docker.errors.DockerException:
        return pytest.skip("请启动DockerDesktop或者Docker服务后再运行测试")

    # 配置参数
    network_name = "hetu_test_cluster_net"
    base_name = "hetu_test_cluster_node"
    # 使用 3 个主节点 (最简集群模式)
    ports = [7000, 7001, 7002]
    containers = []
    network = None

    # --- 1. 清理旧环境 (类似 down -v) ---
    print("ℹ️ 清理旧的 Redis Cluster 容器和网络...")
    for i in range(len(ports)):
        try:
            c = client.containers.get(f"{base_name}_{i}")
            c.kill()
            c.remove()
        except (docker.errors.NotFound, docker.errors.APIError):
            pass
    try:
        client.networks.get(network_name).remove()
    except (docker.errors.NotFound, docker.errors.APIError):
        pass

    # --- 2. 启动新环境 ---
    try:
        # 创建网络
        network = client.networks.create(network_name, driver="bridge")

        print("🚀 正在启动 Redis Cluster 节点...")
        node_internal_ips = []
        node_external_ips = []

        for i, port in enumerate(ports):
            # Redis Cluster 在 Docker NAT 下需要配置 announce-ip 供外部(测试脚本)访问
            # 同时需要映射 数据端口(port) 和 总线端口(port + 10000)
            cmd = [
                "redis-server",
                f"--port {port}",
                "--cluster-enabled yes",
                "--cluster-config-file nodes.conf",
                "--cluster-node-timeout 5000",
                "--appendonly yes",
                "--cluster-announce-hostname host.docker.internal",
                "--cluster-announce-ip host.docker.internal",  # 比如设置布告ip，不然redis insight无法识别，实际上用域名也是可以的
                "--cluster-preferred-endpoint-type hostname",
                f"--cluster-announce-port {port}",
                f"--cluster-announce-bus-port 1{port:04d}",
            ]

            c = client.containers.run(
                "redis:latest",
                command=cmd,
                detach=True,
                # 端口映射: 容器端口 -> 宿主机端口
                ports={f"{port}/tcp": port, f"1{port:04d}/tcp": 10000 + port},
                name=f"{base_name}_{i}",
                auto_remove=True,
                network=network_name,
                hostname=f"redis-node-{i}",
                extra_hosts={"host.docker.internal": "host-gateway"},
            )
            containers.append(c)

            # 获取容器在 Docker 网络内部的 IP，用于集群节点间握手
            c.reload()  # 刷新属性以获取 IP
            internal_ip = c.attrs["NetworkSettings"]["Networks"][network_name][
                "IPAddress"
            ]
            node_internal_ips.append(f"{internal_ip}:{port}")
            node_external_ips.append(f"host.docker.internal:{port}")

        # 等待容器完全启动
        time.sleep(2)

        # --- 3. 创建集群 ---
        print(f"🔗 初始化集群，内部节点: {node_internal_ips}")
        # 在第一个节点内部执行 cluster create 命令
        # 注意：这里必须使用容器间的内部 IP
        create_cmd = f"redis-cli --cluster create {' '.join(node_external_ips)} --cluster-replicas 0 --cluster-yes"

        # 尝试执行创建命令
        exit_code, output = containers[0].exec_run(create_cmd)
        if exit_code != 0 and b"Cluster is already configured" not in output:
            raise Exception(f"Redis Cluster 创建失败: {output.decode()}")
        print(output)

        # --- 4. 验证集群就绪 ---
        from redis.cluster import RedisCluster

        print("⏳ 等待 Redis Cluster 就绪...")
        rc = None
        ready = False
        for i in range(30):
            try:
                # type: ignore 忽略类型检查警告
                rc = RedisCluster(
                    host="127.0.0.1",
                    port=ports[0],
                    socket_connect_timeout=1,
                    socket_timeout=1,
                )  # type: ignore
                info = rc.cluster_info()
                if info.get("cluster_state") == "ok":  # type: ignore
                    print(f"✅ Redis Cluster 已就绪 (耗时 {i}s)")
                    ready = True
                    break
            except Exception as e:
                print(str(e), "如果无法连接，确定wsl的网络模式为Nat")
                pass
            time.sleep(1)
            if rc:
                rc.close()

        if not ready:
            raise Exception("Redis Cluster 启动超时，无法连接")

        yield f"redis://127.0.0.1:{ports[0]}"

    finally:
        # --- 5. 清理资源 ---
        print("ℹ️ 清理 Redis Cluster...")
        for c in containers:
            try:
                c.kill()
                c.remove()
            except (docker.errors.NotFound, docker.errors.APIError):
                pass
        if network:
            try:
                network.remove()
            except (docker.errors.NotFound, docker.errors.APIError):
                pass
