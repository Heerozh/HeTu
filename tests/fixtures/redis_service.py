import socket
import sys
from typing import cast

import docker.errors
import pytest
from fixtures.docker_infra import (
    LOCAL_RANDOM_PORT,
    DockerStack,
    container_ip,
    docker_stack,
    free_ports,
    host_port,
    is_port_conflict,
)


def _start_replicated(stack: DockerStack, image: str, server: str) -> tuple[str, str]:
    """启动一主一从，返回 (master_url, replica_url)"""
    import redis

    master = stack.run(
        "master",
        image,
        ports={"6379/tcp": LOCAL_RANDOM_PORT},
        # 默认全量同步前要等 5 秒看还有没有别的从库，测试只有一个从库
        command=[server, "--repl-diskless-sync-delay 0"],
    )
    replica = stack.run(
        "replica",
        image,
        ports={"6379/tcp": LOCAL_RANDOM_PORT},
        command=[
            server,
            f"--replicaof {container_ip(master)} 6379",
            "--replica-read-only yes",
        ],
    )
    master_url = f"redis://127.0.0.1:{host_port(master, '6379/tcp')}/0"
    replica_url = f"redis://127.0.0.1:{host_port(replica, '6379/tcp')}/0"

    r = redis.Redis.from_url(master_url, socket_timeout=1)
    r_replica = redis.Redis.from_url(replica_url, socket_timeout=1)
    try:
        stack.wait_until(
            lambda: r_replica.info("replication")["master_link_status"] == "up",  # type: ignore
            f"{stack.name} 主从",
        )
        version = r.info("server")["redis_version"]  # type: ignore
    finally:
        r.close()
        r_replica.close()
    print(f"⚠️ 已启动{stack.name} docker({version}): {master_url}, {replica_url}")
    return master_url, replica_url


@pytest.fixture(scope="session")
def ses_redis_service():
    """
    启动redis一主一从docker服务，测试结束后销毁服务
    """
    with docker_stack("redis") as stack:
        yield _start_replicated(stack, "redis:latest", "redis-server")


@pytest.fixture(scope="session")
def ses_valkey_service():
    """
    启动valkey一主一从docker服务，测试结束后销毁服务
    """
    with docker_stack("valkey") as stack:
        yield _start_replicated(stack, "valkey/valkey:latest", "valkey-server")


def _patch_host_docker_internal():
    """
    在Linux下(如GitHub Actions)，host.docker.internal默认无法解析，
    这里通过Monkey Patch让本进程也能将其解析为127.0.0.1
    """
    if not sys.platform.startswith("linux"):
        return
    _getaddrinfo = socket.getaddrinfo
    if getattr(_getaddrinfo, "hetu_patched", False):
        return

    def new_getaddrinfo(host, *args, **kwargs):
        if host == "host.docker.internal":
            host = "127.0.0.1"
        return _getaddrinfo(host, *args, **kwargs)

    new_getaddrinfo.hetu_patched = True  # type: ignore
    socket.getaddrinfo = new_getaddrinfo


def _start_cluster_nodes(stack: DockerStack, nodes: int) -> list[int]:
    """
    启动 cluster 节点，返回各节点端口。节点对外宣告 host.docker.internal:端口，
    客户端按宣告的地址重定向，所以容器内监听端口必须等于宿主机端口，没法让 docker
    随机分配，只能自己挑空闲端口；挑完到 docker 占住之间可能被别人抢走，冲突时重试。
    """
    for attempt in range(3):
        ports = free_ports(nodes)
        try:
            for i, port in enumerate(ports):
                stack.run(
                    f"node{i}",
                    "redis:latest",
                    ports={f"{port}/tcp": port},
                    extra_hosts={"host.docker.internal": "host-gateway"},
                    command=[
                        "redis-server",
                        f"--port {port}",
                        "--cluster-enabled yes",
                        "--cluster-config-file nodes.conf",
                        "--cluster-node-timeout 5000",
                        "--appendonly yes",
                        # 总线只在容器网络内使用，端口固定即可（默认 port+10000 可能越界）
                        "--cluster-port 16379",
                        "--cluster-announce-hostname host.docker.internal",
                        "--cluster-preferred-endpoint-type hostname",
                        f"--cluster-announce-port {port}",
                    ],
                )
            return ports
        except docker.errors.APIError as e:
            if attempt == 2 or not is_port_conflict(e):
                raise
            print(f"ℹ️ Redis Cluster 端口被占用，换端口重试: {e}")
            stack.close()
    raise AssertionError("unreachable")


@pytest.fixture(scope="session")
def ses_redis_cluster_service():
    """
    启动redis cluster docker服务 (纯docker-py实现)，测试结束后销毁服务
    """
    import redis
    from redis.cluster import RedisCluster

    _patch_host_docker_internal()

    with docker_stack("cluster") as stack:
        # 使用 3 个主节点 (最简集群模式)
        ports = _start_cluster_nodes(stack, 3)

        def nodes_up():
            for port in ports:
                with redis.Redis(host="127.0.0.1", port=port, socket_timeout=1) as r:
                    r.ping()
            return True

        stack.wait_until(nodes_up, "Redis Cluster 节点")

        # 在第一个节点内部执行 cluster create 命令。
        # 注意：这里必须使用容器间的内部 IP，不能用 host.docker.internal。
        # 因为该命令在容器内运行，若连 host.docker.internal 需经宿主机 hairpin NAT
        # 回环，在无 Docker Desktop 的 Linux 上(如本地环境)会超时(Connection timed
        # out)；GitHub Actions 的网络恰好允许 hairpin 才没暴露此问题。用内部 IP 走
        # 容器网络直连即可，节点仍通过 --cluster-announce-hostname 对外宣告 hostname，
        # 不影响宿主机上的测试客户端访问。
        addrs = [
            f"{container_ip(c)}:{port}" for c, port in zip(stack.containers, ports)
        ]
        print(f"🔗 初始化集群，内部节点: {addrs}")
        exit_code, output = stack.containers[0].exec_run(
            ["redis-cli", "--cluster", "create", *addrs]
            + ["--cluster-replicas", "0", "--cluster-yes"]
        )
        if exit_code != 0:
            raise RuntimeError(
                f"Redis Cluster 创建失败: {cast(bytes, output).decode()}"
            )

        def cluster_ok():
            rc = RedisCluster(
                host="127.0.0.1",
                port=ports[0],
                socket_connect_timeout=1,
                socket_timeout=1,
            )
            try:
                return rc.cluster_info().get("cluster_state") == "ok"  # type: ignore
            finally:
                rc.close()

        stack.wait_until(
            cluster_ok, "Redis Cluster（如果无法连接，确定wsl的网络模式为Nat）"
        )
        print(f"✅ Redis Cluster 已就绪: {ports}")

        yield f"redis://127.0.0.1:{ports[0]}"
