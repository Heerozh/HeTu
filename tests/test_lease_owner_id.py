"""
Worker ID 租约的持有者标识（hetu/common/helper.py 的 lease_owner_id、node_token）：同时活着的进程之间
不能相同，同一个位置重启前后尽量不变。

撞了的后果：后启动的进程会把先启动的正在用的 Worker ID 当成自己重启前的直接复用，两边续约都成功，静默
发出重复的雪花 ID。host 网络下同一台宿主机上的容器主机名都和宿主机一样、pid 都是 1（实测），原来的
"主机名:pid" 就会撞。
"""

import logging
import multiprocessing
import os
import sys
from types import SimpleNamespace

import pytest
from fixtures.backends import use_redis_family_backend_only

from hetu.common import helper

posix_only = pytest.mark.skipif(
    sys.platform == "win32", reason="Windows 上不用节点 token"
)


@pytest.fixture
def new_node(monkeypatch, tmp_path):
    """
    切到一个"节点"：token 文件换成 tmp_path 下的 name，并清掉本进程缓存的 token，相当于那个容器里新起的
    进程。同一个 name 就是同一个容器（重启前后），不同的 name 是同一台宿主机上主机名相同的不同容器
    """
    monkeypatch.delenv(helper.WORKER_INDEX_ENV, raising=False)
    monkeypatch.setattr(helper, "get_machine_id", lambda: "host-a")

    def switch(name: str = "node"):
        path = tmp_path / name
        monkeypatch.setattr(helper, "NODE_TOKEN_PATH", str(path))
        monkeypatch.setattr(helper, "_node_token", None)
        return path

    switch()
    return switch


@posix_only
def test_owner_id_formats(new_node, monkeypatch):
    """
    机器码:节点 token:进程部分。HeTu 管理进程起的 worker 的进程部分是 序号 + 管理进程的 pid（自己的 pid
    每次重启都变）；工具进程不是管理进程起的，照旧用自己的 pid，带 cli: 前缀
    """
    token = helper.node_token()
    assert token is not None and len(token) == 16
    assert helper.lease_owner_id(123) == f"host-a:{token}:123"
    assert helper.lease_owner_id(123, tool=True) == f"cli:host-a:{token}:123"

    monkeypatch.setenv(helper.WORKER_INDEX_ENV, "3")
    monkeypatch.setattr(os, "getppid", lambda: 4242)
    assert helper.lease_owner_id(123) == f"host-a:{token}:w3:4242"
    assert helper.lease_owner_id(456) == f"host-a:{token}:w3:4242"
    assert helper.lease_owner_id(123, tool=True) == f"cli:host-a:{token}:123"


@posix_only
def test_node_token_persists_per_container(new_node, monkeypatch):
    """第一次用到时随机生成、写进文件；同一个容器重启后（新进程）读到同一个；别的容器各有各的"""
    path_a = new_node("container-a")
    token_a = helper.node_token()
    assert path_a.read_text(encoding="ascii") == token_a

    monkeypatch.setattr(helper, "_node_token", None)  # 重启：新进程
    assert helper.node_token() == token_a

    new_node("container-b")
    assert helper.node_token() not in (None, token_a)


def _token_in_child(path, barrier, results):
    barrier.wait()
    results.put(helper.read_or_create_node_token(path))


@posix_only
def test_workers_starting_together_share_one_token(tmp_path):
    """
    同一个容器里的几个 worker 同时第一次启动：只有一个写成，大家读到同一个 token（用硬链接放到位，
    不会读到写了一半的文件），临时文件都清掉了
    """
    ctx = multiprocessing.get_context("spawn")
    path = str(tmp_path / "token")
    workers = 8
    barrier = ctx.Barrier(workers)
    results = ctx.Queue()
    procs = [
        ctx.Process(target=_token_in_child, args=(path, barrier, results))
        for _ in range(workers)
    ]
    for proc in procs:
        proc.start()
    tokens = [results.get(timeout=60) for _ in range(workers)]
    for proc in procs:
        proc.join(30)
    with open(path, encoding="ascii") as f:
        assert set(tokens) == {f.read()}
    assert [name for name in os.listdir(tmp_path) if name.endswith(".tmp")] == []


@posix_only
def test_node_token_falls_back_when_not_writable(new_node, monkeypatch, caplog):
    """存不进去（只读文件系统等）：用只在本进程有效的 token，照样和别的容器区分得开，只是重启后不复用"""
    unwritable = "/nonexistent-hetu-dir/token"
    monkeypatch.setattr(helper, "NODE_TOKEN_PATH", unwritable)
    caplog.set_level(logging.INFO, logger="HeTu.root")

    assert helper.read_or_create_node_token(unwritable) is None
    assert helper.node_token()
    assert "只在本进程有效" in caplog.text


def test_lease_owner_exited_parses_owner_ids(monkeypatch):
    """
    Windows 上认"租约主人已经退出"：第一段是机器码，最后一段是 pid。管理进程起的 worker 最后一段是管理
    进程的 pid：worker 被强制结束、管理进程还在时会以同一个序号重启、接着用这把租约，不算退出
    """
    exited = {111, 222}
    monkeypatch.setattr(helper, "sys", SimpleNamespace(platform="win32"))
    monkeypatch.setattr(helper, "get_machine_id", lambda: "host-a")
    monkeypatch.setattr(helper, "windows_pid_exited", lambda pid: pid in exited)

    assert helper.lease_owner_exited("host-a:111")
    assert helper.lease_owner_exited(b"cli:host-a:222")
    assert helper.lease_owner_exited("host-a:w0:111")
    assert not helper.lease_owner_exited("host-a:w0:333")
    assert helper.lease_owner_exited("host-a:9bbe2386a56e66d1:111")
    assert not helper.lease_owner_exited("host-b:111")
    assert not helper.lease_owner_exited("pytest-node")


@posix_only
@use_redis_family_backend_only
async def test_containers_sharing_hostname_get_their_own_worker_ids(
    mod_auto_backend, new_node
):
    """
    主机名一样、pid 都是 1 的两个容器（host 网络）：各拿各的 Worker ID，不会复用对方正在用的；同一个容器
    重启后照样复用自己原来的
    """
    from hetu.data.backend.redis.worker_keeper import RedisWorkerKeeper

    aio = mod_auto_backend().master.aio
    new_node("container-a")
    a = RedisWorkerKeeper(1, aio)
    new_node("container-b")
    b = RedisWorkerKeeper(1, aio)
    try:
        id_a = await a.get_worker_id()
        id_b = await b.get_worker_id()
        assert id_b != id_a

        new_node("container-a")  # 容器 a 重启：同一个 token 文件，pid 还是 1
        restarted = RedisWorkerKeeper(1, aio)
        assert restarted.node_id == a.node_id
        assert await restarted.get_worker_id() == id_a
    finally:
        await a.release_worker_id()
        await b.release_worker_id()


@use_redis_family_backend_only
async def test_supervised_worker_restart_reuses_its_worker_id(
    mod_auto_backend, new_node, monkeypatch
):
    """
    管理进程以同一个序号重启的 worker（pid 变了）复用原来的 Worker ID；别的序号不复用；换了管理进程
    （裸机上重启了 hetu start，旧 worker 可能还在退出）也不复用
    """
    from hetu.data.backend.redis.worker_keeper import RedisWorkerKeeper

    aio = mod_auto_backend().master.aio
    monkeypatch.setattr(os, "getppid", lambda: 4242)
    monkeypatch.setenv(helper.WORKER_INDEX_ENV, "0")
    first = RedisWorkerKeeper(100, aio)
    keepers = [first]
    try:
        worker_id = await first.get_worker_id()
        restarted = RedisWorkerKeeper(101, aio)
        assert await restarted.get_worker_id() == worker_id

        monkeypatch.setenv(helper.WORKER_INDEX_ENV, "1")
        keepers.append(other_index := RedisWorkerKeeper(102, aio))
        assert await other_index.get_worker_id() != worker_id

        monkeypatch.setenv(helper.WORKER_INDEX_ENV, "0")
        monkeypatch.setattr(os, "getppid", lambda: 4343)
        keepers.append(new_manager := RedisWorkerKeeper(103, aio))
        assert await new_manager.get_worker_id() != worker_id
    finally:
        for keeper in keepers:
            await keeper.release_worker_id()
