"""
Worker ID 租约的持有者标识和复用（hetu/common/helper.py 的 lease_owner_id、node_token、lease_value，
hetu/data/backend/redis/worker_keeper.py 的 get_worker_id）。

- 持有者标识：同时活着的进程之间尽量不同，同一个位置重启前后不变。host 网络下同一台宿主机上的容器主机名
  都和宿主机一样、pid 都是 1（实测），原来的"主机名:pid"就会撞；
- 租约值 = 标识 + 本进程随机的部分：续约、释放按整个值比对，标识撞了也比对不上；
- 复用：接手标识相同的租约之前，确认它已经没人在续约了；
- 开服读租约不续期。
"""

import asyncio
import contextlib
import logging
import multiprocessing
import os
import sys
import time
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


@pytest.fixture
def fast_reuse(monkeypatch):
    """确认旧租约没人续约的等待从 20 秒缩到 1 秒，测试不用等那么久"""
    from hetu.data.backend.redis import worker_keeper

    monkeypatch.setattr(worker_keeper, "REUSE_STALE_SEC", 1)


@contextlib.asynccontextmanager
async def renewing(keepers):
    """with 里这些 keeper 每 0.2 秒续一次约（活着的进程）。产出续约时发现租约丢了的那些"""
    lost = []

    async def renew_forever(keeper):
        try:
            while True:
                await asyncio.sleep(0.2)
                await keeper.keep_alive()
        except SystemExit:
            lost.append(keeper)

    tasks = [asyncio.create_task(renew_forever(keeper)) for keeper in keepers]
    try:
        yield lost
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


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


def test_lease_value_adds_per_process_part():
    """租约值 = 持有者标识 + "#" + 本进程随机的部分，每个进程都不同；去掉随机部分就是标识"""
    value = helper.lease_value("host-a:tok:1")
    assert value.startswith("host-a:tok:1#")
    assert value != helper.lease_value("host-a:tok:1")
    assert helper.lease_identity(value) == "host-a:tok:1"
    assert helper.lease_identity("host-a:1") == "host-a:1"


@posix_only
def test_sqlite_tool_lease_value_has_per_process_part(new_node):
    """SQLite 上工具进程的租约值同样是 标识 + 本进程随机的部分"""
    from typing import Any, cast

    from hetu.data.backend.worker_keeper import SQLiteToolWorkerKeeper

    first = SQLiteToolWorkerKeeper(cast(Any, None), 123)
    second = SQLiteToolWorkerKeeper(cast(Any, None), 123)
    token = helper.node_token()
    assert helper.lease_identity(first.node_id) == f"cli:host-a:{token}:123"
    assert first.node_id != second.node_id


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
    assert helper.lease_owner_exited("host-a:w0:111#ab12cd34")
    assert not helper.lease_owner_exited("host-a:w0:333#ab12cd34")
    assert not helper.lease_owner_exited("host-b:111")
    assert not helper.lease_owner_exited("pytest-node")


@posix_only
@use_redis_family_backend_only
async def test_containers_sharing_hostname_get_their_own_worker_ids(
    mod_auto_backend, new_node, fast_reuse
):
    """
    主机名一样、pid 都是 1 的两个容器（host 网络）：各拿各的 Worker ID。容器 a 被强杀后重启：同一个 token
    文件、pid 还是 1，旧租约还没过期也没人续约，确认之后接回原来的 id；原来那个进程要是其实还活着，续约会
    发现租约已经不是自己的
    """
    from hetu.data.backend.redis.worker_keeper import RedisWorkerKeeper

    aio = mod_auto_backend().master.aio
    new_node("container-a")
    a = RedisWorkerKeeper(1, aio)
    new_node("container-b")
    b = RedisWorkerKeeper(1, aio)
    keepers = [a, b]
    try:
        id_a = await a.get_worker_id()
        id_b = await b.get_worker_id()
        assert id_b != id_a

        new_node("container-a")
        keepers.append(restarted := RedisWorkerKeeper(1, aio))
        assert restarted.identity == a.identity
        assert restarted.node_id != a.node_id
        assert await restarted.get_worker_id() == id_a
        assert await aio.get(a._key(id_a)) == restarted.node_id.encode()
        with pytest.raises(SystemExit):
            await a.keep_alive()
    finally:
        for keeper in keepers:
            await keeper.release_worker_id()


@posix_only
@use_redis_family_backend_only
async def test_stale_lease_is_taken_over_without_waiting(mod_auto_backend, new_node):
    """重启前的进程早就没在续约了（比如 10 秒前就死了）：不用等，马上接回原来的 id"""
    from hetu.common.snowflake_id import REUSE_STALE_SEC, WORKER_ID_EXPIRE_SEC
    from hetu.data.backend.redis.worker_keeper import RedisWorkerKeeper

    aio = mod_auto_backend().master.aio
    new_node("container-a")
    dead = RedisWorkerKeeper(1, aio)
    worker_id = await dead.get_worker_id()
    # 最后一次续约在 REUSE_STALE_SEC + 5 秒前
    remaining = WORKER_ID_EXPIRE_SEC - REUSE_STALE_SEC - 5
    await aio.pexpire(dead._key(worker_id), remaining * 1000)

    new_node("container-a")
    restarted = RedisWorkerKeeper(1, aio)
    started = time.monotonic()
    try:
        assert await restarted.get_worker_id() == worker_id
        assert time.monotonic() - started < 2
    finally:
        await restarted.release_worker_id()


@posix_only
@use_redis_family_backend_only
async def test_live_twin_lease_is_not_taken_over(
    mod_auto_backend, new_node, fast_reuse, caplog
):
    """
    标识撞了的两个进程（比如 token 文件被打进了镜像），先起的那个还活着、照常续约：后起的不能把它的 id 当成
    自己重启前的接手，另拿一个；先起的照常续约
    """
    from hetu.data.backend.redis.worker_keeper import RedisWorkerKeeper

    aio = mod_auto_backend().master.aio
    new_node("baked")
    alive = RedisWorkerKeeper(1, aio)
    new_node("baked")
    twin = RedisWorkerKeeper(1, aio)
    assert twin.identity == alive.identity
    try:
        id_alive = await alive.get_worker_id()
        async with renewing([alive]) as lost:
            assert await twin.get_worker_id() != id_alive
        assert not lost
        await alive.keep_alive()
        # 标识撞了是部署的问题，打 ERROR
        assert any(
            record.levelno == logging.ERROR and "还有进程在续约" in record.getMessage()
            for record in caplog.records
        )
    finally:
        await alive.release_worker_id()
        await twin.release_worker_id()


@posix_only
@use_redis_family_backend_only
async def test_restart_waits_once_for_all_same_identity_leases(
    mod_auto_backend, new_node, fast_reuse
):
    """
    标识相同的租约有好几把：三个标识撞了的、活着的进程的（id 小），加上本进程重启前留下的（id 最大）。几把
    一起等一次就够，接回重启前那把，活着的都不碰。挨个等的话，每个活着的都要等差不多一整个 REUSE_STALE_SEC
    """
    from hetu.data.backend.redis.worker_keeper import RedisWorkerKeeper

    aio = mod_auto_backend().master.aio
    new_node("baked")
    twins = [RedisWorkerKeeper(1, aio) for _ in range(3)]
    dead = RedisWorkerKeeper(1, aio)
    restarted = RedisWorkerKeeper(1, aio)
    keepers = [*twins, dead, restarted]
    try:
        for worker_id, keeper in enumerate([*twins, dead], start=900):
            await aio.set(keeper._key(worker_id), keeper.node_id, ex=60)
            keeper.worker_id = worker_id
        async with renewing(twins) as lost:
            started = time.monotonic()
            assert await restarted.get_worker_id() == dead.worker_id
            assert time.monotonic() - started < 2
        assert not lost
    finally:
        for keeper in keepers:
            await keeper.release_worker_id()


def test_lease_timing_invariants():
    """
    同标识的进程最早在"距上次续约 REUSE_STALE_SEC 秒"时接手，那时原来的持有者不管是死是活都必须已经
    停止发号：发号围栏的安全期要比它短，并留出两边时钟的偏差；不同标识的要等 key 过期，更晚。安全期至少
    要挺得过一次续约失败
    """
    from hetu.common.snowflake_id import (
        LEASE_SAFE_SEC,
        RENEW_INTERVAL,
        REUSE_STALE_SEC,
        WORKER_ID_EXPIRE_SEC,
    )

    assert LEASE_SAFE_SEC + 5 <= REUSE_STALE_SEC < WORKER_ID_EXPIRE_SEC
    assert LEASE_SAFE_SEC >= 2 * RENEW_INTERVAL


@posix_only
@use_redis_family_backend_only
async def test_takeover_only_after_old_holder_stopped_minting(
    mod_auto_backend, new_node, monkeypatch
):
    """
    标识撞了的两个进程，先起的那个还活着、只是停了续约（卡住了）：后起的接手同一个 id 的时候，先起的那个
    的发号围栏一定已经关了，两边不会一起发号；它醒来续约发现租约丢了。两个时长按比例缩小（安全期 1.5 秒、
    接手门槛 2 秒），真实取值的关系见 test_lease_timing_invariants
    """
    from hetu.common import snowflake_id
    from hetu.data.backend.redis import worker_keeper
    from hetu.data.backend.redis.worker_keeper import RedisWorkerKeeper

    monkeypatch.setattr(snowflake_id, "LEASE_SAFE_SEC", 1.5)
    monkeypatch.setattr(worker_keeper, "REUSE_STALE_SEC", 2)
    aio = mod_auto_backend().master.aio
    new_node("baked")
    stalled = RedisWorkerKeeper(1, aio)
    new_node("baked")
    twin = RedisWorkerKeeper(1, aio)
    assert twin.identity == stalled.identity
    try:
        worker_id = await stalled.get_worker_id()
        assert stalled.lease_deadline is not None
        assert time.monotonic() <= stalled.lease_deadline  # 刚拿到，能发号
        assert await twin.get_worker_id() == worker_id  # 它不续约，被当成死的接手
        assert time.monotonic() > stalled.lease_deadline  # 但这时它早就不发号了
        with pytest.raises(SystemExit):
            await stalled.keep_alive()
    finally:
        await twin.release_worker_id()


@posix_only
@use_redis_family_backend_only
async def test_takeover_wait_is_capped(mod_auto_backend, new_node, fast_reuse):
    """
    标识相同的租约剩余 TTL 比满值还长（手动设过更长的过期、主从切换后两边时钟差）：最多等
    REUSE_STALE_SEC，到时候还没"过期"就当它还有人在续，另拿一个
    """
    from hetu.data.backend.redis.worker_keeper import RedisWorkerKeeper

    aio = mod_auto_backend().master.aio
    new_node("baked")
    odd = RedisWorkerKeeper(1, aio)
    restarted = RedisWorkerKeeper(1, aio)
    key = odd._key(950)
    await aio.set(key, odd.node_id, ex=300)
    started = time.monotonic()
    try:
        assert await restarted.get_worker_id() != 950
        assert time.monotonic() - started < 2
    finally:
        await restarted.release_worker_id()
        await aio.delete(key)


@posix_only
@use_redis_family_backend_only
async def test_renewal_detects_takeover_by_same_identity(mod_auto_backend, new_node):
    """
    本进程卡住超过 60 秒，租约过期后被一个标识相同的进程拿走了同一个 id：续约照样能发现。原来租约值只有
    标识，比对得上，两边接着用同一个 Worker ID
    """
    from hetu.data.backend.redis.worker_keeper import RedisWorkerKeeper

    aio = mod_auto_backend().master.aio
    new_node("baked")
    holder = RedisWorkerKeeper(1, aio)
    worker_id = await holder.get_worker_id()
    key = holder._key(worker_id)
    new_node("baked")
    twin = RedisWorkerKeeper(1, aio)
    assert twin.identity == holder.identity
    try:
        await aio.delete(key)  # 过期了
        await aio.set(key, twin.node_id, ex=60)  # 标识相同的进程用 SET NX 拿走
        with pytest.raises(SystemExit):
            await holder.keep_alive()
        assert holder.lease_deadline is not None
        assert time.monotonic() > holder.lease_deadline  # 发现了就马上停止发号
    finally:
        await aio.delete(key)


@use_redis_family_backend_only
async def test_startup_scan_does_not_extend_other_leases(mod_auto_backend, new_node):
    """
    开服读所有租约时不续期。原来逐个 GETEX，会把扫到的租约（包括已经死掉的）都刷回 60 秒：池子被死租约
    占满时，拿不到 id 的 worker 每秒重试、每次都把它们再续一遍，死租约就永远过不了期
    """
    from hetu.data.backend.redis.worker_keeper import RedisWorkerKeeper

    aio = mod_auto_backend().master.aio
    dead = "snowflake:worker:1010"
    await aio.set(dead, "dead-host:1#00000000", px=30_000)
    keeper = RedisWorkerKeeper(1, aio)
    try:
        await keeper.get_worker_id()
        assert 0 < await aio.pttl(dead) <= 30_000
    finally:
        await keeper.release_worker_id()
        await aio.delete(dead)


@use_redis_family_backend_only
async def test_supervised_worker_restart_reuses_its_worker_id(
    mod_auto_backend, new_node, monkeypatch, fast_reuse
):
    """
    管理进程以同一个序号重启的 worker（pid 变了）接回原来的 Worker ID；别的序号不接；换了管理进程（裸机上
    重启了 hetu start，旧 worker 可能还在退出）也不接
    """
    from hetu.data.backend.redis.worker_keeper import RedisWorkerKeeper

    aio = mod_auto_backend().master.aio
    monkeypatch.setattr(os, "getppid", lambda: 4242)
    monkeypatch.setenv(helper.WORKER_INDEX_ENV, "0")
    first = RedisWorkerKeeper(100, aio)
    keepers = [first]
    try:
        worker_id = await first.get_worker_id()
        keepers.append(restarted := RedisWorkerKeeper(101, aio))
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
