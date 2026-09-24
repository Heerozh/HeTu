"""
HeTuConnectionPool（有上限、池满才排队的连接池）与 get_many 单行直读。
"""

import asyncio
import time
from typing import cast

import numpy as np
import pytest
from fixtures.backends import (
    REDIS_BACKENDS,
    REDIS_FORK_BACKENDS,
    use_redis_family_backend_only,
)
from redis.asyncio import Redis
from redis.event import EventDispatcher
from redis.exceptions import ConnectionError as RedisConnectionError

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend
from hetu.data.backend.redis import RedisBackendClient
from hetu.data.backend.redis.pool import HeTuConnectionPool

SnowflakeID().init(1, 0)

# 该连接池只用在 standalone 模式；原生集群用的是 redis-py 自己的每节点连接池
use_standalone_redis_only = pytest.mark.parametrize(
    "backend_name",
    [b for b in REDIS_BACKENDS + REDIS_FORK_BACKENDS if b != "redis_cluster"],
    indirect=True,
)


def _make_pool(backend: Backend, **kwargs) -> tuple[HeTuConnectionPool, Redis]:
    """在测试库上另建一个池（及接管它的 client），不影响 backend 自己的"""
    url = cast(RedisBackendClient, backend.master).urls[0]
    pool = HeTuConnectionPool.from_url(url, **kwargs)
    return pool, Redis.from_pool(pool)


@use_standalone_redis_only
async def test_pool_used_and_redis_internals_pinned(mod_auto_backend):
    """backend 用的是本池；快路径依赖的 redis-py 内部属性，redis-py 升级改名会先挂在这"""
    backend: Backend = mod_auto_backend()
    for client in (backend.master, backend.servant):
        pool = cast(Redis, cast(RedisBackendClient, client).aio).connection_pool
        assert isinstance(pool, HeTuConnectionPool)
        assert pool._lean

    pool, r = _make_pool(backend, max_connections=4)
    try:
        assert isinstance(pool._lock, asyncio.Lock)
        assert callable(pool.make_connection)
        assert callable(pool.ensure_connection)
        conn = await pool.get_connection()
        assert conn in pool._in_use_connections
        assert conn not in pool._available_connections
        assert conn.should_reconnect() is False
        await pool.release(conn)
        assert conn not in pool._in_use_connections
        assert pool._available_connections[-1] is conn
        # 空闲连接被复用
        assert await pool.get_connection() is conn
        await pool.release(conn)
    finally:
        await r.aclose()


def test_pool_falls_back_with_custom_event_dispatcher():
    """配了自定义事件分发器（可能挂着重新认证等监听者）就不走快路径"""
    assert HeTuConnectionPool(host="127.0.0.1")._lean
    assert not HeTuConnectionPool(
        host="127.0.0.1", event_dispatcher=EventDispatcher()
    )._lean


@use_standalone_redis_only
async def test_pool_bounded_and_queued(mod_auto_backend):
    """并发远多于上限：连接数不超过上限，多出来的排队后都能完成"""
    backend: Backend = mod_auto_backend()
    pool, r = _make_pool(backend, max_connections=2, timeout=5)
    made = 0
    make_connection = pool.make_connection

    def counting_make_connection():
        nonlocal made
        made += 1
        return make_connection()

    pool.make_connection = counting_make_connection  # type: ignore[method-assign]
    try:
        await r.set("pytest:pool:bounded", "v")
        results = await asyncio.gather(
            *(r.get("pytest:pool:bounded") for _ in range(50))
        )
        assert results == [b"v"] * 50
        assert made <= 2
        assert len(pool._in_use_connections) == 0
        assert len(pool._available_connections) <= 2
        assert not pool._waiters
    finally:
        await r.aclose()


@use_standalone_redis_only
async def test_pool_timeout_when_full(mod_auto_backend):
    """池满等过 timeout 抛 ConnectionError，与 BlockingConnectionPool 一致；还回来后立刻可用"""
    backend: Backend = mod_auto_backend()
    pool, r = _make_pool(backend, max_connections=1, timeout=0.2)
    try:
        held = await pool.get_connection()
        started = time.perf_counter()
        with pytest.raises(RedisConnectionError, match="No connection available"):
            await pool.get_connection()
        assert time.perf_counter() - started >= 0.15
        await pool.release(held)
        async with asyncio.timeout(1):
            assert await r.ping()
    finally:
        await r.aclose()


@use_standalone_redis_only
async def test_pool_waiters_fifo(mod_auto_backend):
    """排队者按先来后到拿到连接"""
    backend: Backend = mod_auto_backend()
    pool, r = _make_pool(backend, max_connections=1, timeout=2)
    order: list[int] = []

    async def take(i: int):
        conn = await pool.get_connection()
        order.append(i)
        await pool.release(conn)

    try:
        held = await pool.get_connection()
        tasks = [asyncio.create_task(take(i)) for i in range(5)]
        await asyncio.sleep(0.05)
        assert len(pool._waiters) == 5
        await pool.release(held)
        async with asyncio.timeout(1):
            await asyncio.gather(*tasks)
        assert order == [0, 1, 2, 3, 4]
    finally:
        await r.aclose()


@use_standalone_redis_only
async def test_pool_cancelled_waiter_passes_slot(mod_auto_backend):
    """被叫醒后没来得及用就被取消的排队者，要把名额让给下一个，否则后面的会白等到超时"""
    backend: Backend = mod_auto_backend()
    pool, r = _make_pool(backend, max_connections=1, timeout=2)
    try:
        held = await pool.get_connection()
        first = asyncio.create_task(pool.get_connection())
        second = asyncio.create_task(pool.get_connection())
        await asyncio.sleep(0.05)
        assert len(pool._waiters) == 2
        await pool.release(held)  # 叫醒队头 first
        first.cancel()  # first 还没来得及跑就被取消
        async with asyncio.timeout(1):
            conn = await second
        assert conn is held
        with pytest.raises(asyncio.CancelledError):
            await first
        await pool.release(conn)
        assert len(pool._in_use_connections) == 0
    finally:
        await r.aclose()


@use_standalone_redis_only
async def test_pool_bookkeeping_survives_connect_failure(mod_auto_backend):
    """取连接时连不上：那条连接还回池里，名额不漏"""
    backend: Backend = mod_auto_backend()
    pool, r = _make_pool(backend, max_connections=1, timeout=0.5)
    ensure_connection = pool.ensure_connection

    async def broken(connection):
        raise RedisConnectionError("boom")

    try:
        pool.ensure_connection = broken  # type: ignore[method-assign]
        with pytest.raises(RedisConnectionError, match="boom"):
            await pool.get_connection()
        pool.ensure_connection = ensure_connection  # type: ignore[method-assign]
        assert len(pool._in_use_connections) == 0
        async with asyncio.timeout(1):
            assert await r.ping()
    finally:
        await r.aclose()


@use_standalone_redis_only
async def test_pool_waits_for_lock_held_by_redis(mod_auto_backend):
    """
    池锁被占用时取/还连接走 redis-py 原逻辑、等锁：redis-py 的维护通知处理器会持池锁跨
    await 改池状态，快路径不能和它交错
    """
    backend: Backend = mod_auto_backend()
    pool, r = _make_pool(backend, max_connections=4)
    try:
        assert await r.ping()
        async with pool._lock:
            task = asyncio.ensure_future(r.ping())
            await asyncio.sleep(0.05)
            assert not task.done()
        async with asyncio.timeout(1):
            assert await task
        assert len(pool._in_use_connections) == 0
    finally:
        await r.aclose()


@use_redis_family_backend_only
async def test_get_many_single_row_skips_pipeline(
    filled_item_ref, mod_auto_backend, monkeypatch
):
    """get_many/range 只读 1 行时直接 HGETALL，不建 pipeline；2 行以上照旧批量"""
    backend: Backend = mod_auto_backend()
    servant = cast(RedisBackendClient, backend.servant)
    rows = await servant.range(filled_item_ref, "time", 110, 111, limit=2)
    ids = [int(row.id) for row in rows]
    assert len(ids) == 2

    def no_pipeline(*args, **kwargs):
        raise AssertionError("单行读取不该走 pipeline")

    async def get_many(row_ids) -> list[np.record]:
        return cast(list[np.record], await servant.get_many(filled_item_ref, row_ids))

    for aio in servant._async_ios:
        monkeypatch.setattr(aio, "pipeline", no_pipeline)
    got = await get_many([ids[0]])
    assert len(got) == 1 and got[0].id == ids[0]
    assert await get_many([999999999]) == [None]
    # 入参是迭代器也行
    got = await get_many(iter([ids[1]]))
    assert got[0].id == ids[1]
    assert await get_many([]) == []
    one = await servant.range(filled_item_ref, "time", 110, limit=1)
    assert len(one) == 1 and one[0].id == ids[0]

    monkeypatch.undo()
    got = await get_many(ids)
    assert [row.id for row in got] == ids
