"""
有上限、池满才排队的异步连接池。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
from collections import deque
from typing import Any

from redis.asyncio.connection import AbstractConnection, ConnectionPool
from redis.credentials import StreamingCredentialProvider
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import MaxConnectionsError


class HeTuConnectionPool(ConnectionPool):
    """
    有上限、池满才排队的 redis-py 异步连接池，平时取/还连接不付排队的代价。

    - redis-py 8 的普通 `ConnectionPool` 满了直接抛 `MaxConnectionsError`，默认上限只有 100；
    - `BlockingConnectionPool` 满了会排队，但**每次**取、还连接都要进 `asyncio.Condition`、
      挂一个 `asyncio.timeout` 定时器，生产压测每次 RPC 多出 8%~11% CPU。

    本池：
    - 快路径：有空闲连接或池未满就直接取/还。同时跳过 redis-py 8 普通池每次取/还都有的
      `asyncio.Lock`、OTel 连接池计数和释放事件分发（HeTu 用不到）。
    - 慢路径：池满才进 FIFO 等待队列，有人还连接时叫醒队头；等 `timeout` 秒还没轮到就抛
      `ConnectionError("No connection available.")`，与 `BlockingConnectionPool` 一致。

    以下情况退回 redis-py 原逻辑（带锁、计数、事件）：
    - 配置了流式凭证或自定义事件分发器：重新认证等功能靠释放事件触发；
    - 开启了 redis-py 的 OTel：连接池计数要走原逻辑才准；
    - 池锁正被占用：维护通知处理器会持锁跨 await 改池状态；
    - 还回来的连接需要重连。

    快路径依赖的 redis-py 内部属性（`_available_connections` / `_in_use_connections` /
    `_lock` / `make_connection` / `ensure_connection`）由 tests/test_backend_redis_pool.py
    钉住，redis-py 升级改了名会直接测试失败。

    A bounded redis-py async connection pool that only pays for queueing when it is
    actually full: the fast path takes/returns connections without a lock or timer, and
    callers wait in a FIFO queue (up to `timeout` seconds) only when all
    `max_connections` connections are in use.
    """

    def __init__(self, *args: Any, timeout: float | None = 5.0, **kwargs: Any):
        """
        Parameters
        ----------
        timeout
            池满时排队等待的秒数，None 为一直等。其余参数同 `redis.asyncio.ConnectionPool`。
            Seconds to wait for a free connection when the pool is full; None waits forever.
        """
        super().__init__(*args, **kwargs)
        self.timeout = timeout
        self._waiters: deque[asyncio.Future[None]] = deque()
        self._lean = self._lean_supported(self.connection_kwargs)

    @staticmethod
    def _lean_supported(connection_kwargs: dict[str, Any]) -> bool:
        """快路径跳过的 redis-py 簿记，这些配置下是有用的，得走原逻辑"""
        credential_provider = connection_kwargs.get("credential_provider")
        if isinstance(credential_provider, StreamingCredentialProvider):
            return False
        if connection_kwargs.get("event_dispatcher") is not None:
            return False
        try:
            from redis.observability.providers import get_observability_instance

            manager = get_observability_instance().get_provider_manager()
            otel_on = manager is not None and bool(manager.config.enabled_telemetry)
        except Exception:  # noqa: BLE001 没有该模块或取不到配置，都当作没开 OTel
            otel_on = False
        return not otel_on

    async def get_connection(self, command_name=None, *keys, **options):
        """取一条已连接的连接；池满时排队，超时抛 ConnectionError"""
        connection = await self._try_get_connection()
        if connection is None:
            connection = await self._wait_for_connection()
        return connection

    async def _try_get_connection(self) -> AbstractConnection | None:
        """池没满就取一条（必要时新建），满了返回 None"""
        if not self._lean or self._lock.locked():
            try:
                return await super().get_connection()
            except MaxConnectionsError:
                return None
        # 以下到 add 为止没有 await，asyncio 单线程下不需要锁
        try:
            connection = self._available_connections.pop()
        except IndexError:
            if len(self._in_use_connections) >= self.max_connections:
                return None
            connection = self.make_connection()
        self._in_use_connections.add(connection)
        try:
            await self.ensure_connection(connection)
        except BaseException:
            await self.release(connection)
            raise
        return connection

    async def _wait_for_connection(self) -> AbstractConnection:
        """池满：排队等别人还连接"""
        loop = asyncio.get_running_loop()
        deadline = None if self.timeout is None else loop.time() + self.timeout
        requeue_front = False
        while True:
            waiter: asyncio.Future[None] = loop.create_future()
            if requeue_front:
                # 被叫醒了但名额又被新来的抢走：排回队头，别丢了位置
                self._waiters.appendleft(waiter)
            else:
                self._waiters.append(waiter)
            try:
                async with asyncio.timeout_at(deadline):
                    await waiter
            except BaseException as e:
                if waiter.done() and not waiter.cancelled():
                    # 已被叫醒却没来得及用（被取消或恰好超时）：名额让给下一个
                    self._wake_next()
                if isinstance(e, TimeoutError):
                    raise RedisConnectionError("No connection available.") from e
                raise
            connection = await self._try_get_connection()
            if connection is not None:
                return connection
            requeue_front = True

    def _wake_next(self) -> None:
        waiters = self._waiters
        while waiters:
            waiter = waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                return

    async def release(self, connection: AbstractConnection) -> None:
        """还连接，并叫醒一个排队者"""
        try:
            if (
                self._lean
                and not self._lock.locked()
                and not connection.should_reconnect()
            ):
                self._in_use_connections.remove(connection)
                self._available_connections.append(connection)
            else:
                await super().release(connection)
        finally:
            if self._waiters:
                self._wake_next()

    def reset(self) -> None:
        super().reset()
        # 池已清空，排队者都叫醒去重新取（能直接建新连接）
        waiters = getattr(self, "_waiters", None)
        while waiters:
            self._wake_next()
