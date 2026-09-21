"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import contextlib
import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, final

import msgpack
import sqlalchemy as sa

from ....i18n import _
from ..base import HubMQClient, MQClient

if TYPE_CHECKING:
    from .client import SQLBackendClient

logger = logging.getLogger("HeTu.root")
MAX_SUBSCRIBED = 5000
PULL_BATCH_SIZE = 256
# 避免SQLite等数据库在IN参数过多时触发参数上限/编译开销问题。
MAX_CHANNELS_IN_FILTER = 500


class SQLNotifyHub:
    """
    每个进程（每个 SQLBackendClient）一个：唯一的通知表轮询任务 + "频道 → 本进程内订阅了
    它的连接" 分发表。以前是每个连接各自轮询同一张通知表，DB 负载与在线人数成正比。

    与 Redis pubsub 语义对齐，只消费订阅之后产生的通知：每个频道加入本进程时记下当时通知表的
    最大 id 作为水位，之前的通知不算它的（游标只随命中订阅频道的行前进，可能落后于表尾，
    新频道加入时不能把旧通知重放给它）。没有任何订阅时不轮询。
    """

    def __init__(self, client: SQLBackendClient):
        self._client = client
        self._subs: dict[str, set[MQClient]] = {}
        # 频道加入 hub 时通知表的最大 id：id 不大于它的通知不属于该频道的订阅者
        self._since: dict[str, int] = {}
        self._last_notify_id = 0
        self._large_sub_warned = False
        self._task: asyncio.Task | None = None
        self._closed = False

    @property
    def channels(self) -> set[str]:
        """本进程当前订阅了的频道"""
        return set(self._subs)

    def subscriber_count(self, channel: str) -> int:
        """某频道在本进程内的订阅连接数，测试用"""
        return len(self._subs.get(channel, ()))

    async def _get_current_notify_id(self) -> int:
        # 查不到就让异常抛给 add() 的调用方：游标退回 0 会把整张通知表重放一遍
        table = self._client.notify_table()
        async with self._client.aio.connect() as conn:
            latest = (await conn.execute(sa.select(sa.func.max(table.c.id)))).scalar()
        return int(latest or 0)

    async def add(self, mq: MQClient, channels: Iterable[str]) -> None:
        """登记 mq 对这些频道的订阅；本进程从无到有订阅时重置游标并启动轮询"""
        if self._closed:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        channels = list(channels)
        fresh = [channel for channel in channels if channel not in self._subs]
        if fresh:
            watermark = await self._get_current_notify_id()
            if not self._subs:
                self._last_notify_id = watermark
            for channel in fresh:
                self._since[channel] = watermark
        for channel in channels:
            self._subs.setdefault(channel, set()).add(mq)
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    async def remove(self, mq: MQClient, channels: Iterable[str]) -> None:
        """撤销 mq 对这些频道的订阅。本进程没人订阅了轮询任务会自己退出，别空转打 DB"""
        for channel in channels:
            subs = self._subs.get(channel)
            if subs is None:
                continue
            subs.discard(mq)
            if not subs:
                del self._subs[channel]
                self._since.pop(channel, None)

    async def _stop_task(self) -> None:
        # 只在 close 时取消：在 SQL 语句执行中途取消会弄坏池里的连接
        if self._task is None:
            return
        task, self._task = self._task, None
        if task is not asyncio.current_task():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    @staticmethod
    def _should_use_channel_in_filter(subscribed_count: int) -> bool:
        return subscribed_count <= MAX_CHANNELS_IN_FILTER

    async def poll_once(self) -> tuple[int, int]:
        """
        查一批游标之后的通知并分发给订阅者。返回 (本批查到的行数, 其中命中本进程订阅的条数)。
        回退到按 id 扫描模式时可能整批都是无关频道，命中为 0。
        """
        notify = self._client.notify_table()
        async with self._client.aio.connect() as conn:
            channels = list(self._subs)
            use_channel_filter = self._should_use_channel_in_filter(len(channels))
            if not use_channel_filter and not self._large_sub_warned:
                logger.warning(
                    "⚠️ [💾SQL] 订阅频道过多，pull切换为按id扫描后本地过滤模式，"
                    f"当前订阅数={len(channels)}，阈值={MAX_CHANNELS_IN_FILTER}"
                )
                self._large_sub_warned = True

            stmt = sa.select(notify.c.id, notify.c.channel, notify.c.payload).where(
                notify.c.id > self._last_notify_id
            )
            if use_channel_filter:
                stmt = stmt.where(notify.c.channel.in_(channels))
            stmt = stmt.order_by(notify.c.id.asc()).limit(PULL_BATCH_SIZE)
            rows = (await conn.execute(stmt)).mappings().all()

        hits = 0
        for row in rows:
            msg_id = int(row["id"])
            if msg_id > self._last_notify_id:
                self._last_notify_id = msg_id
            channel_name = str(row["channel"])
            subs = self._subs.get(channel_name)
            if not subs or msg_id <= self._since.get(channel_name, 0):
                continue
            hits += 1
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    _("🔔 [💾SQL] 收到订阅更新通知: {channel_name}").format(
                        channel_name=channel_name
                    )
                )
            payload = row.get("payload")
            ids = msgpack.unpackb(payload) if payload else None
            dropped = 0
            for mq in subs:
                dropped += mq.push_pulled_(channel_name, ids)
            if dropped:
                logger.warning(
                    _(
                        "⚠️ [💾SQL] 订阅更新通知来不及处理，"
                        "丢弃了{seconds}秒前的消息共{count}条"
                    ).format(seconds=MQClient.DROP_AFTER, count=dropped)
                )
        return len(rows), hits

    async def _run(self) -> None:
        interval = 1 / MQClient.UPDATE_FREQUENCY
        while not self._closed:
            if not self._subs:
                # 没人订阅了就退出；下次 add() 看到任务已结束会重新起（此处到 return 无 await）
                self._task = None
                return
            try:
                fetched, _hits = await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(_("❌ [💾SQL] 轮询通知表失败，稍后重试"))
                fetched = 0
            if fetched < PULL_BATCH_SIZE:
                # 追平了（没查满一批）才歇一下；查满一批说明还有积压，立刻接着查
                await asyncio.sleep(interval / 2)

    async def close(self) -> None:
        self._closed = True
        self._subs.clear()
        self._since.clear()
        await self._stop_task()


@final
class SQLMQClient(HubMQClient):
    """
    每个用户连接一个实例：只是本连接订阅集合 + 本地消息队列，
    通知表的轮询由本进程共享的 `SQLNotifyHub` 负责。
    """

    MAX_SUBSCRIBED = MAX_SUBSCRIBED
    LOG_TAG = "💾SQL"
