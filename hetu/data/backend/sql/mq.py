"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
import time
from typing import TYPE_CHECKING, final, override

import sqlalchemy as sa

from ....common.multimap import MultiMap
from ..base import MQClient

if TYPE_CHECKING:
    from .client import SQLBackendClient

logger = logging.getLogger("HeTu.root")
MAX_SUBSCRIBED = 5000
PULL_BATCH_SIZE = 256
# 避免SQLite等数据库在IN参数过多时触发参数上限/编译开销问题。
MAX_CHANNELS_IN_FILTER = 500


@final
class SQLMQClient(MQClient):
    """
    SQL通知表驱动的MQ客户端。
    每个连接一个实例，订阅的channel会从通知表轮询拉取。
    """

    def __init__(self, client: SQLBackendClient):
        self._client = client
        self.subscribed = set()
        self.pulled_deque = MultiMap()
        self.pulled_set = set()
        self._last_notify_id = self._get_current_notify_id_sync()
        self._large_sub_warned = False

    def _get_current_notify_id_sync(self) -> int:
        table = self._client.notify_table()
        try:
            with self._client.io.connect() as conn:
                latest = conn.execute(sa.select(sa.func.max(table.c.id))).scalar()
        except Exception:
            return 0
        return int(latest or 0)

    async def _get_current_notify_id_async(self) -> int:
        table = self._client.notify_table()
        try:
            async with self._client.aio.connect() as conn:
                latest = (
                    await conn.execute(sa.select(sa.func.max(table.c.id)))
                ).scalar()
        except Exception:
            return self._last_notify_id
        return int(latest or 0)

    @override
    async def close(self):
        return None

    @override
    async def subscribe(self, channel_name) -> None:
        if not self.subscribed:
            # 与Redis pubsub语义对齐：只消费订阅之后产生的通知。
            self._last_notify_id = await self._get_current_notify_id_async()
        self.subscribed.add(channel_name)
        if len(self.subscribed) > MAX_SUBSCRIBED:
            logger.warning(
                f"⚠️ [💾SQL] 当前连接订阅数超过全局限制MAX_SUBSCRIBED={MAX_SUBSCRIBED}行，"
            )

    @override
    async def unsubscribe(self, channel_name) -> None:
        self.subscribed.remove(channel_name)

    @override
    async def pull(self) -> None:
        interval = 1 / self.UPDATE_FREQUENCY
        if not self.subscribed:
            await asyncio.sleep(interval)
            return

        notify = self._client.notify_table()
        channels = list(self.subscribed)
        use_channel_filter = self._should_use_channel_in_filter(len(channels))
        if not use_channel_filter and not self._large_sub_warned:
            logger.warning(
                "⚠️ [💾SQL] 订阅频道过多，pull切换为按id扫描后本地过滤模式，"
                f"当前订阅数={len(channels)}，阈值={MAX_CHANNELS_IN_FILTER}"
            )
            self._large_sub_warned = True

        while True:
            async with self._client.aio.connect() as conn:
                stmt = sa.select(notify.c.id, notify.c.channel).where(
                    notify.c.id > self._last_notify_id
                )
                if use_channel_filter:
                    stmt = stmt.where(notify.c.channel.in_(channels))
                stmt = stmt.order_by(notify.c.id.asc()).limit(PULL_BATCH_SIZE)
                rows = (await conn.execute(stmt)).mappings().all()

            if rows:
                has_subscribed_updates = False
                for row in rows:
                    msg_id = int(row["id"])
                    if msg_id > self._last_notify_id:
                        self._last_notify_id = msg_id
                    channel_name = str(row["channel"])
                    if channel_name not in self.subscribed:
                        continue
                    has_subscribed_updates = True
                    logger.debug(f"🔔 [💾SQL] 收到订阅更新通知: {channel_name}")

                    dropped = set(self.pulled_deque.pop(0, time.time() - 120))
                    if dropped:
                        self.pulled_set -= dropped
                        logger.warning(
                            f"⚠️ [💾SQL] 订阅更新通知来不及处理，"
                            f"丢弃了2分钟前的消息共{len(dropped)}条"
                        )

                    if channel_name not in self.pulled_set:
                        self.pulled_deque.add(time.time(), channel_name)
                        self.pulled_set.add(channel_name)

                if has_subscribed_updates:
                    break

                # fallback模式可能读到无关频道，短暂等待避免在高写入流量下空转热循环。
                await asyncio.sleep(interval / 2)
                continue
            await asyncio.sleep(interval / 2)

    @staticmethod
    def _should_use_channel_in_filter(subscribed_count: int) -> bool:
        return subscribed_count <= MAX_CHANNELS_IN_FILTER

    @override
    async def get_message(self) -> set[str]:
        pulled_deque = self.pulled_deque
        interval = 1 / self.UPDATE_FREQUENCY

        while not pulled_deque:
            await asyncio.sleep(interval)

        while True:
            rtn = set(pulled_deque.pop(0, time.time() - interval))
            if rtn:
                self.pulled_set -= rtn
                return rtn
            await asyncio.sleep(interval)

    @property
    @override
    def subscribed_channels(self) -> set[str]:
        return self.subscribed
