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
                latest = (await conn.execute(sa.select(sa.func.max(table.c.id)))).scalar()
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

        while True:
            async with self._client.aio.connect() as conn:
                stmt = (
                    sa.select(notify.c.id, notify.c.channel)
                    .where(
                        notify.c.id > self._last_notify_id,
                        notify.c.channel.in_(channels),
                    )
                    .order_by(notify.c.id.asc())
                    .limit(256)
                )
                rows = (await conn.execute(stmt)).mappings().all()

            if rows:
                break
            await asyncio.sleep(interval / 2)

        for row in rows:
            msg_id = int(row["id"])
            if msg_id > self._last_notify_id:
                self._last_notify_id = msg_id
            channel_name = str(row["channel"])
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
