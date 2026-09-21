"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, final

import msgpack

from ....i18n import _
from ..base import HubMQClient, MQClient
from .pubsub import AsyncKeyspacePubSub

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from redis.asyncio.cluster import RedisCluster

logger = logging.getLogger("HeTu.root")
MAX_SUBSCRIBED = 5000


class PubSubHub:
    """
    每个工作进程（每个 servant BackendClient）一个：唯一的 pubsub 连接（cluster 模式下每个
    节点一条）+ "频道 → 本进程内订阅了它的连接" 的分发表。

    以前是每个 ws 连接一条 pubsub 连接（Redis 连接数 = 在线人数，还各自解析每条消息），
    现在 Redis 只推一次给本进程，这里按频道查表把通知直接塞进各连接的本地队列
    （`MQClient.push_pulled_`），全程不 await、不经任何 asyncio.Queue。

    分发表同时是引用计数：某频道第一个订阅者到来时才真的向 Redis 发 SUBSCRIBE，
    最后一个走了才 UNSUBSCRIBE。
    """

    def __init__(self, client: Redis | RedisCluster):
        self._pubsub = AsyncKeyspacePubSub(client, on_message=self._on_message)
        self._subs: dict[str, set[MQClient]] = {}
        self._closed = False

    @property
    def channels(self) -> set[str]:
        """本进程当前向 Redis 订阅了的频道"""
        return set(self._subs)

    def subscriber_count(self, channel: str) -> int:
        """某频道在本进程内的订阅连接数，测试用"""
        return len(self._subs.get(channel, ()))

    async def add(self, mq: MQClient, channels: Iterable[str]) -> None:
        """
        登记 mq 对这些频道的订阅。只有本进程内第一次出现的频道才向 Redis 发 SUBSCRIBE，
        返回时这些频道都已经订阅生效（别人发出、尚未 ack 的也会等到 ack）。
        """
        if self._closed:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        channels = list(channels)
        fresh = []
        for channel in channels:
            subs = self._subs.get(channel)
            if subs is None:
                self._subs[channel] = {mq}
                fresh.append(channel)
            else:
                subs.add(mq)
        try:
            if fresh:
                await self._pubsub.subscribe(*fresh)
            # 别人先发出、还没 ack 的频道也要等到 ack；它们发送失败时这里一起抛
            await self._pubsub.wait_subscribed(channels)
        except BaseException:
            # 本次发出的频道作废：连同期间搭车登记的其他连接一起撤销（它们在
            # wait_subscribed 里会收到同样的异常），别让后来者以为已经订上了
            for channel in fresh:
                self._subs.pop(channel, None)
            for channel in channels:
                subs = self._subs.get(channel)
                if subs is not None:
                    subs.discard(mq)
                    if not subs:
                        del self._subs[channel]
            raise
        # 等待期间可能被别人的失败撤销了登记
        for channel in channels:
            if mq not in self._subs.get(channel, ()):
                raise ConnectionError(
                    _("频道 {channel} 订阅失败").format(channel=channel)
                )

    async def remove(self, mq: MQClient, channels: Iterable[str]) -> None:
        """撤销 mq 对这些频道的订阅，本进程内没人再订的频道才向 Redis 发 UNSUBSCRIBE"""
        gone = []
        for channel in channels:
            subs = self._subs.get(channel)
            if subs is None:
                continue
            subs.discard(mq)
            if not subs:
                del self._subs[channel]
                gone.append(channel)
        if gone and not self._closed:
            try:
                await self._pubsub.unsubscribe(*gone)
            except Exception as e:  # noqa: BLE001 本地已退订，Redis 侧失败只影响多收几条会被忽略的消息
                logger.warning(
                    _("⚠️ [💾Redis] 取消订阅 {count} 个频道失败：{err}").format(
                        count=len(gone), err=f"{type(e).__name__}:{e}"
                    )
                )

    def _on_message(self, msg: dict) -> None:
        """AsyncKeyspacePubSub 的监听协程收到消息时同步调用，每条消息一次"""
        channel_name = msg["channel"].decode()
        subs = self._subs.get(channel_name)
        if not subs:
            return  # 刚退订、ack 还没回来的频道
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                _("🔔 [💾Redis] 收到订阅更新通知: {channel_name}").format(
                    channel_name=channel_name
                )
            )
        # 表级频道（非keyspace通知）带payload：msgpack的row_id列表
        ids = None
        if not channel_name.startswith("__keyspace@"):
            try:
                ids = msgpack.unpackb(msg["data"])
            except Exception:  # noqa: BLE001 非法payload当作无payload
                ids = None
            if not isinstance(ids, list):
                ids = None
        dropped = 0
        for mq in subs:
            dropped += mq.push_pulled_(channel_name, ids)
        if dropped:
            logger.warning(
                _(
                    "⚠️ [💾Redis] 订阅更新通知来不及处理，"
                    "丢弃了{seconds}秒前的消息共{count}条"
                ).format(seconds=MQClient.DROP_AFTER, count=dropped)
            )

    async def close(self) -> None:
        self._closed = True
        self._subs.clear()
        await self._pubsub.close()


@final
class RedisMQClient(HubMQClient):
    """
    每个用户连接一个实例：只是本连接订阅集合 + 本地消息队列，
    真正的 Redis pubsub 由本进程共享的 `PubSubHub` 持有。
    """

    MAX_SUBSCRIBED = MAX_SUBSCRIBED
    LOG_TAG = "💾Redis"
