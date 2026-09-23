"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, final

import msgpack

from ....i18n import _
from ..base import HubMQClient, MQClient, MQHub
from .pubsub import AsyncKeyspacePubSub

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from redis.asyncio.cluster import RedisCluster

logger = logging.getLogger("HeTu.root")


class PubSubHub(MQHub):
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
        super().__init__()
        self._pubsub = AsyncKeyspacePubSub(
            client, on_message=self._on_message, on_resubscribed=self._on_resubscribed
        )

    async def add(self, mq: MQClient, channels: Iterable[str]) -> None:
        """
        登记 mq 对这些频道的订阅。只有本进程内第一次出现的频道才向 Redis 发 SUBSCRIBE，
        返回时这些频道都已经订阅生效（别人发出、尚未 ack 的也会等到 ack）。
        """
        if self._closed:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        pubsub = self._pubsub
        fresh: list[str] = []
        piggyback: list[str] = []
        # 本次新登记的频道；已登记过的重复 add 是幂等的，失败/取消时不能把它们也撤了
        registered: list[str] = []
        for channel in channels:
            subs = self._subs.get(channel)
            if subs is None:
                self._subs[channel] = {mq}
                registered.append(channel)
                fresh.append(channel)
                continue
            if mq not in subs:
                subs.add(mq)
                registered.append(channel)
            if pubsub.is_subscribing(channel):
                piggyback.append(channel)
            else:
                # 有人登记了但 Redis 侧没在订：上一个发起者的 SUBSCRIBE 失败了、
                # 等它的人还没来得及撤登记，得由本次重新发
                fresh.append(channel)
        # 别人先发出、还没 ack 的频道要等到 ack：future 现在就拿，等自己的 SUBSCRIBE
        # 回来再拿的话，它们中途发送失败会被摘掉，就看不到那个失败了
        acks = pubsub.pending_acks(piggyback)
        try:
            if fresh:
                await pubsub.subscribe(*fresh)
            await pubsub.wait_acks(acks)
        except asyncio.CancelledError:
            # 本调用方自己被取消（连接在拆）：SUBSCRIBE 由 pubsub 层保证照常发出，
            # 这里只撤自己的登记；撤完没人要的频道后台退订，取消流程不能停下来等 ack
            self._release_in_background(mq, registered)
            raise
        except BaseException:
            # 发送失败（发起者和搭车者等的 future 都带着同样的异常，各撤各的登记）；
            # 已发出去的频道撤完没人要就后台退订，别在 Redis 上留下没人收的订阅
            self._release_in_background(mq, registered)
            raise

    def _release_in_background(self, mq: MQClient, channels: Iterable[str]) -> None:
        gone = self._release(mq, channels)
        if gone and not self._closed:
            self._spawn(self._unsubscribe_later(gone))

    async def _unsubscribe_later(self, channels: list[str]) -> None:
        # 排队到真正跑起来之间可能又有人订了这些频道，那就不能退了
        channels = [channel for channel in channels if channel not in self._subs]
        if channels:
            await self._unsubscribe(channels)

    async def _unsubscribe(self, channels: list[str]) -> None:
        try:
            await self._pubsub.unsubscribe(*channels)
        except Exception as e:  # noqa: BLE001 本地已退订，Redis 侧失败只影响多收几条会被忽略的消息
            logger.warning(
                _("⚠️ [💾Redis] 取消订阅 {count} 个频道失败：{err}").format(
                    count=len(channels), err=f"{type(e).__name__}:{e}"
                )
            )

    async def remove(self, mq: MQClient, channels: Iterable[str]) -> None:
        """撤销 mq 对这些频道的订阅，本进程内没人再订的频道才向 Redis 发 UNSUBSCRIBE"""
        gone = self._release(mq, channels)
        if gone and not self._closed:
            await self._unsubscribe(gone)

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
        # 表级频道（非keyspace通知）带payload：msgpack的row_id列表；值频道的消息是空串，
        # 直接当作无payload
        ids = None
        data = msg["data"]
        if data and not channel_name.startswith("__keyspace@"):
            try:
                ids = msgpack.unpackb(data)
            except Exception:  # noqa: BLE001 非法payload当作无payload
                ids = None
            if not isinstance(ids, list):
                ids = None
        self._warn_dropped(self._dispatch(channel_name, ids))

    def _on_resubscribed(self, channels: list[str]) -> None:
        """
        AsyncKeyspacePubSub 节点失效后全部重订生效时同步调用：失效到现在的写入都没有通知，
        给仍有人订的频道各分发一条 `RESYNC`，各连接一个 interval 后补读（行 / 索引订阅
        重读、重跑比对；整表订阅整表重同步）。服务端内部 watch 的回调也照常触发一次，
        断线期间丢的顶号通知由此补查。
        """
        dropped = 0
        for channel in channels:
            if channel in self._subs:
                dropped += self._dispatch(channel, [MQClient.RESYNC])
        self._warn_dropped(dropped)

    @staticmethod
    def _warn_dropped(dropped: int) -> None:
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
        # 后台退订等的 ack 不会再来了，别让它们在 pubsub 关闭时各报一条失败
        await self._cancel_tasks()
        await self._pubsub.close()


@final
class RedisMQClient(HubMQClient):
    """
    每个用户连接一个实例：只是本连接订阅集合 + 本地消息队列，
    真正的 Redis pubsub 由本进程共享的 `PubSubHub` 持有。
    """

    LOG_TAG = "💾Redis"
