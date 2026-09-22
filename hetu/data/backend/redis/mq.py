"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, final, override

import msgpack

from ....i18n import _
from ..base import HubMQClient, MQClient, MQHub
from .pubsub import AsyncKeyspacePubSub

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from redis.asyncio.cluster import RedisCluster

    from ..rowcache import RowCache

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

    def __init__(self, client: Redis | RedisCluster, row_cache: RowCache | None = None):
        super().__init__()
        self._pubsub = AsyncKeyspacePubSub(
            client,
            on_message=self._on_message,
            on_reset=self._on_reset,
            on_restored=self._on_restored,
        )
        # 本进程的行缓存（None 表示不缓存）与本 hub 已在缓存里激活的行频道
        self._row_cache = row_cache
        self._active: set[str] = set()

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
            # 订阅已生效：从现在起这些行的每次变更都会通知到本进程，缓存可以收留它们
            self._activate(registered)
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

    # ------------------------------------------------------------ 行缓存钩子

    def _activate(self, channels: Iterable[str]) -> None:
        """
        把这些频道里的行频道在缓存里激活。只激活仍在 _subs 里、且 pubsub 层确认订阅已 ack
        的：节点恢复期间别的连接刚发出 SUBSCRIBE 还没 ack 的频道也在 _subs 里，这时激活它，
        ack 之前的写入就没有通知、缓存却当它有效；等它自己的 add 拿到 ack 再激活
        """
        cache = self._row_cache
        if cache is None:
            return
        from .client import RedisBackendClient  # client 懒加载本模块，避免循环 import

        pubsub = self._pubsub
        for channel in channels:
            if (
                channel in self._subs
                and channel not in self._active
                and pubsub.is_subscribed(channel)
                and RedisBackendClient.is_row_channel(channel)
            ):
                self._active.add(channel)
                cache.activate(channel, self)

    def _deactivate_all(self) -> None:
        cache = self._row_cache
        if cache is not None:
            for channel in self._active:
                cache.deactivate(channel, self)
        self._active.clear()

    @override
    def _on_channel_gone(self, channel: str) -> None:
        # 在 _release 里同步调用，先于 UNSUBSCRIBE 发出：退订 ack 前的读不能再填充
        if channel in self._active:
            self._active.discard(channel)
            if self._row_cache is not None:
                self._row_cache.deactivate(channel, self)

    def _on_reset(self) -> None:
        """pubsub 节点断了：断到恢复之间的通知已丢，本 hub 激活的行全部失活（缓存随之清掉）"""
        self._deactivate_all()

    def _on_restored(self) -> None:
        """全部频道重新订阅生效：把仍有人订、且已 ack 的行频道重新激活（floor 回到未知，
        下次走权威读）"""
        self._activate(list(self._subs))

    def _on_message(self, msg: dict) -> None:
        """AsyncKeyspacePubSub 的监听协程收到消息时同步调用，每条消息一次"""
        channel_name = msg["channel"].decode()
        # 非 keyspace 频道带 payload：表级 / 索引值频道是 msgpack 的 row_id 列表，
        # 行频道是 msgpack 的整数——该行提交后的新 _version（0 表示删除），只给行缓存用
        ids = None
        if not channel_name.startswith("__keyspace@"):
            try:
                payload = msgpack.unpackb(msg["data"])
            except Exception:  # noqa: BLE001 非法payload当作无payload
                payload = None
            if isinstance(payload, list):
                ids = payload
            elif isinstance(payload, int) and self._row_cache is not None:
                # 先于 _subs 判断：刚退订的频道 notify 也无害（缓存里已没有它）
                self._row_cache.notify(channel_name, payload)
        subs = self._subs.get(channel_name)
        if not subs:
            return  # 刚退订、ack 还没回来的频道
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                _("🔔 [💾Redis] 收到订阅更新通知: {channel_name}").format(
                    channel_name=channel_name
                )
            )
        dropped = self._dispatch(channel_name, ids)
        if dropped:
            logger.warning(
                _(
                    "⚠️ [💾Redis] 订阅更新通知来不及处理，"
                    "丢弃了{seconds}秒前的消息共{count}条"
                ).format(seconds=MQClient.DROP_AFTER, count=dropped)
            )

    async def close(self) -> None:
        self._closed = True
        self._deactivate_all()
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
