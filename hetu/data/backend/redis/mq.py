"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from typing import TYPE_CHECKING, final, override

import msgpack

from ....i18n import _
from ..base import MQClient
from .pubsub import AsyncKeyspacePubSub

if TYPE_CHECKING:
    from .client import RedisBackendClient

logger = logging.getLogger("HeTu.root")
MAX_SUBSCRIBED = 5000


@final
class RedisMQClient(MQClient):
    """
    连接到消息队列的客户端，每个用户连接一个实例。
    本客户端使用AsyncKeyspacePubSub，以redis的pubsub功能作为消息队列，redis的notify功能作为写入通知。
    """

    def __init__(self, client: RedisBackendClient):
        # 2种模式：
        # a. 每个ws连接一个pubsub连接，分发交给servants，结构清晰，目前的模式，但网络占用高
        # b. 每个worker一个pubsub连接，分发交给worker来做，这样连接数较少，但等于2套分发系统结构复杂
        #    且这个方式如果redis维护变更了ip/集群规模等，整个服务会瘫痪，而a方式只要用户重连
        # 这里采用a方式
        self._client = client
        # redis-py库 cluster模式的pubsub不支持异步，不支持gather消息，用自己写的
        self._mq = AsyncKeyspacePubSub(client.aio)

        self.subscribed = set()
        super().__init__()  # 本地消息队列

    @override
    async def close(self):
        return await self._mq.close()

    @override
    async def subscribe(self, *channel_names: str) -> None:
        """订阅频道（可多个，一次往返），频道名通过 client.xxx_channel(table_ref) 获得"""
        if not channel_names:
            return
        await self._mq.subscribe(*channel_names)
        self.subscribed.update(channel_names)
        if len(self.subscribed) > MAX_SUBSCRIBED:
            # 抑制此警告可通过修改hetu.backend.redis.MAX_SUBSCRIBED参数
            logger.warning(
                f"⚠️ [💾Redis] 当前连接订阅数超过全局限制MAX_SUBSCRIBED={MAX_SUBSCRIBED}行，"
            )

    @override
    async def unsubscribe(self, *channel_names: str) -> None:
        """取消订阅频道（可多个），频道名通过 client.xxx_channel(table_ref) 获得"""
        if not channel_names:
            return
        await self._mq.unsubscribe(*channel_names)
        self.subscribed.difference_update(channel_names)

    @override
    async def pull(self) -> None:
        """
        从消息队列接收一条消息到本地队列，消息内容为channel名。每行数据，每个Index，都是一个channel。
        该channel收到了任何消息都说明有数据更新，所以只需要保存channel名。

        这是一个阻塞函数，每个用户连接都需要单独运行一个协程来无限循环轮询它，以此来防止服务器消息堆积。
        消息多时，如果几秒不调用，Redis都会崩。

        Notes
        -----
        * pull下来的消息会合批（重复消息合并）
        * 超过2分钟前的消息会被丢弃，防止堆积
        """

        # 获得更新的频道名，交给基类入队（去重、清旧）。这是每条通知都走的热路径
        msg = await self._mq.get_message()

        if msg is not None:
            channel_name = msg["channel"].decode()
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    _("🔔 [💾Redis] 收到订阅更新通知: {channel_name}").format(
                        channel_name=channel_name
                    )
                )
            # 表级频道（非keyspace通知）带payload：msgpack的row_id列表，按频道合并
            ids = None
            if not channel_name.startswith("__keyspace@"):
                try:
                    ids = msgpack.unpackb(msg["data"])
                except Exception:  # noqa: BLE001 非法payload当作无payload
                    ids = None
                if not isinstance(ids, list):
                    ids = None
            dropped = self.push_pulled_(channel_name, ids)
            if dropped:
                logger.warning(
                    _(
                        "⚠️ [💾Redis] 订阅更新通知来不及处理，"
                        "丢弃了{seconds}秒前的消息共{count}条"
                    ).format(seconds=self.DROP_AFTER, count=dropped)
                )

    @property
    @override
    def subscribed_channels(self) -> set[str]:
        """返回当前订阅的所有频道名"""
        return self._mq.subscribed
