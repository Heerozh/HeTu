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

from ....common.multimap import MultiMap
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
        self.pulled_deque = MultiMap()  # 可按时间查询的消息队列
        self.pulled_set = set()  # 和pulled_deque内容保持一致的set，方便去重

    @override
    async def close(self):
        return await self._mq.close()

    @override
    async def subscribe(self, channel_name) -> None:
        """订阅频道，频道名通过 client.xxx_channel(table_ref) 获得"""
        await self._mq.subscribe(channel_name)
        self.subscribed.add(channel_name)
        if len(self.subscribed) > MAX_SUBSCRIBED:
            # 抑制此警告可通过修改hetu.backend.redis.MAX_SUBSCRIBED参数
            logger.warning(
                f"⚠️ [💾Redis] 当前连接订阅数超过全局限制MAX_SUBSCRIBED={MAX_SUBSCRIBED}行，"
            )

    @override
    async def unsubscribe(self, channel_name) -> None:
        """取消订阅频道，频道名通过 client.xxx_channel(table_ref) 获得"""
        await self._mq.unsubscribe(channel_name)
        self.subscribed.remove(channel_name)

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

        # 获得更新得频道名，如果不在pulled列表中，才添加，列表按添加时间排序
        msg = await self._mq.get_message()

        if msg is not None:
            channel_name = msg["channel"].decode()
            logger.debug(_("🔔 [💾Redis] 收到订阅更新通知: {channel_name}").format(channel_name=channel_name))
            # 为防止deque数据堆积，pop旧消息（1970年到2分钟前），防止队列溢出
            dropped = set(self.pulled_deque.pop(0, time.time() - 120))
            if dropped:
                self.pulled_set -= dropped
                logger.warning(
                    _("⚠️ [💾Redis] 订阅更新通知来不及处理，"
                    "丢弃了2分钟前的消息共{count}条").format(count=len(dropped))
                )

            # 判断是否已在deque中了，去重用。self.get_message也会自动去重，
            # 但get_message一次只取部分(interval)消息，不能完全去重
            if channel_name not in self.pulled_set:
                self.pulled_deque.add(time.time(), channel_name)
                self.pulled_set.add(channel_name)

    @override
    async def get_message(self) -> set[str]:
        """
        pop并返回之前pull()到本地的消息，只pop收到时间大于1/UPDATE_FREQUENCY的消息。
        留1/UPDATE_FREQUENCY时间是为了消息的合批。

        之后SubscriptionBroker会对该消息进行分析，并重新读取数据库获数据。
        如果没有消息，则堵塞到永远。
        """
        pulled_deque = self.pulled_deque

        interval = 1 / self.UPDATE_FREQUENCY
        # 如果没数据，等待直到有数据
        while not pulled_deque:
            await asyncio.sleep(interval)

        while True:
            # 只取超过interval的数据，这样可以减少频繁更新。set一下可以合并相同消息
            rtn = set(pulled_deque.pop(0, time.time() - interval))
            if rtn:
                self.pulled_set -= rtn
                # logger.debug(f"🔔 [💾Redis] 发送通知给客户端: {str(rtn)[0:100]}...")
                return rtn
            await asyncio.sleep(interval)

    @property
    @override
    def subscribed_channels(self) -> set[str]:
        """返回当前订阅的所有频道名"""
        return self._mq.subscribed
