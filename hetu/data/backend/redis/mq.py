#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import asyncio
import itertools
import logging
import random
import struct
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast, final, overload, override

# from msgspec import msgpack  # 不支持关闭bin type，lua 的msgpack库7年没更新了
import msgpack
import numpy as np
import redis


from ..base import MQClient
from ....common.multimap import MultiMap

if TYPE_CHECKING:
    import redis.asyncio
    import redis.asyncio.cluster
    import redis.cluster
    import redis.exceptions
    from hetu.data.backend.redis.client import RedisBackendClient

logger = logging.getLogger("HeTu.root")
MAX_SUBSCRIBED = 5000

import time


class RedisMQClient(MQClient):
    """连接到消息队列的客户端，每个用户连接一个实例。"""

    def __init__(self, client: RedisBackendClient):
        # todo 要测试redis cluster是否能正常pub sub
        # 2种模式：
        # a. 每个ws连接一个pubsub连接，分发交给servants，结构清晰，目前的模式，但网络占用高
        # b. 每个worker一个pubsub连接，分发交给worker来做，这样连接数较少，但等于2套分发系统结构复杂
        self._mq = client.aio.pubsub()  # todo cluster模式的pubsub不支持异步，且调用方法不一样，考虑以后换valkey库试试看
        self.subscribed = set()
        self.pulled_deque = MultiMap()  # 可按时间查询的消息队列
        self.pulled_set = set()  # 和pulled_deque内容保持一致的set，方便去重

    async def close(self):
        return await self._mq.aclose()

    async def subscribe(self, channel_name) -> None:
        await self._mq.subscribe(channel_name)
        self.subscribed.add(channel_name)
        if len(self.subscribed) > MAX_SUBSCRIBED:
            # 抑制此警告可通过修改hetu.backend.redis.MAX_SUBSCRIBED参数
            logger.warning(
                f"⚠️ [💾Redis] 当前连接订阅数超过全局限制MAX_SUBSCRIBED={MAX_SUBSCRIBED}行，"
            )

    async def unsubscribe(self, channel_name) -> None:
        await self._mq.unsubscribe(channel_name)
        self.subscribed.remove(channel_name)

    async def pull(self) -> None:
        mq = self._mq

        # 如果没订阅过内容，那么redis mq的connection是None，无需get_message
        if mq.connection is None:
            await asyncio.sleep(0.5)  # 不写协程就死锁了
            return

        # 获得更新得频道名，如果不在pulled列表中，才添加，列表按添加时间排序
        msg = await mq.get_message(ignore_subscribe_messages=True, timeout=None)
        if msg is not None:
            channel_name = msg["channel"]
            logger.debug(f"🔔 [💾Redis] 收到订阅更新通知: {channel_name}")
            # 为防止deque数据堆积，pop旧消息（1970年到2分钟前），防止队列溢出
            dropped = set(self.pulled_deque.pop(0, time.time() - 120))
            if dropped:
                self.pulled_set -= dropped
                logger.warning(
                    f"⚠️ [💾Redis] 订阅更新通知来不及处理，"
                    f"丢弃了2分钟前的消息共{len(dropped)}条"
                )

            # 判断是否已在deque中了，去重用。self.get_message也会自动去重，
            # 但get_message一次只取部分(interval)消息，不能完全去重
            if channel_name not in self.pulled_set:
                self.pulled_deque.add(time.time(), channel_name)
                self.pulled_set.add(channel_name)

    async def get_message(self) -> set[str]:
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
    def subscribed_channels(self) -> set[str]:
        return set(self._mq.channels) - set(self._mq.pending_unsubscribe_channels)
