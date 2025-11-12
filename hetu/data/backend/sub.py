"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
from typing import Any

import numpy as np

from .base import BaseSubscription, ComponentTable, Backend
from ..component import Permission
from ...system import Context

logger = logging.getLogger("HeTu.root")


class RowSubscription(BaseSubscription):
    __cache = {}

    def __init__(
            self, table: ComponentTable, ctx: Context | None, channel: str, row_id: int
    ):
        self.table = table
        if table.component_cls.is_rls() and ctx and not ctx.is_admin():
            self.rls_ctx = ctx
        else:
            self.rls_ctx = None
        self.channel = channel
        self.row_id = row_id

    @classmethod
    def clear_cache(cls, channel):
        cls.__cache.pop(channel, None)

    async def get_updated(
            self, channel
    ) -> tuple[set[str], set[str], dict[str, dict | None]]:
        # 如果订阅有交叉，这里会重复被调用，需要一个class级别的cache，但外部每次收到channel消息时要清空该cache
        if (cache := RowSubscription.__cache.get(channel, None)) is not None:
            return set(), set(), cache

        row = await self.table.direct_get(self.row_id, row_format="typed_dict")
        if row is None:
            # get_updated主要发给客户端，需要json，所以key直接用str
            rtn = {str(self.row_id): None}
        else:
            ctx = self.rls_ctx
            if ctx is None or ctx.rls_check(self.table.component_cls, row):
                rtn = {str(self.row_id): row}
            else:
                rtn = {str(self.row_id): None}
        RowSubscription.__cache[channel] = rtn
        return set(), set(), rtn

    @property
    def channels(self) -> set[str]:
        return {self.channel}


class IndexSubscription(BaseSubscription):
    def __init__(
            self,
            table: ComponentTable,
            ctx: Context,
            index_channel: str,
            last_query,
            query_param: dict,
    ):
        self.table = table
        if table.component_cls.is_rls() and ctx and not ctx.is_admin():
            self.rls_ctx = ctx
        else:
            self.rls_ctx = None
        self.index_channel = index_channel
        self.query_param = query_param
        self.row_subs: dict[str, RowSubscription] = {}
        self.last_query = last_query

    def add_row_subscriber(self, channel, row_id):
        self.row_subs[channel] = RowSubscription(
            self.table, self.rls_ctx, channel, row_id
        )

    async def get_updated(
            self, channel
    ) -> tuple[set[str], set[str], dict[str, dict | None]]:
        if channel == self.index_channel:
            # 查询index更新，比较row_id是否有变化
            row_ids = await self.table.direct_query(**self.query_param, row_format="id")
            row_ids = set(row_ids)
            inserts = row_ids - self.last_query
            deletes = self.last_query - row_ids
            self.last_query = row_ids
            new_chans = set()
            rem_chans = set()
            rtn = {}
            for row_id in inserts:
                row = await self.table.direct_get(row_id, row_format="typed_dict")
                if row is None:
                    self.last_query.remove(row_id)
                    continue  # 可能是刚添加就删了
                else:
                    ctx = self.rls_ctx
                    if ctx is None or ctx.rls_check(self.table.component_cls, row):
                        rtn[str(row_id)] = row
                    new_chan_name = self.table.channel_name(row_id=row_id)
                    new_chans.add(new_chan_name)
                    self.row_subs[new_chan_name] = RowSubscription(
                        self.table, ctx, new_chan_name, row_id
                    )
            for row_id in deletes:
                rtn[str(row_id)] = None
                rem_chan_name = self.table.channel_name(row_id=row_id)
                rem_chans.add(rem_chan_name)
                self.row_subs.pop(rem_chan_name)

            return new_chans, rem_chans, rtn
        elif channel in self.row_subs:
            return await self.row_subs[channel].get_updated(channel)
        else:
            raise RuntimeError(f"IndexSubscription收到了未知的channel消息: {channel}")

    @property
    def channels(self) -> set[str]:
        return {self.index_channel, *self.row_subs.keys()}


class Subscriptions:
    """
    Component的数据订阅和查询接口
    """

    def __init__(self, backend: Backend):
        self._backend = backend
        self._mq_client = backend.get_mq_client()

        self._subs: dict[str, BaseSubscription] = {}  # key是sub_id
        self._channel_subs: dict[str, set[str]] = {}  # key是频道名， value是set[sub_id]
        self._index_sub_count = 0

    async def close(self):
        return await self._mq_client.close()

    async def mq_pull(self):
        """从MQ获得消息，并存放到本地内存。需要单独的协程反复调用，防止MQ消息堆积。"""
        return await self._mq_client.pull()

    def count(self):
        """获取订阅数，返回row订阅数，index订阅数"""
        return len(self._subs) - self._index_sub_count, self._index_sub_count

    @classmethod
    def _make_query_str(
            cls, table: ComponentTable, index_name: str, left, right, limit, desc
    ):
        return (
            f"{table.component_cls.component_name_}.{index_name}"
            f"[{left}:{right}:{desc and -1 or 1}][:{limit}]"
        )

    @classmethod
    def _has_table_permission(cls, table: ComponentTable, ctx: Context) -> bool:
        """判断caller是否对整个表有权限"""
        comp_permission = table.component_cls.permission_
        # admin和EVERYBODY权限永远返回True
        if comp_permission == Permission.EVERYBODY or ctx.is_admin():
            return True
        else:
            # 其他权限要求至少登陆过
            if comp_permission == Permission.ADMIN:
                return False
            if ctx.caller and ctx.caller > 0:
                return True
            return False

    @classmethod
    def _has_row_permission(
            cls, table: ComponentTable, ctx: Context, row: dict | np.record
    ) -> bool:
        """判断是否对行有权限，首先你要调用_has_table_permission判断是否有表权限"""
        return ctx.rls_check(table.component_cls, row)

    async def subscribe_select(
            self, table: ComponentTable, ctx: Context, value: Any, where: str = "id"
    ) -> tuple[str | None, np.record | None]:
        """
        获取并订阅单行数据，返回订阅id(sub_id: str)和单行数据(row: dict)。
        如果未查询到数据，或rls不符，返回None, None。
        如果是重复订阅，会返回上一次订阅的sub_id。客户端应该写代码防止重复订阅。
        """
        # 首先caller要对整个表有权限
        if not self._has_table_permission(table, ctx):
            return None, None

        if where == "id":
            if (row := await table.direct_get(value, row_format="typed_dict")) is None:
                return None, None
        else:
            rows = await table.direct_query(where, value, limit=1,
                                            row_format='typed_dict')
            if len(rows) == 0:
                return None, None
            row = rows[0]

        # 再次caller要对该row有权限
        if not self._has_row_permission(table, ctx, row):
            return None, None

        # 开始订阅
        sub_id = self._make_query_str(table, "id", row["id"], None, 1, False)
        if sub_id in self._subs:
            logger.warning(f"⚠️ [💾Subscription] {sub_id} 数据重复订阅，检查客户端代码")
            return sub_id, row

        channel_name = table.channel_name(row_id=row["id"])
        await self._mq_client.subscribe(channel_name)

        self._subs[sub_id] = RowSubscription(table, ctx, channel_name, row["id"])
        self._channel_subs.setdefault(channel_name, set()).add(sub_id)
        return sub_id, row

    async def subscribe_query(
            self,
            table: ComponentTable,
            ctx: Context,
            index_name: str,
            left,
            right=None,
            limit=10,
            desc=False,
            force=True,
    ) -> tuple[str | None, list[dict]]:
        """
        获取并订阅多行数据，返回订阅id(sub_id: str)，和多行数据(rows: list[dict])。
        如果未查询到数据，返回None, []。
        但force参数可以强制未查询到数据时也订阅，返回订阅id(sub_id: str)，和[]。
        如果是重复订阅，会返回上一次订阅的sub_id。客户端应该写代码防止重复订阅。

        订阅会观察数据的变化/添加/删除，收到对应通知，由get_updates调用时处理。

        时间复杂度是O(log(N)+M)，N是index的条目数；M是查询到的行数。
        Component权限是RLS时，查询到的行在最后再根据权限值筛选，M为筛选前的行数。

        Notes
        -----
        目前不会对rls权限获得做出反应，由订阅时的rls权限决定。
        - 当某行已查询到的数据，失去rls权限时，**会**收到该行被删除的通知
        - 当某行符合查询条件的数据，之前没权限被剔除，现在新获得rls权限时，**不会**收到该行被添加的通知

        """
        # 首先caller要对整个表有权限，不然就算force也不给订阅
        if not self._has_table_permission(table, ctx):
            logger.warning(
                f"⚠️ [💾Subscription] {table.component_cls.component_name_}无调用权限，"
                f"检查是否非法调用，caller：{ctx.caller}"
            )
            return None, []

        rows = await table.direct_query(
            index_name, left, right, limit, desc, row_format="typed_dict"
        )

        # 如果是rls权限，需要对每行数据进行权限判断
        if table.component_cls.is_rls():
            rows = [row for row in rows if self._has_row_permission(table, ctx, row)]

        if not force and len(rows) == 0:
            return None, rows

        sub_id = self._make_query_str(table, index_name, left, right, limit, desc)
        if sub_id in self._subs:
            logger.warning(f"⚠️ [💾Subscription] {sub_id} 数据重复订阅，检查客户端代码")
            return sub_id, rows

        index_channel = table.channel_name(index_name=index_name)
        await self._mq_client.subscribe(index_channel)

        row_ids = {int(row["id"]) for row in rows}
        idx_sub = IndexSubscription(
            table,
            ctx,
            index_channel,
            row_ids,
            dict(index_name=index_name, left=left, right=right, limit=limit, desc=desc),
        )
        self._subs[sub_id] = idx_sub
        self._channel_subs.setdefault(index_channel, set()).add(sub_id)
        self._index_sub_count = list(map(type, self._subs.values())).count(
            IndexSubscription
        )

        # 还要订阅每行的信息，这样每行数据变更时才能收到消息
        for row_id in row_ids:
            row_channel = table.channel_name(row_id=row_id)
            await self._mq_client.subscribe(row_channel)
            idx_sub.add_row_subscriber(row_channel, row_id)
            self._channel_subs.setdefault(row_channel, set()).add(sub_id)

        return sub_id, rows

    async def unsubscribe(self, sub_id) -> None:
        """取消该sub_id的订阅"""
        if sub_id not in self._subs:
            return

        for channel in self._subs[sub_id].channels:
            self._channel_subs[channel].remove(sub_id)
            if len(self._channel_subs[channel]) == 0:
                await self._mq_client.unsubscribe(channel)
                del self._channel_subs[channel]
        self._subs.pop(sub_id)
        self._index_sub_count = list(map(type, self._subs.values())).count(
            IndexSubscription
        )

    async def get_updates(self, timeout=None) -> dict[str, dict[str, dict]]:
        """
        pop之前Subscriptions.mq_pull()到的数据更新通知，然后通过查询数据库取出最新的值，并返回。
        返回值为dict: key是sub_id；value是更新的行数据，value格式为dict：key是row_id，value是数据库raw值。
        timeout参数主要给单元测试用，None时堵塞到有消息，否则等待timeout秒。

        遇到消息堆积会丢弃通知。

        对于丢失的消息，也许客户端SDK可以通过定期强制刷新的方式弥补，但是对于insert消息的丢失，无法有效判断刷新时机。
        可以考虑如下方式：
             1.RowSubscription/IndexSubscription如果一定时间未收到数据，则强制向服务器取消订阅/重新订阅
                  无法准确判断index消息的丢失，只有index完全没消息时才有效，对中途漏了几个消息的丢失无法弥补
                  重新订阅会带来重复的insert消息，客户端逻辑会有问题
             2.做行更新，就是每个行数据都带时间戳，如果过期就强制更新行，因此delete/update事件可以补回
                  但是无法解决insert消息的丢失
                  可以加一个定期的强制index对比，但时间太短会增加双方负担，时间长用户又能感知到错误
                  这服务器端要多做2个方法，此方法还要另外专门做权限的判断，代码想必不会简洁
            都不怎么好，还是先多测试架构，减少丢失的可能性
        """
        mq = self._mq_client
        channel_subs = self._channel_subs

        rtn = {}
        if timeout is not None:
            try:
                async with asyncio.timeout(timeout):
                    updated_channels = await mq.get_message()
            except TimeoutError:
                return rtn
        else:
            updated_channels = await mq.get_message()
        for channel in updated_channels:
            RowSubscription.clear_cache(channel)
            sub_ids = channel_subs.get(channel, [])
            for sub_id in sub_ids:
                sub = self._subs[sub_id]
                # 获取sub更新的行数据
                new_chans, rem_chans, sub_updates = await sub.get_updated(channel)
                # 如果有行添加或删除，订阅或取消订阅
                for new_chan in new_chans:
                    await mq.subscribe(new_chan)
                    channel_subs.setdefault(new_chan, set()).add(sub_id)
                for rem_chan in rem_chans:
                    channel_subs[rem_chan].remove(sub_id)
                    if len(channel_subs[rem_chan]) == 0:
                        await mq.unsubscribe(rem_chan)
                        del channel_subs[rem_chan]
                # 添加行数据到返回值
                if len(sub_updates) > 0:
                    rtn.setdefault(sub_id, dict()).update(sub_updates)
        return rtn
