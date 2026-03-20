"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Mapping

import numpy as np

from hetu.data.backend import BackendClient, RowFormat
from hetu.data.component import Permission
from hetu.i18n import _

if TYPE_CHECKING:
    from hetu.data.backend import Backend, TableReference
    from hetu.endpoint import Context

logger = logging.getLogger("HeTu.root")


class BaseSubscription:
    async def get_updated(
        self, channel
    ) -> tuple[set[str], set[str], Mapping[int, dict[str, Any] | None]]:
        """
        channel收到通知后，前来调用此get_updated方法。
        返回 {需要新订阅的频道}, {需要取消订阅的频道}, {变更的row_id: 行数据，None表示删除}
        """
        raise NotImplementedError

    @property
    def channels(self) -> set[str]:
        """返回当前订阅关注的频道们"""
        raise NotImplementedError


class RowSubscription(BaseSubscription):
    # 这是get_updates的cache，每次get_updates执行会开始记录cache，
    # 让内部的get_updated重复调用时能免去重复查询，
    # 但get_updated是async的，可能会切换走，所以要用ContextVar隔离
    __cache: ContextVar[dict] = ContextVar("user_row_cache")

    def __init__(
        self,
        table_ref: TableReference,
        servant: BackendClient,
        ctx: Context | None,
        channel: str,
        row_id: int,
    ):
        self.table_ref = table_ref
        self.servant = servant
        if table_ref.comp_cls.is_rls() and ctx and not ctx.is_admin():
            self.rls_ctx = ctx
        else:
            self.rls_ctx = None
        self.channel = channel
        self.row_id = row_id
        if RowSubscription.__cache.get(None) is None:
            RowSubscription.__cache.set({})

    @classmethod
    def clear_cache(cls, channel):
        cache = cls.__cache.get(None)
        if cache:
            cache.pop(channel, None)
        else:
            cls.__cache.set({})

    async def get_updated(
        self, channel
    ) -> tuple[set[str], set[str], Mapping[int, dict[str, Any] | None]]:
        """
        channel收到通知后，前来调用此get_updated方法。
        返回 {空}, {空}, {变更的row_id: 行数据，None表示删除}
        """
        # 如果订阅有交叉，这里会重复被调用，需要一个class级别的cache，但外部每次收到channel消息时要清空该cache
        cache = RowSubscription.__cache.get()
        if (cached := cache.get(channel, None)) is not None:
            return set(), set(), cached

        row = await self.servant.get(self.table_ref, self.row_id, RowFormat.TYPED_DICT)
        if row is None:
            rtn = {self.row_id: None}
        else:
            ctx = self.rls_ctx
            if ctx is None or ctx.rls_check(self.table_ref.comp_cls, row):
                del row["_version"]
                rtn = {self.row_id: row}
            else:
                rtn = {self.row_id: None}
        cache[channel] = rtn
        return set(), set(), rtn

    @property
    def channels(self) -> set[str]:
        """返回当前订阅关注的频道们"""
        return {self.channel}


class IndexSubscription(BaseSubscription):
    def __init__(
        self,
        table_ref: TableReference,
        servant: BackendClient,
        ctx: Context,
        index_channel: str,
        last_range_result,
        query_param: dict,
    ):
        self.table_ref = table_ref
        self.servant = servant
        if table_ref.comp_cls.is_rls() and ctx and not ctx.is_admin():
            self.rls_ctx = ctx
        else:
            self.rls_ctx = None
        self.index_channel = index_channel
        self.query_param = query_param
        self.row_subs: dict[str, RowSubscription] = {}
        self.last_range_result = last_range_result

    def add_row_subscriber(self, channel, row_id):
        self.row_subs[channel] = RowSubscription(
            self.table_ref, self.servant, self.rls_ctx, channel, row_id
        )

    async def get_updated(
        self, channel
    ) -> tuple[set[str], set[str], Mapping[int, dict[str, Any] | None]]:
        """
        channel收到通知后，前来调用此get_updated方法。
        返回 {需要新订阅的频道}, {需要取消订阅的频道}, {变更的row_id: 行数据，None表示删除}
        """
        servant = self.servant
        ref = self.table_ref
        if channel == self.index_channel:
            # 查询index更新，比较row_id是否有变化
            row_ids = await servant.range(
                ref, **self.query_param, row_format=RowFormat.ID_LIST
            )
            row_ids = set(row_ids)
            inserts = row_ids - self.last_range_result
            deletes = self.last_range_result - row_ids
            self.last_range_result = row_ids
            new_chans = set()
            rem_chans = set()
            rtn: dict[int, dict[str, Any] | None] = {}
            for row_id in inserts:
                row = await servant.get(ref, row_id, row_format=RowFormat.TYPED_DICT)
                if row is None:
                    self.last_range_result.remove(row_id)
                    continue  # 可能是刚添加就删了
                else:
                    ctx = self.rls_ctx
                    if ctx is None or ctx.rls_check(ref.comp_cls, row):
                        del row["_version"]
                        rtn[row_id] = row
                    new_chan_name = servant.row_channel(ref, row_id)
                    new_chans.add(new_chan_name)
                    self.row_subs[new_chan_name] = RowSubscription(
                        ref, servant, ctx, new_chan_name, row_id
                    )
            for row_id in deletes:
                rtn[row_id] = None
                rem_chan_name = servant.row_channel(ref, row_id)
                rem_chans.add(rem_chan_name)
                self.row_subs.pop(rem_chan_name)

            return new_chans, rem_chans, rtn
        elif channel in self.row_subs:
            return await self.row_subs[channel].get_updated(channel)
        else:
            raise RuntimeError(
                _("IndexSubscription收到了未知的channel消息: {channel}").format(
                    channel=channel
                )
            )

    @property
    def channels(self) -> set[str]:
        """返回当前订阅关注的频道们"""
        return {self.index_channel, *self.row_subs.keys()}


class SubscriptionBroker:
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
    def make_query_id_(
        cls, table_ref: TableReference, index_name: str, left, right, limit, desc
    ):
        return (
            f"{table_ref.comp_name}.{index_name}"
            f"[{left}:{right}:{desc and -1 or 1}][:{limit}]"
        )

    @classmethod
    def _has_table_permission(cls, table_ref: TableReference, ctx: Context) -> bool:
        """判断caller是否对整个表有权限"""
        comp_permission = table_ref.comp_cls.permission_
        # admin和EVERYBODY权限永远返回True
        if comp_permission == Permission.EVERYBODY or ctx.is_admin():
            return True
        else:
            # 其他权限要求至少登陆过
            if comp_permission == Permission.ADMIN:
                return False
            if ctx.caller:
                return True
            return False

    @classmethod
    def _has_row_permission(
        cls, table_ref: TableReference, ctx: Context, row: dict | np.record
    ) -> bool:
        """判断是否对行有权限，首先你要调用_has_table_permission判断是否有表权限"""
        return ctx.rls_check(table_ref.comp_cls, row)

    async def subscribe_get(
        self,
        table_ref: TableReference,
        ctx: Context,
        index_name: str,
        query_value: int | float | str,
    ) -> tuple[str | None, dict[str, Any] | None]:
        """
        获取并订阅单行数据。
        如果是重复订阅，会返回上一次订阅的sub_id。客户端应该写代码防止重复订阅。

        Returns
        --------
        sub_id: str | None
            订阅id，后续通过该id获取更新。如果未查询到数据，或rls不符，返回None。
        row: dict | None
            订阅的行数据。如果未查询到数据，或rls不符，返回None。
        """
        # 首先caller要对整个表有权限
        if not self._has_table_permission(table_ref, ctx):
            return None, None

        servant = self._backend.servant

        if index_name == "id":
            row = await servant.get(table_ref, int(query_value), RowFormat.TYPED_DICT)
            if row is None:
                return None, None
        else:
            rows = await servant.range(
                table_ref,
                index_name,
                query_value,
                limit=1,
                row_format=RowFormat.TYPED_DICT,
            )
            if len(rows) == 0:
                return None, None
            row = rows[0]
            del row["_version"]

        # 再次caller要对该row有权限
        if not self._has_row_permission(table_ref, ctx, row):
            return None, None

        # 开始订阅
        sub_id = self.make_query_id_(table_ref, "id", row["id"], None, 1, False)
        if sub_id in self._subs:
            logger.warning(
                _("⚠️ [📡Subscription] {sub_id} 数据重复订阅，检查客户端代码").format(
                    sub_id=sub_id
                )
            )
            return sub_id, row

        channel_name = servant.row_channel(table_ref, row["id"])
        await self._mq_client.subscribe(channel_name)
        logger.debug("🆕 [📡Subscription] 订阅了行: %s %s", sub_id, channel_name)

        self._subs[sub_id] = RowSubscription(
            table_ref, servant, ctx, channel_name, row["id"]
        )
        self._channel_subs.setdefault(channel_name, set()).add(sub_id)
        return sub_id, row

    async def subscribe_range(
        self,
        table_ref: TableReference,
        ctx: Context,
        index_name: str,
        left: Any,
        right: Any | None = None,
        limit: int = 10,
        desc: bool = False,
        force: bool = True,
    ) -> tuple[str | None, list[dict]]:
        """
        获取并订阅多行数据。
        如果是重复订阅，会返回上一次订阅的sub_id。客户端应该写代码防止重复订阅。

        订阅会观察数据的变化/添加/删除，收到对应通知，由get_updates调用时处理。

        时间复杂度是O(log(N)+M)，N是index的总行数；M是limit。
        Component权限是RLS时，查询后再根据权限筛选，limit为筛选前的行数，可能会获得少于limit行数据。

        Notes
        -----
        订阅不会对RLS权限获得做出反应，由订阅时的RLS权限决定。
        - 当某行已查询到的数据，失去RLS权限时，**会**收到该行被删除的通知
        - 当某行不符合RLS权限的数据，获得RLS权限时，**不会**收到该行被添加的通知

        RLS权限介绍请看See Also的组件定义。

        Returns
        --------
        sub_id: str | None
            订阅id，后续通过该id获取更新。如果无整表权限，返回None。
            如果force为False，未查询到数据时，也会返回None。
        rows: list[dict[str, Any]]
            订阅的多行数据，如果未查询到数据，返回空列表。

        See Also
        --------
        define_component : 组件定义

        """
        # 首先caller要对整个表有权限，不然就算force也不给订阅
        if not self._has_table_permission(table_ref, ctx):
            logger.warning(
                _(
                    "⚠️ [📡Subscription] {comp_name}无调用权限，"
                    "检查是否非法调用，caller：{caller}"
                ).format(comp_name=table_ref.comp_name, caller=ctx.caller)
            )
            return None, []

        servant = self._backend.servant

        rows = await servant.range(
            table_ref, index_name, left, right, limit, desc, RowFormat.TYPED_DICT
        )
        for row in rows:
            del row["_version"]

        # 如果是rls权限，需要对每行数据进行权限判断
        if table_ref.comp_cls.is_rls():
            rows = [
                row for row in rows if self._has_row_permission(table_ref, ctx, row)
            ]

        if not force and len(rows) == 0:
            return None, rows

        sub_id = self.make_query_id_(table_ref, index_name, left, right, limit, desc)
        if sub_id in self._subs:
            logger.warning(
                _("⚠️ [📡Subscription] {sub_id} 数据重复订阅，检查客户端代码").format(
                    sub_id=sub_id
                )
            )
            return sub_id, rows

        index_channel = servant.index_channel(table_ref, index_name)
        await self._mq_client.subscribe(index_channel)
        logger.debug("🆕 [📡Subscription] 订阅了索引: %s %s", sub_id, index_channel)

        row_ids = {int(row["id"]) for row in rows}
        idx_sub = IndexSubscription(
            table_ref,
            servant,
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
            row_channel = servant.row_channel(table_ref, row_id)
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
        pop之前SubscriptionBroker.mq_pull()到的数据更新通知，然后通过查询数据库取出最新的值，并返回。
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
