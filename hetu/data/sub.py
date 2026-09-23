"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
from collections import Counter
from collections.abc import Callable, Mapping
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, cast

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
        self, channel: str, payload: set[str] | None = None
    ) -> tuple[set[str], set[str], Mapping[int, dict[str, Any] | None]]:
        """
        channel收到通知后，前来调用此get_updated方法。payload是该频道消息携带的数据，
        行/索引频道为None，表级频道为变动的row_id集合。
        返回 {需要新订阅的频道}, {需要取消订阅的频道}, {变更的row_id: 行数据，None表示删除}
        """
        raise NotImplementedError

    @property
    def channels(self) -> set[str]:
        """返回当前订阅关注的频道们"""
        raise NotImplementedError


class RowSubscription(BaseSubscription):
    # 这是get_updates的cache：{行频道: 原始行dict（含_version）| None(行不存在)}。
    # 每个tick开始时重置，tick内先由get_updates批量预读填充，多个订阅交叉命中同一行时
    # 免去重复查询；缓存的是原始行，RLS由各订阅自己判定（同一连接可能挂着不同ctx的订阅）。
    # get_updated是async的，可能会切换走，所以要用ContextVar隔离
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
    def reset_cache_(cls) -> dict:
        """每个tick开始时调用：清空本任务的行缓存并返回它"""
        cache: dict = {}
        cls.__cache.set(cache)
        return cache

    @classmethod
    def prefill_cache_(cls, channel: str, row: dict[str, Any] | None) -> None:
        """get_updates批量预读后填充：row为None表示行不存在"""
        cache = cls.__cache.get(None)
        if cache is None:
            cache = cls.reset_cache_()
        cache[channel] = row

    def decode_row_(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        """按本订阅的RLS判定行是否可见：可见返回去掉_version的拷贝，不可见/不存在返回None"""
        if row is None:
            return None
        ctx = self.rls_ctx
        if ctx is not None and not ctx.rls_check(self.table_ref.comp_cls, row):
            return None
        row = dict(row)  # 缓存里的原始行可能被别的订阅共用，不能就地改
        row.pop("_version", None)
        return row

    async def get_updated(
        self, channel: str, payload: set[str] | None = None
    ) -> tuple[set[str], set[str], Mapping[int, dict[str, Any] | None]]:
        """
        channel收到通知后，前来调用此get_updated方法。
        返回 {空}, {空}, {变更的row_id: 行数据，None表示删除}
        """
        # 如果订阅有交叉，这里会重复被调用，先看本tick的缓存（get_updates会批量预读填好；
        # tick中途新建的行订阅不在预读范围内，这里兜底单行查询）
        cache = RowSubscription.__cache.get(None)
        if cache is None:
            cache = RowSubscription.reset_cache_()
        if channel in cache:
            row = cache[channel]
        else:
            row = await self.servant.get(
                self.table_ref, self.row_id, RowFormat.TYPED_DICT
            )
            cache[channel] = row
        return set(), set(), {self.row_id: self.decode_row_(row)}

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
        # 点查询时是"索引=该值"的频道，区间查询时是整个索引的频道；收到就重跑 range 比对
        self.index_channel = index_channel
        self.query_param = query_param
        self.row_subs: dict[str, RowSubscription] = {}
        self.last_range_result = last_range_result

    def add_row_subscriber(self, channel, row_id):
        self.row_subs[channel] = RowSubscription(
            self.table_ref, self.servant, self.rls_ctx, channel, row_id
        )

    async def get_updated(
        self, channel: str, payload: set[str] | None = None
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
            inserts = list(row_ids - self.last_range_result)
            deletes = self.last_range_result - row_ids
            self.last_range_result = row_ids
            new_chans = set()
            rem_chans = set()
            rtn: dict[int, dict[str, Any] | None] = {}
            # 新进入范围的行一次批量读取（一次往返）
            rows = cast(
                list[dict[str, Any] | None],
                await servant.get_many(ref, inserts, RowFormat.TYPED_DICT)
                if inserts
                else [],
            )
            for row_id, row in zip(inserts, rows):
                if row is None:
                    self.last_range_result.remove(row_id)
                    continue  # 可能是刚添加就删了
                new_chan_name = servant.row_channel(ref, row_id)
                new_chans.add(new_chan_name)
                row_sub = RowSubscription(
                    ref, servant, self.rls_ctx, new_chan_name, row_id
                )
                self.row_subs[new_chan_name] = row_sub
                # 不可见（RLS）的行也要订阅，等它变得可见时才能通知；但现在不推给客户端
                visible = row_sub.decode_row_(row)
                if visible is not None:
                    rtn[row_id] = visible
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


class TableSubscription(BaseSubscription):
    """
    整表订阅：只订阅一个表级频道，消息payload即本tick内变动的row_id集合，
    按id批量重读后推送。适合"行多、行小、很少变"的表，如所有玩家名字。
    """

    def __init__(
        self,
        table_ref: TableReference,
        servant: BackendClient,
        ctx: Context,
        table_channel: str,
        known_ids: set[int],
    ):
        self.table_ref = table_ref
        self.servant = servant
        if table_ref.comp_cls.is_rls() and ctx and not ctx.is_admin():
            self.rls_ctx = ctx
        else:
            self.rls_ctx = None
        self.table_channel = table_channel
        # 已推送给客户端、且客户端仍持有的行id。用于判断"删除/失去RLS"是否需要通知
        self.known_ids = known_ids

    async def get_updated(
        self, channel: str, payload: set[str] | None = None
    ) -> tuple[set[str], set[str], Mapping[int, dict[str, Any] | None]]:
        """
        表级频道收到通知后调用。payload是变动的row_id集合。
        返回 {空}, {空}, {变更的row_id: 行数据，None表示删除或失去RLS权限}
        """
        if channel != self.table_channel:
            raise RuntimeError(
                _("TableSubscription收到了未知的channel消息: {channel}").format(
                    channel=channel
                )
            )
        if not payload:
            return set(), set(), {}

        ids = sorted(int(i) for i in payload)
        rows = cast(
            list[dict[str, Any] | None],
            await self.servant.get_many(self.table_ref, ids, RowFormat.TYPED_DICT),
        )
        comp_cls = self.table_ref.comp_cls
        ctx = self.rls_ctx
        known = self.known_ids
        rtn: dict[int, dict[str, Any] | None] = {}
        for row_id, row in zip(ids, rows):
            if row is not None and (ctx is None or ctx.rls_check(comp_cls, row)):
                del row["_version"]
                rtn[row_id] = row
                known.add(row_id)
            elif row_id in known:
                # 被删除，或失去RLS权限：客户端持有该行，需要通知删除
                rtn[row_id] = None
                known.discard(row_id)
            # 既不可见、客户端也从未持有的行：不推
        return set(), set(), rtn

    @property
    def channels(self) -> set[str]:
        """返回当前订阅关注的频道们"""
        return {self.table_channel}


class SubscriptionBroker:
    """
    Component的数据订阅和查询接口
    """

    def __init__(self, backend: Backend, max_table_rows: int = 100_000):
        """
        Parameters
        ----------
        backend: Backend
            数据库后端
        max_table_rows: int
            单次整表订阅（subscribe_table）允许的最大行数，超过则拒绝订阅。
            一般对应配置项 `MAX_TABLE_SUBSCRIPTION_ROWS`。
        """
        self._backend = backend
        self._mq_client = backend.get_mq_client()
        self._max_table_rows = max_table_rows

        self._subs: dict[str, BaseSubscription] = {}  # key是sub_id
        self._channel_subs: dict[str, set[str]] = {}  # key是频道名， value是set[sub_id]
        self._sub_counts: Counter[type[BaseSubscription]] = Counter()

    async def close(self):
        return await self._mq_client.close()

    async def watch_channel(self, channel: str, callback: Callable[[], None]) -> None:
        """
        服务端内部关注一个频道（如本连接用户的 Connection owner 值频道）：收到通知调
        `callback`，不计入订阅数、不做权限检查；客户端对同一频道的订阅/退订与之互不干扰。
        连接关闭时随 mq_client.close() 一起退订。
        回调在后端通知接收器的监听协程里同步执行，必须非阻塞。
        """
        await self._mq_client.watch(channel, callback)

    def count(self) -> tuple[int, int, int]:
        """获取订阅数，返回 (row订阅数, index订阅数, table订阅数)"""
        return (
            self._sub_counts[RowSubscription],
            self._sub_counts[IndexSubscription],
            self._sub_counts[TableSubscription],
        )

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

        # 先定位 row_id（非主键只查索引，不读行）
        if index_name == "id":
            row_id = int(query_value)
        else:
            ids = await servant.range(
                table_ref,
                index_name,
                query_value,
                limit=1,
                row_format=RowFormat.ID_LIST,
            )
            if len(ids) == 0:
                return None, None
            row_id = int(ids[0])

        sub_id = self.make_query_id_(table_ref, "id", row_id, None, 1, False)
        if sub_id in self._subs:
            logger.warning(
                _("⚠️ [📡Subscription] {sub_id} 数据重复订阅，检查客户端代码").format(
                    sub_id=sub_id
                )
            )
            row = await servant.get(table_ref, row_id, RowFormat.TYPED_DICT)
            if row is None or not self._has_row_permission(table_ref, ctx, row):
                # 行现在不可见（已删除 / 失去行级权限）：回 None 客户端就认为没有订阅、
                # 不会再来 unsub，旧订阅得跟着撤掉，不然它和它的频道会挂到连接结束
                await self.unsubscribe(sub_id)
                return None, None
            del row["_version"]  # 内部版本号不推给客户端
            return sub_id, row

        # 先订后读：读与订之间落下的写入，要么已经在读回的行里，要么随后有通知。
        # 先读后订的话，它既不在读回的行里、也不会有通知，客户端一直拿着旧行
        channel_name = servant.row_channel(table_ref, row_id)
        await self._mq_client.subscribe(channel_name)
        # 订阅一生效就同步登记，不能等读完：get_updates 的退订名单按 _channel_subs 定，
        # 读是真正的 await，期间某个索引订阅把这行放出范围的话，没登记的频道会被它当作
        # 没人要而退订，这里再登记上去的就是一个永远收不到通知的订阅。
        # 读期间到达的通知会由 get_updates 照常推给这个订阅，客户端还没拿到 sub_id 会丢掉
        # 它：对应的写入早于这次读的，读回的行里已经有了
        self._subs[sub_id] = RowSubscription(
            table_ref, servant, ctx, channel_name, row_id
        )
        self._channel_subs.setdefault(channel_name, set()).add(sub_id)
        self._sub_counts[RowSubscription] += 1
        try:
            row = await servant.get(table_ref, row_id, RowFormat.TYPED_DICT)
        except BaseException:
            await self.unsubscribe(sub_id)
            raise
        # 行不存在，或 caller 对该行无权限：撤销登记并退订（同一连接别的订阅还在用这个
        # 频道就留着）
        if row is None or not self._has_row_permission(table_ref, ctx, row):
            await self.unsubscribe(sub_id)
            return None, None
        del row["_version"]  # 内部版本号不推给客户端
        logger.debug(
            _("🆕 [📡Subscription] 订阅了行: {sub_id} {channel_name}").format(
                sub_id=sub_id, channel_name=channel_name
            )
        )
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

        通知范围取决于查询形状：
        - 点查询（省略 `right`，或 `left == right`，如 `owner=me`、`zone=z`）只订"索引=该值"
          的频道，只有这个值上有行进出、或某行该字段变成/不再是这个值时才会被唤醒；
        - 区间查询订整个索引的频道，该索引上任何值的行增删/变更都会唤醒它重跑一次比对。
          热索引（如所有玩家都订自己的背包）请尽量用点查询。

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

        # 点查询只订该值的频道，别的值的变动不会打扰；区间查询订整个索引的频道
        # （index_name 已由上面的 servant.range 校验过存在）。id 没有值频道（commit 不发，
        # 省掉每次 insert/delete 一条通知），点查 id 也订整个 id 索引的频道
        point_value = BackendClient.point_query_value_(
            table_ref.comp_cls.dtype_map_[index_name], left, right
        )
        if point_value is None or index_name == "id":
            index_channel = servant.index_channel(table_ref, index_name)
        else:
            index_channel = servant.index_value_channel(
                table_ref, index_name, point_value
            )
        row_ids = {int(row["id"]) for row in rows}
        idx_sub = IndexSubscription(
            table_ref,
            servant,
            ctx,
            index_channel,
            row_ids,
            dict(index_name=index_name, left=left, right=right, limit=limit, desc=desc),
        )
        # 索引频道 + 每行的行频道（行变更时才能收到消息）一次批量订阅
        row_channels = []
        for row_id in row_ids:
            row_channel = servant.row_channel(table_ref, row_id)
            row_channels.append(row_channel)
            idx_sub.add_row_subscriber(row_channel, row_id)
        await self._mq_client.subscribe(index_channel, *row_channels)
        logger.debug(
            _("🆕 [📡Subscription] 订阅了索引: {sub_id} {index_channel}").format(
                sub_id=sub_id, index_channel=index_channel
            )
        )

        self._subs[sub_id] = idx_sub
        self._channel_subs.setdefault(index_channel, set()).add(sub_id)
        for row_channel in row_channels:
            self._channel_subs.setdefault(row_channel, set()).add(sub_id)
        self._sub_counts[IndexSubscription] += 1

        return sub_id, rows

    async def subscribe_table(
        self,
        table_ref: TableReference,
        ctx: Context,
    ) -> tuple[str | None, list[dict]]:
        """
        获取并订阅整张表。与 `subscribe_range` 语义独立：只订阅一个表级频道，
        不管表有多少行都只占一个订阅，适合"行多、行小、很少变"的表（如所有玩家名字）。
        如果是重复订阅，会返回上一次订阅的sub_id。客户端应该写代码防止重复订阅。

        订阅会观察表内任何行的添加/变化/删除，由get_updates调用时处理。
        代价是每个整表订阅者会收到该表**所有**写入的通知（服务端按RLS过滤后再推），
        所以高频写入的表请继续用 `subscribe_range`。

        Notes
        -----
        与 `subscribe_range` 不同，整表订阅对RLS权限的得失都会做出反应：
        - 当某行失去RLS权限时，**会**收到该行被删除的通知
        - 当某行获得RLS权限时，**会**收到该行被添加的通知

        Returns
        --------
        sub_id: str | None
            订阅id，后续通过该id获取更新。如果无整表权限，或表行数超过
            `max_table_rows`，返回None。
        rows: list[dict[str, Any]]
            caller可见的全部行数据。

        See Also
        --------
        subscribe_range : 范围订阅
        """
        # 首先caller要对整个表有权限
        if not self._has_table_permission(table_ref, ctx):
            logger.warning(
                _(
                    "⚠️ [📡Subscription] {comp_name}无调用权限，"
                    "检查是否非法调用，caller：{caller}"
                ).format(comp_name=table_ref.comp_name, caller=ctx.caller)
            )
            return None, []

        servant = self._backend.servant
        max_rows = self._max_table_rows

        # 全量读取：id是每个Component的隐式unique索引，多读1行用于判断是否超限
        rows = await servant.range(
            table_ref,
            "id",
            float("-inf"),
            float("inf"),
            limit=max_rows + 1,
            row_format=RowFormat.TYPED_DICT,
        )
        if len(rows) > max_rows:
            logger.warning(
                _(
                    "⚠️ [📡Subscription] {comp_name}整表订阅行数超过限制"
                    "MAX_TABLE_SUBSCRIPTION_ROWS={max_rows}，拒绝订阅，caller：{caller}"
                ).format(
                    comp_name=table_ref.comp_name, max_rows=max_rows, caller=ctx.caller
                )
            )
            return None, []
        for row in rows:
            del row["_version"]

        # 如果是rls权限，需要对每行数据进行权限判断
        if table_ref.comp_cls.is_rls():
            rows = [
                row for row in rows if self._has_row_permission(table_ref, ctx, row)
            ]

        sub_id = f"{table_ref.comp_name}.table"
        if sub_id in self._subs:
            logger.warning(
                _("⚠️ [📡Subscription] {sub_id} 数据重复订阅，检查客户端代码").format(
                    sub_id=sub_id
                )
            )
            return sub_id, rows

        table_channel = servant.table_channel(table_ref)
        await self._mq_client.subscribe(table_channel)
        logger.debug(
            _("🆕 [📡Subscription] 订阅了整表: {sub_id} {table_channel}").format(
                sub_id=sub_id, table_channel=table_channel
            )
        )

        self._subs[sub_id] = TableSubscription(
            table_ref,
            servant,
            ctx,
            table_channel,
            {int(row["id"]) for row in rows},
        )
        self._channel_subs.setdefault(table_channel, set()).add(sub_id)
        self._sub_counts[TableSubscription] += 1
        return sub_id, rows

    async def unsubscribe(self, sub_id) -> None:
        """取消该sub_id的订阅"""
        if sub_id not in self._subs:
            return

        rem_chans = []
        for channel in self._subs[sub_id].channels:
            self._channel_subs[channel].remove(sub_id)
            if len(self._channel_subs[channel]) == 0:
                rem_chans.append(channel)
                del self._channel_subs[channel]
        await self._mq_client.unsubscribe(*rem_chans)
        sub = self._subs.pop(sub_id)
        self._sub_counts[type(sub)] -= 1

    async def get_updates(self, timeout=None) -> dict[str, dict[str, dict]]:
        """
        pop后端通知接收器推到本连接本地队列的数据更新通知，然后通过查询数据库取出最新的值，并返回。
        返回值为dict: key是sub_id；value是更新的行数据，value格式为dict：key是row_id，value是数据库raw值。
        timeout参数主要给单元测试用，None时堵塞到有更新，否则最多等待timeout秒（总时长），
        到时返回空dict。

        一批通知读下来可能没有任何要推给客户端的变化（尾随重读、订阅生效后的补读读回的与
        客户端已有的一样，或变化的行对本连接不可见），这时继续等下一批，不返回空结果。
        一次写入引起的推送也可能分在几批里：合并进队头的通知会在一个 interval 后尾随重读，
        它和别的频道的通知谁先弹出取决于时序。

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
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            try:
                async with asyncio.timeout_at(deadline):
                    updated_channels = await self._mq_client.get_message()
            except TimeoutError:
                return {}
            if rtn := await self._apply_notifications(updated_channels):
                return rtn

    async def _apply_notifications(
        self, updated_channels: Mapping[str, set[str] | None]
    ) -> dict[str, dict[str, dict]]:
        """处理一批弹出的通知：重读变更、维护范围进出的频道订阅，返回要推给客户端的更新"""
        mq = self._mq_client
        channel_subs = self._channel_subs
        rtn = {}

        # 本tick变更的行先按表分组一次批量读取，填进RowSubscription的缓存
        RowSubscription.reset_cache_()
        await self._prefetch_rows(updated_channels)

        added: set[str] = set()
        released: set[str] = set()
        for channel, payload in updated_channels.items():
            # 快照迭代：前面的 get_updated 是真正的 await，期间接收协程可能处理了客户端
            # 对本快照里后面某个订阅的 unsub，它已经从 _subs 里弹掉了，跳过即可，
            # 不能 KeyError 把整个连接断掉
            for sub_id in list(channel_subs.get(channel, ())):
                sub = self._subs.get(sub_id)
                if sub is None:
                    continue
                # 获取sub更新的行数据
                new_chans, rem_chans, sub_updates = await sub.get_updated(
                    channel, payload
                )
                if self._subs.get(sub_id) is not sub:
                    # 查库期间这个订阅被 unsub 了（或退了又用同一 id 重订成新对象）：
                    # 它的频道已由 unsubscribe 从 channel_subs 里撤掉，这里再记账会把
                    # 已经不存在的订阅登记回去，更新也不用再推
                    continue
                # 行进入/离开范围：先记账，订阅/退订留到tick末尾各一次批量往返
                for new_chan in new_chans:
                    channel_subs.setdefault(new_chan, set()).add(sub_id)
                    added.add(new_chan)
                for rem_chan in rem_chans:
                    subs = channel_subs.get(rem_chan)
                    if subs is None:
                        continue
                    subs.discard(sub_id)
                    if not subs:
                        del channel_subs[rem_chan]
                        released.add(rem_chan)
                # 添加行数据到返回值
                if len(sub_updates) > 0:
                    rtn.setdefault(sub_id, dict()).update(sub_updates)

        # 同一频道可能在本tick内既被一个订阅加入又被另一个释放，按最终状态定夺；
        # 已订阅过的频道重复subscribe是幂等的
        to_subscribe = [chan for chan in added if chan in channel_subs]
        await mq.subscribe(*to_subscribe)
        # 退订名单必须在等 SUBSCRIBE 回来之后再定：等待期间接收协程可能处理了客户端的
        # 新订阅（subscribe_get 等），把刚释放的行频道又登记回来了——对 mq 来说该频道
        # 一直是订着的，那次 subscribe 不会有任何动作，这里按旧名单退订就会把新订阅
        # 底下的频道退掉，之后它的变更永远推不到客户端
        to_unsubscribe = [chan for chan in released if chan not in channel_subs]
        await mq.unsubscribe(*to_unsubscribe)
        return rtn

    async def _prefetch_rows(self, updated_channels: Mapping[str, Any]) -> None:
        """
        本tick所有收到通知的行频道，按表分组各一次get_many，把原始行填进
        RowSubscription的缓存，循环里的get_updated就不用逐行往返了。
        """
        by_table: dict[TableReference, tuple[list[str], list[int]]] = {}
        for channel in updated_channels:
            for sub_id in self._channel_subs.get(channel, ()):
                sub = self._subs[sub_id]
                if isinstance(sub, RowSubscription):
                    row_sub = sub
                elif isinstance(sub, IndexSubscription):
                    row_sub = sub.row_subs.get(channel)
                else:
                    row_sub = None
                if row_sub is None:
                    continue
                channels, row_ids = by_table.setdefault(row_sub.table_ref, ([], []))
                channels.append(channel)
                row_ids.append(row_sub.row_id)
                break  # 同一频道只需读一次，其他订阅共用缓存
        if not by_table:
            return
        servant = self._backend.servant
        for table_ref, (channels, row_ids) in by_table.items():
            rows = cast(
                list[dict[str, Any] | None],
                await servant.get_many(table_ref, row_ids, RowFormat.TYPED_DICT),
            )
            for channel, row in zip(channels, rows):
                RowSubscription.prefill_cache_(channel, row)
