"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import itertools
import logging
import time
import weakref
from collections import Counter
from collections.abc import Callable, Coroutine, Iterable, Mapping
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Final, cast

import numpy as np

from hetu.data.backend import BackendClient, MQClient, RowFormat
from hetu.data.backend.base import HubMQClient
from hetu.data.component import Permission
from hetu.i18n import _

if TYPE_CHECKING:
    from hetu.data.backend import Backend, TableReference
    from hetu.endpoint import Context

logger = logging.getLogger("HeTu.root")


class _Unknown:
    """RowSubscription.pushed 的哨兵类型，见 UNKNOWN"""

    __slots__ = ()

    def __repr__(self) -> str:
        return "UNKNOWN"


# 还不知道客户端持有什么内容（订阅已登记、初始行还没读回；或读期间已有推送，客户端最终
# 持有哪份说不准）：之后的任何读都照推
UNKNOWN: Final = _Unknown()

# 点查询落在没声明 point_sub 的索引上时已经警告过的：组件类 → {索引名}。按类记（不按名字），
# 测试里同名组件每次重定义都是新类；弱引用，组件类被重定义回收后自动清掉
_point_sub_warned: weakref.WeakKeyDictionary[type, set[str]] = (
    weakref.WeakKeyDictionary()
)


def warn_point_sub_fallback_(table_ref: TableReference, index_name: str) -> None:
    """点查询退化为区间订阅时警告，每个组件类的每个索引只警告一次"""
    warned = _point_sub_warned.setdefault(table_ref.comp_cls, set())
    if index_name in warned:
        return
    warned.add(index_name)
    if index_name == "id":
        # id 不能声明 point_sub，按 id 订单行本该走行订阅
        logger.warning(
            _(
                "⚠️ [📡Subscription] {comp_name} 按 id 点查询的范围订阅会订整个 id 索引的"
                "频道：这张表每次插入、删除都会叫醒它重跑比对。按 id 订单行请用行订阅"
                "（服务端 subscribe_get，客户端 WatchRow）"
            ).format(comp_name=table_ref.comp_name)
        )
        return
    logger.warning(
        _(
            "⚠️ [📡Subscription] {comp_name}.{index_name} 没有声明 point_sub，"
            "点查询订阅退化为区间订阅：该索引上任何写入都会叫醒它重跑比对。"
            "需要高效点订阅请在 property_field 里加 point_sub=True"
        ).format(comp_name=table_ref.comp_name, index_name=index_name)
    )


async def read_whole_table_(
    servant: BackendClient, table_ref: TableReference, max_rows: int
) -> tuple[list[dict[str, Any]], bool]:
    """
    整表读：按 id 升序读原始行（含 _version，不做 RLS 判定），最多 max_rows 行。多读一行
    看后面还有没有，返回 (行, 是否超过上限被截断)。id 是每个 Component 隐式的 unique 索引
    """
    rows = await servant.range(
        table_ref,
        "id",
        float("-inf"),
        float("inf"),
        limit=max_rows + 1,
        row_format=RowFormat.TYPED_DICT,
    )
    return rows[:max_rows], len(rows) > max_rows


def row_fingerprint_(row: Mapping[str, Any] | None) -> int | None:
    """
    行内容的指纹（行不存在为 None），用来判断重读回来的是不是客户端已经持有的那份数据：
    补读、尾随重读大多读回一样的内容，一样就不再推。
    不能只比 _version：同 id 删除后重插时版本从 1 重来，而刚插入没改过的行都是 1。
    原始行含 _version，字段顺序由 dtype 固定。组件不允许子数组字段（define_component 拒绝
    void dtype），行里都是 Python 标量 / str / bytes，repr 是精确的（float 的 repr 能原样
    转回；hash(tuple(values)) 反而分不清 0.0 与 -0.0）。实测每行 1µs 以内，比转 dict 还便宜。
    """
    return None if row is None else hash(repr(row))


class BaseSubscription:
    # 以下几项由 SubscriptionHub 维护（attach / _release），比对逻辑不用管
    # 成员连接 → 该连接给这个订阅的 sub_id
    members: dict[SubscriptionBroker, str]
    # hub 内唯一，定向补读用
    token: int = 0
    # 频道订阅生效、门面登记完成才置真：之前 tick 不处理它
    active: bool = False
    # 最后一个成员离开时置真：处理到一半的 tick 据此丢掉结果
    closed: bool = False

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
        pushed: int | _Unknown | None = UNKNOWN,
    ):
        self.table_ref = table_ref
        self.servant = servant
        if table_ref.comp_cls.is_rls() and ctx and not ctx.is_admin():
            self.rls_ctx = ctx
        else:
            self.rls_ctx = None
        self.channel = channel
        self.row_id = row_id
        # 客户端当前持有的内容指纹（row_fingerprint_）：重读回来一样就不推。
        # 客户端没有这行（行不存在，或 RLS 不可见）时为 None
        self.pushed: int | _Unknown | None = pushed
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

    async def read_(self, channel: str) -> dict[str, Any] | None:
        """读本行的原始行（含 _version，不做 RLS 判定），行不存在为 None"""
        # 如果订阅有交叉，这里会重复被调用，先看本tick的缓存（get_updates会批量预读填好；
        # tick中途新建的行订阅不在预读范围内，这里兜底单行查询）
        cache = RowSubscription.__cache.get(None)
        if cache is None:
            cache = RowSubscription.reset_cache_()
        if channel in cache:
            return cache[channel]
        row = await self.servant.get(self.table_ref, self.row_id, RowFormat.TYPED_DICT)
        cache[channel] = row
        return row

    async def get_updated(
        self, channel: str, payload: set[str] | None = None
    ) -> tuple[set[str], set[str], Mapping[int, dict[str, Any] | None]]:
        """
        channel收到通知后，前来调用此get_updated方法。
        返回 {空}, {空}, {变更的row_id: 行数据，None表示删除}；读回的与客户端已持有的
        一样时不返回任何更新。
        """
        row = await self.read_(channel)
        visible = self.decode_row_(row)
        # 不可见的行客户端没有，和行不存在一样记 None：它在不可见期间怎么变都不推，
        # 推 None 等于把不可见行的 id 告诉了客户端
        fingerprint = None if visible is None else row_fingerprint_(row)
        if fingerprint == self.pushed:
            return set(), set(), {}
        self.pushed = fingerprint
        return set(), set(), {self.row_id: visible}

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
        point_value: np.generic | None = None,
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
        # 订的是值频道时为该值（已按 dtype 规范化），否则 None。值频道只在有行"进入"时才有
        # 通知，行"离开"（删除、字段改走）要靠结果里各行的行频道发现，见 get_updated
        self.point_value = point_value

    def add_row_subscriber(self, channel: str, row_id: int, pushed: int | None):
        """登记初始结果里的一行，pushed 是客户端拿到的那份的指纹"""
        self.row_subs[channel] = RowSubscription(
            self.table_ref, self.servant, self.rls_ctx, channel, row_id, pushed
        )

    async def get_updated(
        self, channel: str, payload: set[str] | None = None
    ) -> tuple[set[str], set[str], Mapping[int, dict[str, Any] | None]]:
        """
        channel收到通知后，前来调用此get_updated方法。
        返回 {需要新订阅的频道}, {需要取消订阅的频道}, {变更的row_id: 行数据，None表示删除}
        """
        if channel == self.index_channel:
            return await self._rerange()
        row_sub = self.row_subs.get(channel)
        if row_sub is None:
            raise RuntimeError(
                _("IndexSubscription收到了未知的channel消息: {channel}").format(
                    channel=channel
                )
            )
        if self.point_value is not None and self._left_(await row_sub.read_(channel)):
            # 值频道只在有行"进入"时才有通知：这行离开了该值（删除 / 字段改走）只能在这里
            # 发现。重跑比对：推 None、退订它的行频道，并补进被 limit 截在外面的行
            new_chans, rem_chans, rtn = await self._rerange()
            if channel in self.row_subs:
                # 重跑后它仍在结果里（读到的索引与行不是同一时刻的）：照常推行更新
                rtn.update((await row_sub.get_updated(channel))[2])
            return new_chans, rem_chans, rtn
        return await row_sub.get_updated(channel)

    def _left_(self, row: dict[str, Any] | None) -> bool:
        """值频道点查询：读回的原始行是否已不在该值上（行不存在，或字段不等于该值）"""
        if row is None:
            return True
        index_name = self.query_param["index_name"]
        dtype = self.table_ref.comp_cls.dtype_map_[index_name]
        return dtype.type(row[index_name]) != self.point_value

    async def _rerange(
        self,
    ) -> tuple[set[str], set[str], dict[int, dict[str, Any] | None]]:
        """重跑 range，与上次结果比对：新进入的行订上行频道并推送，离开的行推 None 并退订"""
        servant = self.servant
        ref = self.table_ref
        row_ids = await servant.range(
            ref, **self.query_param, row_format=RowFormat.ID_LIST
        )
        row_ids = set(row_ids)
        inserts = list(row_ids - self.last_range_result)
        deletes = self.last_range_result - row_ids
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
        # 读完才改状态：读出错时 hub 会定向重读，上次的结果原样留着，重读才能再算出同样的进出
        self.last_range_result = row_ids
        for row_id, row in zip(inserts, rows):
            if row is None:
                self.last_range_result.remove(row_id)
                continue  # 可能是刚添加就删了
            new_chan_name = servant.row_channel(ref, row_id)
            new_chans.add(new_chan_name)
            row_sub = RowSubscription(ref, servant, self.rls_ctx, new_chan_name, row_id)
            self.row_subs[new_chan_name] = row_sub
            # 不可见（RLS）的行也要订阅，等它变得可见时才能通知；但现在不推给客户端
            visible = row_sub.decode_row_(row)
            row_sub.pushed = None if visible is None else row_fingerprint_(row)
            if visible is not None:
                rtn[row_id] = visible
        for row_id in deletes:
            rem_chan_name = servant.row_channel(ref, row_id)
            rem_chans.add(rem_chan_name)
            # 客户端手里没有这行的（RLS 不可见，或已经推过 None）离开时不推
            if self.row_subs.pop(rem_chan_name).pushed is not None:
                rtn[row_id] = None

        return new_chans, rem_chans, rtn

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
        max_rows: int,
    ):
        """建好时处于初始化中，初始全量读完成后调 `finish_init_`"""
        self.table_ref = table_ref
        self.servant = servant
        if table_ref.comp_cls.is_rls() and ctx and not ctx.is_admin():
            self.rls_ctx = ctx
        else:
            self.rls_ctx = None
        self.table_channel = table_channel
        # 已推送给客户端、且客户端仍持有的行id。用于判断"删除/失去RLS"是否需要通知；
        # 初始化完成时由初始行填上
        self.known_ids: set[int] = set()
        # 整表重同步（RESYNC）最多读多少行，同 subscribe_table 的上限
        self.max_rows = max_rows
        # 上一批读过的可见行 → 内容指纹。不按行常驻（known_ids 已是每连接一份）：尾随重读
        # 就是紧接着的那一批，只要它读回的与上一批一样就不重复推
        self.last_read: dict[int, int | None] = {}
        # 初始化中（订阅已生效、初始全量读还没完成）为 set：这期间弹出的通知不读库也不推，
        # 只把 row_id 攒在这里，初始读完成后重新入队——客户端还没拿到 sub_id，推了会被
        # 丢掉，而初始读又未必包含这些写入。初始化完成后为 None
        self.pending: set[str] | None = set()

    def finish_init_(self, rows: list[dict[str, Any]]) -> set[str]:
        """
        初始全量读完成（rows 为客户端将拿到的行，含 _version）：记下客户端持有的行，退出
        初始化。返回初始化期间攒下的 row_id，调用方要把它们重新入队重读；初始读已经包含的
        读回一样，不会再推
        """
        assert self.pending is not None, "重复完成初始化"
        pending, self.pending = self.pending, None
        self.known_ids = {int(row["id"]) for row in rows}
        self.last_read = {
            int(row["id"]): row_fingerprint_(row)
            for row in rows
            if str(row["id"]) in pending
        }
        return pending

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
        if self.pending is not None:
            self.pending.update(payload)
            return set(), set(), {}
        if MQClient.RESYNC in payload:
            return set(), set(), await self._resync()

        ids = sorted(int(i) for i in payload)
        rows = cast(
            list[dict[str, Any] | None],
            await self.servant.get_many(self.table_ref, ids, RowFormat.TYPED_DICT),
        )
        comp_cls = self.table_ref.comp_cls
        ctx = self.rls_ctx
        known = self.known_ids
        last_read = self.last_read
        self.last_read = {}
        rtn: dict[int, dict[str, Any] | None] = {}
        for row_id, row in zip(ids, rows):
            if row is not None and (ctx is None or ctx.rls_check(comp_cls, row)):
                fingerprint = self.last_read[row_id] = row_fingerprint_(row)
                if row_id in known and last_read.get(row_id) == fingerprint:
                    continue  # 上一批刚推过一模一样的（尾随重读）
                del row["_version"]
                rtn[row_id] = row
                known.add(row_id)
            elif row_id in known:
                # 被删除，或失去RLS权限：客户端持有该行，需要通知删除
                rtn[row_id] = None
                known.discard(row_id)
            # 既不可见、客户端也从未持有的行：不推
        return set(), set(), rtn

    async def _resync(self) -> dict[int, dict[str, Any] | None]:
        """
        这段时间的变更不可知（pubsub 断线重连，期间的通知全丢了）：整表重读，推所有可见行，
        已知但读不到、或不再可见的行推 None。
        """
        rows, truncated = await read_whole_table_(
            self.servant, self.table_ref, self.max_rows
        )
        comp_cls = self.table_ref.comp_cls
        ctx = self.rls_ctx
        known = self.known_ids
        rtn: dict[int, dict[str, Any] | None] = {}
        seen: set[int] = set()
        for row in rows:
            row_id = int(row["id"])
            seen.add(row_id)
            if ctx is None or ctx.rls_check(comp_cls, row):
                del row["_version"]
                rtn[row_id] = row
                known.add(row_id)
            elif row_id in known:
                rtn[row_id] = None
                known.discard(row_id)
        # 超过上限时后面还有行没读到（按 id 升序），只对读到的 id 范围内的已知行判删除
        bound = max(seen) if seen and truncated else None
        for row_id in known - seen:
            if bound is None or row_id <= bound:
                rtn[row_id] = None
                known.discard(row_id)
        self.last_read = {}
        return rtn

    @property
    def channels(self) -> set[str]:
        """返回当前订阅关注的频道们"""
        return {self.table_channel}


# 定向补读在 MQ 队列里的键："\0{token}\0{频道}"。真实频道名不会以 NUL 开头（设计稿 §4.4）
_TARGETED = "\0"
# 一个 tick 里连续处理这么多个订阅就让出一次事件循环，别长时间饿死接收 / 发送协程
_YIELD_EVERY = 256
# 错误日志的限流间隔（秒）：Redis 挂着时每个 tick 都会出错、都会重试，别刷屏
_ERROR_LOG_INTERVAL = 10.0


class _Tick:
    """一个 tick 的簿记"""

    __slots__ = ("added", "released", "staged")

    def __init__(self) -> None:
        # 本 tick 新增的频道 → 新增它的订阅（新订上的频道要给它们定向补读）
        self.added: dict[str, set[BaseSubscription]] = {}
        # 本 tick 没人要了的频道
        self.released: set[str] = set()
        # 暂存的更新：成员连接 → {sub_id: (订阅, 合并后的更新)}，tick 末尾才交给连接
        self.staged: dict[
            SubscriptionBroker,
            dict[str, tuple[BaseSubscription, dict[int, dict[str, Any] | None]]],
        ] = {}


class SubscriptionHub:
    """
    worker 级订阅器：每个 worker（进程）的每个 backend 一个，worker 内所有连接的订阅都在这里处理。
    持有唯一的 MQClient（本地队列：合批、尾随重读，见 `MQClient`）和一个处理循环：弹出一批通知 →
    按表批量预读行 → 各订阅 `get_updated`（订阅之间并发）→ 记账 → 按成员暂存更新；tick 末尾统一
    订阅 / 退订频道，再把更新交给各连接的门面（`SubscriptionBroker`）。

    一个 worker 一个队列、一条时间线，尾随重读与补读的保证与每连接一个队列时相同。订阅层的补读
    定向到单个订阅（`reread_for`），不按频道重跑 worker 里所有订阅。设计见
    docs/superpowers/specs/2026-09-28-worker-subscriptions-design.md。

    The worker-level subscription engine, one per backend per worker process. It owns the only
    MQClient (local queue with batching and trailing re-reads) and one processing loop that
    serves the subscriptions of every connection, handing the updates to each connection's
    `SubscriptionBroker` at the end of a tick.
    """

    def __init__(self, backend: Backend, autostart: bool = True):
        """
        Parameters
        ----------
        backend: Backend
            数据库后端。一般用 `SubscriptionHub.of(backend)` 取 backend 共享的那个。
        autostart: bool
            True 起后台处理循环（生产）。False 为手动模式（测试用）：不起循环，由门面的
            `get_updates` 自己弹出一批、跑一个 tick，不调它通知就留在队列里。
        """
        self._backend = backend
        mq = backend.get_mq_client()
        if isinstance(mq, HubMQClient):
            mq.MAX_SUBSCRIBED = None  # 订的是整个 worker 的频道，单连接的告警在门面做
        self._mq = mq
        self._autostart = autostart
        # 频道 → 订了它的订阅（含 attach 中还没生效的占位）
        self._channel_subs: dict[str, set[BaseSubscription]] = {}
        # token → 订阅，定向补读用
        self._by_token: dict[int, BaseSubscription] = {}
        self._tokens = itertools.count(1)
        # SUBSCRIBE 已经回来的频道。MQClient.subscribed 在 SUBSCRIBE 发出时就记上，不能用来判断
        # 是否已生效（见 _settle_channels 的 fresh）
        self._effective: set[str] = set()
        # 后台任务（attach 的订阅、放掉的频道的退订）：不随调用方取消，close 时统一取消
        self._tasks: set[asyncio.Task] = set()
        self._task: asyncio.Task | None = None
        # 手动模式：几个门面并发驱动时一次只跑一个 tick
        self._step_lock = asyncio.Lock()
        # 错误日志限流：上次记的时刻，以及之后压下没记的次数
        self._error_logged_at = float("-inf")
        self._errors_muted = 0
        self._closed = False
        if autostart:
            self._start()

    @classmethod
    def of(cls, backend: Backend) -> SubscriptionHub:
        """
        backend 共享的订阅器：第一次调用时在当前事件循环里建，挂在 backend 上
        （`Backend.sub_hub_`），随 `Backend.close()` 关闭。
        """
        hub = backend.sub_hub_
        if hub is None or hub._closed:
            hub = backend.sub_hub_ = cls(backend)
        return hub

    @property
    def mq(self) -> MQClient:
        """本订阅器的 MQClient（测试、压测看队列用）"""
        return self._mq

    @property
    def autostart(self) -> bool:
        """是否有后台处理循环；False 为手动模式，由门面的 get_updates 驱动"""
        return self._autostart

    @property
    def interval(self) -> float:
        """合批间隔，也是副本复制延迟的预算（1/UPDATE_FREQUENCY）"""
        return 1 / self._mq.UPDATE_FREQUENCY

    # === === === 订阅的登记 === === ===

    async def attach(
        self, sub: BaseSubscription, broker: SubscriptionBroker, sub_id: str
    ) -> None:
        """
        登记 sub，订阅它的频道（`sub.channels`），返回时频道都已生效。

        先占位（`active=False`）再订阅：期间 tick / 别的 detach 定退订名单时看得到占位，不会把
        这些频道退掉；tick 也不处理未生效的订阅。返回后由调用方在登记到门面的同一个同步段里置
        `sub.active`：由订阅任务自己置的话，门面登记之前 tick 算好的推送会因 sub_id 没登记被丢掉，
        指纹却已更新，之后的补读读回一样也不再推。

        订阅放进 hub 自己的任务里跑，调用方 shield 着等：调用方被取消（连接在拆）时订阅照常完成，
        不会把几个连接共用的 MQClient 对这些频道的登记撤掉（别的连接可能正搭着同一个 SUBSCRIBE）。
        失败或被取消时撤掉占位再抛出。
        """
        if self._closed:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        if self._autostart:
            self._start()  # 处理循环意外结束过的话重新拉起
        sub.members = {broker: sub_id}
        sub.token = next(self._tokens)
        sub.active = False
        sub.closed = False
        self._by_token[sub.token] = sub
        channels = list(sub.channels)
        channel_subs = self._channel_subs
        for channel in channels:
            channel_subs.setdefault(channel, set()).add(sub)
        try:
            await asyncio.shield(self._spawn(self._subscribe(channels)))
        except BaseException:
            gone = self._release(broker, (sub,))
            if gone and not self._closed:
                self._spawn(self._unsubscribe(gone))
            raise

    async def detach(self, broker: SubscriptionBroker, *subs: BaseSubscription) -> None:
        """
        撤掉 broker 在这些订阅上的成员身份：成员撤空的订阅关闭（`closed`）、撤登记，退订 worker
        里没人再要的频道（等退订回来才返回）
        """
        gone = self._release(broker, subs)
        if gone and not self._closed:
            await self._unsubscribe(gone)

    def _release(
        self, broker: SubscriptionBroker, subs: Iterable[BaseSubscription]
    ) -> list[str]:
        """同步撤掉成员身份与登记，返回 worker 里因此没人要的频道"""
        channel_subs = self._channel_subs
        gone: list[str] = []
        for sub in subs:
            sub.members.pop(broker, None)
            if sub.members or sub.closed:
                continue
            sub.closed = True
            self._by_token.pop(sub.token, None)
            # tick 处理它的途中退订的话，sub.channels 可能已含 tick 还没登记的新行，也可能少了
            # tick 还没撤掉的旧行：前者不在频道表里，跳过；后者由 tick 回来后撤（见 _process）
            for channel in sub.channels:
                subs_on = channel_subs.get(channel)
                if subs_on is None or sub not in subs_on:
                    continue
                subs_on.discard(sub)
                if not subs_on:
                    del channel_subs[channel]
                    gone.append(channel)
        return gone

    async def _subscribe(self, channels: list[str]) -> None:
        """订阅频道，回来后把仍订着的记为已生效"""
        mq = self._mq
        await mq.subscribe(*channels)
        subscribed = mq.subscribed_channels
        self._effective.update(ch for ch in channels if ch in subscribed)

    async def _unsubscribe(self, channels: list[str]) -> None:
        """退订频道。排队到真正跑起来之间可能又有人订了（含 attach 的占位），那就不能退"""
        channels = [ch for ch in channels if ch not in self._channel_subs]
        if not channels:
            return
        self._effective.difference_update(channels)
        await self._mq.unsubscribe(*channels)

    def reread_for(
        self,
        sub: BaseSubscription,
        *channels: str,
        payload: Iterable[Any] | None = None,
    ) -> None:
        """
        定向补读：interval 后只让 sub 重读这些频道（payload 同 `MQClient.request_reread`，表级
        频道为 row_id）。在同一个 MQ 队列里用虚拟键入队，享有同样的延迟、合批、尾随重读与积压
        丢弃，不按频道重跑 worker 里其他订着它们的订阅（设计稿 §4.4）
        """
        if not channels:
            return
        prefix = f"{_TARGETED}{sub.token}{_TARGETED}"
        self._mq.request_reread(*[prefix + ch for ch in channels], payload=payload)

    # === === === 处理循环 === === ===

    def _start(self) -> None:
        """起后台处理循环；它意外结束了的话再起一个"""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="SubscriptionHub")
            self._task.add_done_callback(self._on_run_done)

    def _on_run_done(self, task: asyncio.Task) -> None:
        if self._closed:
            return
        if task.cancelled():
            logger.warning(
                _("⚠️ [📡Subscription] 订阅处理循环被取消，下次订阅时重新拉起")
            )
        elif (exc := task.exception()) is not None:
            logger.error(
                _("❌ [📡Subscription] 订阅处理循环异常结束，下次订阅时重新拉起"),
                exc_info=exc,
            )

    async def _run(self) -> None:
        mq = self._mq
        while True:
            batch = await mq.get_message()
            try:
                await self._tick(batch)
            # 读错误在 tick 里都兜住并重试了；这里兜 bug，处理循环不能停
            except Exception as e:  # noqa: BLE001
                self._log_error(_("处理订阅通知异常"), e)

    def _log_error(self, what: str, exc: BaseException, requeued: bool = True) -> None:
        """
        tick 里出错：记错误日志（带栈）。限流：每 _ERROR_LOG_INTERVAL 秒最多一条，带上此前压下的
        次数。requeued：出错的读已重新入队，一个 interval 后重试（设计稿 §6）
        """
        now = time.monotonic()
        if now - self._error_logged_at < _ERROR_LOG_INTERVAL:
            self._errors_muted += 1
            return
        muted, self._errors_muted = self._errors_muted, 0
        self._error_logged_at = now
        err = f"{type(exc).__name__}:{exc}"
        if requeued:
            msg = _(
                "❌ [📡Subscription] {what}：{err}，已重新入队稍后重试"
                "（上次记录以来另有 {muted} 次出错未记）"
            ).format(what=what, err=err, muted=muted)
        else:
            msg = _(
                "❌ [📡Subscription] {what}：{err}（上次记录以来另有 {muted} 次出错未记）"
            ).format(what=what, err=err, muted=muted)
        logger.error(msg, exc_info=exc)

    async def step_(self, deadline: float | None, ready: Callable[[], bool]) -> bool:
        """
        手动模式（测试用）：弹出一批通知跑一个 tick。几个门面并发调用时一次只跑一个 tick；拿到
        锁时 ready() 已为真（别人驱动的 tick 已把更新交来）就直接返回。deadline（loop.time()）前
        等不到可弹出的通知返回 False。
        """
        try:
            async with asyncio.timeout_at(deadline):
                await self._step_lock.acquire()
        except TimeoutError:
            return False
        try:
            if ready():
                return True
            try:
                async with asyncio.timeout_at(deadline):
                    batch = await self._mq.get_message()
            except TimeoutError:
                return False
            await self._tick(batch)
            return True
        finally:
            self._step_lock.release()

    async def _tick(self, batch: Mapping[str, set[str] | None]) -> None:
        """处理一批弹出的通知（设计稿 §4.3）"""
        work = await self._repair(self._collect(batch))
        if not work:
            return
        # 本 tick 要读的行先按表批量读取，填进 RowSubscription 的缓存
        RowSubscription.reset_cache_()
        await self._prefetch_rows(work)
        tick = _Tick()
        try:
            await self._process_all(work, tick)
            await self._settle_channels(tick)
        finally:
            # 放在最后：推给客户端的新行在其行频道订阅生效之后，get_updates 拿到的也总是完整的
            # tick。中途出错也把已算好的交出去
            for broker, entries in tick.staged.items():
                broker.deliver_(entries)

    def _collect(
        self, batch: Mapping[str, set[str] | None]
    ) -> dict[BaseSubscription, list[tuple[str, set[str] | None]]]:
        """
        按订阅分组：真实频道交给订了它的所有已生效订阅；定向补读只交给它指定的订阅（还订着那个
        频道的话）。每个订阅内保持弹出顺序。
        """
        channel_subs = self._channel_subs
        work: dict[BaseSubscription, list[tuple[str, set[str] | None]]] = {}
        for key, payload in batch.items():
            if key.startswith(_TARGETED):
                token, channel = key[1:].split(_TARGETED, 1)
                sub = self._by_token.get(int(token))
                if (
                    sub is not None
                    and sub.active
                    and sub in channel_subs.get(channel, ())
                ):
                    work.setdefault(sub, []).append((channel, payload))
                continue
            for sub in channel_subs.get(key, ()):
                if sub.active:
                    work.setdefault(sub, []).append((key, payload))
        return work

    async def _repair(
        self, work: dict[BaseSubscription, list[tuple[str, set[str] | None]]]
    ) -> dict[BaseSubscription, list[tuple[str, set[str] | None]]]:
        """
        本批涉及的频道若 hub 没订着（此前 tick 末尾订阅它失败了，见 _settle_channels），先补订。
        不管补订成败，这些频道本 tick 都不处理、按真实频道重新入队：订上了的一个 interval 后再读
        （要在订阅生效之后读，失败期间的写入没有通知），没订上的到时再补
        """
        subscribed = self._mq.subscribed_channels
        missing: dict[str, set[str] | None] = {}
        for items in work.values():
            for channel, payload in items:
                if channel in subscribed:
                    continue
                known = missing.get(channel)
                if payload is None:
                    missing.setdefault(channel, None)
                else:
                    missing[channel] = payload if known is None else known | payload
        if not missing:
            return work
        try:
            await self._subscribe(list(missing))
        except Exception as e:  # noqa: BLE001 没订上的到时再补
            self._log_error(_("补订频道出错"), e)
        for channel, payload in missing.items():
            self._mq.request_reread(channel, payload=payload)
        return {
            sub: kept
            for sub, items in work.items()
            if (kept := [item for item in items if item[0] not in missing])
        }

    async def _prefetch_rows(
        self, work: Mapping[BaseSubscription, list[tuple[str, set[str] | None]]]
    ) -> None:
        """
        本 tick 各订阅要处理的行频道按表分组，各一次 get_many，把原始行填进 RowSubscription 的
        每 tick 缓存：同一行在 worker 内只读一次，get_updated 也不用逐行往返。这些读都在本批弹出
        之后发出，离各订阅要覆盖的通知都已至少一个 interval。

        某张表读失败（Redis 抖动、有行解码不了……）只是这张表不填缓存：它的订阅在 get_updated 里
        各自单行读（`RowSubscription.read_` 的兜底），读不出的由 `_process` 记日志、定向重读。一行
        坏数据只卡住订了它的订阅，不牵连同批的别的行、别的表（设计稿 §6）。
        """
        by_table: dict[TableReference, tuple[list[str], list[int]]] = {}
        seen: set[str] = set()
        for sub, items in work.items():
            for channel, _payload in items:
                if channel in seen:
                    continue
                if isinstance(sub, RowSubscription):
                    row_sub: RowSubscription | None = sub
                elif isinstance(sub, IndexSubscription):
                    row_sub = sub.row_subs.get(channel)
                else:
                    break
                if row_sub is None:
                    continue  # 索引频道
                seen.add(channel)
                channels, row_ids = by_table.setdefault(row_sub.table_ref, ([], []))
                channels.append(channel)
                row_ids.append(row_sub.row_id)
        if not by_table:
            return
        servant = self._backend.servant
        for table_ref, (channels, row_ids) in by_table.items():
            try:
                rows = cast(
                    list[dict[str, Any] | None],
                    await servant.get_many(table_ref, row_ids, RowFormat.TYPED_DICT),
                )
            except Exception as e:  # noqa: BLE001 不填缓存，订阅各自单行读
                self._log_error(
                    _("预读 {comp_name} 的行出错，这批改为逐行读").format(
                        comp_name=table_ref.comp_name
                    ),
                    e,
                    requeued=False,
                )
                continue
            for channel, row in zip(channels, rows):
                RowSubscription.prefill_cache_(channel, row)

    async def _process_all(
        self,
        work: Mapping[BaseSubscription, list[tuple[str, set[str] | None]]],
        tick: _Tick,
    ) -> None:
        """
        订阅之间并发：从缓存命中、不需要 I/O 的当场跑完（eager task，不进调度），要读库的并发
        执行，读的往返不串起来。
        eager task 直接用 Task 构造：3.14 的 create_task(..., eager_start=True) 要事件循环的
        create_task 收这个参数，生产在 Linux / macOS 上跑的 uvloop 不收（TypeError）
        """
        loop = asyncio.get_running_loop()
        pending: list[asyncio.Task] = []
        for count, (sub, items) in enumerate(work.items(), 1):
            task = asyncio.Task(
                self._process(sub, items, tick), loop=loop, eager_start=True
            )
            if not task.done():
                pending.append(task)
            if count % _YIELD_EVERY == 0:
                await asyncio.sleep(0)
        if pending:
            await asyncio.gather(*pending)

    async def _process(
        self,
        sub: BaseSubscription,
        items: list[tuple[str, set[str] | None]],
        tick: _Tick,
    ) -> None:
        """一个订阅按顺序处理它这批的频道：记账、按成员暂存更新"""
        channel_subs = self._channel_subs
        for channel, payload in items:
            # 已退订；或本 tick 里它自己的处理刚把这行放出了范围（分组在前，这里得现查）
            if sub.closed or sub not in channel_subs.get(channel, ()):
                continue
            try:
                new_chans, rem_chans, updates = await sub.get_updated(channel, payload)
            except Exception as e:  # noqa: BLE001 定向重读重试，不牵连别的订阅
                # 读库出错（Redis 抖动等）：不牵连别的订阅、不断开连接。给它定向重读这个频道，
                # 一个 interval 后重试（设计稿 §6）
                self._log_error(
                    _("订阅处理通知出错，频道 {channel}").format(channel=channel), e
                )
                if not sub.closed:
                    self.reread_for(sub, channel, payload=payload)
                continue
            closed = sub.closed
            # 行进入/离开范围：先记账，订阅/退订留到 tick 末尾各一次批量往返。查库期间被退订的
            # 不再登记新行，但离开的行照样撤掉：退订时按那一刻的频道撤，可能还没撤到它们
            if not closed:
                for new_chan in new_chans:
                    channel_subs.setdefault(new_chan, set()).add(sub)
                    tick.added.setdefault(new_chan, set()).add(sub)
            for rem_chan in rem_chans:
                subs_on = channel_subs.get(rem_chan)
                if subs_on is None or sub not in subs_on:
                    continue
                subs_on.discard(sub)
                if not subs_on:
                    del channel_subs[rem_chan]
                    tick.released.add(rem_chan)
            if closed:
                return
            if updates:
                self._stage(tick, sub, updates)

    @staticmethod
    def _stage(
        tick: _Tick,
        sub: BaseSubscription,
        updates: Mapping[int, dict[str, Any] | None],
    ) -> None:
        """按成员暂存：同一个 tick 里同一订阅的几次更新合并，后到的覆盖先到的"""
        staged = tick.staged
        for broker, sub_id in sub.members.items():
            entries = staged.setdefault(broker, {})
            entry = entries.get(sub_id)
            if entry is not None and entry[0] is sub:
                entry[1].update(updates)
            else:
                entries[sub_id] = (sub, dict(updates))

    async def _settle_channels(self, tick: _Tick) -> None:
        """tick 末尾：订阅新增的频道、给新订上的定向补读、退订没人要的"""
        channel_subs = self._channel_subs
        # 同一频道可能在本 tick 内既被一个订阅加入又被另一个释放，按最终状态定夺；
        # 已订阅过的频道重复 subscribe 是幂等的
        to_subscribe = [chan for chan in tick.added if chan in channel_subs]
        # hub 在本 tick 之前没有生效地订着的：订阅读这行在前、订阅生效在后，其间的写入不会有
        # 通知，值频道又不发"离开"。给新增它的订阅定向补读（读回一样就不推）。已生效的不用：
        # 之后的写入都有通知进本队列，tick 结束前订阅已登记好（设计稿 §4.4）
        fresh = [chan for chan in to_subscribe if chan not in self._effective]
        if to_subscribe:
            try:
                await self._subscribe(to_subscribe)
            except Exception as e:  # noqa: BLE001 重新入队，弹出时补订
                # 订阅失败：频道仍留在频道表里，按真实频道重新入队，弹出时先补订（见 _repair）
                self._log_error(_("订阅新进入范围的行频道出错"), e)
                for chan in to_subscribe:
                    self._mq.request_reread(chan)
                fresh = []  # 补订成功后会按真实频道重读，不用定向补读
        for chan in fresh:
            for sub in tick.added[chan]:
                if not sub.closed and sub in channel_subs.get(chan, ()):
                    self.reread_for(sub, chan)
        # 退订名单必须在等 SUBSCRIBE 回来之后再定：等待期间接收协程可能登记了新订阅（attach 的
        # 占位），把刚释放的频道又要回去了
        to_unsubscribe = [chan for chan in tick.released if chan not in channel_subs]
        if to_unsubscribe:
            await self._unsubscribe(to_unsubscribe)

    # === === === 生命周期 === === ===

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task:
        """hub 自己的后台任务：不随调用方取消，保存引用免得被 gc，close 时统一取消"""
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._on_spawned_done)
        return task

    def _on_spawned_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and (exc := task.exception()) is not None:
            # 等它的调用方会收到同样的异常；调用方已被取消的话只剩这一条记录
            logger.warning(
                _("⚠️ [📡Subscription] 订阅 / 退订频道失败：{err}").format(
                    err=f"{type(exc).__name__}:{exc}"
                )
            )

    async def close(self) -> None:
        """关闭处理循环与 MQClient（`Backend.close` 调用）。之后这个 hub 上的订阅都不再推送"""
        if self._closed:
            return
        self._closed = True
        tasks = [
            t for t in (self._task, *self._tasks) if t is not None and not t.done()
        ]
        self._task = None
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._channel_subs.clear()
        self._by_token.clear()
        self._effective.clear()
        await self._mq.close()


class SubscriptionBroker:
    """
    Component的数据订阅和查询接口，每个连接一个。订阅本身在本 worker 共享的 `SubscriptionHub`
    里处理，本对象是连接的门面：权限检查、sub_id、同连接的重复订阅与订阅数，以及待发区——hub 在
    tick 末尾把本连接的更新交到这里，`get_updates` 取走。

    The per-connection facade of component subscriptions. The subscriptions themselves are
    processed by the worker-wide `SubscriptionHub`; this object handles permissions, sub
    ids, per-connection duplicates and quotas, and the outbox that `get_updates` drains.

    订阅推送是尽力而为的最终一致：正常负载下约 99% 的情况，客户端会在 1~2 个
    `1/UPDATE_FREQUENCY`（默认 100~200ms）内收到最新数据；Redis 压力过大（副本复制延迟
    超过约 100ms）时，客户端可能残留旧数据，直到该行下次变更。需要强一致的判断请放在
    System 里读（写事务有乐观锁兜底），不要依赖客户端手里的订阅数据。
    原因：通知不带内容，收到后去随机副本重读，发通知的节点和读的节点可能不是同一个。
    每条通知之后，以及订阅生效、pubsub 断线重订生效之后，都至少隔一个 interval 再读一次
    （见 `MQClient` 的尾随重读与 `request_reread`），副本复制延迟在这个预算内就一定读到
    最新值。

    Subscriptions are best-effort and eventually consistent: under normal load a client
    gets the latest data in about 99% of cases, usually within one or two update ticks
    (100-200 ms by default). When Redis is overloaded (replica lag above ~100 ms), a client
    may keep stale data until that row changes again. Make decisions that need strong
    consistency inside a System (write transactions are guarded by optimistic locking).
    """

    # 单个连接订阅频道数的告警线，按订阅时登记的频道数估算（tick 里行进出范围不计）；只是告警
    MAX_SUBSCRIBED = 5000

    def __init__(
        self,
        backend: Backend,
        max_table_rows: int = 100_000,
        hub: SubscriptionHub | None = None,
    ):
        """
        Parameters
        ----------
        backend: Backend
            数据库后端
        max_table_rows: int
            单次整表订阅（subscribe_table）允许的最大行数，超过则拒绝订阅。
            一般对应配置项 `MAX_TABLE_SUBSCRIPTION_ROWS`。
        hub: SubscriptionHub | None
            处理订阅的 worker 级订阅器，默认取 backend 共享的那个（`SubscriptionHub.of`）。
            测试可以传入自己的实例。
            The worker-level engine; defaults to the one shared by `backend`.
        """
        self._backend = backend
        self._hub = hub if hub is not None else SubscriptionHub.of(backend)
        self._max_table_rows = max_table_rows

        self._subs: dict[str, BaseSubscription] = {}  # key是sub_id
        self._sub_counts: Counter[type[BaseSubscription]] = Counter()
        # 待发区：hub 在 tick 末尾交来的更新 {sub_id: {row_id: 行 | None}}，get_updates 取走
        self._outbox: dict[str, dict[int, dict[str, Any] | None]] = {}
        self._arrived = asyncio.Event()
        # 服务端内部关注（watch_channel）用的 MQClient，第一次用时才建
        self._watch_mq: MQClient | None = None
        # MAX_SUBSCRIBED 告警用：各订阅登记时的频道数
        self._channel_counts: dict[str, int] = {}
        self._channel_count = 0
        self._closed = False

    async def close(self):
        """撤掉本连接的全部订阅，关掉内部关注用的 MQClient。连接拆除路径上调用，后端出错也不抛"""
        self._closed = True
        subs = list(self._subs.values())
        self._subs.clear()
        self._sub_counts.clear()
        self._channel_counts.clear()
        self._channel_count = 0
        self._outbox.clear()
        if subs:
            try:
                await self._hub.detach(self, *subs)
            except Exception as e:  # noqa: BLE001 拆连接不能因为后端异常半途而废
                logger.warning(
                    _("⚠️ [📡Subscription] 关闭连接时取消订阅失败：{err}").format(
                        err=f"{type(e).__name__}:{e}"
                    )
                )
        if self._watch_mq is not None:
            await self._watch_mq.close()

    async def watch_channel(self, channel: str, callback: Callable[[], None]) -> None:
        """
        服务端内部关注一个频道（如本连接用户的 Connection owner 值频道）：收到通知调
        `callback`，不计入订阅数、不做权限检查，也不经过 hub 的队列；客户端对同一频道的订阅/
        退订与之互不干扰。连接关闭（`close`）时一起退订。
        回调在后端通知接收器的监听协程里同步执行，必须非阻塞。
        """
        if self._watch_mq is None:
            self._watch_mq = self._backend.get_mq_client()
        await self._watch_mq.watch(channel, callback)

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
            return bool(ctx.caller)

    @classmethod
    def _has_row_permission(
        cls, table_ref: TableReference, ctx: Context, row: dict | np.record
    ) -> bool:
        """判断是否对行有权限，首先你要调用_has_table_permission判断是否有表权限"""
        return ctx.rls_check(table_ref.comp_cls, row)

    async def _attach(self, sub_id: str, sub: BaseSubscription) -> None:
        """
        订阅 sub 的频道（返回时已生效）并登记到本连接。active 与登记在同一个同步段里置：tick 在
        此之前不处理它（设计稿 §4.6）
        """
        await self._hub.attach(sub, self, sub_id)
        if self._closed:
            # 等订阅生效期间连接被关了：撤掉刚订上的，别留在 hub 里
            await self._hub.detach(self, sub)
            raise ConnectionError(_("连接已关闭，已调用过close"))
        sub.active = True
        self._subs[sub_id] = sub
        self._sub_counts[type(sub)] += 1
        channels = len(sub.channels)
        self._channel_counts[sub_id] = channels
        self._channel_count += channels
        if self._channel_count > self.MAX_SUBSCRIBED:
            logger.warning(
                _(
                    "⚠️ [{tag}] 当前连接订阅数超过全局限制MAX_SUBSCRIBED={limit}行"
                ).format(tag="📡Subscription", limit=self.MAX_SUBSCRIBED)
            )

    async def subscribe_get(
        self,
        table_ref: TableReference,
        ctx: Context,
        index_name: str,
        query_value: int | float | str,
    ) -> tuple[str | None, dict[str, Any] | None]:
        """
        获取并订阅单行数据。
        如果是重复订阅，会返回上一次订阅的sub_id，已有的订阅照旧。回复里的数据是这次重新
        读的（可能落在滞后的副本上），不代表订阅已推给客户端的内容：客户端应沿用已有的订阅
        对象、不要拿它覆盖本地数据（官方 SDK 就是这样）。客户端应该写代码防止重复订阅。

        推送是尽力而为的最终一致：约 99% 的情况收到最新数据，Redis 压力过大时可能残留旧数据
        直到该行下次变更（见类说明）。
        Best-effort, eventually consistent: ~99% of the time the latest data arrives; stale
        data may remain under Redis overload until the row changes again (see the class doc).

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
        row_sub = RowSubscription(table_ref, servant, ctx, channel_name, row_id)
        # 订阅一生效就登记，不能等读完：tick 的退订名单按频道表定，读是真正的 await，期间某个
        # 索引订阅把这行放出范围的话，没登记的频道会被当作没人要而退订，这里再登记上去的就是
        # 一个永远收不到通知的订阅。
        # 读期间到达的通知会由 tick 照常推给这个订阅，那次推送可能先于、也可能晚于 sub 回复
        # 送达，见读完后对 pushed 的处理
        await self._attach(sub_id, row_sub)
        try:
            row = await servant.get(table_ref, row_id, RowFormat.TYPED_DICT)
        except BaseException:
            await self.unsubscribe(sub_id)
            raise
        # 行不存在，或 caller 对该行无权限：撤销登记并退订（本 worker 别的订阅还在用这个
        # 频道就留着）
        if row is None or not self._has_row_permission(table_ref, ctx, row):
            await self.unsubscribe(sub_id)
            return None, None
        if row_sub.pushed is UNKNOWN:
            # 读期间没有 tick 碰过这个订阅：客户端拿到的就是这份初始行
            row_sub.pushed = row_fingerprint_(row)
        else:
            # 读期间已有 tick 替它算好了推送。那次推送先于 sub 回复送达的话，客户端还没有
            # sub_id 会丢掉它；晚于回复的话会盖掉回复里的行——客户端最终持有哪份说不准，
            # 保持 UNKNOWN，让下面的补读无条件推一次最新的
            row_sub.pushed = UNKNOWN
        # 订阅生效前已在别的节点上应用、这次读到的副本却还没应用的写入，不会再有通知：
        # 隔一个 interval 补读一次（读回一样就不推）
        self._hub.reread_for(row_sub, channel_name)
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
        如果是重复订阅，会返回上一次订阅的sub_id，已有的订阅照旧。回复里的数据是这次重新
        读的（可能落在滞后的副本上），不代表订阅已推给客户端的内容：客户端应沿用已有的订阅
        对象、不要拿它覆盖本地数据（官方 SDK 就是这样）。客户端应该写代码防止重复订阅。

        订阅会观察数据的变化/添加/删除，收到对应通知，由get_updates调用时处理。

        时间复杂度是O(log(N)+M)，N是index的总行数；M是limit。
        Component权限是RLS时，查询后再根据权限筛选，limit为筛选前的行数，可能会获得少于limit行数据。

        Notes
        -----
        RLS 权限的得失：
        - 当某行已查询到的数据，失去RLS权限时，**会**收到该行被删除的通知
        - 范围内起初不可见的行，之后获得RLS权限时**会**推送该行：订阅生效后的补读会把
          范围内不可见的行一并订上（只订不推）

        RLS权限介绍请看See Also的组件定义。

        推送是尽力而为的最终一致：约 99% 的情况收到最新数据，Redis 压力过大时可能残留旧数据
        直到该行下次变更（见类说明）。
        Best-effort, eventually consistent: ~99% of the time the latest data arrives; stale
        data may remain under Redis overload until the row changes again (see the class doc).

        通知范围取决于查询形状：
        - 点查询（省略 `right`，或 `left == right`，如 `owner=me`、`zone=z`），且索引声明了
          `point_sub`：只订"索引=该值"的频道，只有这个值上有行进出时才会被唤醒；
        - 区间查询，或索引没有声明 `point_sub` 的点查询：订整个索引的频道，该索引上任何值的
          行增删/变更都会唤醒它重跑一次比对（点查询落到这里时服务器会警告一次）。
          热索引（如所有玩家都订自己的背包）请用点查询，并给索引声明 `point_sub=True`。
          `id` 不能声明 `point_sub`，按 id 订单行请用 `subscribe_get`。

        Point queries (`right` omitted or equal to `left`) on an index declared with
        `point_sub` only wake up when rows enter or leave that value. Range queries, and
        point queries on undeclared indexes (warned once), wake up on any write to the
        index. `id` cannot declare `point_sub`; watch a single row by id with
        `subscribe_get`.

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
        # 客户端拿到的是这些初始行：记下内容指纹，之后重读回来一样就不再推
        pushed = {int(row["id"]): row_fingerprint_(row) for row in rows}
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
        # （index_name 已由上面的 servant.range 校验过存在）。值频道只有声明了 point_sub 的
        # 索引才有（commit 只给它们发）：没声明的点查询退化为订整个索引的频道并警告一次。
        # id 不能声明 point_sub，点查 id 也退化并警告（该用 subscribe_get）
        point_value = BackendClient.point_query_value_(
            table_ref.comp_cls.dtype_map_[index_name], left, right
        )
        if point_value is not None and index_name in table_ref.comp_cls.point_subs_:
            index_channel = servant.index_value_channel(
                table_ref, index_name, point_value
            )
        else:
            if point_value is not None:
                warn_point_sub_fallback_(table_ref, index_name)
            index_channel = servant.index_channel(table_ref, index_name)
            point_value = None  # 订的是整个索引的频道，离开会由它通知
        row_ids = {int(row["id"]) for row in rows}
        idx_sub = IndexSubscription(
            table_ref,
            servant,
            ctx,
            index_channel,
            row_ids,
            {
                "index_name": index_name,
                "left": left,
                "right": right,
                "limit": limit,
                "desc": desc,
            },
            point_value,
        )
        # 索引频道 + 每行的行频道（行变更时才能收到消息）一次批量订阅
        row_channels = []
        for row_id in row_ids:
            row_channel = servant.row_channel(table_ref, row_id)
            row_channels.append(row_channel)
            idx_sub.add_row_subscriber(row_channel, row_id, pushed[row_id])
        await self._attach(sub_id, idx_sub)
        logger.debug(
            _("🆕 [📡Subscription] 订阅了索引: {sub_id} {index_channel}").format(
                sub_id=sub_id, index_channel=index_channel
            )
        )
        # 先读后订：读与订阅生效之间的写入不会有通知，生效前已在别的节点上应用、读到的副本
        # 却还没应用的写入也不会再有通知。隔一个 interval 补读一次：重跑范围比对、重读各行
        # （读回一样就不推）
        self._hub.reread_for(idx_sub, index_channel, *row_channels)

        return sub_id, rows

    async def subscribe_table(
        self,
        table_ref: TableReference,
        ctx: Context,
    ) -> tuple[str | None, list[dict]]:
        """
        获取并订阅整张表。与 `subscribe_range` 语义独立：只订阅一个表级频道，
        不管表有多少行都只占一个订阅，适合"行多、行小、很少变"的表（如所有玩家名字）。
        如果是重复订阅，会返回上一次订阅的sub_id，已有的订阅照旧。回复里的数据是这次重新
        读的（可能落在滞后的副本上），不代表订阅已推给客户端的内容：客户端应沿用已有的订阅
        对象、不要拿它覆盖本地数据（官方 SDK 就是这样）。客户端应该写代码防止重复订阅。

        订阅会观察表内任何行的添加/变化/删除，由get_updates调用时处理。
        代价是每个整表订阅者会收到该表**所有**写入的通知（服务端按RLS过滤后再推），
        所以高频写入的表请继续用 `subscribe_range`。

        组件必须声明 `table_sub=True`（`define_component` 的参数）：commit 只给声明了的组件
        发表频道，未声明的组件整表订阅会被拒绝（返回 None）。
        The component must be declared with `table_sub=True`; otherwise the subscription
        is rejected, since commits only publish table channels for declared components.

        Notes
        -----
        与 `subscribe_range` 不同，整表订阅对RLS权限的得失都会做出反应：
        - 当某行失去RLS权限时，**会**收到该行被删除的通知
        - 当某行获得RLS权限时，**会**收到该行被添加的通知

        Returns
        --------
        sub_id: str | None
            订阅id，后续通过该id获取更新。如果组件没有声明 `table_sub`、无整表权限，
            或表行数超过 `max_table_rows`，返回None。重复订阅时表已超过上限，同样返回
            None，并撤掉已有的订阅。
        rows: list[dict[str, Any]]
            caller可见的全部行数据。

        See Also
        --------
        subscribe_range : 范围订阅
        begin_subscribe_table : 拆成两段的写法（服务器的接收协程用它）
        """
        return await (await self.begin_subscribe_table(table_ref, ctx))

    async def begin_subscribe_table(
        self, table_ref: TableReference, ctx: Context
    ) -> Coroutine[Any, Any, tuple[str | None, list[dict]]]:
        """
        `subscribe_table` 的前半段：检查、订阅表级频道、登记（重复订阅也在这里认出来）。
        返回后半段的协程，await 它得到与 `subscribe_table` 一样的 (sub_id, rows)。
        后半段要先等一个 interval 再全量读（复制延迟预算），拆开是为了让服务器的接收协程
        登记完就去处理下一条消息，后半段交给后台任务。返回的协程必须 await 完或者取消，
        否则订阅一直停在初始化中。

        The first half of `subscribe_table`: checks, subscribing to the table channel and
        registering. Returns a coroutine for the second half (wait one interval, then read
        the whole table), so that the server's receiver can handle the next message
        meanwhile. The coroutine must be awaited or cancelled.
        """
        # 没声明 table_sub 的组件，commit 不发表频道：订上了也永远收不到通知
        if not table_ref.comp_cls.table_sub_:
            logger.warning(
                _(
                    "⚠️ [📡Subscription] {comp_name} 没有声明 table_sub，不允许整表订阅；"
                    "需要的话请在 define_component 里加 table_sub=True，caller：{caller}"
                ).format(comp_name=table_ref.comp_name, caller=ctx.caller)
            )
            return self._settled(None, [])
        # 首先caller要对整个表有权限
        if not self._has_table_permission(table_ref, ctx):
            logger.warning(
                _(
                    "⚠️ [📡Subscription] {comp_name}无调用权限，"
                    "检查是否非法调用，caller：{caller}"
                ).format(comp_name=table_ref.comp_name, caller=ctx.caller)
            )
            return self._settled(None, [])

        servant = self._backend.servant
        sub_id = f"{table_ref.comp_name}.table"
        if (existing := self._subs.get(sub_id)) is not None:
            logger.warning(
                _("⚠️ [📡Subscription] {sub_id} 数据重复订阅，检查客户端代码").format(
                    sub_id=sub_id
                )
            )
            return self._reread_table(table_ref, ctx, sub_id, existing)

        # 先订阅、一生效就登记（同 subscribe_get）。初始全量读完成之前弹出的通知由
        # TableSubscription 攒着（pending），读完再重新入队
        table_channel = servant.table_channel(table_ref)
        tbl_sub = TableSubscription(
            table_ref, servant, ctx, table_channel, self._max_table_rows
        )
        await self._attach(sub_id, tbl_sub)
        return self._finish_subscribe_table(ctx, sub_id, tbl_sub)

    @staticmethod
    async def _settled(
        sub_id: str | None, rows: list[dict]
    ) -> tuple[str | None, list[dict]]:
        """前半段就有结果时的后半段"""
        return sub_id, rows

    async def _reread_table(
        self,
        table_ref: TableReference,
        ctx: Context,
        sub_id: str,
        existing: BaseSubscription,
    ) -> tuple[str | None, list[dict]]:
        """重复整表订阅：已有的订阅照旧，只把当前可见的行再读一遍返回；表已超过行数
        上限时撤掉已有的订阅，返回 None"""
        rows = await self._read_whole_table(table_ref, ctx, self._backend.servant)
        if rows is None:
            # 回 None 客户端就认为没有订阅、不会再来 unsub（同 subscribe_get 重复订阅时
            # 行已不可见），旧订阅得跟着撤掉，不然它和表频道会挂到连接结束，还占着整表
            # 订阅数。读的这段时间里它可能已被退订、sub_id 又登记给了新的订阅，那个别去动
            if self._subs.get(sub_id) is existing:
                await self.unsubscribe(sub_id)
            return None, []
        for row in rows:
            del row["_version"]
        return sub_id, rows

    async def _finish_subscribe_table(
        self, ctx: Context, sub_id: str, tbl_sub: TableSubscription
    ) -> tuple[str | None, list[dict]]:
        """整表订阅的后半段：隔一个 interval 全量读，完成初始化"""
        try:
            # 整表重读太贵，不做补读：订阅生效后隔一个 interval 才全量读。生效前已在别的
            # 节点上应用的写入不会再有通知，隔这一下，读到的副本也已应用（复制延迟在预算内）
            await asyncio.sleep(self._hub.interval)
            rows = await self._read_whole_table(tbl_sub.table_ref, ctx, tbl_sub.servant)
        except BaseException:
            if self._subs.get(sub_id) is tbl_sub:
                await self.unsubscribe(sub_id)
            raise
        if self._subs.get(sub_id) is not tbl_sub:
            # 等的这段时间里被退订了（后半段在后台跑，接收协程照常处理 unsub）：
            # 这个 sub_id 可能已经重新登记给了新的订阅，别去动它
            return None, []
        if rows is None:  # 超过行数上限
            await self.unsubscribe(sub_id)
            return None, []

        # 初始化期间攒下的通知定向重新入队重读；初始读已经包含的（读回一样）不再推
        if pending := tbl_sub.finish_init_(rows):
            self._hub.reread_for(tbl_sub, tbl_sub.table_channel, payload=pending)
        for row in rows:
            del row["_version"]
        logger.debug(
            _("🆕 [📡Subscription] 订阅了整表: {sub_id} {table_channel}").format(
                sub_id=sub_id, table_channel=tbl_sub.table_channel
            )
        )
        return sub_id, rows

    async def _read_whole_table(
        self, table_ref: TableReference, ctx: Context, servant: BackendClient
    ) -> list[dict[str, Any]] | None:
        """全量读取 caller 可见的行（含 _version）；超过 max_table_rows 返回 None"""
        max_rows = self._max_table_rows
        rows, truncated = await read_whole_table_(servant, table_ref, max_rows)
        if truncated:
            logger.warning(
                _(
                    "⚠️ [📡Subscription] {comp_name}整表订阅行数超过限制"
                    "MAX_TABLE_SUBSCRIPTION_ROWS={max_rows}，拒绝订阅，caller：{caller}"
                ).format(
                    comp_name=table_ref.comp_name, max_rows=max_rows, caller=ctx.caller
                )
            )
            return None
        # 如果是rls权限，需要对每行数据进行权限判断
        if table_ref.comp_cls.is_rls():
            rows = [
                row for row in rows if self._has_row_permission(table_ref, ctx, row)
            ]
        return rows

    async def unsubscribe(self, sub_id) -> None:
        """取消该sub_id的订阅"""
        sub = self._subs.pop(sub_id, None)
        if sub is None:
            return
        self._sub_counts[type(sub)] -= 1
        self._channel_count -= self._channel_counts.pop(sub_id, 0)
        # 已交到待发区、还没被取走的更新不再推
        self._outbox.pop(sub_id, None)
        await self._hub.detach(self, sub)

    def deliver_(
        self,
        entries: Mapping[
            str, tuple[BaseSubscription, Mapping[int, dict[str, Any] | None]]
        ],
    ) -> None:
        """
        hub 在 tick 末尾调用：本 tick 暂存的更新并进待发区（按 sub_id / row_id，后到的覆盖先到
        的），唤醒 get_updates。退订了、或退订后同 id 重订成了别的订阅的丢掉
        """
        if self._closed:
            return
        outbox = self._outbox
        subs = self._subs
        delivered = False
        for sub_id, (sub, updates) in entries.items():
            if subs.get(sub_id) is not sub:
                continue
            outbox.setdefault(sub_id, {}).update(updates)
            delivered = True
        if delivered:
            self._arrived.set()

    def _has_updates(self) -> bool:
        return bool(self._outbox)

    async def get_updates(self, timeout=None) -> dict[str, dict[int, Any]]:
        """
        取走待发区里的数据更新：hub 处理本连接订阅的通知、重读数据库后，在 tick 末尾交到这里。
        返回值为dict: key是sub_id；value是更新的行数据，value格式为dict：key是row_id，value是
        数据库raw值（None 表示删除或不再可见）。
        timeout参数主要给单元测试用，None时堵塞到有更新，否则最多等待timeout秒（总时长），
        到时返回空dict。

        待发区只收到真有变化的更新（尾随重读、订阅生效后的补读读回的与客户端已有的一样，或变化
        的行对本连接不可见，都不会交来），所以不会拿到空结果。一次写入引起的推送也可能分在几批
        里：合并进队头的通知会在一个 interval 后尾随重读，它和别的频道的通知谁先弹出取决于时序。
        没来取的期间，几个 tick 的更新按 sub_id / row_id 合并，后到的覆盖先到的。

        hub 是手动模式（测试用）时，由这里弹出通知、跑 tick。

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
        hub = self._hub
        while not self._outbox:
            if hub.autostart:
                # clear 与 wait 之间没有 await，不会漏掉中间交来的更新
                self._arrived.clear()
                try:
                    async with asyncio.timeout_at(deadline):
                        await self._arrived.wait()
                except TimeoutError:
                    return {}
            elif not await hub.step_(deadline, self._has_updates):
                return {}
        updates, self._outbox = self._outbox, {}
        return updates
