"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import contextvars
import itertools
import logging
import time
import weakref
from collections import Counter
from collections.abc import Callable, Coroutine, Iterable, Mapping
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, NamedTuple, cast

import numpy as np

from hetu.data.backend import BackendClient, MQClient, RowFormat
from hetu.data.component import Permission
from hetu.i18n import _
from hetu.safelogging.filter import ContextFilter

if TYPE_CHECKING:
    from hetu.data.backend import Backend, TableReference
    from hetu.endpoint import Context

logger = logging.getLogger("HeTu.root")

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


def rls_ctx_(table_ref: TableReference, ctx: Context | None) -> Context | None:
    """订阅按哪个 ctx 判定行级权限：组件是 RLS 且 ctx 不是 admin 时是 ctx，否则 None（不用判定，
    可以在 worker 内与同一查询的订阅共享）"""
    if table_ref.comp_cls.is_rls() and ctx and not ctx.is_admin():
        return ctx
    return None


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
    # 以下几项由 SubscriptionHub 维护（open_ / _init / _release），比对逻辑不用管
    # 成员连接 → 该连接给这个订阅的 sub_id
    members: dict[SubscriptionBroker, str]
    # 等初始化结果的成员 → 它们的 future（见 SubscriptionHub.open_），初始化完成时清空
    waiters: dict[SubscriptionBroker, list[asyncio.Future[list[dict[str, Any]] | None]]]
    # hub 内唯一，定向补读用
    token: int = 0
    # 生效：tick 处理它的通知。行 / 范围订阅初始化完成才生效（之前弹出的通知由生效时的补读覆盖）；
    # 整表订阅订上频道就生效，初始化期间的通知攒进 pending
    active: bool = False
    # 初始化完成、订阅成立
    ready: bool = False
    # 最后一个成员离开时置真：处理到一半的 tick、初始化据此丢掉结果
    closed: bool = False
    # 接连读出错的次数：重试按它退避，读成功清零
    failures: int = 0
    # 初始化任务（hub 的后台任务），成员撤空时取消；完成后为 None
    init_task: asyncio.Task | None = None
    # 共享键：worker 内同一查询的连接共用这个订阅（见 SubscriptionHub.open_）。私有订阅为 None
    share_key: tuple | None = None
    # 共享订阅的快照：成员的客户端手里现在有什么 {row_id: 行}，初始化完成时由初始行填上，之后
    # 随暂存的更新改（见 SubscriptionHub._stage）；后加入的成员拿它做回复。私有订阅为 None。
    # 行对象与推送共用，之后都不再改
    snapshot: dict[int, dict[str, Any]] | None = None
    # 按回复顺序排好的快照（snapshot_rows_ 的缓存），快照或顺序变了就清掉（hub 与比对逻辑各管各的）
    snapshot_list: list[dict[str, Any]] | None = None
    # 有成员在等重读判定频道（SubscriptionHub.recheck_）时：序号大于它的 tick 里重读到才算数
    recheck_after: int = 0

    def use_servant_(self, servant: BackendClient) -> None:
        """之后的读改走这个副本（读出错时 hub 换一个随机副本重试）。没有自己副本的订阅什么也不做"""

    def recheck_channel_(self) -> str | None:
        """
        判定"有没有"要重读的频道（行订阅是行频道，范围订阅是索引频道）：成员按快照要回"没有"之前，
        等订阅重读一次它（见 `SubscriptionHub.recheck_`）。None 为不支持
        """
        return None

    def snapshot_rows_(self) -> list[dict[str, Any]]:
        """
        快照里的行，按回复的顺序排好（后加入的成员的回复）。排好的列表缓存到快照下次变动，后加入的
        成员共用这一份（只读）：整表订阅的快照可能有十万行，每个后加入者重排一遍太贵
        """
        rows = self.snapshot_list
        if rows is None:
            assert self.snapshot is not None, "私有订阅没有快照"
            rows = self.snapshot_list = self.order_snapshot_(self.snapshot)
        return rows

    def order_snapshot_(
        self, snapshot: dict[int, dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """快照按回复的顺序排成列表"""
        return list(snapshot.values())

    async def initialize_(self, hub: SubscriptionHub) -> list[dict[str, Any]] | None:
        """
        初始化：订上频道（`hub.attach_`）、读初始行，返回回复成员用的行（按本订阅可见、不含
        _version），None 表示订阅不成立（行不存在 / 不可见，整表超过上限）。hub 在自己的任务里
        调用，出错时换副本重试（再调一次，已订上的频道不会重复订）；返回后 hub 在同一个同步段里
        让订阅生效、把结果交给等着的成员。要补读的（生效前被跳过的通知等）在这里定向补读。
        默认只订上频道，没有初始行。
        """
        await hub.attach_(self)
        return []

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
        pushed: int | None = None,
    ):
        self.table_ref = table_ref
        self.servant = servant
        self.rls_ctx = rls_ctx_(table_ref, ctx)
        self.channel = channel
        self.row_id = row_id
        # 客户端当前持有的内容指纹（row_fingerprint_）：重读回来一样就不推。
        # 客户端没有这行（行不存在，或 RLS 不可见）时为 None
        self.pushed: int | None = pushed
        if RowSubscription.__cache.get(None) is None:
            RowSubscription.__cache.set({})

    def use_servant_(self, servant: BackendClient) -> None:
        self.servant = servant

    def recheck_channel_(self) -> str | None:
        return self.channel

    async def initialize_(self, hub: SubscriptionHub) -> list[dict[str, Any]] | None:
        """先订后读：读与订之间落下的写入，要么已经在读回的行里，要么随后有通知。先读后订的话，
        它既不在读回的行里、也不会有通知，客户端一直拿着旧行"""
        await hub.attach_(self)
        row = await self.servant.get(self.table_ref, self.row_id, RowFormat.TYPED_DICT)
        visible = self.decode_row_(row)
        if visible is None:
            return None  # 行不存在，或 caller 对该行无权限
        self.pushed = row_fingerprint_(row)
        # 订阅生效前已在别的节点上应用、这次读到的副本却还没应用的写入不会再有通知，初始化期间
        # 弹出的通知也被跳过了（订阅还没生效）：生效后隔一个 interval 补读一次（读回一样就不推）
        hub.reread_for(self, self.channel)
        return [visible]

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
        query_param: dict,
        point_value: np.generic | None = None,
    ):
        """建好时结果为空，由 `initialize_` 读初始结果"""
        self.table_ref = table_ref
        self.servant = servant
        self.rls_ctx = rls_ctx_(table_ref, ctx)
        # 点查询时是"索引=该值"的频道，区间查询时是整个索引的频道；收到就重跑 range 比对
        self.index_channel = index_channel
        self.query_param = query_param
        self.row_subs: dict[str, RowSubscription] = {}
        self.last_range_result: set[int] = set()
        # 最近一次读 range 的 id 顺序（索引顺序）：共享订阅按它排快照，后加入者的回复与首次
        # 订阅的回复一样按索引排。只改非索引字段的更新不影响顺序
        self.order: list[int] = []
        # 订的是值频道时为该值（已按 dtype 规范化），否则 None。值频道只在有行"进入"时才有
        # 通知，行"离开"（删除、字段改走）要靠结果里各行的行频道发现，见 get_updated
        self.point_value = point_value

    def use_servant_(self, servant: BackendClient) -> None:
        self.servant = servant
        for row_sub in self.row_subs.values():
            row_sub.servant = servant

    def recheck_channel_(self) -> str | None:
        return self.index_channel

    def order_snapshot_(
        self, snapshot: dict[int, dict[str, Any]]
    ) -> list[dict[str, Any]]:
        return [snapshot[row_id] for row_id in self.order if row_id in snapshot]

    async def initialize_(self, hub: SubscriptionHub) -> list[dict[str, Any]] | None:
        """先读后订：读 range 拿到初始行、填好比对状态，再订上索引频道与各行的行频道"""
        servant = self.servant
        ref = self.table_ref
        rows = cast(
            list[dict[str, Any]],
            await servant.range(
                ref, **self.query_param, row_format=RowFormat.TYPED_DICT
            ),
        )
        comp_cls = ref.comp_cls
        ctx = self.rls_ctx
        row_subs: dict[str, RowSubscription] = {}
        visible: list[dict[str, Any]] = []
        for row in rows:
            # 不可见（RLS）的行先不订：生效后的补读重跑范围比对时会把它们订上（只订不推）
            if ctx is not None and not ctx.rls_check(comp_cls, row):
                continue
            row_id = int(row["id"])
            channel = servant.row_channel(ref, row_id)
            # 客户端拿到的是这些初始行：记下内容指纹，之后重读回来一样就不再推
            row_subs[channel] = RowSubscription(
                ref, servant, ctx, channel, row_id, row_fingerprint_(row)
            )
            del row["_version"]
            visible.append(row)
        self.row_subs = row_subs
        self.last_range_result = {row_sub.row_id for row_sub in row_subs.values()}
        self.order = [row_sub.row_id for row_sub in row_subs.values()]
        # 索引频道 + 每行的行频道（行变更时才能收到消息）一次批量订阅
        await hub.attach_(self)
        # 读与订阅生效之间的写入不会有通知，生效前已在别的节点上应用、读到的副本却还没应用的
        # 写入也不会再有通知，初始化期间弹出的通知也被跳过了（订阅还没生效）：生效后隔一个
        # interval 补读一次，重跑范围比对、重读各行（读回一样就不推）
        hub.reread_for(self, *self.channels)
        return visible

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
        order = cast(
            list[int],
            await servant.range(ref, **self.query_param, row_format=RowFormat.ID_LIST),
        )
        row_ids = set(order)
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
        # 新行都判完（RLS 判定可能出错）才改状态：中途出错时 hub 会定向重读，上次的结果原样
        # 留着，重读才能再算出同样的进出
        new_subs: dict[str, RowSubscription] = {}
        for row_id, row in zip(inserts, rows):
            if row is None:
                row_ids.discard(row_id)
                continue  # 可能是刚添加就删了
            new_chan_name = servant.row_channel(ref, row_id)
            row_sub = RowSubscription(ref, servant, self.rls_ctx, new_chan_name, row_id)
            # 不可见（RLS）的行也要订阅，等它变得可见时才能通知；但现在不推给客户端
            visible = row_sub.decode_row_(row)
            row_sub.pushed = None if visible is None else row_fingerprint_(row)
            new_subs[new_chan_name] = row_sub
            if visible is not None:
                rtn[row_id] = visible
        self.last_range_result = row_ids
        self.order = [row_id for row_id in order if row_id in row_ids]
        self.snapshot_list = None  # 顺序可能变了（只改了索引字段的行，快照内容还没变）
        self.row_subs.update(new_subs)
        new_chans.update(new_subs)
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
        """建好时处于初始化中，由 `initialize_` 全量读、完成初始化"""
        self.table_ref = table_ref
        self.servant = servant
        self.rls_ctx = rls_ctx_(table_ref, ctx)
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

    def use_servant_(self, servant: BackendClient) -> None:
        self.servant = servant

    def order_snapshot_(
        self, snapshot: dict[int, dict[str, Any]]
    ) -> list[dict[str, Any]]:
        return [snapshot[row_id] for row_id in sorted(snapshot)]  # 同全量读，按 id 升序

    async def initialize_(self, hub: SubscriptionHub) -> list[dict[str, Any]] | None:
        """
        先订阅、订上就生效：初始全量读完成之前弹出的通知攒进 pending（见 get_updated），读完再
        定向重读。整表重读太贵，不做补读：订阅生效后隔一个 interval 才全量读，生效前已在别的节点
        上应用的写入不会再有通知，隔这一下，读到的副本也已应用（复制延迟在预算内）
        """
        await hub.attach_(self, active=True)
        await asyncio.sleep(hub.interval)
        ref = self.table_ref
        rows, truncated = await read_whole_table_(self.servant, ref, self.max_rows)
        if truncated:
            logger.warning(
                _(
                    "⚠️ [📡Subscription] {comp_name}整表订阅行数超过限制"
                    "MAX_TABLE_SUBSCRIPTION_ROWS={max_rows}，拒绝订阅"
                ).format(comp_name=ref.comp_name, max_rows=self.max_rows)
            )
            return None
        ctx = self.rls_ctx
        if ctx is not None:
            rows = [row for row in rows if ctx.rls_check(ref.comp_cls, row)]
        # 初始化期间攒下的通知定向重新入队重读；初始读已经包含的（读回一样）不再推
        if pending := self.finish_init_(rows):
            hub.reread_for(self, self.table_channel, payload=pending)
        for row in rows:
            del row["_version"]
        return rows

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
        # 整批都判完（RLS 判定可能出错）才改状态：中途出错时 hub 会定向重读这批，重读才能再算出
        # 同样的推送（先从 known_ids 撤掉的删除，重读时它的 None 就推不出去了）
        read_now: dict[int, int | None] = {}
        rtn: dict[int, dict[str, Any] | None] = {}
        for row_id, row in zip(ids, rows):
            if row is not None and (ctx is None or ctx.rls_check(comp_cls, row)):
                fingerprint = read_now[row_id] = row_fingerprint_(row)
                if row_id in known and last_read.get(row_id) == fingerprint:
                    continue  # 上一批刚推过一模一样的（尾随重读）
                del row["_version"]
                rtn[row_id] = row
            elif row_id in known:
                # 被删除，或失去RLS权限：客户端持有该行，需要通知删除
                rtn[row_id] = None
            # 既不可见、客户端也从未持有的行：不推
        self.last_read = read_now
        for row_id, row in rtn.items():
            if row is None:
                known.discard(row_id)
            else:
                known.add(row_id)
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
        # 同 get_updated：都判完才改 known_ids，中途出错重试能再算出同样的推送
        for row in rows:
            row_id = int(row["id"])
            seen.add(row_id)
            if ctx is None or ctx.rls_check(comp_cls, row):
                del row["_version"]
                rtn[row_id] = row
            elif row_id in known:
                rtn[row_id] = None
        # 超过上限时后面还有行没读到（按 id 升序），只对读到的 id 范围内的已知行判删除
        bound = max(seen) if seen and truncated else None
        for row_id in known - seen:
            if bound is None or row_id <= bound:
                rtn[row_id] = None
        for row_id, row in rtn.items():
            if row is None:
                known.discard(row_id)
            else:
                known.add(row_id)
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
# 同一个订阅接连读出错时重试间隔的上限（秒）：按 1、2、4… 个 interval 退避
_RETRY_BACKOFF_MAX = 5.0
# 后台处理循环因 bug 意外结束后，隔多少秒重新拉起
_RUN_RESTART_DELAY = 1.0
# hub 处理循环的日志上下文（同连接的 [连接id|地址|用户] 格式）
_HUB_LOG_CONTEXT = "[None|None|SubscriptionHub]"


class _Tick:
    """一个 tick 的簿记"""

    __slots__ = ("added", "joined", "released", "seq", "since", "staged")

    def __init__(self, seq: int, since: int) -> None:
        # 本 tick 的序号（hub 里从 1 递增）：等重读的成员按它认读是不是在请求之后（见 recheck_）
        self.seq = seq
        # tick 开始（本 tick 的任何读之前）时 hub 的生效序号：之后才生效的频道，订阅读它可能在
        # 生效之前，要补读（见 _subscribe_added）
        self.since = since
        # 本 tick 新增的频道 → 新增它的订阅（新订上的频道要给它们定向补读）
        self.added: dict[str, set[BaseSubscription]] = {}
        # 本 tick 没人要了的频道
        self.released: set[str] = set()
        # 暂存的更新：订阅 → 本 tick 合并后的更新（后到的覆盖先到的）。tick 末尾交给那时的各成员，
        # 成员共用这一份（见 SubscriptionHub._deliver）
        self.staged: dict[BaseSubscription, dict[int, dict[str, Any] | None]] = {}
        # tick 中途加入、加入时订阅已有暂存的成员：(订阅, 连接) → 加入那一刻的暂存（拷贝）。它的
        # 快照已含这些，交付时只给它加入之后变了的
        self.joined: dict[
            tuple[BaseSubscription, SubscriptionBroker],
            dict[int, dict[str, Any] | None],
        ] = {}


class _ErrorKind:
    """一类错误（说明模板 + 异常类型）的日志限流状态"""

    __slots__ = ("last_seen", "logged_at", "muted", "timer", "what")

    def __init__(self, what: str) -> None:
        # 最近一次记日志时填好占位符的说明，恢复日志用
        self.what = what
        self.logged_at = float("-inf")
        self.last_seen = float("-inf")
        # 最近一次记日志之后压下没记的次数
        self.muted = 0
        # 检查它是否已静默（该补恢复日志）的定时器
        self.timer: asyncio.TimerHandle | None = None


class _Subscribing:
    """一次在途的 MQClient.subscribe：hub 里同一频道同时只有一次，后来要这个频道的等它的结果"""

    __slots__ = ("channels", "done")

    def __init__(self, channels: list[str]) -> None:
        self.channels = channels
        self.done: asyncio.Future[None] = asyncio.get_running_loop().create_future()


class SubscriptionHub:
    """
    worker 级订阅器：每个 worker（进程）的每个 backend 一个，worker 内所有连接的订阅都在这里处理。
    持有唯一的 MQClient（本地队列：合批、尾随重读，见 `MQClient`）和一个处理循环：弹出一批通知 →
    按表批量预读行 → 各订阅 `get_updated`（订阅之间并发）→ 记账 → 按订阅暂存更新；tick 末尾统一
    订阅 / 退订频道，再把各订阅的更新交给它这时的成员（各连接的门面 `SubscriptionBroker`）。

    一个 worker 一个队列、一条时间线，尾随重读与补读的保证与每连接一个队列时相同。订阅层的补读
    定向到单个订阅（`reread_for`），不按频道重跑 worker 里所有订阅。通知接连不断时相邻 tick 之间
    留一个合批窗口（`TICK_SPACING_INTERVALS`）。
    不按 RLS 判定可见的订阅按查询共享：同一查询在 worker 里只有一个订阅对象，订了它的连接都是
    成员，每条通知的读与比对只做一次；后加入的成员拿 hub 维护的快照做回复。订阅在 hub 自己的
    任务里初始化，成员先登记、再等初始化结果。设计见
    docs/superpowers/specs/2026-09-28-worker-subscriptions-design.md、
    docs/superpowers/specs/2026-09-29-shared-subscriptions-design.md。

    The worker-level subscription engine, one per backend per worker process. It owns the only
    MQClient (local queue with batching and trailing re-reads) and one processing loop that
    serves the subscriptions of every connection, handing the updates to each connection's
    `SubscriptionBroker` at the end of a tick. Under a continuous stream of notifications,
    consecutive ticks are spaced by `TICK_SPACING_INTERVALS` so that they batch up.
    Subscriptions not filtered by row-level security are shared per query: one subscription
    object per query per worker, joined by every connection that subscribes to it.
    """

    # tick 末尾等新增频道的 SUBSCRIBE 回来最多等几个 interval，之后照常交付：推给客户端的新行尽量
    # 在它的行频道订阅生效之后，但 ack 迟迟不来（比如某个节点的 pubsub 连接半开）时不能冻住整个
    # worker 的推送。晚回来的由订阅任务自己补读 / 补订（见 _subscribe_added）
    SUBSCRIBE_WAIT_INTERVALS: float = 1
    # 合批窗口：相邻两个 tick 的开始至少隔这么多个 interval（设计稿 §4.8）。MQClient 只弹出已满一个
    # interval 的通知，通知接连不断时每次只弹出刚满期的一两条；弹一批跑一个 tick 的话，建任务、等订阅
    # 回执、预读往返这些每 tick 的固定开销会被放大到每秒上千次（实测私有订阅反而比每连接一个队列更费
    # CPU）。隔一小段，这期间满期的通知攒成一批，代价是延迟最多多这么一段。零星的通知不受影响（上一个
    # tick 早已过去，满期就处理）；tick 本身已经超过窗口时也不再多等。手动模式（测试）不等。
    # 窗口越大批越大、越省 CPU，但一个 tick 里同时存活的对象越多，高负载时更容易触发全量 GC、事件循环
    # 一口气处理得更久（RPC 尾延迟上升）：实测 5ms 已拿到大部分收益，10ms 起尾延迟明显变差
    TICK_SPACING_INTERVALS: float = 0.05
    # 订阅初始化（SUBSCRIBE、读初始行）出错时重试几次，间隔按 1、2、4… 个 interval 退避、每次换一个
    # 副本；默认 3 次共约 0.7 秒，还不行才让等着的连接失败（断开）
    INIT_RETRIES: int = 3

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
        self._mq = backend.get_mq_client()
        self._autostart = autostart
        # 频道 → 订了它的订阅（含 attach 中还没生效的占位）
        self._channel_subs: dict[str, set[BaseSubscription]] = {}
        # token → 订阅，定向补读用
        self._by_token: dict[int, BaseSubscription] = {}
        self._tokens = itertools.count(1)
        # SUBSCRIBE 已经回来的频道 → 生效序号（每次有频道生效加一）。MQClient.subscribed 在
        # SUBSCRIBE 发出时就记上，不能用来判断是否已生效（见 _repair、_subscribe_added 的 fresh）
        self._effective: dict[str, int] = {}
        self._effective_seq = 0
        # 正在订阅的频道 → 那一次在途的 subscribe（见 _subscribe）
        self._inflight: dict[str, _Subscribing] = {}
        # 共享登记表：共享键 → 同一查询的订阅（设计稿 2026-09-29 §3.1）。私有订阅不进表；订阅
        # 关闭、不成立、初始化失败时撤掉
        self._shared: dict[tuple, BaseSubscription] = {}
        # 成员的推送全都卡住的订阅先攒着不读的通知：订阅 → {频道: payload}；有成员取走待发区、
        # 或有新成员加入时重读（见 _collect、resume_、join_）
        self._parked: dict[BaseSubscription, dict[str, set[str] | None]] = {}
        # 同上，按成员连接索引：连接 → 它在的、攒着通知的订阅。连接取走待发区时只看自己的这几个
        # （resume_），不用扫它的全部订阅（一个连接可能订着上千个）
        self._parked_by: dict[SubscriptionBroker, set[BaseSubscription]] = {}
        # 后台任务（频道的订阅、退订、补订）：不随调用方取消，close 时统一取消
        self._tasks: set[asyncio.Task] = set()
        self._task: asyncio.Task | None = None
        # 正在跑的 tick（从建起到交付完）：tick 中途加入的成员要按它的暂存记下加入时已有的（见 join_）
        self._staging: _Tick | None = None
        # 已开始的 tick 数（最近一个 tick 的序号）
        self._ticks = 0
        # 手动模式：几个门面并发驱动时一次只跑一个 tick
        self._step_lock = asyncio.Lock()
        # 错误日志按类别限流：(说明模板, 异常类型) → 状态，见 _log_error
        self._errors: dict[tuple[str, type[BaseException]], _ErrorKind] = {}
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

    def shared_(self, key: tuple | None) -> BaseSubscription | None:
        """共享键对应的订阅（worker 里同一查询的那个）；没有、或 key 为 None（私有）时为 None"""
        return None if key is None else self._shared.get(key)

    def open_(
        self,
        sub: BaseSubscription,
        broker: SubscriptionBroker,
        sub_id: str,
        key: tuple | None = None,
    ) -> asyncio.Future[list[dict[str, Any]] | None]:
        """
        登记新建的订阅 sub 与它的第一个成员，在 hub 自己的任务里初始化（`sub.initialize_`：订上
        频道、读初始行，出错换副本重试，见 `_init`），不挂在任何连接上。key 不为 None 时登记为该
        查询的共享订阅，之后同一查询的连接加入它（`join_`）。返回该成员等初始化结果的 future，
        见 `wait_`。
        成员当场登记（设计稿 2026-09-29 §3.3）：订阅在初始化完成之前不产生任何推送（行 / 范围订阅
        还没生效，整表订阅只攒 pending），之后的推送都排在成员拿到结果之后。
        """
        if self._closed:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        if self._autostart:
            self._start()  # 处理循环意外结束过的话重新拉起
        sub.members = {broker: sub_id}
        sub.waiters = {}
        sub.token = next(self._tokens)
        sub.active = sub.ready = sub.closed = False
        sub.share_key = key
        sub.snapshot = sub.snapshot_list = None
        self._by_token[sub.token] = sub
        if key is not None:
            self._shared[key] = sub
        # 新的 Context：不带着第一个成员连接的日志身份与 Request（同 _start）
        sub.init_task = self._spawn(self._init(sub), context=contextvars.Context())
        return self.wait_(sub, broker)

    def join_(
        self, sub: BaseSubscription, broker: SubscriptionBroker, sub_id: str
    ) -> asyncio.Future[list[dict[str, Any]] | None]:
        """
        broker 加入 worker 里已有的共享订阅 sub（同一查询），返回等初始化结果的 future，见 `wait_`。
        已就绪的当场拿快照：之后暂存的更新都会交给它，快照已含之前的（设计稿 2026-09-29 §3.2）。
        tick 中途加入、这个 tick 已给订阅暂存了更新的，记下已暂存的：交付时只给它之后变了的
        """
        if self._autostart:
            self._start()  # 处理循环意外结束过的话重新拉起：热门查询都是加入，不能只靠新建
        sub.members[broker] = sub_id
        tick = self._staging
        if tick is not None and (staged := tick.staged.get(sub)):
            tick.joined[sub, broker] = dict(staged)
        # 新成员的推送没卡着：成员全都卡着时攒下的通知现在重读，不然要等哪个老成员取走待发区，
        # 新成员拿到的快照才会动
        self._unpark(sub)
        return self.wait_(sub, broker)

    def wait_(
        self, sub: BaseSubscription, broker: SubscriptionBroker
    ) -> asyncio.Future[list[dict[str, Any]] | None]:
        """
        broker 等 sub 的初始化结果：回复用的行（已就绪的共享订阅当场给快照；私有订阅不维护快照，
        已就绪时给空列表，只表示订阅成立），None 表示订阅不成立；重试用尽时是那次的异常。broker 在
        初始化完成前离开、订阅已关闭时为 None；hub 已关闭时是 ConnectionError。
        初始化已经有了结果的当场给：之后没人会再交代等着的（重复订阅的后半段可能这时才开始跑）
        """
        waiter: asyncio.Future[list[dict[str, Any]] | None] = (
            asyncio.get_running_loop().create_future()
        )
        if self._closed:
            waiter.set_exception(ConnectionError(_("连接已关闭，已调用过close")))
        elif sub.closed:
            waiter.set_result(None)
        elif sub.ready:
            waiter.set_result([] if sub.snapshot is None else sub.snapshot_rows_())
        elif sub.init_task is None:
            # 初始化已经结束、订阅没成立（行不存在 / 超过上限，或重试用尽）：成员们各自在退订
            waiter.set_result(None)
        else:
            sub.waiters.setdefault(broker, []).append(waiter)
        return waiter

    def recheck_(
        self, sub: BaseSubscription, broker: SubscriptionBroker
    ) -> asyncio.Future[list[dict[str, Any]] | None]:
        """
        broker（已就绪的共享订阅 sub 的成员）要按快照回"没有"（行不存在、范围为空）之前：等 sub 重读
        一次判定频道（`recheck_channel_`），再给那之后的快照。快照落后于提交至少一个 interval（通知
        隔一个 interval 才读），成员的推送全都卡着时一直不动：据它回"没有"的话，随后才推来的行这个
        连接再也收不到（它已不是成员）。本调用之后才开始的 tick 里重读到才算数，与 dev 订阅时读库
        一样新。为此定向补读一次；等的期间订阅不算卡住（见 `_stalled`）。
        订阅已关闭、没有快照（私有）或不支持时回 None 或当前快照，hub 已关闭时是 ConnectionError。
        broker 等的期间离开时为 None
        """
        waiter: asyncio.Future[list[dict[str, Any]] | None] = (
            asyncio.get_running_loop().create_future()
        )
        channel = sub.recheck_channel_()
        if self._closed:
            waiter.set_exception(ConnectionError(_("连接已关闭，已调用过close")))
        elif sub.closed or not sub.ready or sub.snapshot is None:
            waiter.set_result(None)
        elif channel is None:
            waiter.set_result(sub.snapshot_rows_())
        else:
            if self._autostart:
                self._start()
            sub.waiters.setdefault(broker, []).append(waiter)
            sub.recheck_after = self._ticks
            self.reread_for(sub, channel)
        return waiter

    def _rechecked(self, sub: BaseSubscription) -> None:
        """sub 在请求之后重读到了判定频道：等着的成员拿这时的快照（已含本 tick 暂存的）"""
        waiters, sub.waiters = sub.waiters, {}
        rows = sub.snapshot_rows_()
        for futures in waiters.values():
            for waiter in futures:
                if not waiter.done():
                    waiter.set_result(rows)

    def _unshare(self, sub: BaseSubscription) -> None:
        """从共享登记表撤掉 sub（表里登记的还是它的话）：之后同一查询新建订阅"""
        key = sub.share_key
        if key is not None and self._shared.get(key) is sub:
            del self._shared[key]

    async def attach_(self, sub: BaseSubscription, active: bool = False) -> None:
        """
        订阅 sub 的频道（`sub.channels`），返回时都已生效；由 `sub.initialize_` 调用，可以重复调。

        先占位再订阅：期间 tick / 别的 detach 定退订名单时看得到占位，不会把这些频道退掉。行 / 范围
        订阅这时还不生效（tick 不处理它），初始化完成时由 hub 置 `active`；active=True（整表订阅）
        订上就生效。对 MQClient 的 subscribe 在 hub 自己的任务里跑（见 `_subscribe`）：初始化被取消
        （成员撤空）时订阅照常完成，不会把别的订阅正等着的同一个 SUBSCRIBE 撤掉。失败或被取消时撤掉
        占位再抛出（初始化会重试）。
        """
        channels = list(sub.channels)
        channel_subs = self._channel_subs
        for channel in channels:
            channel_subs.setdefault(channel, set()).add(sub)
        try:
            await self._subscribe(channels)
        except BaseException:
            gone = self._drop_channels(sub, channels)
            if gone and not self._closed:
                self._spawn(self._unsubscribe(gone))
            raise
        if active and not sub.closed:
            sub.active = True

    async def detach(self, broker: SubscriptionBroker, *subs: BaseSubscription) -> None:
        """
        撤掉 broker 在这些订阅上的成员身份：成员撤空的订阅关闭（`closed`）、撤登记、取消还没完成
        的初始化，退订 worker 里没人再要的频道（等退订回来才返回）
        """
        gone = self._release(broker, subs)
        if gone and not self._closed:
            await self._unsubscribe(gone)

    def _release(
        self, broker: SubscriptionBroker, subs: Iterable[BaseSubscription]
    ) -> list[str]:
        """同步撤掉成员身份与登记，返回 worker 里因此没人要的频道"""
        gone: list[str] = []
        for sub in subs:
            sub.members.pop(broker, None)
            if sub in self._parked:
                self._unindex_parked(broker, sub)
            # 还在等初始化结果（或重读判定频道）的：不用等了，它的后半段回 None
            for waiter in sub.waiters.pop(broker, ()):
                if not waiter.done():
                    waiter.set_result(None)
            if sub.members or sub.closed:
                continue
            sub.closed = True
            self._by_token.pop(sub.token, None)
            self._unshare(sub)
            self._parked.pop(sub, None)
            task, sub.init_task = sub.init_task, None
            if task is not None:
                task.cancel()
            # tick 处理它的途中退订的话，sub.channels 可能已含 tick 还没登记的新行，也可能少了
            # tick 还没撤掉的旧行：前者不在频道表里，跳过；后者由 tick 回来后撤（见 _process）
            gone.extend(self._drop_channels(sub, sub.channels))
        return gone

    def _drop_channels(
        self, sub: BaseSubscription, channels: Iterable[str]
    ) -> list[str]:
        """从频道表撤掉 sub 在这些频道上的登记（没登记的跳过），返回因此没人要的频道"""
        channel_subs = self._channel_subs
        gone: list[str] = []
        for channel in channels:
            subs_on = channel_subs.get(channel)
            if subs_on is None or sub not in subs_on:
                continue
            subs_on.discard(sub)
            if not subs_on:
                del channel_subs[channel]
                gone.append(channel)
        return gone

    async def _subscribe(self, channels: Iterable[str]) -> None:
        """
        订阅频道，返回时都已生效（记进 `_effective`），失败抛出。

        worker 里所有调用方（各连接的 attach、tick 末尾、补订）共用一个 MQClient，而 MQClient 按
        客户端登记、不按调用方：对同一频道重叠的两次 subscribe，一次失败回滚会把另一次搭车订上、
        已经返回成功的频道一并撤掉。所以同一频道同时只发一次：已生效的跳过，正在订的等那一次的
        结果（它失败，等它的都失败），其余合成一次发出。发送放在 hub 自己的任务里，调用方被取消
        也照常跑完，等它的人不受牵连
        """
        waits: set[asyncio.Future[None]] = set()
        own: list[str] = []
        for channel in channels:
            if channel in self._effective:
                continue
            ticket = self._inflight.get(channel)
            if ticket is None:
                own.append(channel)
            else:
                waits.add(ticket.done)
        if own:
            ticket = _Subscribing(own)
            for channel in own:
                self._inflight[channel] = ticket
            self._spawn(self._send_subscribe(ticket))
            waits.add(ticket.done)
        if waits:
            # asyncio.wait 只旁观：本调用方被取消不会取消共享的 future
            done, _pending = await asyncio.wait(waits)
            for fut in done:
                fut.result()

    async def _send_subscribe(self, ticket: _Subscribing) -> None:
        """发一次 MQClient.subscribe（hub 的后台任务），回来后把仍订着的记为已生效，结果交给等它的人"""
        mq = self._mq
        try:
            await mq.subscribe(*ticket.channels)
        except BaseException as e:
            for channel in ticket.channels:
                if self._inflight.get(channel) is ticket:
                    del self._inflight[channel]
            # 共享的 future 里不放 CancelledError（只会来自 close），免得等它的人以为是自己被取消
            exc = (
                ConnectionError(_("连接已关闭，已调用过close"))
                if isinstance(e, asyncio.CancelledError)
                else e
            )
            ticket.done.set_exception(exc)
            ticket.done.exception()  # 没人等的话别在 gc 时报 "never retrieved"
            raise
        subscribed = mq.subscribed_channels
        self._effective_seq += 1
        for channel in ticket.channels:
            # 等 ack 期间被退订了的（见 _unsubscribe）不算：它回来的 SUBSCRIBE 已经作废
            if self._inflight.get(channel) is ticket:
                del self._inflight[channel]
                if channel in subscribed:
                    self._effective[channel] = self._effective_seq
        ticket.done.set_result(None)

    async def _unsubscribe(self, channels: list[str]) -> None:
        """退订频道。排队到真正跑起来之间可能又有人订了（含 attach 的占位），那就不能退"""
        channels = [ch for ch in channels if ch not in self._channel_subs]
        if not channels:
            return
        for channel in channels:
            self._effective.pop(channel, None)
            # 在途的订阅作废：它回来也不算生效，之后再要这个频道的另发一次 SUBSCRIBE
            self._inflight.pop(channel, None)
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

    # === === === 初始化 === === ===

    async def _init(self, sub: BaseSubscription) -> None:
        """
        初始化一个订阅（hub 的后台任务，见 `open_`），出错时有限重试（`_initialize`）。成员撤空时
        本任务被取消。
        不论怎么结束，等着的成员都要有交代：不然它们一直等（服务器里发送循环卡在占位上，之后的回复、
        推送全堵住），同一查询之后的订阅者也都加入这个死订阅
        """
        ContextFilter.set_log_context(_HUB_LOG_CONTEXT)
        try:
            rows = await self._initialize(sub)
        except Exception as e:  # noqa: BLE001 重试用尽，交给等着的成员
            sub.init_task = None
            self._fail_init(sub, e)
            return
        except BaseException:
            # 本任务被取消：成员撤空（_release）、hub 关闭（close）时已经交代过。别的（事件循环关闭时
            # 的取消、KeyboardInterrupt 等）也不能让等着的成员一直等
            sub.init_task = None
            if not (sub.closed or self._closed):
                interrupted = ConnectionError(_("订阅的初始化被中断"))
                self._fail_init(sub, interrupted, log=False)
            raise
        sub.init_task = None
        self._ready(sub, rows)

    async def _initialize(self, sub: BaseSubscription) -> list[dict[str, Any]] | None:
        """
        带重试的 `sub.initialize_`：出错（副本挂了、Redis 抖动）时换一个随机副本，按 1、2、4… 个
        interval 退避重试，共 INIT_RETRIES 次，还不行抛出最后那次的错误（设计稿 2026-09-29 §3.4）：
        一次抖动不至于让一批连接一起断开重连，Redis 长时间不可用时回复也不会一直不来（排在它后面的
        回复都在等它）。后端调用里漏出来的 CancelledError（本任务并没有被取消）也当作出错
        """
        task = asyncio.current_task()
        attempt = 0
        while True:
            try:
                return await sub.initialize_(self)
            except asyncio.CancelledError as e:
                if task is None or task.cancelling():
                    raise  # 本任务被取消
                err: Exception = RuntimeError(
                    _("后端调用意外抛出 CancelledError（订阅的初始化并没有被取消）")
                )
                err.__cause__ = e
            except Exception as e:  # noqa: BLE001 换副本重试，用尽了抛给 _init
                err = e
            if attempt >= self.INIT_RETRIES:
                raise err
            self._log_error(
                _("初始化订阅 {sub_id} 出错，换副本稍后重试"),
                err,
                requeued=False,
                sub_id=next(iter(sub.members.values()), None),
            )
            sub.use_servant_(self._backend.servant)
            await asyncio.sleep(self.interval * 2**attempt)
            attempt += 1

    def _ready(self, sub: BaseSubscription, rows: list[dict[str, Any]] | None) -> None:
        """
        初始化完成，与 initialize_ 返回在同一个同步段里：订阅生效，结果交给等着的成员（共享订阅
        以它填上快照，交出去的就是快照）。之后暂存的更新都交给成员表里的连接，与交出去的结果
        衔接得上。
        订阅不成立（rows 为 None）的不生效、撤掉共享登记，等着的成员都回 None，由它们各自退订
        （等退订回来才返回，同以前）
        """
        if sub.closed:
            return
        waiters, sub.waiters = sub.waiters, {}
        if rows is None:
            sub.active = False
            self._unshare(sub)
        else:
            sub.active = sub.ready = True
            if sub.share_key is not None:
                sub.snapshot = {int(row["id"]): row for row in rows}
                sub.snapshot_list = None
        for futures in waiters.values():
            for waiter in futures:
                if not waiter.done():
                    waiter.set_result(rows)

    def _fail_init(
        self, sub: BaseSubscription, exc: Exception, log: bool = True
    ) -> None:
        """
        初始化失败（重试用尽，log 为真时记日志；或被中断）：订阅不生效、撤掉共享登记，等着的成员都
        拿到这个异常，由它们各自退订
        """
        if log:
            self._log_error(
                _("初始化订阅 {sub_id} 出错，重试 {retries} 次仍失败"),
                exc,
                requeued=False,
                sub_id=next(iter(sub.members.values()), None),
                retries=self.INIT_RETRIES,
            )
        sub.active = False
        self._unshare(sub)
        waiters, sub.waiters = sub.waiters, {}
        for futures in waiters.values():
            for waiter in futures:
                if not waiter.done():
                    waiter.set_exception(exc)
                    waiter.exception()  # 等它的人被取消了的话，别在 gc 时报 "never retrieved"

    # === === === 处理循环 === === ===

    def _start(self) -> None:
        """
        起后台处理循环；它意外结束了的话再起一个。
        用一个全新的 contextvars.Context：hub 是在某个连接的协程里懒建 / 重新拉起的，复制那个连接
        的 context 的话，此后整个 worker 的订阅日志都带着它的身份（id / IP），它的 Request 也一直被
        引用着释放不掉
        """
        if self._task is None or self._task.done():
            ctx = contextvars.Context()
            self._task = asyncio.create_task(
                self._run(), name="SubscriptionHub", context=ctx
            )
            self._task.add_done_callback(self._on_run_done, context=ctx)

    def _on_run_done(self, task: asyncio.Task) -> None:
        if self._closed or task is not self._task:
            return
        if task.cancelled():
            logger.warning(
                _("⚠️ [📡Subscription] 订阅处理循环被取消，下次订阅时重新拉起")
            )
        elif (exc := task.exception()) is not None:
            # 因 bug 结束：过一会儿自己拉起，不能等到下一次有连接订阅（只在登录时订阅的玩法里
            # 可能要停很久）。隔一会儿，别在出错的地方原地打转
            logger.error(
                _(
                    "❌ [📡Subscription] 订阅处理循环异常结束，{delay} 秒后重新拉起"
                ).format(delay=_RUN_RESTART_DELAY),
                exc_info=exc,
            )
            asyncio.get_running_loop().call_later(_RUN_RESTART_DELAY, self._restart)

    def _restart(self) -> None:
        if not self._closed and self._autostart:
            self._start()

    async def _run(self) -> None:
        # hub 自己的日志标签（本任务的 Context 是 _start 新建的，不影响任何连接）
        ContextFilter.set_log_context(_HUB_LOG_CONTEXT)
        mq = self._mq
        loop = asyncio.get_running_loop()
        while True:
            batch = await mq.get_message()
            started = loop.time()
            try:
                await self._tick(batch)
            # 读错误在 tick 里都兜住并重试了；这里兜 bug，处理循环不能停。这批通知已经弹出，没处理
            # 完的就丢了
            except Exception as e:  # noqa: BLE001
                self._log_error(
                    _("处理订阅通知异常（bug），这批通知可能没处理完"),
                    e,
                    requeued=False,
                )
            # 合批窗口（TICK_SPACING_INTERVALS）从本 tick 开始时算：tick 本身已经超过窗口就不再等
            rest = started + self.TICK_SPACING_INTERVALS * self.interval - loop.time()
            if rest > 0:
                await asyncio.sleep(rest)

    def _log_error(
        self,
        what: str,
        exc: BaseException,
        requeued: bool = True,
        **fields: Any,
    ) -> None:
        """
        tick 里出错：记错误日志。what 是（已翻译的）说明模板，fields 填它的占位符。requeued：出错
        的读已重新入队，稍后重试（设计稿 §6）。
        按类别（说明模板 + 异常类型）限流：每类每 _ERROR_LOG_INTERVAL 秒最多一条，带上此前压下的
        次数；一类错误第一次出现时带栈。一个订阅持续出错（每个 interval 重试一次）不会占着窗口把
        新冒出来的另一类错误压掉。一类错误静默满一个间隔后补一条恢复日志（_check_quiet）。
        翻译出来的模板占位符对不上（.po 由 CD 机翻同步）时不能抛：这是 tick 的出错路径，抛出去
        就丢了重试、甚至让处理循环结束。退回原文
        """
        now = time.monotonic()
        key = (what, type(exc))
        kind = self._errors.get(key)
        first = kind is None
        if kind is None:
            kind = self._errors[key] = _ErrorKind(what)
        kind.last_seen = now
        if kind.timer is None:
            kind.timer = asyncio.get_running_loop().call_later(
                _ERROR_LOG_INTERVAL, self._check_quiet, key
            )
        if now - kind.logged_at < _ERROR_LOG_INTERVAL:
            kind.muted += 1
            return
        muted, kind.muted = kind.muted, 0
        kind.logged_at = now
        err = f"{type(exc).__name__}:{exc}"
        try:
            kind.what = what.format(**fields)
            if requeued:
                msg = _(
                    "❌ [📡Subscription] {what}：{err}，已重新入队稍后重试"
                    "（上次记录以来另有 {muted} 次出错未记）"
                ).format(what=kind.what, err=err, muted=muted)
            else:
                msg = _(
                    "❌ [📡Subscription] {what}：{err}（上次记录以来另有 {muted} 次出错未记）"
                ).format(what=kind.what, err=err, muted=muted)
        except KeyError, IndexError, ValueError:
            msg = f"❌ [📡Subscription] {what} {fields}: {err} (muted {muted})"
        logger.error(msg, exc_info=exc if first else None)

    def _check_quiet(self, key: tuple[str, type[BaseException]]) -> None:
        """一类错误静默满一个间隔：补一条恢复日志（带上压下的次数），下次再出现时重新带栈"""
        kind = self._errors.get(key)
        if kind is None:
            return
        kind.timer = None
        quiet_for = time.monotonic() - kind.last_seen
        if quiet_for < _ERROR_LOG_INTERVAL:
            kind.timer = asyncio.get_running_loop().call_later(
                _ERROR_LOG_INTERVAL - quiet_for, self._check_quiet, key
            )
            return
        del self._errors[key]
        try:
            msg = _(
                "✅ [📡Subscription] {what}：{err} 已停止（最后一条日志之后另有 {muted} 次未记）"
            ).format(what=kind.what, err=key[1].__name__, muted=kind.muted)
        except KeyError, IndexError, ValueError:
            msg = f"✅ [📡Subscription] {kind.what}: {key[1].__name__} stopped ({kind.muted})"
        logger.info(msg)

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
        work = self._repair(self._collect(batch))
        if not work:
            return
        self._ticks += 1
        # 在本 tick 的任何读之前记下
        tick = self._staging = _Tick(self._ticks, self._effective_seq)
        try:
            # 本 tick 要读的行先按表批量读取，填进 RowSubscription 的缓存
            RowSubscription.reset_cache_()
            await self._prefetch_rows(work)
            await self._process_all(work, tick)
        finally:
            try:
                if not self._closed:
                    # 中途出错也要把已记账的频道订上 / 退掉：新行的频道不订上就永远收不到通知，
                    # 放掉的一直订着
                    await self._settle_channels(tick)
            finally:
                # 放在最后：推给客户端的新行一般在其行频道订阅生效之后（SUBSCRIBE 最多等
                # SUBSCRIBE_WAIT_INTERVALS），get_updates 拿到的也总是完整的 tick。中途出错也把
                # 已算好的交出去
                self._staging = None
                self._deliver(tick)

    def _collect(
        self, batch: Mapping[str, set[str] | None]
    ) -> dict[BaseSubscription, list[tuple[str, set[str] | None]]]:
        """
        按订阅分组：真实频道交给订了它的所有已生效订阅；定向补读只交给它指定的订阅（还订着那个
        频道的话）。每个订阅内保持弹出顺序。
        成员的推送全都卡着的订阅先不读（`_park`）：恢复按拉取驱动的背压，同 dev 不调 get_updates
        就不读
        """
        channel_subs = self._channel_subs
        stalled = self._stalled
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
                    if stalled(sub):
                        self._park(sub, channel, payload)
                    else:
                        work.setdefault(sub, []).append((channel, payload))
                continue
            for sub in channel_subs.get(key, ()):
                if not sub.active:
                    continue
                if stalled(sub):
                    self._park(sub, key, payload)
                else:
                    work.setdefault(sub, []).append((key, payload))
        return work

    def _stalled(self, sub: BaseSubscription) -> bool:
        """
        订阅的成员是不是全都推送卡住了：待发区还有上次交的没取走，而且没在 get_updates 里等着取
        （推送阻塞在 push_queue 上）。有一个没卡着就照常处理，卡着的成员待发区按 sub_id / row_id
        合并，上限是这个订阅的数据量。成员表为空不算卡住（all([]) 为真，攒下的通知就没人来重读了）。
        有人在等它的结果（初始化、重读判定频道）也不算：等着的回复占着位，卡着的正是它后面的推送。
        手动模式由 get_updates 自己跑 tick，不存在卡住
        """
        members = sub.members
        return (
            self._autostart
            and bool(members)
            and not sub.waiters
            and all(broker.stalled_() for broker in members)
        )

    def _park(
        self, sub: BaseSubscription, channel: str, payload: set[str] | None
    ) -> None:
        """成员全都卡着的订阅这个频道先不读，攒着，有成员取走待发区（resume_）或有新成员加入时重读"""
        items = self._parked.get(sub)
        if items is None:
            items = self._parked[sub] = {}
            for broker in sub.members:
                self._parked_by.setdefault(broker, set()).add(sub)
        if payload is None:
            items.setdefault(channel, None)
        elif (known := items.get(channel)) is None:
            items[channel] = set(payload)
        else:
            known.update(payload)

    def _unpark(self, sub: BaseSubscription) -> None:
        """sub 攒着的通知全部定向重读：interval 后照常读、推最新的"""
        items = self._parked.pop(sub, None)
        if items is None:
            return
        for broker in sub.members:
            self._unindex_parked(broker, sub)
        if sub.closed or not sub.active:
            return
        for channel, payload in items.items():
            self.reread_for(sub, channel, payload=payload)

    def _unindex_parked(
        self, broker: SubscriptionBroker, sub: BaseSubscription
    ) -> None:
        parked = self._parked_by.get(broker)
        if parked is not None:
            parked.discard(sub)
            if not parked:
                del self._parked_by[broker]

    def resume_(self, broker: SubscriptionBroker) -> None:
        """门面取走了待发区：它所在的订阅攒下的通知都重读（它没卡着了，订阅不再"全都卡着"）"""
        parked = self._parked_by.pop(broker, None)
        if parked:
            for sub in parked:
                self._unpark(sub)

    def _repair(
        self, work: dict[BaseSubscription, list[tuple[str, set[str] | None]]]
    ) -> dict[BaseSubscription, list[tuple[str, set[str] | None]]]:
        """
        本批涉及的频道若 hub 没生效地订着（此前 tick 末尾订阅它失败了，见 _subscribe_added），本
        tick 不处理它们，交给后台补订（_resubscribe）：订上之后再按真实频道重新入队，一个 interval
        后读（要在订阅生效之后读，失败期间的写入没有通知），没订上的也重新入队、到时再补。不等补订
        回来，别让它卡住本批别的订阅的交付。
        按 `_effective` 判断，不看 MQClient.subscribed：后者在 SUBSCRIBE 发出时就记上，别的连接的
        SUBSCRIBE 还在途时会被当作已订好、当场就读；那次 SUBSCRIBE 随后失败的话频道就成了孤儿。
        在途的由补订去等它（见 _subscribe）
        """
        effective = self._effective
        missing: dict[str, set[str] | None] = {}
        for items in work.values():
            for channel, payload in items:
                if channel in effective:
                    continue
                known = missing.get(channel)
                if payload is None:
                    missing.setdefault(channel, None)
                else:
                    missing[channel] = payload if known is None else known | payload
        if not missing:
            return work
        self._spawn(self._resubscribe(missing))
        return {
            sub: kept
            for sub, items in work.items()
            if (kept := [item for item in items if item[0] not in missing])
        }

    async def _resubscribe(self, missing: dict[str, set[str] | None]) -> None:
        """补订（hub 的后台任务）：不管成败，补订的频道都按真实频道重新入队"""
        try:
            await self._subscribe(list(missing))
        except Exception as e:  # noqa: BLE001 没订上的到时再补
            self._log_error(_("补订频道出错"), e)
        for channel, payload in missing.items():
            self._mq.request_reread(channel, payload=payload)

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
        if len(by_table) == 1:
            await self._prefetch_table(*next(iter(by_table.items())))
        elif by_table:
            # 各表并发读、各选一个随机副本：串行的话 worker 里每个连接的交付都要等所有表的往返
            # 加起来（每张表的读在 _prefetch_table 里自己兜住错误，gather 不会中途抛出）
            await asyncio.gather(
                *(self._prefetch_table(ref, table) for ref, table in by_table.items())
            )

    async def _prefetch_table(
        self, table_ref: TableReference, table: tuple[list[str], list[int]]
    ) -> None:
        """预读一张表的这些行，填进 RowSubscription 的每 tick 缓存；读失败这张表不填"""
        channels, row_ids = table
        try:
            rows = cast(
                list[dict[str, Any] | None],
                await self._backend.servant.get_many(
                    table_ref, row_ids, RowFormat.TYPED_DICT
                ),
            )
        except Exception as e:  # noqa: BLE001 不填缓存，订阅各自单行读
            self._log_error(
                _("预读 {comp_name} 的行出错，这批改为逐行读"),
                e,
                requeued=False,
                comp_name=table_ref.comp_name,
            )
            return
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
            elif not task.cancelled() and (exc := task.exception()) is not None:
                # 当场跑完的也要取异常：不进 pending 的话没人取，悄无声息地丢了
                self._log_error(_("处理订阅通知异常（bug）"), exc, requeued=False)
            if count % _YIELD_EVERY == 0:
                await asyncio.sleep(0)
        if pending:
            # 逃出 _process 的异常（bug）不能中止整个 tick：别的还在读的要等它们跑完，不然它们
            # 之后把更新暂存进已经交付过的 tick，推送丢了、指纹却已推进
            for result in await asyncio.gather(*pending, return_exceptions=True):
                if isinstance(result, Exception):
                    self._log_error(
                        _("处理订阅通知异常（bug）"), result, requeued=False
                    )

    async def _process(
        self,
        sub: BaseSubscription,
        items: list[tuple[str, set[str] | None]],
        tick: _Tick,
    ) -> None:
        """一个订阅按顺序处理它这批的频道：记账、暂存更新"""
        channel_subs = self._channel_subs
        # 有成员在等它重读判定频道（recheck_）
        recheck = sub.recheck_channel_() if sub.waiters and sub.ready else None
        rechecked = False
        for channel, payload in items:
            # 已退订；或本 tick 里它自己的处理刚把这行放出了范围（分组在前，这里得现查）
            if sub.closed or sub not in channel_subs.get(channel, ()):
                continue
            try:
                new_chans, rem_chans, updates = await sub.get_updated(channel, payload)
            except Exception as e:  # noqa: BLE001 定向重读重试，不牵连别的订阅
                # 读库出错（Redis 抖动等）：不牵连别的订阅、不断开连接。给它定向重读这个频道，
                # 稍后重试（设计稿 §6）。订阅的读固定走订阅时选的副本，它可能挂了：换一个随机
                # 副本再试，别每次都打同一个死节点
                self._log_error(
                    _("订阅处理通知出错，频道 {channel}"), e, channel=channel
                )
                if not sub.closed:
                    sub.use_servant_(self._backend.servant)
                    self._retry_later(sub, channel, payload)
                continue
            sub.failures = 0
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
            if channel == recheck:
                rechecked = True
        # 请求之后才开始的 tick 里重读到了判定频道：等着的成员拿这时的快照
        if rechecked and tick.seq > sub.recheck_after and sub.waiters:
            self._rechecked(sub)

    def _retry_later(
        self, sub: BaseSubscription, channel: str, payload: set[str] | None
    ) -> None:
        """
        出错的读定向重读重试。同一个订阅接连出错按 1、2、4… 个 interval 退避，封顶
        _RETRY_BACKOFF_MAX 秒：入队本身就要等一个 interval，多出来的用 call_later 推迟入队
        """
        sub.failures += 1
        interval = self.interval
        backoff = interval * 2 ** min(sub.failures - 1, 16)
        delay = min(backoff, _RETRY_BACKOFF_MAX) - interval
        if delay <= 0:
            self.reread_for(sub, channel, payload=payload)
            return
        asyncio.get_running_loop().call_later(
            delay, self._retry_now, sub, channel, payload
        )

    def _retry_now(
        self, sub: BaseSubscription, channel: str, payload: set[str] | None
    ) -> None:
        if not self._closed and not sub.closed:
            self.reread_for(sub, channel, payload=payload)

    @staticmethod
    def _stage(
        tick: _Tick,
        sub: BaseSubscription,
        updates: Mapping[int, dict[str, Any] | None],
    ) -> None:
        """
        按订阅暂存：同一个 tick 里同一订阅的几次更新合并成一份，后到的覆盖先到的，tick 末尾交给
        那时的各成员（`_deliver`）。不按成员各拷一份：共享订阅的成员成百上千时，每个变动的频道都要
        走一遍成员表。共享订阅的快照在这里跟着改，不等交付：tick 中途加入的成员拿到的快照已含
        这次更新（设计稿 2026-09-29 §3.2）
        """
        snapshot = sub.snapshot
        if snapshot is not None:
            for row_id, row in updates.items():
                if row is None:
                    snapshot.pop(row_id, None)
                else:
                    snapshot[row_id] = row
            sub.snapshot_list = None
        staged = tick.staged.get(sub)
        if staged is None:
            tick.staged[sub] = dict(updates)
        else:
            staged.update(updates)

    @staticmethod
    def _deliver(tick: _Tick) -> None:
        """
        tick 末尾：各订阅合并好的更新交给这时的成员，成员们共用这一份（只读）。成员按交付这一刻
        的成员表：tick 中途退订的不给，退订后又重订的按重订算，退订前暂存给它的不会交给新的它。
        tick 中途加入的，快照已含加入时已暂存的，只给它之后变了的（行对象每次更新都是新读出来的，
        按对象认）。按 row_id 覆盖 / 删除的结果与快照一致：没变的行快照里本来就是这个值
        """
        joined = tick.joined
        for sub, updates in tick.staged.items():
            for broker, sub_id in sub.members.items():
                seen = joined.get((sub, broker)) if joined else None
                if seen is None:
                    broker.deliver_(sub_id, sub, updates)
                    continue
                fresh = {
                    row_id: row
                    for row_id, row in updates.items()
                    if row_id not in seen or seen[row_id] is not row
                }
                if fresh:
                    broker.deliver_(sub_id, sub, fresh)

    async def _settle_channels(self, tick: _Tick) -> None:
        """
        tick 末尾：订阅新增的频道、给新订上的定向补读、退订没人要的。
        只等 SUBSCRIBE 回来、最多 SUBSCRIBE_WAIT_INTERVALS 个 interval：推给客户端的新行尽量在它
        的行频道订阅生效之后，但 ack 迟迟不来时不能冻住整个 worker 的交付，晚回来的补读 / 补订由
        订阅任务自己做（_subscribe_added）。退订不等：交付不依赖它，放到后台，跑起来时还会按频道
        表复查（见 _unsubscribe）
        """
        channel_subs = self._channel_subs
        # 同一频道可能在本 tick 内既被一个订阅加入又被另一个释放，按最终状态定夺；
        # 已订阅过的频道重复 subscribe 是幂等的
        to_subscribe = [chan for chan in tick.added if chan in channel_subs]
        if to_subscribe:
            task = self._spawn(self._subscribe_added(to_subscribe, tick))
            await asyncio.wait(
                [task], timeout=self.SUBSCRIBE_WAIT_INTERVALS * self.interval
            )
        # 退订名单在等 SUBSCRIBE 之后再定：等待期间接收协程可能登记了新订阅（attach 的占位），
        # 把刚释放的频道又要回去了
        to_unsubscribe = [chan for chan in tick.released if chan not in channel_subs]
        if to_unsubscribe:
            self._spawn(self._unsubscribe(to_unsubscribe))

    async def _subscribe_added(self, channels: list[str], tick: _Tick) -> None:
        """
        tick 末尾新增频道的订阅（hub 的后台任务），订上后给 fresh 的新增者定向补读。
        fresh：本 tick 开始之后才生效的频道。订阅在本 tick 里读这行，可能在它生效之前，其间的写入
        不会有通知，值频道又不发"离开"，要定向补读（读回一样就不推）。本 tick 开始前就已生效的不用：
        之后的写入都有通知进本队列，tick 结束前订阅已登记好。不能按 tick 末尾生效与否判断：别的
        连接订同一行的 SUBSCRIBE 可能恰好在读之后、tick 结束之前回来（设计稿 §4.4）。
        失败时频道仍留在频道表里，按真实频道重新入队，弹出时先补订（见 _repair），补订成功后会按
        真实频道重读，不用定向补读
        """
        try:
            await self._subscribe(channels)
        except Exception as e:  # noqa: BLE001 重新入队，弹出时补订
            self._log_error(_("订阅新进入范围的行频道出错"), e)
            for chan in channels:
                self._mq.request_reread(chan)
            return
        channel_subs = self._channel_subs
        effective = self._effective
        for chan in channels:
            seq = effective.get(chan)
            if seq is None or seq <= tick.since:
                continue  # 本 tick 之前就已生效；或已被退订，订阅不再要它
            for sub in tick.added[chan]:
                if not sub.closed and sub in channel_subs.get(chan, ()):
                    self.reread_for(sub, chan)

    # === === === 生命周期 === === ===

    def _spawn(
        self,
        coro: Coroutine[Any, Any, None],
        context: contextvars.Context | None = None,
    ) -> asyncio.Task:
        """hub 自己的后台任务：不随调用方取消，保存引用免得被 gc，close 时统一取消"""
        task = asyncio.create_task(coro, context=context)
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
        # 初始化被取消了的订阅，还在等结果的成员不能一直等下去
        closed = ConnectionError(_("连接已关闭，已调用过close"))
        for sub in self._by_token.values():
            waiters, sub.waiters = sub.waiters, {}
            for futures in waiters.values():
                for waiter in futures:
                    if not waiter.done():
                        waiter.set_exception(closed)
                        waiter.exception()
        self._channel_subs.clear()
        self._by_token.clear()
        self._shared.clear()
        self._effective.clear()
        self._inflight.clear()
        self._parked.clear()
        self._parked_by.clear()
        for kind in self._errors.values():
            if kind.timer is not None:
                kind.timer.cancel()
        self._errors.clear()
        await self._mq.close()


class _Pending(NamedTuple):
    """订阅前半段的登记，交给后半段（`SubscriptionBroker._finish`）"""

    sub_id: str
    sub: BaseSubscription
    # 这次登记的记号（见 SubscriptionBroker._regs）
    reg: object
    # 等初始化结果的 future（见 SubscriptionHub.wait_）
    waiter: asyncio.Future[list[dict[str, Any]] | None]
    # 加入的是 worker 里已有的共享订阅，不是自己新建的：回复是快照，不是为本次订阅读的
    joined: bool


class SubscriptionBroker:
    """
    Component的数据订阅和查询接口，每个连接一个。订阅本身在本 worker 共享的 `SubscriptionHub`
    里处理，本对象是连接的门面：权限检查、sub_id、同连接的重复订阅与订阅数，以及待发区——hub 在
    tick 末尾把本连接的更新交到这里，服务端的发送循环直接取走（`bind_sender_` / `take_updates_`），
    别的用法（测试等）用 `get_updates` 等着取。

    The per-connection facade of component subscriptions. The subscriptions themselves are
    processed by the worker-wide `SubscriptionHub`; this object handles permissions, sub
    ids, per-connection duplicates and quotas, and the outbox. The server's send loop drains
    the outbox directly (`bind_sender_` / `take_updates_`); other callers use `get_updates`.

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
    MAX_SUBSCRIBED: int = 5000

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
        # sub_id → 这次登记的记号（每次登记一个新的）：后半段、重复订阅据此认出 sub_id 还是不是它们
        # 那次登记。按订阅对象认不出来：退订后同一连接重订同一查询，会重新加入同一个共享订阅对象
        self._regs: dict[str, object] = {}
        self._sub_counts: Counter[type[BaseSubscription]] = Counter()
        # 待发区：hub 在 tick 末尾交来的更新 {sub_id: {row_id: 行 | None}}，get_updates 取走
        self._outbox: dict[str, dict[int, dict[str, Any] | None]] = {}
        # 待发区里与同一订阅的别的成员共用的那几份（hub 交来的原样，只读）：再合并时先拷一份
        self._borrowed: set[str] = set()
        self._arrived = asyncio.Event()
        # 取待发区的一方（服务端发送循环，或 get_updates）正空闲等着：推送没卡住
        self._waiting = False
        # 服务端发送循环的叫醒函数（bind_sender_）：它空闲等着时交来更新就叫一次，醒来前不重复叫
        self._wake: Callable[[], None] | None = None
        self._wake_pending = False
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
        self._regs.clear()
        self._sub_counts.clear()
        self._channel_counts.clear()
        self._channel_count = 0
        self._outbox.clear()
        self._borrowed.clear()
        # 两个退订并发，只等一个往返。内部关注的先撤：MQClient.close 在第一次 await 之前就同步
        # 清掉回调，等订阅退订回来的期间顶号检测不会再触发（它会去 master 核一次，白读）
        detach = asyncio.ensure_future(self._detach_all(subs)) if subs else None
        try:
            if self._watch_mq is not None:
                await self._watch_mq.close()
        finally:
            if detach is not None:
                await detach

    async def _detach_all(self, subs: list[BaseSubscription]) -> None:
        try:
            await self._hub.detach(self, *subs)
        except Exception as e:  # noqa: BLE001 拆连接不能因为后端异常半途而废
            logger.warning(
                _("⚠️ [📡Subscription] 关闭连接时取消订阅失败：{err}").format(
                    err=f"{type(e).__name__}:{e}"
                )
            )

    async def watch_channel(self, channel: str, callback: Callable[[], None]) -> None:
        """
        服务端内部关注一个频道（如本连接用户的 Connection owner 值频道）：收到通知调
        `callback`，不计入订阅数、不做权限检查，也不经过 hub 的队列；客户端对同一频道的订阅/
        退订与之互不干扰。连接关闭（`close`）时一起退订。
        回调在后端通知接收器的监听协程里同步执行，必须非阻塞。
        """
        if self._closed:
            raise ConnectionError(_("连接已关闭，已调用过close"))
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

    @staticmethod
    def _share_key(
        table_ref: TableReference, ctx: Context, query: tuple
    ) -> tuple | None:
        """
        共享键（设计稿 2026-09-29 §3.1）：同一张表上同一查询的订阅在 worker 内共用一个订阅对象。
        按 RLS 判定可见的订阅（组件是 RLS、ctx 不是 admin）私有，返回 None。
        不用 sub_id 当键：right=None 与字符串 "None" 拼出同一个 sub_id，共享后会让别人拿到另一个
        查询的结果。查询里的值用 repr（None / bool / int / float / str 之间不会撞，1 与 1.0 分开只是
        少共享些），同键一定同 sub_id：一个连接不会以两个 sub_id 加入同一个订阅。表不用
        TableReference：Table 多一个 backend 字段，与同地址的 TableReference 互不相等
        """
        if rls_ctx_(table_ref, ctx) is not None:
            return None
        return (
            table_ref.comp_cls,
            table_ref.instance_name,
            table_ref.cluster_id,
            *query,
        )

    def _subscribe(
        self,
        sub_id: str,
        key: tuple | None,
        make: Callable[[], BaseSubscription],
    ) -> _Pending:
        """
        前半段的登记：可共享的（key 不为 None）先找 worker 里同一查询的订阅，有就加入，没有就新建
        （make）、在 hub 里开订阅（初始化在 hub 的任务里跑）；登记到本连接、计入订阅数。返回的登记
        交给后半段（`_finish`）
        """
        sub = self._hub.shared_(key)
        if sub is None:
            return self._open(sub_id, make(), key)
        return self._join(sub_id, sub)

    def _open(
        self, sub_id: str, sub: BaseSubscription, key: tuple | None = None
    ) -> _Pending:
        """新建的订阅：在 hub 里开订阅（key 不为 None 时登记为共享），登记到本连接"""
        if self._closed:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        waiter = self._hub.open_(sub, self, sub_id, key)
        return _Pending(sub_id, sub, self._register(sub_id, sub), waiter, False)

    def _join(self, sub_id: str, sub: BaseSubscription) -> _Pending:
        """加入 worker 里同一查询的共享订阅，登记到本连接"""
        if self._closed:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        waiter = self._hub.join_(sub, self, sub_id)
        return _Pending(sub_id, sub, self._register(sub_id, sub), waiter, True)

    def _register(self, sub_id: str, sub: BaseSubscription) -> object:
        """登记到本连接、计入订阅数，返回这次登记的记号（见 `_regs`）"""
        self._subs[sub_id] = sub
        self._sub_counts[type(sub)] += 1
        reg = self._regs[sub_id] = object()
        return reg

    def _current(self, sub_id: str, reg: object) -> bool:
        """sub_id 还是 reg 那次登记：期间没被退订（退订后又重订的也不算）"""
        return self._regs.get(sub_id) is reg

    async def _finish(self, p: _Pending) -> list[dict[str, Any]] | None:
        """
        后半段：等订阅初始化完成，返回回复用的行。订阅不成立（行不存在 / 不可见，整表超过上限）、
        或等的期间被退订了返回 None。初始化失败（重试用尽）或本调用被取消时撤掉这个订阅再抛出
        """
        sub_id = p.sub_id
        try:
            rows = await p.waiter
        except BaseException:
            if self._current(sub_id, p.reg):
                await self.unsubscribe(sub_id)
            raise
        if not self._current(sub_id, p.reg):
            # 等的这段时间里被退订了（后半段在后台跑，接收协程照常处理 unsub）：这个 sub_id 可能
            # 已经重新登记（重订同一查询会重新加入同一个共享订阅对象），别去动它
            return None
        if rows is None:
            # 回 None 客户端就认为没有订阅、不会再来 unsub，订阅得跟着撤掉
            await self.unsubscribe(sub_id)
            return None
        # MAX_SUBSCRIBED 告警按订阅时登记的频道数估算（范围订阅读完才知道有几行）
        channels = len(p.sub.channels)
        self._channel_counts[sub_id] = channels
        self._channel_count += channels
        if self._channel_count > self.MAX_SUBSCRIBED:
            logger.warning(
                _(
                    "⚠️ [{tag}] 当前连接订阅数超过全局限制MAX_SUBSCRIBED={limit}行"
                ).format(tag="📡Subscription", limit=self.MAX_SUBSCRIBED)
            )
        return rows

    async def subscribe_get(
        self,
        table_ref: TableReference,
        ctx: Context,
        index_name: str,
        query_value: int | float | str,
    ) -> tuple[str | None, dict[str, Any] | None]:
        """
        获取并订阅单行数据。
        如果是重复订阅，会返回上一次订阅的sub_id，已有的订阅照旧。回复里的数据：在 worker
        内共享的订阅（组件不按 RLS 判定可见，或 caller 是 admin）是订阅已推给客户端的内容；
        按 RLS 私有的订阅是这次重新读的（可能落在滞后的副本上）。客户端应沿用已有的订阅
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

        See Also
        --------
        begin_subscribe_get : 拆成两段的写法（服务器的接收协程用它）
        """
        finish = await self.begin_subscribe_get(table_ref, ctx, index_name, query_value)
        return await finish

    async def begin_subscribe_get(
        self,
        table_ref: TableReference,
        ctx: Context,
        index_name: str,
        query_value: int | float | str,
    ) -> Coroutine[Any, Any, tuple[str | None, dict[str, Any] | None]]:
        """
        `subscribe_get` 的前半段：检查、定位 row_id（按非 id 索引订阅时查一次索引）、登记（重复订阅
        也在这里认出来），订阅的初始化交给 worker 级订阅器。返回后半段的协程，await 它得到与
        `subscribe_get` 一样的 (sub_id, row)。拆开是为了让服务器的接收协程登记完就去处理下一条
        消息，后半段交给后台任务。返回的协程必须 await 完或者取消，否则订阅一直算在本连接上。

        The first half of `subscribe_get`: checks, locating the row id and registering (the
        subscription is initialized by the worker-level hub). Returns a coroutine for the
        second half, so that the server's receiver can handle the next message meanwhile.
        The coroutine must be awaited or cancelled.
        """
        # 首先caller要对整个表有权限
        if not self._has_table_permission(table_ref, ctx):
            return self._settled(None, None)

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
                return self._settled(None, None)
            row_id = int(ids[0])

        sub_id = self.make_query_id_(table_ref, "id", row_id, None, 1, False)
        if (existing := self._subs.get(sub_id)) is not None:
            logger.warning(
                _("⚠️ [📡Subscription] {sub_id} 数据重复订阅，检查客户端代码").format(
                    sub_id=sub_id
                )
            )
            reg = self._regs[sub_id]
            if existing.share_key is not None:
                return self._repeat_get(sub_id, existing, reg)
            return self._reread_get(table_ref, ctx, sub_id, existing, reg, row_id)

        def make() -> RowSubscription:
            channel = servant.row_channel(table_ref, row_id)
            return RowSubscription(table_ref, servant, ctx, channel, row_id)

        key = self._share_key(table_ref, ctx, ("get", row_id))
        return self._finish_get(self._subscribe(sub_id, key, make))

    async def _finish_get(
        self, p: _Pending
    ) -> tuple[str | None, dict[str, Any] | None]:
        """
        行订阅的后半段：行不存在、或 caller 对该行无权限时订阅不成立，回 None。加入的共享订阅这行
        已经不在了（成员们都已收到 None，快照为空）时同样回 None、不加入，先重读一次行确认
        （`_recheck`）
        """
        rows = await self._finish(p)
        if rows == [] and p.joined:
            rows = await self._recheck(p.sub_id, p.sub, p.reg)
        if not rows:
            if rows is not None:
                await self.unsubscribe(p.sub_id)
            return None, None
        logger.debug(
            _("🆕 [📡Subscription] 订阅了行: {sub_id} {channel_name}").format(
                sub_id=p.sub_id, channel_name=cast(RowSubscription, p.sub).channel
            )
        )
        return p.sub_id, rows[0]

    async def _repeat_get(
        self, sub_id: str, existing: BaseSubscription, reg: object
    ) -> tuple[str | None, dict[str, Any] | None]:
        """
        重复的共享行订阅：回它的快照，不读库；这行已经不在了时撤掉已有的订阅，撤之前先重读一次行
        确认（`_recheck`）
        """
        rows = await self._await_existing(sub_id, existing, reg)
        if rows == []:
            rows = await self._recheck(sub_id, existing, reg)
        if not rows:
            if rows is not None:
                await self.unsubscribe(sub_id)
            return None, None
        return sub_id, rows[0]

    async def _await_existing(
        self, sub_id: str, sub: BaseSubscription, reg: object
    ) -> list[dict[str, Any]] | None:
        """
        重复订阅：等已有的订阅 sub（sub_id 的 reg 那次登记）初始化有结果。共享订阅回它的快照（订阅
        已推给客户端的内容），不读库；私有订阅不维护快照，成立的回空列表。订阅不成立、已关闭，或等的
        期间被退订了回 None（撤掉订阅的事由第一次订阅的后半段做）
        """
        rows = await self._hub.wait_(sub, self)
        if rows is None or not self._current(sub_id, reg):
            return None
        return rows

    async def _recheck(
        self, sub_id: str, sub: BaseSubscription, reg: object
    ) -> list[dict[str, Any]] | None:
        """
        要按共享订阅的快照回"没有"（行不存在、force=False 的范围为空）之前：等订阅重读一次判定频道，
        返回那之后的快照（见 `SubscriptionHub.recheck_`）。快照落后于提交，据它回"没有"的话，随后才
        推来的行这个连接再也收不到。自己新建的订阅不用：初始读就是为这次订阅读的。等的期间被退订了
        回 None。手动模式（测试）没有处理循环，自己驱动 tick
        """
        hub = self._hub
        waiter = hub.recheck_(sub, self)
        if not hub.autostart:
            while not waiter.done():
                await hub.step_(None, waiter.done)
        rows = await waiter
        if rows is None or not self._current(sub_id, reg):
            return None
        return rows

    async def _reread_get(
        self,
        table_ref: TableReference,
        ctx: Context,
        sub_id: str,
        existing: BaseSubscription,
        reg: object,
        row_id: int,
    ) -> tuple[str | None, dict[str, Any] | None]:
        """
        重复的私有行订阅：已有的订阅照旧，只把行再读一遍返回；行现在不可见时撤掉已有的订阅。已有的
        还在初始化的先等它的结果：它不成立的话回 None（第一次订阅的后半段撤掉它），不能换个副本读到
        行就回 sub_id，服务端却已没有登记
        """
        if await self._await_existing(sub_id, existing, reg) is None:
            return None, None
        row = await self._backend.servant.get(table_ref, row_id, RowFormat.TYPED_DICT)
        if not self._current(sub_id, reg):
            # 读的这段时间里被退订了，sub_id 可能又登记给了新的订阅，别去动它
            return None, None
        if row is None or not self._has_row_permission(table_ref, ctx, row):
            # 行现在不可见（已删除 / 失去行级权限）：回 None 客户端就认为没有订阅、不会再来
            # unsub，旧订阅得跟着撤掉，不然它和它的频道会挂到连接结束
            await self.unsubscribe(sub_id)
            return None, None
        del row["_version"]  # 内部版本号不推给客户端
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
        如果是重复订阅，会返回上一次订阅的sub_id，已有的订阅照旧。回复里的数据：在 worker
        内共享的订阅（组件不按 RLS 判定可见，或 caller 是 admin）是订阅已推给客户端的内容；
        按 RLS 私有的订阅是这次重新读的（可能落在滞后的副本上）。客户端应沿用已有的订阅
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
        begin_subscribe_range : 拆成两段的写法（服务器的接收协程用它）

        """
        finish = await self.begin_subscribe_range(
            table_ref, ctx, index_name, left, right, limit, desc, force
        )
        return await finish

    async def begin_subscribe_range(
        self,
        table_ref: TableReference,
        ctx: Context,
        index_name: str,
        left: Any,
        right: Any | None = None,
        limit: int = 10,
        desc: bool = False,
        force: bool = True,
    ) -> Coroutine[Any, Any, tuple[str | None, list[dict]]]:
        """
        `subscribe_range` 的前半段：检查（含查询参数）、登记（重复订阅也在这里认出来），订阅的
        初始化交给 worker 级订阅器。返回后半段的协程，await 它得到与 `subscribe_range` 一样的
        (sub_id, rows)，用法同 `begin_subscribe_get`。

        The first half of `subscribe_range`; see `begin_subscribe_get`.
        """
        # 首先caller要对整个表有权限，不然就算force也不给订阅
        if not self._has_table_permission(table_ref, ctx):
            logger.warning(
                _(
                    "⚠️ [📡Subscription] {comp_name}无调用权限，"
                    "检查是否非法调用，caller：{caller}"
                ).format(comp_name=table_ref.comp_name, caller=ctx.caller)
            )
            return self._settled(None, [])

        servant = self._backend.servant
        # 查询参数（limit 是整数、索引存在、边界合法）当场校验、不合法当场抛出，不等到后台读的时候
        # （后台读出错会换副本重试）
        servant.check_range_(table_ref, index_name, left, right, limit, desc)

        sub_id = self.make_query_id_(table_ref, index_name, left, right, limit, desc)
        if (existing := self._subs.get(sub_id)) is not None:
            logger.warning(
                _("⚠️ [📡Subscription] {sub_id} 数据重复订阅，检查客户端代码").format(
                    sub_id=sub_id
                )
            )
            reg = self._regs[sub_id]
            if existing.share_key is not None:
                return self._repeat_range(sub_id, existing, reg, force)
            return self._reread_range(
                table_ref,
                ctx,
                sub_id,
                existing,
                reg,
                (index_name, left, right, limit, desc),
                force,
            )

        def make() -> IndexSubscription:
            # 点查询只订该值的频道，别的值的变动不会打扰；区间查询订整个索引的频道。值频道只有
            # 声明了 point_sub 的索引才有（commit 只给它们发）：没声明的点查询退化为订整个索引
            # 的频道并警告一次。id 不能声明 point_sub，点查 id 也退化并警告（该用 subscribe_get）
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
            return IndexSubscription(
                table_ref,
                servant,
                ctx,
                index_channel,
                {
                    "index_name": index_name,
                    "left": left,
                    "right": right,
                    "limit": limit,
                    "desc": desc,
                },
                point_value,
            )

        # desc 按真假（sub_id 也是），客户端传 1 或 True 是同一个查询
        query = ("range", index_name, repr(left), repr(right), repr(limit), bool(desc))
        p = self._subscribe(sub_id, self._share_key(table_ref, ctx, query), make)
        return self._finish_range(p, force)

    async def _finish_range(
        self, p: _Pending, force: bool
    ) -> tuple[str | None, list[dict]]:
        """
        范围订阅的后半段：force 为 False 且没查到（可见的）数据时不订阅。加入的共享订阅快照为空的，
        先重读一次索引确认（`_recheck`）
        """
        rows = await self._finish(p)
        if rows is None:
            return None, []
        if not force and not rows:
            if p.joined:
                rows = await self._recheck(p.sub_id, p.sub, p.reg)
                if rows is None:
                    return None, []
            if not rows:
                await self.unsubscribe(p.sub_id)
                return None, []
        logger.debug(
            _("🆕 [📡Subscription] 订阅了索引: {sub_id} {index_channel}").format(
                sub_id=p.sub_id,
                index_channel=cast(IndexSubscription, p.sub).index_channel,
            )
        )
        return p.sub_id, rows

    async def _repeat_range(
        self, sub_id: str, existing: BaseSubscription, reg: object, force: bool
    ) -> tuple[str | None, list[dict]]:
        """
        重复的共享范围订阅：回它的快照，不读库。force 为 False 且快照为空的，回 None 之前先重读一次
        索引确认（`_recheck`）
        """
        rows = await self._await_existing(sub_id, existing, reg)
        if rows == [] and not force:
            rows = await self._recheck(sub_id, existing, reg)
        if rows is None:
            return None, []
        if not force and not rows:
            return None, rows
        return sub_id, rows

    async def _reread_range(
        self,
        table_ref: TableReference,
        ctx: Context,
        sub_id: str,
        existing: BaseSubscription,
        reg: object,
        query: tuple[str, Any, Any, int, bool],
        force: bool,
    ) -> tuple[str | None, list[dict]]:
        """
        重复的私有范围订阅：已有的订阅照旧，只把范围再读一遍返回（query 为 index_name, left, right,
        limit, desc）。已有的还在初始化的先等它的结果，同 `_reread_get`
        """
        if await self._await_existing(sub_id, existing, reg) is None:
            return None, []
        rows = await self._backend.servant.range(
            table_ref, *query, RowFormat.TYPED_DICT
        )
        if not self._current(sub_id, reg):
            return None, []  # 读的这段时间里被退订了
        for row in rows:
            del row["_version"]
        # 如果是rls权限，需要对每行数据进行权限判断
        if table_ref.comp_cls.is_rls():
            rows = [
                row for row in rows if self._has_row_permission(table_ref, ctx, row)
            ]
        if not force and len(rows) == 0:
            return None, rows
        return sub_id, rows

    async def subscribe_table(
        self,
        table_ref: TableReference,
        ctx: Context,
    ) -> tuple[str | None, list[dict]]:
        """
        获取并订阅整张表。与 `subscribe_range` 语义独立：只订阅一个表级频道，
        不管表有多少行都只占一个订阅，适合"行多、行小、很少变"的表（如所有玩家名字）。
        如果是重复订阅，会返回上一次订阅的sub_id，已有的订阅照旧。回复里的数据：在 worker
        内共享的订阅（组件不按 RLS 判定可见，或 caller 是 admin）是订阅已推给客户端的内容；
        按 RLS 私有的订阅是这次重新读的（可能落在滞后的副本上）。客户端应沿用已有的订阅
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
        `subscribe_table` 的前半段：检查、登记（重复订阅也在这里认出来），订阅的初始化（订阅表级
        频道，隔一个 interval 再全量读）交给 worker 级订阅器。返回后半段的协程，await 它得到与
        `subscribe_table` 一样的 (sub_id, rows)，用法同 `begin_subscribe_get`。

        The first half of `subscribe_table`; see `begin_subscribe_get`.
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
            reg = self._regs[sub_id]
            if existing.share_key is not None:
                return self._repeat_table(table_ref, ctx, sub_id, existing, reg)
            return self._reread_table(table_ref, ctx, sub_id, existing, reg)

        def make() -> TableSubscription:
            table_channel = servant.table_channel(table_ref)
            return TableSubscription(
                table_ref, servant, ctx, table_channel, self._max_table_rows
            )

        # 行数上限也是查询的一部分：订阅按建它的连接的上限读初始行、做 RESYNC（服务器里各连接的上限
        # 都是 MAX_TABLE_SUBSCRIPTION_ROWS，照样共享）
        key = self._share_key(table_ref, ctx, ("table", self._max_table_rows))
        return self._finish_table(table_ref, ctx, self._subscribe(sub_id, key, make))

    async def _finish_table(
        self, table_ref: TableReference, ctx: Context, p: _Pending
    ) -> tuple[str | None, list[dict]]:
        """
        整表订阅的后半段：表超过行数上限时订阅不成立，回 None。加入的共享订阅，表已经超过本连接的
        上限时同样回 None，不加入
        """
        rows = await self._finish(p)
        if rows is None:
            return None, []
        if len(rows) > self._max_table_rows:
            self._warn_table_cap(table_ref, ctx)
            await self.unsubscribe(p.sub_id)
            return None, []
        logger.debug(
            _("🆕 [📡Subscription] 订阅了整表: {sub_id} {table_channel}").format(
                sub_id=p.sub_id,
                table_channel=cast(TableSubscription, p.sub).table_channel,
            )
        )
        return p.sub_id, rows

    async def _repeat_table(
        self,
        table_ref: TableReference,
        ctx: Context,
        sub_id: str,
        existing: BaseSubscription,
        reg: object,
    ) -> tuple[str | None, list[dict]]:
        """重复的共享整表订阅：回它的快照，不读库；表已超过行数上限时撤掉已有的订阅"""
        rows = await self._await_existing(sub_id, existing, reg)
        if rows is None:
            return None, []
        if len(rows) > self._max_table_rows:
            self._warn_table_cap(table_ref, ctx)
            await self.unsubscribe(sub_id)
            return None, []
        return sub_id, rows

    def _warn_table_cap(self, table_ref: TableReference, ctx: Context) -> None:
        logger.warning(
            _(
                "⚠️ [📡Subscription] {comp_name}整表订阅行数超过限制"
                "MAX_TABLE_SUBSCRIPTION_ROWS={max_rows}，拒绝订阅，caller：{caller}"
            ).format(
                comp_name=table_ref.comp_name,
                max_rows=self._max_table_rows,
                caller=ctx.caller,
            )
        )

    @staticmethod
    async def _settled(sub_id: str | None, data: Any) -> tuple[str | None, Any]:
        """前半段就有结果时的后半段"""
        return sub_id, data

    async def _reread_table(
        self,
        table_ref: TableReference,
        ctx: Context,
        sub_id: str,
        existing: BaseSubscription,
        reg: object,
    ) -> tuple[str | None, list[dict]]:
        """重复的私有整表订阅：已有的订阅照旧，只把当前可见的行再读一遍返回；表已超过行数
        上限时撤掉已有的订阅，返回 None。已有的还在初始化的先等它的结果，同 `_reread_get`"""
        if await self._await_existing(sub_id, existing, reg) is None:
            return None, []
        rows = await self._read_whole_table(table_ref, ctx, self._backend.servant)
        if not self._current(sub_id, reg):
            # 读的这段时间里被退订了，sub_id 可能又登记给了新的订阅，别去动它
            return None, []
        if rows is None:
            # 回 None 客户端就认为没有订阅、不会再来 unsub（同 subscribe_get 重复订阅时
            # 行已不可见），旧订阅得跟着撤掉，不然它和表频道会挂到连接结束，还占着整表
            # 订阅数
            await self.unsubscribe(sub_id)
            return None, []
        for row in rows:
            del row["_version"]
        return sub_id, rows

    async def _read_whole_table(
        self, table_ref: TableReference, ctx: Context, servant: BackendClient
    ) -> list[dict[str, Any]] | None:
        """全量读取 caller 可见的行（含 _version）；超过 max_table_rows 返回 None"""
        rows, truncated = await read_whole_table_(
            servant, table_ref, self._max_table_rows
        )
        if truncated:
            self._warn_table_cap(table_ref, ctx)
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
        del self._regs[sub_id]
        self._sub_counts[type(sub)] -= 1
        self._channel_count -= self._channel_counts.pop(sub_id, 0)
        # 已交到待发区、还没被取走的更新不再推
        self._outbox.pop(sub_id, None)
        self._borrowed.discard(sub_id)
        await self._hub.detach(self, sub)
        if not self._outbox:
            # 待发区空了，连接不再算卡着：卡着时本连接别的订阅攒下的通知要重读，同取走待发区。
            # 否则只有取走待发区才重读，而待发区已经空了，攒着的变动一直推不出去
            self._hub.resume_(self)

    def deliver_(
        self,
        sub_id: str,
        sub: BaseSubscription,
        updates: dict[int, dict[str, Any] | None],
    ) -> None:
        """
        hub 在 tick 末尾调用：订阅 sub 本 tick 合并好的更新并进待发区（按 row_id，后到的覆盖先到
        的），唤醒 get_updates。updates 与这个订阅的别的成员共用、只读：待发区里没有这个 sub_id 时
        原样放进去，还有上次没取走的才拷一份合并。退订了、或退订后同 id 重订成了别的订阅的丢掉
        """
        if self._closed or self._subs.get(sub_id) is not sub:
            return
        outbox = self._outbox
        pending = outbox.get(sub_id)
        if pending is None:
            outbox[sub_id] = updates
            self._borrowed.add(sub_id)
        elif sub_id in self._borrowed:
            merged = dict(pending)
            merged.update(updates)
            outbox[sub_id] = merged
            self._borrowed.discard(sub_id)
        else:
            pending.update(updates)
        self._arrived.set()
        if self._waiting and self._wake is not None and not self._wake_pending:
            self._wake_pending = True
            self._wake()

    def bind_sender_(self, wake: Callable[[], None]) -> None:
        """
        服务端的发送循环直接取待发区（`take_updates_`），不另开协程等 `get_updates`：它空闲等着
        （`idle_`）时 hub 交来更新，调 wake 叫醒它一次（在 hub 的处理循环里同步调用，不能阻塞）。
        同一个门面只用一种取法
        """
        self._wake = wake

    def idle_(self, idle: bool) -> None:
        """
        发送循环进入 / 离开空闲等待（等 push_queue、等订阅回复的占位）。空闲期间交来、还没取走的
        更新不算推送卡住，hub 照常读、合并进待发区；醒来时清掉叫醒标记，下次空闲还能再叫
        """
        self._waiting = idle
        if not idle:
            self._wake_pending = False

    def take_updates_(self) -> dict[str, dict[int, Any]]:
        """
        不等待地取走待发区（空就返回空 dict），格式同 `get_updates`。取走之后推送卡着时攒下的
        通知要重读
        """
        if not self._outbox:
            return {}
        updates, self._outbox = self._outbox, {}
        self._borrowed.clear()
        # 推送卡着时 hub 攒下没读的通知，现在重读
        self._hub.resume_(self)
        return updates

    def _has_updates(self) -> bool:
        return bool(self._outbox)

    def stalled_(self) -> bool:
        """
        推送卡住了：待发区里还有上次交来的没取走，而取的一方不在空闲等着（服务端发送循环卡在
        ws.send 上，客户端网络拥塞）。hub 据此先不为本连接读库，等它取走待发区再重读
        """
        return bool(self._outbox) and not self._waiting

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
        返回的各 sub_id 的 dict 与同一共享订阅的别的连接共用，只读，不要改。

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
                self._waiting = True
                try:
                    async with asyncio.timeout_at(deadline):
                        await self._arrived.wait()
                except TimeoutError:
                    return {}
                finally:
                    self._waiting = False
            elif not await hub.step_(deadline, self._has_updates):
                return {}
        return self.take_updates_()
