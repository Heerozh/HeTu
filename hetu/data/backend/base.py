"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com


                               Backend相关结构
    ┌─────────────────┐      ┌────────────────┐       ┌───────────────────┐
    │     MQClient    │      │  BackendClient │       │  TableMaintenance │
    │消息队列连接(每连接)│─────►│  数据库连接/操作 │◄──────┤    组件表维护类     │
    └─────────────────┘      └────────────────┘       └───────────────────┘
    继承此类实现各种通知队列      继承此类实现各种数据库         继承此类实现表维护
            ▲                        ▲                         ▲
            │                        └───────────┬─────────────┘
 数据订阅结构 │                                    │ 数据事务结构
  ┌─────────┴──────────┐               ┌─────────┴──────────┐
  │ SubscriptionBroker │               │      Backend       │
  │ 每连接一个的消息管理器 │               │  数据库连接管理器    │ 每个进程一个Backend
  └────────────────────┘               └────────────────────┘
            ▲                                    ▲
  ┌─────────┴──────────┐                ┌────────┴─────────┐
  │ 用户连接(Websocket) │                │      Session     │
  │   等待Subs返回消息   │                │     事务处理类     │
  └────────────────────┘                └──────────────────┘
                                                 ▲
                                       ┌─────────┴──────────┐
                                       │  SessionRepository │
                                       │   组件相关事务操作    │
                                       └────────────────────┘

"""

import asyncio
import hashlib
import importlib
import logging
import struct
import time
import warnings
from collections import deque
from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Literal, final, overload

import numpy as np

from ...i18n import _

if TYPE_CHECKING:
    from ..component import BaseComponent
    from .idmap import IdentityMap
    from .table import TableReference

logger = logging.getLogger("HeTu.root")


class RaceCondition(Exception):
    """
    事务竞态异常，表示当前事务基于过期状态提交失败，可以丢弃本次Session并重试。

    常见触发场景包括：

    - 提交 `update` / `delete` 时，目标行 `_version` 已变化或行已被删除；
    - 提交时，本事务只读过（未修改）的行 `_version` 已变化或行已被删除，即事务依赖了
      陈旧读（stale read）；纯读的行同样参与乐观锁校验；
    - 提交 `insert` / `update` 时，主键或 unique 值已被其他事务占用，**且本事务此前曾
      `get`（或等值 `range`）观察到该值不存在**（negative observation，见
      `IdentityMap.mark_absent`）：基于过期快照的乐观并发失败，重试后 `get` 会命中对方的
      行并走正确分支；`upsert` 的锚定字段被并发插入是其典型场景。从未观察过的冲突则是
      `UniqueViolation`；
    - 表维护、连接保活等内部流程检测到依赖状态已被其他执行流改变。

    `SystemCaller` 和 `Session.retry(...)` 会捕获此异常并重新执行事务。
    """

    pass


class UniqueViolation(IndexError):
    """
    唯一索引违反异常，表示写入会破坏主键 / unique 约束，且是**确定性**冲突，不应被自动重试。

    判定在两处进行：

    - `SessionRepository.insert(...)` / `update(...)`：只检查本地 IdentityMap，同一事务内
      两行写入相同 unique 值时立即抛出（0 往返）；
    - `commit()`（`async with session` 退出、`SystemCaller` 提交、`ctx.session_commit()`）：
      由后端原子检查主键与 unique 索引（本事务删除的行不算冲突）。数据库已有同值记录、且
      本事务从未 `get` 观察过该值不存在时抛出此异常（典型如“用户名已被占用”）；若曾观察其
      不存在，则改抛 `RaceCondition` 交由重试机制处理。同时存在两类冲突时竞态优先。

    异常消息包含组件名、字段名、行 id 与操作（insert / update）。要在事务内对“已存在”
    分支处理，请先 `get` 该值（读空会自动登记 negative observation），或调用
    `SessionRepository.is_unique_conflicts` 提前检查。
    """

    pass


class RowFormat(Enum):
    """行格式枚举"""

    RAW = 0  # 未经类型转换的dict格式，具体类型由数据库决定
    STRUCT = 1  # 默认值：按Component定义严格转换的np.record（c-struct like）类型
    TYPED_DICT = 2  # 先转换成STRUCT，再转换成dict的类型。
    ID_LIST = 3  # 只返回list of row id，只能用于range查询


def to_sortable_bytes(value: np.generic) -> bytes:
    """
    将np类型的值转换为可排序的bytes，用于索引。
    Redis 后端用它做索引 zset 的 member，两个后端都用它给索引值频道命名（见 `sortable_token`）。
    """
    dtype = value.dtype
    if np.issubdtype(dtype, np.signedinteger):
        data = value.item() + (1 << 63)
        return struct.pack(">Q", data)
    elif np.issubdtype(dtype, np.unsignedinteger):
        return struct.pack(">Q", value)
    elif np.issubdtype(dtype, np.floating):
        double = value.item()
        packed = struct.pack(">d", value)
        [u64] = struct.unpack(">Q", packed)
        # IEEE 754 浮点数排序调整
        if double >= 0:
            # 正数让符号位变1
            u64 = u64 | (1 << 63)
        else:
            # 负数要全部取反，因为浮点负数是绝对值，变成int那种从0xFF递减
            u64 = ~u64 & 0xFFFFFFFFFFFFFFFF
        return struct.pack(">Q", u64)
    elif np.issubdtype(dtype, np.str_):
        encoded = value.item().encode("utf-8")
        # 变长类型把 0x00 转义成 0x00 0xff，使 member 的 value 段能用单个 0x00 自分隔
        # （定长的数字/bool 不会和终止符混淆，无需转义）。详见 _exc_index/range_normalize_
        return encoded.replace(b"\x00", b"\x00\xff")
    elif np.issubdtype(dtype, np.bytes_):
        return value.item().replace(b"\x00", b"\x00\xff")
    elif np.issubdtype(dtype, np.bool_):
        return b"\x01" if value else b"\x00"
    assert False, _("不可排序的索引类型: {dtype}").format(dtype=dtype)


def sortable_token(sortable: bytes) -> str:
    """
    索引值的 sortable bytes → 频道名里的 token。≤32 字节直接 hex（数值都是 8 字节，16 位 hex
    可读且无歧义），更长的字符串取 blake2b-128 摘要并加 `h` 前缀（hex 里不会出现 h）。
    commit 侧和订阅侧、两个后端都用这一个函数，保证同一个值落到同一个频道；摘要碰撞只会让
    订阅者多做一次无害的重查，不会漏通知。

    Channel-name token of an index value: hex for <=32 bytes, else 'h' + blake2b-128 hex.
    """
    if len(sortable) <= 32:
        return sortable.hex()
    return "h" + hashlib.blake2b(sortable, digest_size=16).hexdigest()


def peel_bound_(
    value: float | str | bytes | bool,
) -> tuple[float | str | bytes | bool, bool | None]:
    """
    剥掉区间边界值开头的 `(` / `[` 前缀（str/bytes 才有），返回 (值, 是否闭区间)；
    没有前缀时第二项为 None，由调用方按默认（闭区间）处理。
    两个后端的 range_normalize_ 和 point_query_value_ 都用这一个，规则只写一处。
    """
    if isinstance(value, (str, bytes)) and len(value) >= 1:
        ch = value[0:1]  # bytes 必须用范围切片
        if ch in ("(", b"("):
            return value[1:], False
        if ch in ("[", b"["):
            return value[1:], True
    return value, None


class BackendClient:
    """
    数据库后端的连接类，Backend会用此类创建master, servant连接。

    继承写法：
    class PostgresClient(BackendClient, alias="postgres")

    服务器启动时，Backend会根据Config中type配置，寻找对应alias初始化Client。
    继承此类，完善所有NotImplementedError的方法。
    """

    def index_channel(self, table_ref: TableReference, index_name: str):
        """
        返回整个索引的频道名。该索引上任何值的行增删、任何一行该字段的变更都会通知到该频道，
        供区间订阅用；点查询请用 `index_value_channel`，免得被无关的值叫醒。
        """
        raise NotImplementedError

    def index_value_channel(
        self, table_ref: TableReference, index_name: str, value: Any
    ) -> str:
        """
        返回索引某一个值的频道名。只有 `index_name == value` 的行被 insert/delete，或某行该
        字段从/到这个值变化时，commit 才向此频道发一条消息，payload 为本次事务变动的
        row_id（str）列表；一个事务每个 (索引, 值) 只发一条。点查询订阅用它代替
        `index_channel`，别的值的变动不会打扰。value 先按组件 dtype 规范化
        （`dtype.type(value)`），所以 10、"10"、10.0 得到同一个频道。
        id 索引没有值频道（commit 不发）：点查 id 请订行频道或整个 id 索引的频道。

        Channel of one index value: published on commit only when a row with that value is
        inserted/deleted or a row's field changes from/to it (payload: touched row ids).
        Point-query subscriptions use it instead of `index_channel`.
        """
        raise NotImplementedError

    @staticmethod
    def point_query_value_(
        dtype: np.dtype, left: Any, right: Any | None
    ) -> np.generic | None:
        """
        判断 range 查询是否退化为点查询（right 省略或 left == right），是则返回按 dtype
        规范化后的值，否则返回 None。与 `range_normalize_` 的 peel 规则一致：str/bytes 值的
        `(` 前缀表示开区间，不算点查询；`[` 前缀剥掉。dtype 转换失败（int 索引传 ±inf、
        非法字符串）或 NaN 也返回 None，由调用方回退到整个索引的频道。
        """
        left, left_inclusive = peel_bound_(left)
        right, right_inclusive = (
            (left, left_inclusive) if right is None else peel_bound_(right)
        )
        if left_inclusive is False or right_inclusive is False:
            return None  # 开区间不算点查询
        try:
            left_value, right_value = dtype.type(left), dtype.type(right)
        except ValueError, OverflowError, TypeError:
            return None
        if left_value != right_value:  # NaN != NaN 也在这里回退
            return None
        return left_value

    def row_channel(self, table_ref: TableReference, row_id: int):
        """返回行数据的频道名。如果行有变动，会通知到该频道"""
        raise NotImplementedError

    def table_channel(self, table_ref: TableReference):
        """
        返回表级变更频道名。表内任何行 insert/update/delete，都会向该频道发送一条消息，
        payload 为本次事务变动的 row_id（str）列表。一个事务一张表只发一条。
        """
        raise NotImplementedError

    def __init_subclass__(cls, **kwargs):
        """让继承子类自动注册alias"""
        super().__init_subclass__()
        BackendClientFactory.register(kwargs["alias"], cls)

    def __init__(self, endpoint: Any, is_servant, **kwargs):
        """
        建立数据库连接。
        endpoint为config中master，或者servants的内容。
        is_servant指定endpoint是否为从节点，从节点只读。
        """
        self.endpoint = endpoint
        self.is_servant = is_servant

    async def close(self):
        """关闭数据库连接，释放资源。"""
        raise NotImplementedError

    def post_configure(
        self, components: Iterable[type[BaseComponent]] | None = None
    ) -> None:
        """
        对数据库做的配置工作放在这，可以做些减少运维压力的工作，或是需要项目加载完成后才能做的初始化工作。
        此项在服务器完全加载完毕后才会执行，在测试环境中，也是最后调用。

        components: 要做 schema 检查的组件列表；None 表示取 `SystemClusters` 里被 System
        引用的全部组件（服务器默认）。
        """
        raise NotImplementedError

    async def is_synced(self, checkpoint: Any = None) -> tuple[bool, Any]:
        """
        在master库上查询待各个savants数据库同步状态，防止后续事务获取不到数据。
        主要用于关键节点，比如创建新用户连接。
        checkpoint指数据检查点，如写入日志的行数，检查该点之前的数据是否已同步完成。

        返回是否已完成同步，以及master最新checkpoint（可以用来下一次查询）。
        """
        # assert not self.is_servant, "is_synced只能在master上调用"
        raise NotImplementedError

    # 类型注解部分
    @overload
    async def get(
        self,
        table_ref: TableReference,
        row_id: int,
        row_format: Literal[RowFormat.STRUCT] = RowFormat.STRUCT,
    ) -> np.record | None: ...
    @overload
    async def get(
        self,
        table_ref: TableReference,
        row_id: int,
        row_format: Literal[RowFormat.RAW] = ...,
    ) -> dict[str, str] | None: ...
    @overload
    async def get(
        self,
        table_ref: TableReference,
        row_id: int,
        row_format: Literal[RowFormat.TYPED_DICT] = ...,
    ) -> dict[str, Any] | None: ...
    @overload
    async def get(
        self,
        table_ref: TableReference,
        row_id: int,
        row_format: RowFormat = ...,
    ) -> np.record | dict[str, str] | dict[str, Any] | None: ...
    async def get(
        self, table_ref: TableReference, row_id: int, row_format=RowFormat.STRUCT
    ) -> np.record | dict[str, Any] | None:
        """
        从数据库直接获取单行数据。

        Parameters
        ----------
        table_ref: TableReference
            表信息，指定Component、实例名、分片簇id。
        row_id: int
            row id主键
        row_format
            返回数据解码格式，见 "Returns"

        Returns
        -------
        row: np.record or dict[str, any] or None
            如果未查询到匹配数据，则返回 None。
            否则根据 `row_format` 参数返回以下格式之一：

            - RowFormat.STRUCT - **默认值**
                返回 np.record (c-struct) 的单行数据
            - RowFormat.RAW
                返回无类型的原始数据 (dict[str, str])
            - RowFormat.TYPED_DICT
                返回符合Component定义的，有格式的dict类型。
        """
        raise NotImplementedError

    async def get_many(
        self,
        table_ref: TableReference,
        row_ids: Iterable[int],
        row_format: RowFormat = RowFormat.STRUCT,
    ) -> list[np.record | dict[str, str] | dict[str, Any] | None]:
        """
        批量获取多行数据，一次往返（或按块分批）读取，比循环调用 `get` 快得多。

        Parameters
        ----------
        table_ref: TableReference
            表信息，指定Component、实例名、分片簇id。
        row_ids: Iterable[int]
            row id主键列表。
        row_format
            返回数据解码格式，同 `get`，但不支持 `RowFormat.ID_LIST`。

        Returns
        -------
        rows: list
            与 `row_ids` 顺序一一对应，不存在的行位置为 None。
        """
        raise NotImplementedError

    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 10,
        desc: bool = False,
        row_format: Literal[RowFormat.STRUCT] = RowFormat.STRUCT,
    ) -> np.recarray: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 10,
        desc: bool = False,
        row_format: Literal[RowFormat.RAW] = ...,
    ) -> list[dict[str, str]]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 10,
        desc: bool = False,
        row_format: Literal[RowFormat.TYPED_DICT] = ...,
    ) -> list[dict[str, Any]]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 10,
        desc: bool = False,
        row_format: Literal[RowFormat.ID_LIST] = ...,
    ) -> list[int]: ...
    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 10,
        desc: bool = False,
        row_format: RowFormat = ...,
    ) -> np.recarray | list[dict[str, str]] | list[dict[str, Any]] | list[int]: ...
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 10,
        desc: bool = False,
        row_format=RowFormat.STRUCT,
    ):
        """
        从数据库直接查询索引 `index_name`，返回在 [`left`, `right`] 闭区间内数据。
        如果 `right` 为 `None`，则查询等于 `left` 的数据，限制 `limit` 条。

        Parameters
        ----------
        table_ref: TableReference
            表信息，指定Component、实例名、分片簇id。
        index_name: str
            查询Component中的哪条索引
        left, right: str or number
            查询范围，闭区间。字符串查询时，可以在开头指定是[闭区间，还是(开区间。
            如果right不填写，则精确查询等于left的数据。
        limit: int
            限制返回的行数，越少越快。负数表示不限制行数。
        desc: bool
            是否降序排列
        row_format
            返回数据解码格式，见 "Returns"

        Returns
        -------
        row: np.recarray or list[id] or list[dict]
            根据 `row_format` 参数返回以下格式之一：

            - RowFormat.STRUCT - **默认值**
                返回 `numpy.recarray`，如果没有查询到数据，返回空 `numpy.recarray`。
                `numpy.recarray` 是一种 c-struct array。
            - RowFormat.RAW
                返回无类型的原始数据 (dict[str, str]) 列表，如果没有查询到数据，返回空list
            - RowFormat.TYPED_DICT
                返回符合Component定义的，有格式的dict类型列表，如果没有查询到数据，返回空list
            - RowFormat.ID_LIST
                返回查询到的 row id 列表，如果没有查询到数据，返回空list

        Notes
        -----
        如何复合条件查询？
        请利用python的特性，先在数据库上筛选出最少量的数据，然后本地二次筛选::

            items = client.range(ref, "owner", player_id, limit=100)
            few_items = items[items.amount < 10]

        由于python numpy支持SIMD，比直接在数据库复合查询快。
        """
        raise NotImplementedError

    async def commit(self, idmap: IdentityMap) -> None:
        """
        使用事务，向数据库提交IdentityMap中的所有数据修改

        Exceptions
        --------
        RaceCondition
            数据已被其他事务修改（版本不符）；或主键 / unique 冲突命中了本事务曾 `get`
            观察其不存在的值（基于过期快照），可重试
        UniqueViolation
            主键 / unique 值已被占用，且本事务从未观察其不存在：确定性冲突，不重试

        """
        raise NotImplementedError

    async def direct_set(
        self, table_ref: TableReference, id_: int, **kwargs: str
    ) -> None:
        """
        UNSAFE! 只用于易失数据! 不会做类型检查!

        直接写入属性到数据库，避免session必须要执行get+事务2条指令。
        仅支持非索引字段，索引字段更新是非原子性的，必须使用事务。
        注意此方法可能导致写入数据到已删除的行，请确保逻辑。

        一些系统级别的临时数据，使用直接写入的方式效率会更高，但不保证数据一致性。
        """
        assert table_ref.comp_cls.volatile_, "direct_set只能用于易失数据的Component"
        raise NotImplementedError

    def get_table_maintenance(self) -> TableMaintenance:
        """
        获取表维护对象，根据不同后端类型返回不同的实现。
        """
        raise NotImplementedError

    def get_mq_client(self) -> MQClient:
        """获取消息队列连接"""
        raise NotImplementedError


class BackendClientFactory:
    _registry: dict[str, type[BackendClient]] = {}

    # 内置后端按 alias 懒加载：import 对应子包即触发 BackendClient.__init_subclass__ 注册。
    # 不在 hetu.data.backend 包顶层 eager import，`import hetu` 就不会同时加载
    # redis 与 sqlalchemy 两套重依赖。第三方后端仍靠显式 import 自己的模块注册。
    _BUILTIN_MODULES: ClassVar[dict[str, str]] = {
        "redis": "hetu.data.backend.redis",
        "sql": "hetu.data.backend.sql",
    }

    @staticmethod
    def register(alias: str, client_cls: type[BackendClient]) -> None:
        BackendClientFactory._registry[alias.lower()] = client_cls

    @staticmethod
    def create(
        alias: str, endpoint: Any, is_servant, config: dict[str, Any]
    ) -> BackendClient:
        alias = alias.lower()
        if alias not in BackendClientFactory._registry:
            module = BackendClientFactory._BUILTIN_MODULES.get(alias)
            if module:
                importlib.import_module(module)
        if alias not in BackendClientFactory._registry:
            raise NotImplementedError(_("{alias} 后端未实现").format(alias=alias))
        return BackendClientFactory._registry[alias](endpoint, is_servant, **config)


class TableMaintenance:
    """
    组件表维护类，继承此类实现具体的维护逻辑。

    服务器启动时会用check_table检查各个组件表的状态，并会调用create_table创建新表。

    其他方法仅在CLI相关命令时才会启用。
    """

    @dataclass
    class TableMeta:
        """组件表的meta信息结构"""

        cluster_id: int
        version: str
        json: str
        extra: dict

    def get(self, ref: TableReference, row_id: int) -> np.record | None:
        """获取指定表的指定行数据"""
        raise NotImplementedError

    def range(
        self, ref: TableReference, index_name: str, left: Any, right: Any = None
    ) -> list[int]:
        """按索引范围查询指定表的数据"""
        raise NotImplementedError

    def get_all_row_id(self, ref: TableReference) -> list[int]:
        """获取指定表的所有row id"""
        raise NotImplementedError

    def delete_row(self, ref: TableReference, row_id: int):
        """删除指定表的指定行数据"""
        raise NotImplementedError()

    def upsert_row(self, ref: TableReference, row_data: np.record):
        """更新指定表的一行数据，如果不存在就插入"""
        raise NotImplementedError()

    def read_meta(
        self, instance_name: str, comp: type[BaseComponent] | str
    ) -> TableMeta | None:
        """读取组件表在数据库中的meta信息，如果不存在则返回None。

        `comp` 可以是组件类，也可以只是组件名：meta 只按 `instance_name + 组件名` 定位，
        不持有本地类定义的进程（如 headless client）按名字即可读到服务器写入的 schema
        与 cluster_id。
        """
        raise NotImplementedError

    @staticmethod
    def comp_name_of_(comp: type[BaseComponent] | str) -> str:
        """内部方法，把组件类或组件名统一成组件名"""
        return comp if isinstance(comp, str) else comp.name_

    def get_lock(self) -> AbstractContextManager:
        """获得一个可以锁整个数据库的with锁，在获得锁之前堵塞，获得锁之后可以安全的进行表结构变更等操作，操作完成后释放锁"""
        raise NotImplementedError

    def do_create_table_(self, table_ref: TableReference) -> TableMeta:
        """实际创建组件表的逻辑实现，返回创建后的TableMeta"""
        raise NotImplementedError

    def do_rename_table_(self, from_: TableReference, to_: TableReference) -> None:
        """修改表名的实现，迁移组件表cluster_id用的就是这个，因为水平分片根据表名决定"""
        raise NotImplementedError

    def do_drop_table_(self, table_ref: TableReference) -> int:
        """实际drop组件表数据的逻辑实现，返回删除的行数"""
        raise NotImplementedError

    def do_rebuild_index_(self, table_ref: TableReference) -> int:
        """实际重建组件表索引的逻辑实现，返回重建的行数"""
        raise NotImplementedError

    # === === ===

    def __init__(self, master: BackendClient):
        """传入master连接的BackendClient实例"""
        self.client = master

    @final
    def check_table(self, table_ref: TableReference) -> tuple[str, TableMeta | None]:
        """
        检查组件表在数据库中的状态。
        此方法检查各个组件表的meta键值。

        Parameters
        ----------
        table_ref: TableReference
            传入当前版本的组件表引用，也就是最新的Component定义，最新的Cluster id。
            这些最新引用一般通过ComponentManager获得。

        Returns
        -------
        status: str
            "not_exists" - 表不存在
            "ok" - 表存在且状态正常
            "cluster_mismatch" - 表存在但cluster_id不匹配
            "schema_mismatch" - 表存在但schema不匹配
        meta: TableMeta or None
            组件表的meta信息。用于直接传给migration_cluster_id和migration_schema
        """
        # 从数据库获取已存的组件信息
        meta = self.read_meta(table_ref.instance_name, table_ref.comp_cls)
        if not meta:
            return "not_exists", None
        else:
            version = hashlib.md5(table_ref.comp_cls.json_.encode("utf-8")).hexdigest()
            # 如果cluster_id改变，则迁移改key名，必须先检查cluster_id
            if meta.cluster_id != table_ref.cluster_id:
                return "cluster_mismatch", meta

            # 如果版本不一致，组件结构可能有变化，也可能只是改权限，总之调用迁移代码
            if meta.version != version:
                return "schema_mismatch", meta

        return "ok", meta

    def create_table(self, table_ref: TableReference) -> TableMeta:
        """
        创建组件表。如果已存在，会抛出RaceCondition异常。
        返回组件表的meta信息。
        """
        with self.get_lock():
            if (status := self.check_table(table_ref)[0]) != "not_exists":
                raise RaceCondition(
                    _(
                        "[💾TABLE_MAINT][{comp_name}组件] 无法创建表，组件表状态不对，目前为：{status}"
                    ).format(comp_name=table_ref.comp_name, status=status)
                )
            # 创建表
            logger.info(
                _(
                    "  ➖ [💾TABLE_MAINT][{comp_name}组件] 组件无meta信息，数据不存在，正在创建空表..."
                ).format(comp_name=table_ref.comp_name)
            )
            ret = self.do_create_table_(table_ref)
            logger.info(
                _("  ✔️ [💾TABLE_MAINT][{comp_name}组件] 空表创建完成").format(
                    comp_name=table_ref.comp_name
                )
            )
            return ret

    # 无需drop_table, 此类操作适合人工删除

    def migration_cluster_id(
        self, table_ref: TableReference, old_meta: TableMeta
    ) -> None:
        """迁移组件表的cluster_id"""
        from ..component import BaseComponent
        from .table import TableReference

        with self.get_lock():
            if (status := self.check_table(table_ref)[0]) != "cluster_mismatch":
                raise RaceCondition(
                    _(
                        "[💾TABLE_MAINT][{comp_name}组件] 无法迁移cluster id，组件表状态不对，目前为：{status}"
                    ).format(comp_name=table_ref.comp_name, status=status)
                )
            old_cluster_id = old_meta.cluster_id
            logger.warning(
                _(
                    "  ⚠️ [💾TABLE_MAINT][{comp_name}组件] "
                    "cluster_id 由 {old_id} 变更为 {new_id}，将尝试迁移cluster数据..."
                ).format(
                    comp_name=table_ref.comp_name,
                    old_id=old_cluster_id,
                    new_id=table_ref.cluster_id,
                )
            )
            # 只修改cluster_id
            from_ref = TableReference(
                comp_cls=BaseComponent.load_json(old_meta.json),
                instance_name=table_ref.instance_name,
                cluster_id=old_cluster_id,
            )
            to_ref = TableReference(
                comp_cls=from_ref.comp_cls,
                instance_name=from_ref.instance_name,
                cluster_id=table_ref.cluster_id,
            )
            return self.do_rename_table_(from_ref, to_ref)

    def migration_schema(
        self, app_file: str, table_ref: TableReference, old_meta: TableMeta, force=False
    ) -> bool:
        """
        迁移组件表的schema，本方法必须在migration_cluster_id之后执行。
        此方法调用后需要rebuild_index

        本方法将先寻找是否有迁移脚本，如果有则调用脚本进行迁移，否则使用默认迁移逻辑。

        默认迁移逻辑无法处理数据被删除的情况，以及类型转换失败的情况，
        force参数指定是否强制迁移，也就是遇到上述情况直接丢弃数据。
        """
        with self.get_lock():
            if (status := self.check_table(table_ref)[0]) != "schema_mismatch":
                raise RaceCondition(
                    _(
                        "[💾TABLE_MAINT][{comp_name}组件] 无法迁移，组件表状态不对，目前为：{status}"
                    ).format(comp_name=table_ref.comp_name, status=status)
                )
            from ..migration import MigrationScript

            migrator = MigrationScript(app_file, table_ref, old_meta)

            # 准备和检测
            status = migrator.prepare()
            if status == "unsafe":
                if not force:
                    return False
            elif status == "skip":
                return True

            # 获取所有row id
            row_ids = self.get_all_row_id(table_ref)
            migrator.upgrade(row_ids, self)
            return True

    def flush(self, table_ref: TableReference, force=False) -> None:
        """
        清空易失性组件表数据，force为True时强制清空任意组件表。
        注意：此操作会删除所有数据！
        """
        if force:
            warnings.warn(_("flush正在强制删除所有数据，此方式只建议维护代码调用。"))

        # 如果非持久化组件，则允许调用flush主动清空数据
        if table_ref.comp_cls.volatile_ or force:
            logger.info(
                _(
                    "⌚ [💾TABLE_MAINT][{comp_name}组件] 对非持久化组件flush清空数据中..."
                ).format(comp_name=table_ref.comp_name)
            )

            with self.get_lock():
                count = self.do_drop_table_(table_ref)
                self.do_create_table_(table_ref)

            logger.info(
                _("✅ [💾TABLE_MAINT][{comp_name}组件] 已删除{count}个键值").format(
                    comp_name=table_ref.comp_name, count=count
                )
            )
        else:
            raise ValueError(
                _("{comp_name}是持久化组件，不允许flush操作").format(
                    comp_name=table_ref.comp_name
                )
            )

    def rebuild_index(self, table_ref: TableReference) -> None:
        """重建组件表的索引数据"""
        logger.info(
            _("  ➖ [💾TABLE_MAINT][{comp_name}组件] 正在重建索引...").format(
                comp_name=table_ref.comp_name
            )
        )
        with self.get_lock():
            count = self.do_rebuild_index_(table_ref)
            if count == 0:
                logger.info(
                    _(
                        "  ✔️ [💾TABLE_MAINT][{comp_name}组件] 无数据，无需重建索引。"
                    ).format(comp_name=table_ref.comp_name)
                )
            else:
                logger.info(
                    _(
                        "  ✔️ [💾TABLE_MAINT][{comp_name}组件] 索引重建完成, "
                        "{count}行 * {num_indexes}个索引。"
                    ).format(
                        comp_name=table_ref.comp_name,
                        count=count,
                        num_indexes=len(table_ref.comp_cls.indexes_),
                    )
                )


# === === === === === === 数据订阅 === === === === === ===


class MQClient:
    """
    连接到消息队列的客户端，每个用户连接一个实例。
    继承此类实现数据库写入通知和消息队列的结合。

    本地消息队列由基类维护：后端每个进程共享的通知接收器（如 Redis 的 `PubSubHub`、SQL 的
    `SQLNotifyHub`）收到本连接订阅的频道通知后调 `push_pulled_()` 入队，
    `get_message()` 按 tick 合批弹出。队列只在最老一端弹出，所以是个纯 FIFO。

    尾随重读：通知不带内容，订阅者收到后去读的是随机副本，发通知的节点与读的节点可能不是
    同一个。所以每条通知之后至少隔一个 interval（1/UPDATE_FREQUENCY）才读，这个 interval
    同时是副本复制延迟的预算：单条通知入队后要等 interval 才弹出，天然满足；合并进队头的
    通知离弹出可能不足 interval，弹出时再给它补排一次（见 `get_message`）。复制延迟超过
    预算（Redis 压力过大）时仍可能读到旧值，所以订阅推送是尽力而为的最终一致。
    """

    # todo 加入到config中去，设置服务器的通知tick
    UPDATE_FREQUENCY = 10  # 控制客户端所有订阅的数据（如果有变动），每秒更新几次
    # 本地队列里超过这么多秒没被get_message取走的通知直接丢弃，防止堆积
    DROP_AFTER = 120
    # 表级频道 payload 里的特殊 row_id：这段时间的变更不可知（如 pubsub 断线重连），整表重同步
    RESYNC = "*"

    def __init__(self) -> None:
        # 以下三者内容保持一致（一个频道名在队列里最多出现一次）：
        # (收到时刻 time.monotonic(), 频道名)，按收到时间入队
        self.pulled_deque: deque[tuple[float, str]] = deque()
        # 队列里已有的频道名，去重用
        self.pulled_set: set[str] = set()
        # 表级频道合并后的payload：channel -> 变动的row_id集合
        self.pulled_payload: dict[str, set[str]] = {}
        # 频道已在队列里时又来的通知（被合并）：最近一条的收到时刻，尾随重读的依据
        self._late: dict[str, float] = {}
        # 以及这些迟到通知带来的 row_id（表级频道）：尾随重读只需要重读它们
        self._late_payload: dict[str, set[str]] = {}
        # 服务端内部关注的频道 → 回调（见 watch）
        self._watchers: dict[str, Callable[[], None]] = {}
        # 队列从空变为非空的信号：get_message 空闲时等它，而不是定时醒来看队列
        self._arrived = asyncio.Event()

    async def close(self):
        """取消本连接的全部订阅并释放资源"""
        raise NotImplementedError

    async def watch(self, channel_name: str, callback: Callable[[], None]) -> None:
        """
        服务端内部关注一个频道：订阅它，收到通知时同步调用 `callback`。与客户端订阅
        （`subscribe`）互不干扰：同一频道客户端也订了的话通知照常入队，客户端 `unsubscribe`
        它也不会把这里的关注退掉；只随 `close()` 一起退订。
        回调在后端通知接收器的监听协程里执行，必须非阻塞（置个标记 / create_task），
        不得 await、不得开事务。
        """
        raise NotImplementedError

    def push_pulled_(self, channel_name: str, payload_ids: Iterable[Any] | None) -> int:
        """
        供后端的通知接收器调用：把一条收到的通知放进本地队列（重复频道只保留最早那条，
        以便下个tick就被取走；index更新大都是remove/add两条一起来，靠这个合并），
        表级频道的 payload 按频道合并。消息内容只有channel名：每行数据、每个Index都是
        一个channel，该channel收到了任何消息都说明有数据更新。
        入队前先丢掉超过 `DROP_AFTER` 秒还没被取走的旧通知，返回丢弃的条数，由调用方打日志。
        `watch` 关注的频道先走回调；客户端没订它就到此为止，订了的话照常入队。

        这是每条通知都走的热路径：常态下队头不会过期，只花一次 O(1) 的比较。
        """
        callback = self._watchers.get(channel_name)
        if callback is not None:
            try:
                callback()
            except Exception:  # 别让一个回调拖垮通知接收器的监听协程
                logger.exception(
                    _("⚠️ [MQ] 频道 {channel} 的内部回调异常").format(
                        channel=channel_name
                    )
                )
            if channel_name not in self.subscribed_channels:
                return 0
        return self._enqueue(channel_name, payload_ids)

    def request_reread(
        self, *channel_names: str, payload: Iterable[Any] | None = None
    ) -> None:
        """
        把这些频道当作刚收到一条通知放进本地队列（不触发 `watch` 回调），interval 后照常
        弹出、重读。用于订阅生效后的补读：生效之前已在别的节点上应用、读到的副本却还没应用
        的写入，既不在初始读回的行里，也不会再有通知。

        Queue the channels as if they had just been notified (``watch`` callbacks are not
        fired), so they are re-read one interval later. Used right after a subscription
        becomes active: a write that another node applied before that moment, but the
        replica we read from had not, is neither in the initial rows nor notified again.
        """
        for channel_name in channel_names:
            self._enqueue(channel_name, payload)

    def _enqueue(self, channel_name: str, payload_ids: Iterable[Any] | None) -> int:
        """放进本地队列（同频道合并），返回因 `DROP_AFTER` 丢弃的旧通知条数"""
        now = time.monotonic()
        dropped = 0
        dq = self.pulled_deque
        if dq and dq[0][0] < now - self.DROP_AFTER:
            cutoff = now - self.DROP_AFTER
            while dq and dq[0][0] < cutoff:
                stale = dq.popleft()[1]
                self.pulled_set.discard(stale)
                self.pulled_payload.pop(stale, None)
                self._late.pop(stale, None)
                self._late_payload.pop(stale, None)
                dropped += 1

        ids = None if payload_ids is None else {str(i) for i in payload_ids}
        # 先清旧再合并payload，这样本条消息的payload不会被上面的清理顺手删掉
        if ids is not None:
            self.pulled_payload.setdefault(channel_name, set()).update(ids)
        if channel_name not in self.pulled_set:
            dq.append((now, channel_name))
            self.pulled_set.add(channel_name)
            self._arrived.set()
        else:
            # 合并进已在队列里的那条：弹出时可能离这条不足一个 interval，记下来由
            # get_message 决定要不要补排尾随重读
            self._late[channel_name] = now
            if ids is not None:
                self._late_payload.setdefault(channel_name, set()).update(ids)
        return dropped

    async def get_message(self) -> dict[str, set[str] | None]:
        """
        pop并返回之前pull()到本地的消息，只pop收到时间大于1/UPDATE_FREQUENCY的消息。
        留1/UPDATE_FREQUENCY时间是为了消息的合批。

        返回 {channel名: payload}。行/索引频道的payload为None；
        表级频道的payload为这段时间内合并的变动row_id（str）集合。

        弹出的频道若有合并进来、且离现在不足 interval 的通知，会以现在的时刻重新入队，
        interval 后再弹出一次（尾随重读，见类注释）。

        之后SubscriptionBroker会对该消息进行分析，并重新读取数据库获数据。
        如果没有消息，则堵塞到永远。
        """
        dq = self.pulled_deque
        interval = 1 / self.UPDATE_FREQUENCY
        while True:
            if not dq:
                # 没数据就等 push_pulled_ 的信号。每个连接一个本协程，空闲时定时醒来看队列
                # 是纯粹的底噪（每 1000 个空闲连接约占一个核的 1.6%），等信号则零成本。
                # clear 与 wait 之间没有 await，不会漏掉中间到达的消息
                self._arrived.clear()
                await self._arrived.wait()
                continue
            # 只取收到超过interval的数据，这样可以减少频繁更新（合批）：队列按时间有序，
            # 队头还没到时间就精确睡到那一刻，醒来再看一次队头（期间可能被 DROP_AFTER 清掉）
            wait = dq[0][0] + interval - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
                continue
            cutoff = time.monotonic() - interval
            rtn: dict[str, set[str] | None] = {}
            trailing: list[tuple[str, set[str] | None]] = []
            while dq and dq[0][0] <= cutoff:
                channel_name = dq.popleft()[1]
                self.pulled_set.discard(channel_name)
                rtn[channel_name] = self.pulled_payload.pop(channel_name, None)
                late = self._late.pop(channel_name, None)
                late_ids = self._late_payload.pop(channel_name, None)
                if late is not None and late > cutoff:
                    trailing.append((channel_name, late_ids))
            # 合并进来的通知离这次读不足一个 interval：读可能落在还没应用它的副本上，它的
            # 通知却已经合并掉了。重新入队，interval 后再读一次。用现在的时刻而不是迟到那条
            # 的时刻，队列才保持按时间有序；持续写入时它正好顶替下一批的队头，读的次数不变
            if trailing:
                now = time.monotonic()
                for channel_name, late_ids in trailing:
                    dq.append((now, channel_name))
                    self.pulled_set.add(channel_name)
                    if late_ids is not None:
                        self.pulled_payload[channel_name] = late_ids
            if rtn:
                return rtn

    async def subscribe(self, *channel_names: str) -> None:
        """订阅频道，可一次订阅多个，全部订阅成功后返回。实现应把多个频道合并成尽量少的往返。"""
        raise NotImplementedError

    async def unsubscribe(self, *channel_names: str) -> None:
        """取消订阅频道，可一次取消多个"""
        raise NotImplementedError

    @property
    def subscribed_channels(self) -> set[str]:
        """返回当前订阅的频道名"""
        raise NotImplementedError


class MQHub:
    """
    每个进程共享的通知接收器基类：持有到后端的唯一订阅连接/轮询任务，维护"频道 → 本进程内
    订阅了它的 MQClient"分发表（同时是引用计数），收到通知后调各 MQClient 的 `push_pulled_`。
    这里是与后端无关的登记/撤销/分发/后台任务簿记；后端实现 `add` / `remove` 和自己的收发。
    `HubMQClient` 只依赖 `add` / `remove`。
    """

    def __init__(self) -> None:
        self._subs: dict[str, set[MQClient]] = {}
        # 后台任务（退订、取水位……）：不随调用方一起取消，保存引用免得被 gc，close 时统一取消
        self._tasks: set[asyncio.Task] = set()
        self._closed = False

    @property
    def channels(self) -> set[str]:
        """本进程当前向后端订阅了的频道"""
        return set(self._subs)

    def subscriber_count(self, channel: str) -> int:
        """某频道在本进程内的订阅连接数，测试用"""
        return len(self._subs.get(channel, ()))

    async def add(self, mq: MQClient, channels: Iterable[str]) -> None:
        """登记 mq 对这些频道的订阅，返回时订阅已生效"""
        raise NotImplementedError

    async def remove(self, mq: MQClient, channels: Iterable[str]) -> None:
        """撤销 mq 对这些频道的订阅，本进程内没人再订的频道才真正向后端退订"""
        raise NotImplementedError

    def _release(self, mq: MQClient, channels: Iterable[str]) -> list[str]:
        """撤销 mq 对这些频道的登记，返回本进程内因此没人再订的频道"""
        gone = []
        for channel in channels:
            subs = self._subs.get(channel)
            if subs is None:
                continue
            subs.discard(mq)
            if not subs:
                del self._subs[channel]
                self._on_channel_gone(channel)
                gone.append(channel)
        return gone

    def _on_channel_gone(self, channel: str) -> None:
        """某频道在本进程内没人订了：子类清理自己按频道记的状态"""

    def _dispatch(self, channel_name: str, ids: list | None) -> int:
        """把一条通知塞进本进程订阅了该频道的各连接的本地队列，返回丢弃的过期通知条数"""
        dropped = 0
        for mq in self._subs.get(channel_name, ()):
            dropped += mq.push_pulled_(channel_name, ids)
        return dropped

    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        task.add_done_callback(self._tasks.discard)
        self._tasks.add(task)
        return task

    async def _cancel_tasks(self) -> None:
        tasks = [t for t in self._tasks if not t.done()]
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()


class HubMQClient(MQClient):
    """
    挂在进程共享 `MQHub` 上的轻量 MQClient：本身只记录本连接订阅了哪些频道，
    订阅/退订转发给 hub。后端实现只需继承并指定 `LOG_TAG`。
    """

    # 单个连接订阅频道数的告警线；子类可覆盖
    MAX_SUBSCRIBED = 5000
    LOG_TAG = "MQ"

    def __init__(self, hub: MQHub):
        super().__init__()  # 本地消息队列
        self._hub = hub
        # 客户端订阅的频道；服务端内部关注（watch）的频道另记一份，两者可以重叠：
        # hub 按 MQClient 计数，同一频道只登记一次，所以客户端退订时要看它是不是还被关注着
        self.subscribed: set[str] = set()
        self._watched: set[str] = set()
        self._closed = False

    async def close(self):
        """取消本连接的全部订阅（含内部关注的）。连接拆除路径上调用，后端出错也不抛"""
        self._closed = True
        channels = self.subscribed | self._watched
        self.subscribed = set()
        self._watched = set()
        self._watchers.clear()
        if channels:
            try:
                await self._hub.remove(self, channels)
            except Exception as e:  # noqa: BLE001 拆连接不能因为后端异常半途而废
                logger.warning(
                    _("⚠️ [{tag}] 关闭连接时取消订阅失败：{err}").format(
                        tag=self.LOG_TAG, err=f"{type(e).__name__}:{e}"
                    )
                )

    async def watch(self, channel_name: str, callback: Callable[[], None]) -> None:
        if self._closed:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        # 先登记回调再订阅：订阅生效到登记之间的通知不能落进客户端推送队列
        self._watchers[channel_name] = callback
        self._watched.add(channel_name)
        try:
            await self._hub.add(self, [channel_name])
        except BaseException:
            self._watched.discard(channel_name)
            self._watchers.pop(channel_name, None)
            raise
        if self._closed:
            await self._hub.remove(self, [channel_name])
            raise ConnectionError(_("连接已关闭，已调用过close"))

    async def subscribe(self, *channel_names: str) -> None:
        """订阅频道（可多个，一次往返），频道名通过 client.xxx_channel(table_ref) 获得"""
        if not channel_names:
            return
        if self._closed:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        # 先记再等：等 ack 期间本连接可能又 unsubscribe/close 了其中的频道，由它们从
        # subscribed 和 hub 里撤掉；add 返回后不能再把这些频道加回来
        new = [name for name in channel_names if name not in self.subscribed]
        self.subscribed.update(new)
        try:
            await self._hub.add(self, channel_names)
        except BaseException:
            self.subscribed.difference_update(new)
            raise
        if self._closed:
            # 等订阅生效期间连接被关了：撤销刚登记的订阅，别留在 hub 里
            await self._hub.remove(self, channel_names)
            raise ConnectionError(_("连接已关闭，已调用过close"))
        if len(self.subscribed) > self.MAX_SUBSCRIBED:
            logger.warning(
                _(
                    "⚠️ [{tag}] 当前连接订阅数超过全局限制MAX_SUBSCRIBED={limit}行"
                ).format(tag=self.LOG_TAG, limit=self.MAX_SUBSCRIBED)
            )

    async def unsubscribe(self, *channel_names: str) -> None:
        """取消订阅频道（可多个），频道名通过 client.xxx_channel(table_ref) 获得"""
        if not channel_names:
            return
        self.subscribed.difference_update(channel_names)
        # 服务端还关注着的频道只是客户端不要了，hub 里的登记得留着
        gone = [name for name in channel_names if name not in self._watched]
        if gone:
            await self._hub.remove(self, gone)

    @property
    def subscribed_channels(self) -> set[str]:
        """返回当前连接订阅的所有频道名"""
        return self.subscribed
