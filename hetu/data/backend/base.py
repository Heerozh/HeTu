"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com


                               Backend相关结构
    ┌─────────────────┐      ┌────────────────┐       ┌───────────────────┐
    │     MQClient    │      │  BackendClient │       │  TableMaintenance │
    │消息队列(每个订阅器)│─────►│  数据库连接/操作 │◄──────┤    组件表维护类     │
    └─────────────────┘      └────────────────┘       └───────────────────┘
    继承此类实现各种通知队列      继承此类实现各种数据库         继承此类实现表维护
            ▲                        ▲                         ▲
            │                        └───────────┬─────────────┘
 数据订阅结构 │                                    │ 数据事务结构
  ┌─────────┴──────────┐               ┌─────────┴──────────┐
  │  SubscriptionHub   │               │      Backend       │
  │ 每个进程一个的订阅处理器 │               │  数据库连接管理器    │ 每个进程一个Backend
  └────────────────────┘               └────────────────────┘
            ▲                                    ▲
  ┌─────────┴──────────┐                ┌────────┴─────────┐
  │ SubscriptionBroker │                │      Session     │
  │ 每个连接一个的订阅门面 │                │     事务处理类     │
  └────────────────────┘                └──────────────────┘
            ▲                                    ▲
  ┌─────────┴──────────┐               ┌─────────┴──────────┐
  │ 用户连接(Websocket) │               │  SessionRepository │
  │   等待Subs返回消息   │               │   组件相关事务操作    │
  └────────────────────┘               └────────────────────┘

"""

import asyncio
import hashlib
import importlib
import logging
import math
import random
import re
import struct
import time
import warnings
from collections import deque
from collections.abc import (
    Callable,
    Collection,
    Coroutine,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    NamedTuple,
    final,
    overload,
)

import numpy as np

from ...i18n import _

if TYPE_CHECKING:
    from ..component import BaseComponent
    from .idmap import IdentityMap, RangeObservation
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
    - 提交时，本事务 `range`（及非 unique 列的 `get`）读过的区间变了：同样的查询现在会
      返回不同的行，典型如别的事务往区间里插了一行（幻读）、或读到了滞后的副本；读取
      过程中索引里的行被改走 / 删掉（读到的不是任何一刻的区间）同样判竞态。见
      `SessionRepository.range` 的 `phantom_check`；
    - 表维护、连接保活等内部流程检测到依赖状态已被其他执行流改变。

    `SystemCaller` 和 `Session.retry(...)` 会捕获此异常并重新执行事务。
    """


class InconsistentRangeRead(RaceCondition):
    """
    range 读到的行和索引对不上：索引里有这个 id，取行时却读不到，或读到的行已经不在索引
    说的那个值上。

    通常是正常的竞态（两次读之间行被删改了），重试即可。如果同一行每次重试都对不上，就是
    索引里残留了和行数据不一致的项（比如维护脚本只删了行、没删索引），重试不会自愈，
    `SystemCaller` 会打一条 error 日志提示重建索引。
    """

    def __init__(self, comp_name: str, index_name: str, row_id: int):
        super().__init__(comp_name, index_name, row_id)
        self.comp_name = comp_name
        self.index_name = index_name
        self.row_id = row_id

    def __str__(self) -> str:
        return (
            f"RACE: Inconsistent range read {self.comp_name}.{self.index_name} "
            f"id={self.row_id}"
        )


class CommitSeqDiverged(Exception):
    """
    The commit_seq sequence no longer matches what the consumer recorded.

    提交序号列的序列和库外消费方记下的对不上了，由 `BackendClient.seq_read` 抛出。重试不会好，
    所以不继承 RaceCondition：这一组要停下来、报警，由人来处理，别的组照常消费。

    `reason` 是下面三种之一：

    - ``"reused"``：游标那一号现在是另一行，号被复用了。可能是数据回退（故障切换丢了最后几次提交、
      AOF / RDB 重启、整库从备份恢复、易失表清空），也可能是计数器落后于数据之后又发了一遍（手工删了
      计数器、内存淘汰、只导入了行、改了组件名或字段名之后重新校准）；
    - ``"anchor_missing"``：游标那一行没了，后面却有号。数据回退了，或者游标那一行被误删了（按文档的
      裁剪规则，游标那一行不会被正常删掉）；
    - ``"duplicate"``：组里有重号，计数器落后于数据时会发出重号。

    属性：`comp_name`、`group`（调用方给的原值）、`seq`（出问题的那一号）、`expected_id`、
    `found_id`（`anchor_missing` 时为 None）、`reason`。
    """

    def __init__(
        self,
        comp_name: str,
        group: Mapping[str, Any] | None,
        seq: int,
        expected_id: int,
        found_id: int | None,
        reason: str,
    ):
        super().__init__(comp_name, group, seq, expected_id, found_id, reason)
        self.comp_name = comp_name
        self.group = group
        self.seq = seq
        self.expected_id = expected_id
        self.found_id = found_id
        self.reason = reason

    def __str__(self) -> str:
        return (
            f"COMMIT_SEQ: {self.reason} {self.comp_name} group={self.group!r} "
            f"seq={self.seq} expected_id={self.expected_id} found_id={self.found_id}"
        )


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


class RowFormat(Enum):
    """行格式枚举"""

    RAW = 0  # 未经类型转换的dict格式，具体类型由数据库决定
    STRUCT = 1  # 默认值：按Component定义严格转换的np.record（c-struct like）类型
    TYPED_DICT = 2  # 先转换成STRUCT，再转换成dict的类型。
    ID_LIST = 3  # 只返回list of row id，只能用于range查询


class SeqReadResult[RowsT](NamedTuple):
    """
    Result of `BackendClient.seq_read`: the rows read and what blocked the read.

    提交序号列增量读（`seq_read`）的结果，可以直接解包：``rows, blocked = ...``。

    - `rows`：号大于 `after` 的行，按号升序，格式按 `row_format`；
    - `blocked`：``(号, 行 id)`` 或 None。索引里有、这次却没取到行的第一个 member，它挡住了后面的
      行；None 表示没被挡住。取行落到落后的副本、读完索引之后行被删了都会挡住，下一次通常就好了；
      同一个 ``(号, 行 id)`` 一直挡着，多半是索引残留（索引里有、行没有）。
    """

    rows: RowsT
    blocked: tuple[int, int] | None


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
        if math.isnan(double):
            # NaN 不论符号位一律按正 NaN 编码：commit 写索引前把值转成文本再解析，符号位丢了，
            # 事务里给本事务的行算的排序键要和写进去的一致
            double = math.nan
        packed = struct.pack(">d", double)
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
    # define_component 已把 bool 字段强制转成 int8，组件 dtype 里不会出现 bool，走不到
    # elif np.issubdtype(dtype, np.bool_):
    #     return b"\x01" if value else b"\x00"
    assert False, _("不可排序的索引类型: {dtype}").format(dtype=dtype)


def to_sortable_bytes_list(values: np.ndarray) -> list[bytes]:
    """
    `to_sortable_bytes` 的批量版：把一列同 dtype 的值逐个编码，结果与逐个调用相同。数值列
    向量化，字符串列逐个编码。
    """
    dtype = values.dtype
    if np.issubdtype(dtype, np.signedinteger):
        # 加 2^63 就是翻转 int64 的符号位
        keys = values.astype(np.int64).view(np.uint64) ^ np.uint64(1 << 63)
    elif np.issubdtype(dtype, np.unsignedinteger):
        keys = values.astype(np.uint64)
    elif np.issubdtype(dtype, np.floating):
        doubles = values.astype(np.float64)
        nan = np.isnan(doubles)
        if nan.any():
            # 同 to_sortable_bytes：NaN 一律按正 NaN 编码
            doubles = np.where(nan, np.nan, doubles)
        bits = doubles.view(np.uint64)
        # 同 to_sortable_bytes：>= 0（含 -0.0）置符号位，负数和 NaN 整体取反
        keys = np.where(doubles >= 0, bits | np.uint64(1 << 63), ~bits)
    elif np.issubdtype(dtype, np.str_):
        return [
            value.encode("utf-8").replace(b"\x00", b"\x00\xff")
            for value in values.tolist()
        ]
    elif np.issubdtype(dtype, np.bytes_):
        return [value.replace(b"\x00", b"\x00\xff") for value in values.tolist()]
    else:
        assert False, _("不可排序的索引类型: {dtype}").format(dtype=dtype)
    raw = keys.astype(">u8").tobytes()
    return [raw[i : i + 8] for i in range(0, len(raw), 8)]


def from_sortable_bytes(dtype: np.dtype, sortable: bytes) -> np.generic:
    """
    `to_sortable_bytes` 的逆：索引 member 的值段还原成 dtype 的值。只读索引就能拿到值，不用再
    读行（见 `BackendClient.range_index_`）。
    """
    if np.issubdtype(dtype, np.signedinteger):
        (data,) = struct.unpack(">Q", sortable)
        return dtype.type(data - (1 << 63))
    elif np.issubdtype(dtype, np.unsignedinteger):
        (data,) = struct.unpack(">Q", sortable)
        return dtype.type(data)
    elif np.issubdtype(dtype, np.floating):
        (u64,) = struct.unpack(">Q", sortable)
        # 编码时正数置了符号位、负数整体取反，这里反过来
        if u64 & (1 << 63):
            u64 &= ~(1 << 63)
        else:
            u64 = ~u64 & 0xFFFFFFFFFFFFFFFF
        (double,) = struct.unpack(">d", struct.pack(">Q", u64))
        return dtype.type(double)
    elif np.issubdtype(dtype, np.str_):
        return dtype.type(sortable.replace(b"\x00\xff", b"\x00").decode("utf-8"))
    elif np.issubdtype(dtype, np.bytes_):
        return dtype.type(sortable.replace(b"\x00\xff", b"\x00"))
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


# 索引 member 是 值段 + 0x00 + id 段。id 段是 id + 2^63 的 16 位小写 hex，也就是 int64 的
# `to_sortable_bytes` 的 hex：定宽，整个 int64 范围都保序（含按 key 的未来调用用的负 id），
# 同一个值下的行就按 id 的数值排；只有 0-9a-f，不含 0x00 / 0xff，"最后一个 0x00 是终止符"、
# 区间两端的 \x00 / \x00\xff 照旧成立。以前是不补零的十进制，雪花 id 从 18 位进到 19 位
# （2033-07）之后同值的行会乱序，"取某个值下最新 N 行"会取错
ROW_ID_TOKEN_LEN = 16
_ROW_ID_OFFSET = 1 << 63


def row_id_token_(row_id: int | np.integer) -> bytes:
    """内部方法：行 id → 索引 member 末尾的 id 段（见 `ROW_ID_TOKEN_LEN`）"""
    # 先转 int：np.int64 加 2^63 会溢出
    return b"%016x" % (int(row_id) + _ROW_ID_OFFSET)


def index_member_(value: bytes, row_id: int | np.integer) -> bytes:
    """内部方法：值段 + 行 id → 索引 member"""
    return value + b"\x00" + row_id_token_(row_id)


def member_row_id_(member: bytes) -> int:
    """内部方法：索引 member → 行 id。id 段定宽，取最后 `ROW_ID_TOKEN_LEN` 个字节"""
    return int(member[-ROW_ID_TOKEN_LEN:], 16) - _ROW_ID_OFFSET


def member_value_(member: bytes) -> bytes:
    """内部方法：索引 member 去掉 0x00 + id 段，剩下的值段（复合索引带组前缀）"""
    return member[: -ROW_ID_TOKEN_LEN - 1]


# 迁移脚本搬完了数据、重建索引却失败时（unique 冲突、提交序号列组内重复……）写进 meta 的版本号。
# 这时数据已经是新定义的、索引没建成、提交序号列的计数器也没校准：版本号故意和任何定义都对不上，
# 服务器拒绝启动；修好数据后再跑 hetu upgrade，会按当前定义直接重建
REBUILD_PENDING_VERSION = "rebuild-pending"

# 提交序号列（commit_seq）的号的上限（不含）：提交脚本里的数是 Lua 的 double，再往上不精确。
# 提交时到了上限返回 SEQ 失败，重建时库里已有的号到了上限报错
COMMIT_SEQ_LIMIT = 1 << 53

# Redis 认的整数串（string2ll）：可选负号加不以 0 开头的数字，或者就是 "0"
_REDIS_INT = re.compile(rb"0|-?[1-9][0-9]*")


def parse_redis_int_(raw: bytes) -> int | None:
    """
    内部方法：按 Redis string2ll 的规则（INCRBY 认的整数）解析一个字符串值，不是整数返回 None。
    可选的负号加不以 0 开头的数字，或者就是 "0"，在 int64 范围里；Python 的 int() 会放过
    " 5"、"+5"、"05"、"1_0"，两个后端就对不上了
    """
    if not _REDIS_INT.fullmatch(raw):
        return None
    value = int(raw)
    return value if -(1 << 63) <= value < (1 << 63) else None


# 索引的存储格式版本，并进表的 schema 版本号（见 `schema_version_`）。member 的编码一变就加一：
# 旧库每张表的版本号都对不上，服务器拒绝启动，`hetu upgrade` 按新格式重建索引。
# 1：id 段是不补零的十进制；2：id 段定宽（`ROW_ID_TOKEN_LEN`）
INDEX_FORMAT = 2


def schema_version_(json_: str, index_format: int = INDEX_FORMAT) -> str:
    """
    内部方法：组件表 meta 里的 schema 版本号，由组件定义的 json 与索引格式版本一起算出。只改了
    权限这类不动数据的定义版本号也会变，`hetu upgrade` 会把 meta 补写成新的。迁移脚本的文件名
    也用它（`*_v旧版本_to_v新版本.py`）。索引格式 1 的版本号就是 json 的 md5，旧版 HeTu 写的
    meta 和生成的迁移脚本都是这个
    """
    data = json_ if index_format == 1 else f"{json_}\nindex_format={index_format}"
    return hashlib.md5(data.encode("utf-8")).hexdigest()


def schema_versions_(json_: str) -> set[str]:
    """内部方法：这份组件定义在各个索引格式下的版本号，认旧版 HeTu 生成的迁移脚本用"""
    return {schema_version_(json_, fmt) for fmt in range(1, INDEX_FORMAT + 1)}


def index_format_stale_(meta: TableMaintenance.TableMeta) -> bool:
    """
    内部方法：表的索引是不是旧格式。meta 的版本号写入时总是由同一份 meta 的 json 按当时的
    `INDEX_FORMAT` 算出来，按当前格式重算对不上就是旧格式写的
    """
    return meta.version != schema_version_(meta.json)


# 复合索引里 str / bytes 分组列编码后面跟的终止符。不能用单个 0x00：值的转义是 0x00 → 0x00 0xff，
# 下一列的编码以 0xff 开头时（整数最大值、浮点 +inf / NaN、0xff 开头的 bytes）比较会越过
# 终止符，把 "a\x00b" 组的行算进 "a" 组的区间。0x00 0x01 和转义的 0x00 0xff 在第二个字节就分开了
GROUP_TERMINATOR = b"\x00\x01"


def _group_value_error(comp_cls: type[BaseComponent], col: str, value: Any, why: str):
    return ValueError(
        _("{comp_name}.{col} 的分组值 {value} 不合法：{why}").format(
            comp_name=comp_cls.name_, col=col, value=repr(value), why=why
        )
    )


def overflows_column_(comp_cls: type[BaseComponent], field: str, value: Any) -> bool:
    """
    内部方法：str / bytes 值存进字符串列（U / S）会不会被截断。列宽按字符数（S 列是字节数），
    结尾的 `\\x00` 是补齐、本来就存不住，不算在内。别的类型的值、别的列都返回 False
    """
    if comp_cls.dtype_map_[field].kind not in "US":
        return False
    if isinstance(value, str):
        value = value.rstrip("\x00")
    elif isinstance(value, bytes):
        value = value.rstrip(b"\x00")
    else:
        return False
    return len(value) > comp_cls.str_max_len(field)


def resolve_group_value_(comp_cls: type[BaseComponent], col: str, value: Any) -> Any:
    """
    内部方法：一个分组值按分组列的 dtype 规范化成 python 值，不合法抛 ValueError。

    不能直接 `dtype.type(value)`：整数会静默回绕或截断（`np.int8(np.int64(300))` 是 44、
    `np.int8(1.5)` 是 1），python int 越界抛的又是 OverflowError；字符串结尾的 `\\x00` 也还在，
    和存进库里的值对不上。所以整数要先按精确值判断范围；字符串超过列宽同样报错（存进去会被
    截断，前 N 个字符相同的值落进同一组，按截断后的值查会拿到别的组的行），放得下的按存储
    规则（`np.array(v, dtype)[()]`）去掉结尾的 `\\x00`。

    整数列也收数字字符串，和单列索引的查询值同一套解析（`exact_number_`）：先按整数、不行再按
    小数，所以 `"3"`、`"3.0"` 都等于 3。开闭区间的 `(` / `[` 前缀分组值用不上，带了就解析不了
    """
    dtype = comp_cls.dtype_map_[col]
    kind = dtype.kind
    if kind in "iu":
        if isinstance(value, (bool, np.bool_, int, np.integer)):
            number = int(value)
        elif isinstance(value, (float, np.floating, str, bytes)):
            try:
                exact = exact_number_(value)
            except ValueError:  # 含 bytes 不是 utf-8 时的 UnicodeDecodeError
                raise _group_value_error(comp_cls, col, value, _("不是数字")) from None
            if isinstance(exact, float):
                if not exact.is_integer():  # 小数、±inf、NaN
                    raise _group_value_error(comp_cls, col, value, _("不是整数"))
                exact = int(exact)
            number = exact
        else:
            raise _group_value_error(
                comp_cls, col, value, _("整数列只收数字或数字字符串")
            )
        info = np.iinfo(dtype)
        if not info.min <= number <= info.max:
            raise _group_value_error(
                comp_cls, col, value, _("超出 {dtype} 的范围").format(dtype=dtype)
            )
        return number
    if kind == "U":
        if not isinstance(value, str):
            raise _group_value_error(comp_cls, col, value, _("str 列只收 str"))
    elif kind == "S":
        if isinstance(value, str):
            try:
                value = value.encode("ascii")
            except UnicodeEncodeError:
                raise _group_value_error(
                    comp_cls, col, value, _("bytes 列的 str 值只能是 ASCII")
                ) from None
        elif not isinstance(value, bytes):
            raise _group_value_error(comp_cls, col, value, _("bytes 列只收 bytes"))
    if kind in "US":
        if overflows_column_(comp_cls, col, value):
            raise _group_value_error(
                comp_cls,
                col,
                value,
                _("超过列宽 {max_len}").format(max_len=comp_cls.str_max_len(col)),
            )
        return np.array(value, dtype=dtype)[()].item()
    # define_component 已限定分组列的类型，走不到
    raise _group_value_error(comp_cls, col, value, _("不支持的分组列类型"))


def resolve_group_(
    comp_cls: type[BaseComponent], index_name: str, group: Any
) -> tuple[Any, ...]:
    """
    内部方法：查询 / 订阅给的分组值（{分组列: 值}）→ 按分组列声明顺序、规范化后的元组。单列索引
    返回 ()。复合索引没给、少给、多给分组值，单列索引给了分组值，值不合法，都抛 ValueError。
    None 与 {} 等价。索引不存在时也抛 ValueError。
    """
    columns = comp_cls.index_groups_.get(index_name)
    if columns is None:
        raise ValueError(
            _("{comp_name} 组件没有叫 {index_name} 的索引").format(
                comp_name=comp_cls.name_, index_name=index_name
            )
        )
    if group is not None and not isinstance(group, Mapping):
        raise ValueError(
            _("分组值必须是 {{分组列: 值}} 的 dict，收到：{group}").format(
                group=repr(group)
            )
        )
    if not group:
        if columns:
            raise ValueError(
                _(
                    "{comp_name}.{index_name} 是按 {columns} 分组的索引，"
                    "查询要同时给出这些分组列的值"
                ).format(
                    comp_name=comp_cls.name_,
                    index_name=index_name,
                    columns=", ".join(columns),
                )
            )
        return ()
    if not columns:
        raise ValueError(
            _("{comp_name}.{index_name} 不是复合索引，不能给分组值").format(
                comp_name=comp_cls.name_, index_name=index_name
            )
        )
    # 先查键的类型再比集合：msgpack 解出来的键可能是 int / bytes，混着排序会抛 TypeError
    if not all(type(key) is str for key in group) or set(group) != set(columns):
        raise ValueError(
            _(
                "{comp_name}.{index_name} 的分组列是 {columns}，分组值要正好给出这些列，"
                "收到：{keys}"
            ).format(
                comp_name=comp_cls.name_,
                index_name=index_name,
                columns=", ".join(columns),
                keys=repr(list(group)),
            )
        )
    return tuple(resolve_group_value_(comp_cls, col, group[col]) for col in columns)


def _group_part(dtype: np.dtype, value: Any) -> bytes:
    encoded = to_sortable_bytes(dtype.type(value))
    return encoded + GROUP_TERMINATOR if dtype.kind in "US" else encoded


def group_prefix_(
    comp_cls: type[BaseComponent], index_name: str, values: tuple[Any, ...]
) -> bytes:
    """内部方法：`resolve_group_` 之后的分组值 → 复合索引 member 的组前缀 P，单列索引是 b"" """
    dtype_map = comp_cls.dtype_map_
    return b"".join(
        _group_part(dtype_map[col], value)
        for col, value in zip(comp_cls.index_groups_[index_name], values)
    )


def row_group_prefix_(
    comp_cls: type[BaseComponent], index_name: str, row: Any
) -> bytes:
    """
    内部方法：一行在复合索引上的组前缀 P，单列索引是 b""。row 是 np.record，或者 commit 用的
    {列: str / bytes}（按列 dtype 转回）
    """
    dtype_map = comp_cls.dtype_map_
    return b"".join(
        _group_part(dtype_map[col], row[col])
        for col in comp_cls.index_groups_[index_name]
    )


def index_value_bytes_(
    comp_cls: type[BaseComponent], index_name: str, row: Any
) -> bytes:
    """
    内部方法：一行在索引上的值段，即 member 去掉 `\\x00 + id`：组前缀 P 加本字段的编码，单列索引
    就是本字段的编码。row 同 `row_group_prefix_`
    """
    value = to_sortable_bytes(comp_cls.dtype_map_[index_name].type(row[index_name]))
    if not comp_cls.index_groups_[index_name]:
        return value
    return row_group_prefix_(comp_cls, index_name, row) + value


def index_value_bytes_list_(
    comp_cls: type[BaseComponent], index_name: str, rows: np.ndarray
) -> list[bytes]:
    """`index_value_bytes_` 的批量版，结果与逐行调用相同：每列向量化编码再逐行拼接"""
    values = to_sortable_bytes_list(rows[index_name])
    groups = comp_cls.index_groups_[index_name]
    if not groups:
        return values
    parts: list[list[bytes]] = []
    for col in groups:
        encoded = to_sortable_bytes_list(rows[col])
        if comp_cls.dtype_map_[col].kind in "US":
            encoded = [part + GROUP_TERMINATOR for part in encoded]
        parts.append(encoded)
    parts.append(values)
    return [b"".join(row_parts) for row_parts in zip(*parts)]


def group_query_key_(group: Mapping[str, Any] | None) -> tuple[tuple[str, str], ...]:
    """
    内部方法：订阅的 sub_id、共享键里代表分组值的部分，按原值（repr）、键按名字排序。和 left /
    right 一样不规范化：共享键与 sub_id 用同一个口径才能保证"同键一定同 sub_id"；1 与 True、
    3 与 3.0 分成两个订阅只是少共享些
    """
    if not group:
        return ()
    return tuple((key, repr(group[key])) for key in sorted(group))


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


def literal_bound_(value: Any) -> Any:
    """
    内部方法：点查的值按字面比较时用的区间边界。str / bytes 以 `(` / `[` 开头时前面补一个 `[`：
    `peel_bound_` 只剥掉补上的这一个，剩下的原样比较，开头的 `(` / `[` 不会被当成开 / 闭区间
    标记。get、upsert、订阅单行这类点查用它；range 的边界照旧解释前缀
    """
    if isinstance(value, str) and value[:1] in ("(", "["):
        return "[" + value
    if isinstance(value, bytes) and value[:1] in (b"(", b"["):
        return b"[" + value
    return value


def inverted_bounds_error_(lower: Any, upper: Any) -> ValueError:
    """内部方法：区间下界大于上界（多半是参数传反了）的报错，两个后端共用这一句"""
    return ValueError(
        _(
            "区间的下界大于上界：下界={lower}，上界={upper}"
            "（desc=True 时也按 (下界, 上界) 传）"
        ).format(lower=repr(lower), upper=repr(upper))
    )


def exact_number_(value: Any) -> int | float:
    """
    内部方法：整数索引的区间边界（已剥掉 `(` / `[` 前缀）转成精确的 Python 数。不按 dtype
    转换，越界不会溢出、小数也不会被截断。str / bytes 先按整数解析，不行再按浮点，都不行
    抛 ValueError。
    """
    if isinstance(value, (bool, int, np.integer)):
        return int(value)
    if isinstance(value, (str, bytes)):
        text = value.decode() if isinstance(value, bytes) else value
        try:
            return int(text)
        except ValueError:
            return float(text)
    return float(value)


def normalize_int_bounds_(
    dtype: np.dtype,
    lower: float,
    lower_inclusive: bool,
    upper: float,
    upper_inclusive: bool,
) -> tuple[int, int] | None:
    """
    内部方法：整数索引的区间按数学含义收成 dtype 范围内的闭区间 (lo, hi)，区间里没有整数时
    返回 None。边界先用 `exact_number_` 转成精确的数：

    - 小数边界向区间内取整：x >= 0.5 即 x >= 1，x <= 1.5 即 x <= 1；
    - 开区间的整数边界收进一格：x > 5 即 x >= 6；
    - 超出 dtype 范围的边界（含 ±inf）钳到极值：int8 列上 x <= 1000 即 x <= 127。整个区间
      都在范围外，或者像 (inf, inf)、(1.2, 1.8) 这样里面没有整数，就是空。

    NaN 边界抛 ValueError。下界大于上界要调用方先用原始值判定：那是参数传反了，应该报错，
    而不是当成空区间。Redis 与 SQLite 后端都经 `RedisModelClient.range_normalize_` 按这个规则
    处理整数区间。
    """
    info = np.iinfo(dtype)

    def edge(value: float, inclusive: bool, inward: int) -> float:
        # inward：下界 +1（往上收）、上界 -1（往下收）。±inf 原样返回，由下面钳到极值
        if isinstance(value, float):
            if math.isnan(value):
                raise ValueError(_("整数索引的区间边界不能是 NaN"))
            if math.isinf(value):
                return value
            if not value.is_integer():
                return math.ceil(value) if inward > 0 else math.floor(value)
            value = int(value)
        return value if inclusive else value + inward

    lo = max(edge(lower, lower_inclusive, 1), info.min)
    hi = min(edge(upper, upper_inclusive, -1), info.max)
    if lo > hi:
        return None
    return int(lo), int(hi)


def detach_rows_(batch: np.recarray) -> Iterator[np.record]:
    """
    内部方法：把一次解码的一批行逐行拷成互相独立的 record，给 `get_many` 用。直接交出整批
    数组的视图的话，调用方只留一行也会拖住整批。
    """
    # 迭代普通 ndarray 比迭代 recarray 快，元素照样是 np.record
    return (record.copy() for record in batch.view(np.ndarray))


class BackendClient:
    """
    数据库后端的连接类，Backend会用此类创建master, servant连接。

    继承写法：
    class MyStoreClient(BackendClient, alias="mystore")

    服务器启动时，Backend会根据Config中type配置，寻找对应alias初始化Client。
    继承此类，完善所有NotImplementedError的方法。
    """

    # 表级频道名的后缀，各后端的 table_channel 都是 cluster_prefix 加它；通知接收器靠它认出
    # 哪些频道的 payload 是 row_id 集合
    TABLE_CHANNEL_SUFFIX = ":table"

    def index_channel(
        self,
        table_ref: TableReference,
        index_name: str,
        group: Mapping[str, Any] | None = None,
    ) -> str:
        """
        返回索引的频道名。该索引上任何值的行增删、任何一行该字段的变更都会通知到该频道，
        供区间订阅用；点查询请用 `index_value_channel`，免得被无关的值叫醒。
        复合索引（groupby）每组单独存放，必须用 group 给出全部分组列，得到的是这一组的频道：
        只有这一组的行进出、组里有行的本字段变了才会通知，别的组的写入不会。单列索引不能给 group。

        Channel of an index: every insert / delete / change of the indexed field
        notifies it. A composite (groupby) index is stored per group: pass `group`
        with every group-by column to get the channel of that group, which only
        writes to that group notify. Single-column indexes take no `group`.
        """
        raise NotImplementedError

    def index_value_channel(
        self, table_ref: TableReference, index_name: str, value: Any
    ) -> str:
        """
        返回索引某一个值的频道名。只有声明了 `point_sub` 的索引才有值频道（commit 只给它们
        发），未声明的索引调用会抛 `ValueError`——订一个永远没人发的频道只会静默收不到通知。
        只在有行"进入"这个值时（insert，或某行该字段改成这个值），commit 才向此频道发一条
        消息，消息不带内容；一个事务每个 (索引, 值) 只发一条。行"离开"（delete、字段改走）
        不发：点查询订阅本来就订着结果里每一行的行频道，由它发现（见 IndexSubscription）。
        点查询订阅用它代替 `index_channel`，别的值的变动不会打扰。value 先按组件 dtype 规范化
        （`dtype.type(value)`），所以 10、"10"、10.0 得到同一个频道。
        id 索引没有值频道：点查 id 请订行频道或整个 id 索引的频道。复合索引（groupby）没有值频道，
        调用抛 `ValueError`：按组订阅请用 `index_channel(…, group=…)`，它只被这一组的写入叫醒。

        Channel of one index value, only for indexes declared with `point_sub` (raises
        `ValueError` otherwise). Point-query subscriptions use it instead of
        `index_channel` so writes to other values don't wake them up. Composite
        (groupby) indexes have none: use `index_channel(..., group=...)`.
        """
        raise NotImplementedError

    @staticmethod
    def require_point_sub_(table_ref: TableReference, index_name: str) -> None:
        """内部方法：索引没有声明 point_sub（commit 不发它的值频道）时抛 ValueError"""
        if index_name not in table_ref.comp_cls.point_subs_:
            raise ValueError(
                _(
                    "{comp_name}.{index_name} 没有声明 point_sub，commit 不会发它的值频道；"
                    "请在 property_field 里加 point_sub=True"
                ).format(comp_name=table_ref.comp_name, index_name=index_name)
            )

    @staticmethod
    def point_query_value_(
        dtype: np.dtype, left: Any, right: Any | None
    ) -> np.generic | None:
        """
        判断 range 查询是否退化为点查询（right 省略或 left == right），是则返回按 dtype
        规范化后的值，否则返回 None。与 `range_normalize_` 的 peel 规则一致：str/bytes 值的
        `(` 前缀表示开区间，不算点查询；`[` 前缀剥掉。dtype 转换失败（非法字符串）或 NaN
        也返回 None，由调用方回退到整个索引的频道。整数索引的边界要是 dtype 范围内的整数才算：
        小数、越界、±inf 的区间里没有这个值（见 `normalize_int_bounds_`），拿截断后的值去订
        值频道、登记"读空"都不对。
        """
        left, left_inclusive = peel_bound_(left)
        right, right_inclusive = (
            (left, left_inclusive) if right is None else peel_bound_(right)
        )
        if left_inclusive is False or right_inclusive is False:
            return None  # 开区间不算点查询
        if issubclass(dtype.type, np.integer):
            try:
                value, other = exact_number_(left), exact_number_(right)
            except ValueError, TypeError:
                return None
            if value != other or (isinstance(value, float) and not value.is_integer()):
                return None  # 两端不同、小数、±inf、NaN
            info = np.iinfo(dtype)
            if not info.min <= int(value) <= info.max:
                return None
            return dtype.type(int(value))
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
        返回表级变更频道名。只有声明了 `table_sub` 的组件，commit 才向它发消息：表内任何行
        insert/update/delete，一个事务一张表发一条，payload 为本次事务变动的 row_id（str）列表。

        Channel of table-level changes. Commits publish to it only for components
        declared with `table_sub`: one message per transaction per table (any insert,
        update or delete), whose payload is the list of row ids (str) that transaction
        changed.
        """
        raise NotImplementedError

    def __init_subclass__(cls, **kwargs):
        """让继承子类自动注册alias；不带 alias 的中间基类（如 RedisModelClient）不注册"""
        super().__init_subclass__()
        if alias := kwargs.get("alias"):
            BackendClientFactory.register(alias, cls)

    def __init__(self, endpoint: Any, is_servant, **kwargs):
        """
        建立数据库连接。
        endpoint为config中master，或者servants的内容。
        is_servant指定endpoint是否为从节点，从节点只读。
        """
        self.endpoint = endpoint
        self.is_servant = is_servant

    @classmethod
    def check_config_(cls, config: dict) -> None:
        """
        内部方法：`Backend` 建连接之前检查整段配置（config 为 BACKENDS[i]），不合适就抛 ValueError。
        默认什么都不查。
        """

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
            与 `row_ids` 顺序一一对应，不存在的行位置为 None。各行互相独立，只留其中一行
            不会拖住整批的内存。
        """
        raise NotImplementedError

    async def get_many_array_(
        self, table_ref: TableReference, row_ids: list[int]
    ) -> tuple[np.recarray, list[int]]:
        """
        内部方法，事务里的 range 取行用：同 `get_many`，但读到的行按 `row_ids` 顺序一次解码
        成一个 recarray 直接返回，省掉逐行拆开再拼回去；另返回读不到的 id。
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
        *,
        group: Mapping[str, Any] | None = None,
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
        *,
        group: Mapping[str, Any] | None = None,
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
        *,
        group: Mapping[str, Any] | None = None,
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
        *,
        group: Mapping[str, Any] | None = None,
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
        *,
        group: Mapping[str, Any] | None = None,
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
        *,
        group: Mapping[str, Any] | None = None,
    ):
        """
        从数据库直接查询索引 `index_name`，返回在 [`left`, `right`] 闭区间内数据。
        如果 `right` 为 `None`，则查询等于 `left` 的数据，限制 `limit` 条。
        复合索引（groupby）要用 `group` 给出全部分组列的值，结果只含这一组的行。

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
        group: Mapping[str, Any] | None
            复合索引的分组值 {分组列: 值}，必须正好给出全部分组列；单列索引不能给。

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
        "某一组里按某字段排序"请声明复合索引（`property_field(groupby=...)`），查询时给出
        `group`。其他组合条件，先在数据库上筛选出最少量的数据，然后本地二次筛选::

            items = client.range(ref, "owner", player_id, limit=100)
            few_items = items[items.amount < 10]

        由于python numpy支持SIMD，比直接在数据库复合查询快。
        """
        raise NotImplementedError

    def check_range_(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None,
        limit: int,
        desc: bool,
        *,
        group: Mapping[str, Any] | None = None,
    ) -> None:
        """
        内部方法：校验一次 `range` 查询的参数，不合法抛 TypeError / ValueError，不访问数据库。订阅在
        接收协程里先校验，读库放到后台（见 SubscriptionBroker.begin_subscribe_range）：不合法的查询
        当场报错，不用登记后在后台读了、重试了才失败。
        这里只校验 limit 是整数（负数表示不限行数）；索引、边界由子类校验，默认不校验，由之后的
        range 读报错
        """
        if type(limit) is bool or not isinstance(limit, int):
            raise TypeError(
                _("range 的 limit 必须是整数，收到：{limit}").format(limit=repr(limit))
            )

    async def range_index_(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 10,
        desc: bool = False,
        *,
        group: Mapping[str, Any] | None = None,
    ) -> list[tuple[Any, int]]:
        """
        内部方法：只读索引，返回区间内前 `limit` 个 `(索引值, 行 id)`，区间、limit、desc、group 的
        规则同 `range`。不读行，一次往返；索引值按组件 dtype 还原（numpy 标量），复合索引只还原
        本字段的值。

        只要索引上的值、不要整行时用它，比如未来调用的调度只想知道最早一条什么时候到期：先读索引
        再读行是两次往返，中间那行可能已被改走，读到的行值就不是这次区间里的值了。
        """
        raise NotImplementedError

    async def range_read_(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None,
        limit: int,
        desc: bool,
        *,
        group: Mapping[str, Any] | None = None,
    ) -> tuple[list[int], RangeObservation]:
        """
        内部方法，事务里的 range 读用：执行与 `range(..., RowFormat.ID_LIST)` 相同的查询，
        同时返回这次读取的观察。之后 `commit` 据此校验同样的查询是否仍返回这些行（防幻读，
        见 `SessionRepository.range`）。`limit` 不能为 0。
        观察里的 `members` 是读到的各行的排序键，`query` 是这次查询匹配的排序键闭区间（见
        `index_sort_keys_`）。
        """
        raise NotImplementedError

    def index_sort_keys_(
        self, table_ref: TableReference, index_name: str, rows: np.ndarray
    ) -> list[bytes]:
        """
        内部方法：`rows` 这些行在 `index_name` 索引上的排序键。range 按排序键的字节序返回行
        （desc 时倒序），与 `range_read_` 观察里的 `members` / `query` 是同一套键。事务里的
        range 用它把本事务的改动按数据库的规则合并进结果（见 `SessionRepository.range`）。
        """
        raise NotImplementedError

    def index_query_keys_(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None,
        *,
        group: Mapping[str, Any] | None = None,
    ) -> tuple[bytes, bytes]:
        """
        内部方法：一次 range 查询匹配的排序键闭区间 (lo, hi)，同 `range_read_` 观察里的
        `query`，不访问数据库；区间为空时 lo > hi。事务里的 range 在读之前用它数本事务删改的
        行有几行原值在区间里（数据库会读到它们，要多读这么多行）
        """
        raise NotImplementedError

    def shrink_observation_(
        self, obs: RangeObservation, last_key: bytes, desc: bool
    ) -> None:
        """
        内部方法：把 `range_read_` 得到的观察收到排序键 `last_key` 为止（含），只留之前读到的
        行；`last_key` 不在观察范围内时不动。事务里的 range 合并了本事务的行再截断时用：看到
        的最后一行之后的库里行没取，提交时不用校验。`desc` 与读取时相同。
        """
        raise NotImplementedError

    async def commit(self, idmap: IdentityMap) -> None:
        """
        使用事务，向数据库提交IdentityMap中的所有数据修改

        Exceptions
        --------
        RaceCondition
            数据已被其他事务修改（版本不符）；或主键 / unique 冲突命中了本事务曾 `get`
            观察其不存在的值（基于过期快照）；或本事务 range 读过的区间变了（幻读、读取
            期间行被改走）。可重试
        UniqueViolation
            主键 / unique 值已被占用，且本事务从未观察其不存在：确定性冲突，不重试

        """
        raise NotImplementedError

    async def seq_head(
        self, table_ref: TableReference, group: Mapping[str, Any] | None = None
    ) -> int:
        """
        Read the latest number issued by one commit_seq group.

        读提交序号列（`commit_seq`）一个组最近发出的号，也就是这一组发到了第几号。还没发过号是 0，
        易失表维护清空之后也是 0。非事务读，走副本，可能落后。

        计数器和索引是两个 key，读计数器不和任何一次读在同一个快照里：只能用来发现有新号、监控
        积压、报警，**不能**拿来把游标推过没见到行的号。库外消费方的写法见 `seq_read`。

        Parameters
        ----------
        table_ref: TableReference
            表信息，指定Component、实例名、分片簇id。组件要声明了 `commit_seq`。
        group: Mapping[str, Any] | None
            分组值 {分组列: 值}，规则同 `range` 的 `group`：带 groupby 的必须正好给出全部分组列，
            不带 groupby 的不给。

        Returns
        -------
        head: int
            这一组最近发出的号，没发过号是 0。

        Raises
        ------
        ValueError
            组件没有提交序号列，或者分组值不对。
        RuntimeError
            计数器的值被写坏了（不是 Redis 认的非负整数）。`hetu upgrade` 重建索引时会把它校准回来。

        Notes
        -----
        Redis 上计数器 key 被写成了别的类型时，这里读成 0，不报错；这时提交本身会报 WRONGTYPE。
        """
        return (await self.seq_head_many(table_ref, [group]))[0]

    async def seq_head_many(
        self,
        table_ref: TableReference,
        groups: Sequence[Mapping[str, Any] | None],
    ) -> list[int]:
        """
        Read the latest numbers issued by many commit_seq groups in one round trip.

        批量读多个组的计数器，每个组同 `seq_head`。一次往返：组很多时按块拆成几条 MGET，放在
        一个 pipeline 里。返回值和 `groups` 一一对应；空列表不访问数据库。有一个计数器被写坏，
        整批抛 RuntimeError。

        Parameters
        ----------
        table_ref: TableReference
            表信息，指定Component、实例名、分片簇id。组件要声明了 `commit_seq`。
        groups: Sequence[Mapping[str, Any] | None]
            每一项是一个组的分组值，规则同 `seq_head` 的 `group`。

        Returns
        -------
        heads: list[int]
            每个组最近发出的号，没发过号是 0，顺序同 `groups`。

        Raises
        ------
        ValueError
            组件没有提交序号列，或者某一项分组值不对（在访问数据库之前检查）。
        RuntimeError
            有计数器的值被写坏了，报错里带着它的 key。
        """
        raise NotImplementedError

    @overload
    async def seq_read(
        self,
        table_ref: TableReference,
        after: int,
        limit: int = 100,
        *,
        group: Mapping[str, Any] | None = None,
        after_id: int | None = None,
        row_format: Literal[RowFormat.STRUCT] = RowFormat.STRUCT,
    ) -> SeqReadResult[np.recarray]: ...
    @overload
    async def seq_read(
        self,
        table_ref: TableReference,
        after: int,
        limit: int = 100,
        *,
        group: Mapping[str, Any] | None = None,
        after_id: int | None = None,
        row_format: Literal[RowFormat.RAW] = ...,
    ) -> SeqReadResult[list[dict[str, str]]]: ...
    @overload
    async def seq_read(
        self,
        table_ref: TableReference,
        after: int,
        limit: int = 100,
        *,
        group: Mapping[str, Any] | None = None,
        after_id: int | None = None,
        row_format: Literal[RowFormat.TYPED_DICT] = ...,
    ) -> SeqReadResult[list[dict[str, Any]]]: ...
    @overload
    async def seq_read(
        self,
        table_ref: TableReference,
        after: int,
        limit: int = 100,
        *,
        group: Mapping[str, Any] | None = None,
        after_id: int | None = None,
        row_format: RowFormat = ...,
    ) -> SeqReadResult[np.recarray | list[dict[str, str]] | list[dict[str, Any]]]: ...
    async def seq_read(
        self,
        table_ref: TableReference,
        after: int,
        limit: int = 100,
        *,
        group: Mapping[str, Any] | None = None,
        after_id: int | None = None,
        row_format=RowFormat.STRUCT,
    ) -> SeqReadResult[Any]:
        """
        Read a commit_seq group incrementally after a cursor, confirming gaps on the way.

        给库外消费方用的提交序号列增量读：读游标 `after` 之后的行，顺带确认缺口。非事务读，走副本，
        不需要 master。

        一次读钉在一个节点上：先一条 ZRANGE 读这一组在 `after` 之后的 member（这个节点上的原子快照），
        再从同一个节点取行。号按提交顺序发，一个节点上能读到 n 号，比它小的号要么也读得到，要么那一行
        已经不在了，所以：

        - 返回号大于 `after` 的行，按号升序。设 n 是最后一行的号（没有返回行时 n = after），
          (after, n] 里没出现在结果里的号都已确认不存在，游标推进到 n；
        - 取行时读不到的行（取行落到落后的副本、读完索引之后被删了、索引残留）**不跳过**：停在它
          前面，它记为 `blocked`，下次再读；
        - 计数器（`seq_head`）不和这次读在同一个快照里，不能用来把游标推过没见到行的号。

        注意：没有行的号一律当缺口跳过，分不清处理过了才删的、还是还没处理就被删了。裁剪只删号严格
        小于所有消费方**已经落盘的**游标的行，删之前先核对，见文档"binlog 式同步"一节。

        Parameters
        ----------
        table_ref: TableReference
            表信息，指定Component、实例名、分片簇id。组件要声明了 `commit_seq`。
        after: int
            游标，也就是最后处理过的那一号；从头读给 0。0 <= after < 2^53。
        limit: int
            最多返回的行数，正整数。一次读太多会长时间占住副本，一般几百到几千行。
        group: Mapping[str, Any] | None
            分组值 {分组列: 值}，规则同 `range` 的 `group`。
        after_id: int | None
            游标那一号的行的雪花 id（锚点）。给了就核对序列：游标那一号换了一行、游标那一行没了而
            后面有号、组里有重号，都抛 `CommitSeqDiverged`。给的时候 `after` 必须 >= 1。不给就不核对，
            也就发现不了号被复用。
        row_format
            返回数据解码格式，同 `range`，但不支持 `RowFormat.ID_LIST`。

        Returns
        -------
        result: SeqReadResult
            ``(rows, blocked)``。`rows` 是号大于 `after` 的行；`blocked` 是挡住后面的行的
            ``(号, 行 id)``，没被挡住是 None。`rows` 为空且 `blocked` 为 None：读的那个节点上
            `after` 之后没有任何行（没有新号、最新的行被删了，或者这个节点落后）。

        Raises
        ------
        CommitSeqDiverged
            给了 `after_id`，而序列和它对不上；或者组里有重号。
        ValueError
            组件没有提交序号列、分组值不对、参数取值不对，或者 `row_format` 是 `ID_LIST`。
        TypeError
            整数参数的类型不对（收 int 和 `np.integer`，不收 bool）。
        """
        raise NotImplementedError

    async def direct_set(
        self, table_ref: TableReference, id_: int, **kwargs: str
    ) -> bool:
        """
        UNSAFE! 只用于易失数据! 不会做类型检查!

        直接写入属性到数据库，避免session必须要执行get+事务2条指令。
        仅支持非索引字段，索引字段更新是非原子性的，必须使用事务。
        一些系统级别的临时数据，使用直接写入的方式效率会更高，但不保证数据一致性。

        只改已存在的行：行不存在（比如已被删掉），或行里没有要写的字段时，什么都不写，
        返回 False；写入了返回 True。不会建出只有这几个字段的残缺行。（以前不返回值，
        实现方请补上；调用方只把 False 当作没写上。）

        这是维护类写入：不改 `_version`、不参与乐观锁，也**不保证**触发订阅通知——行订阅可能
        立刻收到，也可能等该行下一次事务写入时一起推；整表订阅收不到。需要订阅方及时看到的数据
        请走事务。（Redis 上行频道就是行 key 的 keyspace 通知，会顺带触发；SQLite 后端不发。）

        UNSAFE, volatile components only, no type checks. Only updates an existing row:
        if the row (or one of the fields) does not exist, nothing is written and False is
        returned; True means the fields were written (it used to return nothing:
        implementers should add the return value; callers only treat False as not
        written). A maintenance-class write: it does
        not bump `_version`, takes no part in optimistic locking, and is **not guaranteed**
        to notify subscribers (a row subscriber may see it at once or only with the row's
        next transactional write; table subscribers never do). Use a transaction for data
        that subscribers must see promptly.
        """
        assert table_ref.comp_cls.volatile_, "direct_set只能用于易失数据的Component"
        raise NotImplementedError

    def get_table_maintenance(self) -> TableMaintenance:
        """
        获取表维护对象，根据不同后端类型返回不同的实现。
        """
        raise NotImplementedError

    def get_mq_client(self, *others: BackendClient) -> MQClient:
        """
        获取消息队列连接。others 是同一个后端的其他连接（其余 servant）：给了的话，返回的 MQClient 挂在
        本连接和它们各自的通知接收器上，频道分到各处订阅，某处订阅失败或断线时换到别处（见
        `HubMQClient`）。
        """
        raise NotImplementedError


class BackendClientFactory:
    _registry: ClassVar[dict[str, type[BackendClient]]] = {}

    # 内置后端按 alias 懒加载：import 对应子包即触发 BackendClient.__init_subclass__ 注册。
    # 不在 hetu.data.backend 包顶层 eager import，`import hetu` 就不会把用不到的后端（如
    # redis-py）一起加载。第三方后端仍靠显式 import 自己的模块注册。
    _BUILTIN_MODULES: ClassVar[dict[str, str]] = {
        "redis": "hetu.data.backend.redis",
        "sqlite": "hetu.data.backend.sqlite",
    }

    @staticmethod
    def register(alias: str, client_cls: type[BackendClient]) -> None:
        BackendClientFactory._registry[alias.lower()] = client_cls

    @staticmethod
    def client_class(alias: str) -> type[BackendClient]:
        """按 alias 取后端的客户端类，内置后端按需 import"""
        alias = alias.lower()
        if alias == "sql" and alias not in BackendClientFactory._registry:
            raise ValueError(
                _(
                    "SQL 后端已移除：SQLite 请把 type 改成 SQLite（地址不变），"
                    "PostgreSQL / MariaDB 不再支持"
                )
            )
        if alias not in BackendClientFactory._registry:
            module = BackendClientFactory._BUILTIN_MODULES.get(alias)
            if module:
                importlib.import_module(module)
        if alias not in BackendClientFactory._registry:
            raise NotImplementedError(_("{alias} 后端未实现").format(alias=alias))
        return BackendClientFactory._registry[alias]


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
        self,
        ref: TableReference,
        index_name: str,
        left: Any,
        right: Any = None,
        *,
        group: Mapping[str, Any] | None = None,
    ) -> list[int]:
        """按索引范围查询指定表的数据。复合索引要用 group 给出全部分组列的值"""
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
        """
        实际重建组件表索引的逻辑实现，返回重建的行数。要按行数据整个重建（行已不存在的
        索引残留要清掉），并且原子替换：中途失败时旧索引原样保留。
        """
        raise NotImplementedError

    def do_update_meta_(
        self, table_ref: TableReference, version: str | None = None
    ) -> None:
        """
        把组件表的meta改写成table_ref的定义（json/version/cluster_id），不动表数据。version
        不给时按定义算；迁移搬完数据却没能重建索引时，写 `REBUILD_PENDING_VERSION`
        """
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
            version = schema_version_(table_ref.comp_cls.json_)
            # 如果cluster_id改变，则迁移改key名，必须先检查cluster_id
            if meta.cluster_id != table_ref.cluster_id:
                return "cluster_mismatch", meta

            # 如果版本不一致，组件结构可能有变化，也可能只是改权限、或者索引是旧格式写的
            # （INDEX_FORMAT 变了），总之调用迁移代码
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

        易失组件不走迁移脚本，直接按新定义重建表：数据本来就会在 `hetu upgrade` 里清空。
        """
        with self.get_lock():
            if (status := self.check_table(table_ref)[0]) != "schema_mismatch":
                raise RaceCondition(
                    _(
                        "[💾TABLE_MAINT][{comp_name}组件] 无法迁移，组件表状态不对，目前为：{status}"
                    ).format(comp_name=table_ref.comp_name, status=status)
                )
            # hetu upgrade 迁移完紧接着就 flush_volatile，搬过去的数据也是清掉，所以不搬，
            # 删属性、改类型也就谈不上有损
            if table_ref.comp_cls.volatile_:
                self.do_drop_table_(table_ref)
                self.do_create_table_(table_ref)
                logger.warning(
                    _(
                        "  ✔️ [💾MIGRATION][{comp_name}组件] 易失组件，已按新定义重建表"
                    ).format(comp_name=table_ref.comp_name)
                )
                return True
            # 定义没变、只是索引格式（INDEX_FORMAT）变了：不搬数据，也不找迁移脚本（找不到会在
            # 用户的 maint/migration 里给每个组件生成一份空跑的默认脚本），重建索引、补写 meta
            if old_meta.json == table_ref.comp_cls.json_:
                self._finish_schema_migration(table_ref)
                return True
            from ..migration import MigrationScript

            migrator = MigrationScript(app_file, table_ref, old_meta)

            # 准备和检测
            status = migrator.prepare()
            if status == "unsafe" and not force:
                return False
            if status != "skip":
                # 获取所有row id
                row_ids = self.get_all_row_id(table_ref)
                migrator.upgrade(row_ids, self)
            self._finish_schema_migration(table_ref)
            return True

    def _finish_schema_migration(self, table_ref: TableReference) -> None:
        """
        迁移脚本只管搬数据：dtype 没变的那几级（只改了 table_sub / point_sub、索引、权限等）
        判 skip 不执行，表的 meta 还停在旧版本，check_table 会一直报 schema_mismatch。
        这里把 meta 补写成当前定义；索引定义变了（比如 point_sub 打开了 index）、或者索引是
        旧格式写的（`INDEX_FORMAT` 变了），先按当前定义重建索引。这一步不看
        `--no-rebuild-index`：不重建就查不到 / 查错
        """
        from ..component import BaseComponent

        comp_cls = table_ref.comp_cls
        meta = self.read_meta(table_ref.instance_name, comp_cls)
        assert meta
        if meta.version == schema_version_(comp_cls.json_):
            return  # 迁移脚本已按当前定义重建了表
        stored = BaseComponent.load_json(meta.json)
        if meta.version == REBUILD_PENDING_VERSION:
            # 上次迁移脚本搬完了数据、重建索引却失败了（见 MigrationScript.upgrade）
            self.do_rebuild_index_(table_ref)
            logger.warning(
                _(
                    "  ✔️ [💾MIGRATION][{comp_name}组件] 上次迁移搬完数据后重建索引失败，"
                    "这次已重建Index"
                ).format(comp_name=table_ref.comp_name)
            )
        # 分组列也要比：给已有索引加 / 去 groupby 时 indexes_、uniques_ 都不变，不重建的话
        # 库里留着旧格式的 member。提交序号列也要比：给已有索引的字段打开 commit_seq 时上面几项
        # 都不变，不重建的话计数器没人校准，会从 1 开始发、撞上已有的号
        elif (
            stored.indexes_,
            stored.uniques_,
            stored.index_groups_,
            stored.commit_seq_,
        ) != (
            comp_cls.indexes_,
            comp_cls.uniques_,
            comp_cls.index_groups_,
            comp_cls.commit_seq_,
        ):
            self.do_rebuild_index_(table_ref)
            logger.warning(
                _(
                    "  ✔️ [💾MIGRATION][{comp_name}组件] 索引定义有变更，已重建Index"
                ).format(comp_name=table_ref.comp_name)
            )
        elif index_format_stale_(meta):
            self.do_rebuild_index_(table_ref)
            logger.warning(
                _(
                    "  ✔️ [💾MIGRATION][{comp_name}组件] 索引的存储格式有变更，已按新格式"
                    "重建Index"
                ).format(comp_name=table_ref.comp_name)
            )
        self.do_update_meta_(table_ref)
        logger.warning(
            _(
                "  ✔️ [💾MIGRATION][{comp_name}组件] 已把 schema 版本更新为当前定义"
            ).format(comp_name=table_ref.comp_name)
        )

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
        """
        按行数据重建组件表的索引，修掉索引残留。扫描行与覆盖索引之间的写入会丢，必须停服
        执行（`hetu upgrade` 默认会调用）。
        """
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
    连接到消息队列的客户端。每个 worker 级订阅器（`hetu.data.sub.SubscriptionHub`）一个实例，
    连接做服务端内部关注（`watch`）时另有自己的一个。
    继承此类实现数据库写入通知和消息队列的结合。

    本地消息队列由基类维护：后端每个进程共享的通知接收器（如 Redis 的 `PubSubHub`、SQLite 的
    `SQLiteNotifyHub`）收到本客户端订阅的频道通知后调 `push_pulled_()` 入队，
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
    # 栅栏（SubscriptionHub.fence_）隔多少秒才入队：给"commit 返回 → 它的通知进本 worker 的本地
    # 队列"留的余量，之后才到的通知栅栏不等。Redis 副本应用 + 经 pubsub 推到 worker 平时约 1ms
    # （设计稿 2026-10-10-rpcs-sync §4.6）
    FENCE_DELAY: float = 0.02
    # 表级频道 payload 里的特殊 row_id：这段时间的变更不可知（如 pubsub 断线重连），整表重同步
    RESYNC = "*"
    # 日志里的后端标签，后端实现覆盖
    LOG_TAG = "MQ"

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
        dropped = 0
        for channel_name in channel_names:
            dropped += self._enqueue(channel_name, payload)
        if dropped:  # 入队顺手清掉的积压，和收到通知时一样要留下日志
            logger.warning(
                _(
                    "⚠️ [{tag}] 订阅更新通知来不及处理，丢弃了{seconds}秒前的消息共{count}条"
                ).format(tag=self.LOG_TAG, seconds=self.DROP_AFTER, count=dropped)
            )

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
                # 没数据就等 push_pulled_ 的信号。空闲时定时醒来看队列是纯粹的底噪（以前每个
                # 连接一个本协程时，每 1000 个空闲连接约占一个核的 1.6%），等信号则零成本。
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
                payload = self.pulled_payload.pop(channel_name, None)
                rtn[channel_name] = payload
                late = self._late.pop(channel_name, None)
                late_ids = self._late_payload.pop(channel_name, None)
                if late is not None and late > cutoff:
                    trailing.append((channel_name, late_ids))
                    if payload and late_ids and self.RESYNC in late_ids:
                        # 整表重同步是整表重读、全量重推：迟到的 RESYNC 这次读还不满预算，
                        # 只留给尾随重读做一次，这次只读原有的 row_id
                        payload.discard(self.RESYNC)
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

    def lost_(self, hub: MQHub, channels: list[str]) -> None:
        """
        通知接收器 hub 到后端的连接断了（如 Redis 副本挂掉），本客户端在它上面的这些频道恢复之前收不到
        通知（`MQHub.lost_` 调用，同步、不能 await）。默认什么也不做：由 hub 自己重连重订、补发
        （`MQHub.resync_`）。挂在几个 hub 上的实现（`HubMQClient`）把它们换到别的 hub。
        """

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

    @staticmethod
    def is_table_channel_(channel: str) -> bool:
        """表级频道（commit 主动发，payload 是 row_id 列表）；行 / 索引频道是 keyspace 通知"""
        return not channel.startswith("__keyspace@") and channel.endswith(
            BackendClient.TABLE_CHANNEL_SUFFIX
        )

    def resync_(self, channels: Iterable[str]) -> int:
        """
        这段时间的通知丢了（Redis 的 pubsub 断线、SQLite 的通知被清理）：给这些频道里本进程仍有人
        订的各分发一条通知，各连接一个 interval 后补读（行 / 索引订阅重读、重跑比对；整表订阅整表
        重同步）。只有表级频道带 `RESYNC`：它的 payload 本来就是 row_id 集合，整表订阅靠这个标记
        整表重读；行 / 索引（含值）频道照约定 payload 为 None。服务端内部 watch 的回调也照常触发。
        返回丢弃的过期通知条数，由调用方打日志。
        """
        dropped = 0
        for channel in channels:
            if channel in self._subs:
                ids = [MQClient.RESYNC] if self.is_table_channel_(channel) else None
                dropped += self._dispatch(channel, ids)
        return dropped

    def lost_(self, channels: Iterable[str]) -> None:
        """
        到后端的连接断了（Redis 的 pubsub 节点失效）：这些频道的通知在恢复之前收不到。按订阅者分组交给
        各 MQClient（`MQClient.lost_`）：挂在几个 hub 上的会把频道换到别的 hub 重订、补读。仍登记在这里
        的，恢复流程照旧重订、补发（`resync_`）。同步调用，不能 await
        """
        per_mq: dict[MQClient, list[str]] = {}
        for channel in channels:
            for mq in self._subs.get(channel, ()):
                per_mq.setdefault(mq, []).append(channel)
        for mq, lost in per_mq.items():
            try:
                mq.lost_(self, lost)
            except Exception:  # 一个订阅者出错不能拖累别人，也不能拖垮恢复流程
                logger.exception(_("⚠️ [MQ] 通知接收器断线的回调异常"))

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
    挂在进程共享 `MQHub` 上的轻量 MQClient：本身只记录自己订阅了哪些频道，订阅/退订转发给 hub。
    后端实现只需继承并指定 `LOG_TAG`。

    可以挂在几个 hub 上（每个 servant 一个，见 `Backend.get_mq_client`）：仍是一个本地队列，频道按哈希
    分到各 hub 订阅（rendezvous 哈希，每个实例随机加盐：同一频道在不同 worker 落在不同的副本上）。
    - 分到的 hub 订阅失败：它冷却 `HUB_COOLDOWN` 秒不再分新频道，这些频道换下一个 hub 订；都订不上才
      抛出，并撤掉本次在别处新登记的；
    - hub 到后端的连接断了（`lost_`，如副本挂掉）：它上面的频道换到别的 hub 重订，订上之后各补一条
      通知让订阅者重读（断线期间的写入没有通知，同 `MQHub.resync_`）；换不成的留在原处，等它自己重连
      重订、补发。
    只挂一个 hub 时与以前一样：断线由 hub 自己重连重订、补发 RESYNC。

    单个连接订阅频道数的告警（MAX_SUBSCRIBED）在门面 `SubscriptionBroker` 做：订阅都走 worker 级
    订阅器的这一个 MQClient，它订的是整个 worker 的频道。

    A lightweight MQClient on top of process-wide `MQHub`s. With several hubs (one per
    servant) it keeps one local queue, spreads channels over the hubs by salted rendezvous
    hashing, and moves them to another hub when a hub fails to subscribe or loses its
    connection (re-reading the moved channels once they are subscribed again).
    """

    # 某个 hub 订阅失败或断线后，这么多秒内不给它分新频道（到时再试；已经换到别处的不搬回来）
    HUB_COOLDOWN: float = 5.0

    def __init__(self, hub: MQHub, *more_hubs: MQHub):
        super().__init__()  # 本地消息队列
        self._hubs: tuple[MQHub, ...] = (hub, *more_hubs)
        # 只挂一个 hub 时就是它（多个时是第一个）
        self._hub = hub
        # 客户端订阅的频道；服务端内部关注（watch）的频道另记一份，两者可以重叠：
        # hub 按 MQClient 计数，同一频道只登记一次，所以客户端退订时要看它是不是还被关注着
        self.subscribed: set[str] = set()
        self._watched: set[str] = set()
        # 以下只在挂了几个 hub 时用。频道 → 登记在哪个 hub 上（登记中的也算）
        self._route: dict[str, MQHub] = {}
        self._salt = random.getrandbits(64)
        # hub → 冷却到期的时刻（time.monotonic）
        self._cooldown: dict[MQHub, float] = {}
        # 正在换 hub 的频道 → 换完时完成的 future
        self._moving: dict[str, asyncio.Future[None]] = {}
        # 后台任务（换 hub、撤掉旧 hub 上的登记）：保存引用免得被 gc，close 时统一取消
        self._tasks: set[asyncio.Task] = set()
        self._closed = False

    async def close(self):
        """取消本客户端的全部订阅（含内部关注的）。拆除路径上调用，后端出错也不抛"""
        self._closed = True
        # 回调在第一次 await 之前就同步清掉：退订回来之前的通知不再触发它
        channels = self.subscribed | self._watched | set(self._route)
        self.subscribed = set()
        self._watched = set()
        self._watchers.clear()
        self._route.clear()
        tasks = [task for task in self._tasks if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if channels:
            try:
                # 每个 hub 都撤一遍（hub 只撤本客户端登记过的）：换过 hub 的频道新旧两边都可能还有
                await asyncio.gather(
                    *(hub.remove(self, channels) for hub in self._hubs)
                )
            except Exception as e:  # noqa: BLE001 拆连接不能因为后端异常半途而废
                logger.warning(
                    _("⚠️ [{tag}] 关闭连接时取消订阅失败：{err}").format(
                        tag=self.LOG_TAG, err=f"{type(e).__name__}:{e}"
                    )
                )

    async def watch(self, channel_name: str, callback: Callable[[], None]) -> None:
        if self._closed:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        await self._wait_moving((channel_name,))
        # 先登记回调再订阅：订阅生效到登记之间的通知不能落进客户端推送队列
        self._watchers[channel_name] = callback
        self._watched.add(channel_name)
        try:
            await self._add((channel_name,))
        except BaseException:
            self._watched.discard(channel_name)
            self._watchers.pop(channel_name, None)
            raise
        if self._closed:
            await self._remove((channel_name,))
            raise ConnectionError(_("连接已关闭，已调用过close"))

    async def subscribe(self, *channel_names: str) -> None:
        """
        订阅频道（可多个，每个 hub 一次往返），频道名通过 client.xxx_channel(table_ref) 获得。
        失败时撤掉本次新登记的频道：登记按 MQClient 记、不按调用方，几个调用方共用一个 MQClient
        时，重叠的另一次 subscribe 搭车订上的也会被一并撤掉。所以共用时要由调用方保证同一频道同时
        只有一次在途（见 `hetu.data.sub.SubscriptionHub._subscribe`）
        """
        if not channel_names:
            return
        if self._closed:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        # 正在换 hub 的频道先等换完，它们的 hub 才定下来
        await self._wait_moving(channel_names)
        # 先记再等：等 ack 期间本连接可能又 unsubscribe/close 了其中的频道，由它们从
        # subscribed 和 hub 里撤掉；add 返回后不能再把这些频道加回来
        new = [name for name in channel_names if name not in self.subscribed]
        self.subscribed.update(new)
        try:
            await self._add(channel_names)
        except BaseException:
            self.subscribed.difference_update(new)
            raise
        if self._closed:
            # 等订阅生效期间连接被关了：撤销刚登记的订阅，别留在 hub 里
            await self._remove(channel_names)
            raise ConnectionError(_("连接已关闭，已调用过close"))

    async def unsubscribe(self, *channel_names: str) -> None:
        """取消订阅频道（可多个），频道名通过 client.xxx_channel(table_ref) 获得"""
        if not channel_names:
            return
        self.subscribed.difference_update(channel_names)
        # 服务端还关注着的频道只是客户端不要了，hub 里的登记得留着
        gone = [name for name in channel_names if name not in self._watched]
        if gone:
            await self._remove(gone)

    @property
    def subscribed_channels(self) -> set[str]:
        """返回当前连接订阅的所有频道名"""
        return self.subscribed

    # === === === 挂在几个 hub 上时：分配、换 hub === === ===

    def _candidates(self, channel: str, exclude: Collection[MQHub] = ()) -> list[MQHub]:
        """频道可选的 hub，按优先顺序：rendezvous 哈希从高到低，冷却中的排到最后"""
        now = time.monotonic()
        salt = self._salt
        ranked = sorted(
            (self._cooldown.get(hub, 0.0) > now, -hash((salt, i, channel)), i)
            for i, hub in enumerate(self._hubs)
            if hub not in exclude
        )
        return [self._hubs[i] for _cooling, _score, i in ranked]

    def _cool_down(self, hub: MQHub) -> None:
        self._cooldown[hub] = time.monotonic() + self.HUB_COOLDOWN

    async def _wait_moving(self, channels: Iterable[str]) -> None:
        """这些频道里有正在换 hub 的就等它换完（没有时不让出事件循环）"""
        moving = self._moving
        if not moving:
            return
        futures = {moving[ch] for ch in channels if ch in moving}
        if futures:
            await asyncio.wait(futures)  # 只旁观：本调用方被取消不会取消换 hub

    async def _add(self, channels: Iterable[str]) -> None:
        """
        在各频道的 hub 上登记订阅，返回时都已生效。只挂一个 hub 时就是 `hub.add`。

        挂了几个 hub 时：登记过的照旧登记在原 hub 上（重复 add 幂等，会等在途的 ack）；没登记过的按
        `_candidates` 挑一个。某个 hub 登记失败：它冷却，本次新分给它的频道换下一个候选再试；本次之前
        就登记在它上面的不在这里搬（断线由 `lost_` 搬），直接抛出。抛出（含被取消）时本次新分配的
        一律撤掉，在别处已登记上的也撤掉：与 `hub.add` 一样，失败不留半截登记
        """
        if len(self._hubs) == 1:
            await self._hub.add(self, channels)
            return
        route = self._route
        # 本次新分配 hub 的频道 → 在哪些 hub 上失败过
        tried: dict[str, set[MQHub]] = {}
        # 本次新登记成功的，出错时要撤掉
        registered: list[tuple[MQHub, list[str]]] = []
        todo = list(dict.fromkeys(channels))
        error: BaseException | None = None
        try:
            while todo:
                groups: dict[MQHub, list[str]] = {}
                for channel in todo:
                    hub = route.get(channel)
                    if hub is None:
                        candidates = self._candidates(
                            channel, tried.setdefault(channel, set())
                        )
                        if not candidates:
                            assert error is not None
                            raise error  # 每个 hub 都试过了
                        hub = route[channel] = candidates[0]
                    groups.setdefault(hub, []).append(channel)
                done: dict[MQHub, list[str]] = {}
                try:
                    failed = await self._add_groups(groups, done)
                finally:
                    registered.extend(
                        (hub, [ch for ch in group if ch in tried])
                        for hub, group in done.items()
                    )
                todo = []
                for hub, exc in failed.items():
                    self._cool_down(hub)
                    error = exc
                    for channel in groups[hub]:
                        if channel not in tried:
                            raise exc
                        if route.get(channel) is hub:
                            del route[channel]
                        tried[channel].add(hub)
                        todo.append(channel)
        except BaseException:
            for channel in tried:
                route.pop(channel, None)
            stale = [(hub, group) for hub, group in registered if group]
            if stale:
                self._spawn(self._drop(stale))
            raise

    async def _add_groups(
        self, groups: dict[MQHub, list[str]], done: dict[MQHub, list[str]]
    ) -> dict[MQHub, Exception]:
        """各 hub 并发登记，登记成功的记进 done（被取消时调用方据此撤掉），返回登记失败的"""
        failed: dict[MQHub, Exception] = {}

        async def add(hub: MQHub, group: list[str]) -> None:
            try:
                await hub.add(self, group)
            except Exception as e:  # noqa: BLE001 换下一个 hub，或由调用方抛出
                failed[hub] = e
            else:
                done[hub] = group

        await asyncio.gather(*(add(hub, group) for hub, group in groups.items()))
        return failed

    async def _remove(self, channels: Iterable[str]) -> None:
        """
        撤掉这些频道在各自 hub 上的登记（等退订回来）。只挂一个 hub 时就是 `hub.remove`。
        正在换 hub 的，这里撤旧 hub 上的，新 hub 上的由换 hub 的任务换完时发现没人要了再撤
        """
        if len(self._hubs) == 1:
            await self._hub.remove(self, channels)
            return
        groups: dict[MQHub, list[str]] = {}
        for channel in channels:
            hub = self._route.pop(channel, None)
            if hub is not None:
                groups.setdefault(hub, []).append(channel)
        if groups:
            await asyncio.gather(
                *(hub.remove(self, group) for hub, group in groups.items())
            )

    async def _drop(self, groups: Iterable[tuple[MQHub, list[str]]]) -> None:
        """撤掉这些 hub 上的登记（后台任务）"""
        await asyncio.gather(*(hub.remove(self, group) for hub, group in groups))

    def lost_(self, hub: MQHub, channels: list[str]) -> None:
        """
        hub 到后端的连接断了，这些频道的通知在它恢复之前收不到（`MQHub.lost_` 调用）。挂了几个 hub
        时：它冷却，这些频道在后台换到别的 hub（`_move`）。只挂一个 hub 时什么也不做
        """
        if len(self._hubs) < 2 or self._closed:
            return
        self._cool_down(hub)
        moving = [
            channel
            for channel in channels
            if self._route.get(channel) is hub and channel not in self._moving
        ]
        if not moving:
            return
        done: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        for channel in moving:
            self._moving[channel] = done
        self._spawn(self._move(hub, moving, done))

    async def _move(
        self, old: MQHub, channels: list[str], done: asyncio.Future[None]
    ) -> None:
        """
        把断线的 hub 上的这些频道换到别的 hub：先在新 hub 订上，再撤掉旧 hub 上的登记（它的重订名单
        随之去掉这些频道），然后各补一条通知让订阅者重读：新订阅生效之后发出的读，才覆盖得了断线期间
        的写入（同 `MQHub.resync_`：表级频道带 RESYNC，内部关注的频道触发回调）。换不成的留在原处，
        等旧 hub 自己重连重订、补发
        """
        route = self._route
        try:
            moved: list[str] = []
            tried: set[MQHub] = {old}
            todo = channels
            while todo:
                groups: dict[MQHub, list[str]] = {}
                for channel in todo:
                    candidates = self._candidates(channel, tried)
                    if candidates:
                        groups.setdefault(candidates[0], []).append(channel)
                if not groups:
                    break
                failed = await self._add_groups(groups, {})
                todo = []
                for hub, group in groups.items():
                    if hub in failed:
                        self._cool_down(hub)
                        tried.add(hub)
                        todo.extend(group)
                        continue
                    for channel in group:
                        route[channel] = hub
                    moved.extend(group)
            if not moved:
                logger.debug(
                    "[%s] %d channels stay on the lost hub, no other hub took them",
                    self.LOG_TAG,
                    len(channels),
                )
                return
            self._spawn(old.remove(self, moved))
            # 换 hub 的途中不要了（退订了、也不再关注）的：新 hub 上的也撤掉
            unwanted: dict[MQHub, list[str]] = {}
            dropped = 0
            for channel in moved:
                if channel not in self.subscribed and channel not in self._watched:
                    unwanted.setdefault(route.pop(channel), []).append(channel)
                    continue
                ids = [self.RESYNC] if MQHub.is_table_channel_(channel) else None
                dropped += self.push_pulled_(channel, ids)
            if unwanted:
                self._spawn(self._drop(unwanted.items()))
            if dropped:  # 入队顺手清掉的积压，和收到通知时一样要留下日志
                logger.warning(
                    _(
                        "⚠️ [{tag}] 订阅更新通知来不及处理，丢弃了{seconds}秒前的消息共{count}条"
                    ).format(tag=self.LOG_TAG, seconds=self.DROP_AFTER, count=dropped)
                )
        finally:
            for channel in channels:
                if self._moving.get(channel) is done:
                    del self._moving[channel]
            done.set_result(None)

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._on_task_done)
        return task

    def _on_task_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and (exc := task.exception()) is not None:
            logger.warning(
                _("⚠️ [{tag}] 订阅 / 退订频道失败：{err}").format(
                    tag=self.LOG_TAG, err=f"{type(exc).__name__}:{exc}"
                )
            )
