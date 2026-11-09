"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

Redis 数据模型：key 布局、索引编码、区间规范化、行解码、range 观察校验、commit payload 组装。

Redis 后端与 SQLite 后端（在 SQLite 上模拟同一个数据模型）共用这里的纯逻辑。本模块不做 I/O，也不
import redis-py；两个后端只在存储原语与提交脚本的执行上不同：Redis 用 redis-py 与 `redis/commit_v2.lua`，
SQLite 用 sqlite3 与它的 Python 版（`sqlite/commit.py`）。
"""

import itertools
import math
import random
from collections.abc import Awaitable, Iterable, Mapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Never,
    cast,
    overload,
    override,
)

# from msgspec import msgpack  # 不支持关闭bin type，lua 的msgpack库7年没更新了
import msgpack
import numpy as np

from ...i18n import _
from .base import (
    COMMIT_SEQ_LIMIT,
    BackendClient,
    CommitSeqDiverged,
    InconsistentRangeRead,
    RaceCondition,
    RowFormat,
    SeqReadResult,
    UniqueViolation,
    detach_rows_,
    exact_number_,
    from_sortable_bytes,
    group_prefix_,
    index_member_,
    index_value_bytes_,
    index_value_bytes_list_,
    inverted_bounds_error_,
    member_row_id_,
    member_value_,
    normalize_int_bounds_,
    parse_redis_int_,
    peel_bound_,
    resolve_group_,
    row_group_prefix_,
    row_id_token_,
    sortable_token,
    to_sortable_bytes,
)
from .idmap import RangeObservation

if TYPE_CHECKING:
    from ..component import BaseComponent
    from .idmap import IdentityMap
    from .table import TableReference

msg_packer = msgpack.Packer(use_bin_type=False)

# 行的 _version 是 int32：改到上限后回绕到 1。版本校验只比相等，回绕不影响；改成 int64 的话
# 每张表的结构都变了，所有表都得迁移
MAX_VERSION = 2**31 - 1
# 新插入的行版本号从 [1, 2^24] 里随机取，不从 1 开始：同一个 id 删掉又插回来时（显式 id、
# upsert(id=)、按 key 的未来调用），读到旧行的事务提交时 VER 才不会把新行当成旧行（ABA）。
# 范围不取大，给 int32 留足改动次数的余量
FIRST_VERSION_SPAN = 1 << 24


class CommitSeqScan:
    """
    重建索引时统计提交序号列（commit_seq）：每组最大的号、组内重复的号、到了上限的号。两个后端的
    `do_rebuild_index_` 在建的阶段逐行 `add`，建完先 `raise_for_problems`（有问题就报错，所有旧
    索引、计数器原样保留），再按 `counters()` 把计数器只升不降地校准到每组最大的号，然后才换上
    索引（先校准：换上之后、校准之前进程被杀的话，计数器低于库里的数据，会发出重号）。

    号 ≤ 0 表示没有号（新加 commit_seq 列时历史行填的 0），不参与统计。
    """

    # 报错里最多列出几组
    MAX_LISTED = 10

    def __init__(self, comp_cls: type[BaseComponent], comp_name: str) -> None:
        field = comp_cls.commit_seq_
        assert field is not None
        self.comp_cls = comp_cls
        self.comp_name = comp_name
        self.field = field
        self.groups = comp_cls.index_groups_[field]
        self.max_by_prefix: dict[bytes, int] = {}
        self._seen: set[tuple[bytes, int]] = set()
        self.duplicates: list[str] = []
        self.overflows: list[str] = []

    def _describe(self, struct: np.record, seq: int) -> str:
        values = ", ".join(f"{col}={struct[col].item()!r}" for col in self.groups)
        return f"({values}{', ' if values else ''}{self.field}={seq})"

    def add(self, struct: np.record, member: bytes) -> None:
        """一行：struct 是这一行（至少分组列和本字段已按 dtype 转好），member 是它在这个索引上的"""
        seq = int(struct[self.field])
        if seq <= 0:
            return
        # 值段是 P + 号的 8 字节编码
        prefix = member_value_(member)[:-8]
        if seq >= COMMIT_SEQ_LIMIT and len(self.overflows) < self.MAX_LISTED:
            self.overflows.append(self._describe(struct, seq))
        if (prefix, seq) in self._seen:
            if len(self.duplicates) < self.MAX_LISTED:
                self.duplicates.append(self._describe(struct, seq))
            return
        self._seen.add((prefix, seq))
        if seq > self.max_by_prefix.get(prefix, 0):
            self.max_by_prefix[prefix] = seq

    def raise_for_problems(self) -> None:
        """组内有重复的号、有号到了上限：报错，要求先写迁移脚本重新编号"""
        if self.duplicates:
            raise RuntimeError(
                _(
                    "组件{comp_name}的提交序号列`{field}`在重建索引时发现组内重复的号：{rows}"
                    "（最多列出{limit}个）。重复的号会让消费方静默漏掉后一行，请先写迁移脚本"
                    "重新编号。"
                ).format(
                    comp_name=self.comp_name,
                    field=self.field,
                    rows=", ".join(self.duplicates),
                    limit=self.MAX_LISTED,
                )
            )
        if self.overflows:
            raise RuntimeError(
                _(
                    "组件{comp_name}的提交序号列`{field}`在重建索引时发现号达到了上限 2^53："
                    "{rows}（最多列出{limit}个），之后这些组每次提交都会失败。请先写迁移脚本"
                    "重新编号。"
                ).format(
                    comp_name=self.comp_name,
                    field=self.field,
                    rows=", ".join(self.overflows),
                    limit=self.MAX_LISTED,
                )
            )

    def counters(self, cluster_prefix: str) -> list[tuple[str, int]]:
        """[(计数器 key, 这一组最大的号)]，没有行的组不在里面（计数器不动）"""
        return [
            (RedisModelClient.seq_counter_key_(cluster_prefix, self.comp_cls, p), seq)
            for p, seq in self.max_by_prefix.items()
        ]


def seq_int_arg_(name: str, value: Any) -> int:
    """`seq_read` 的整数参数：收 int 和 np.integer（比如直接传 rows[-1].seq），不收 bool"""
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, np.integer)):
        raise TypeError(
            _("seq_read 的 {name} 必须是整数，收到：{value}").format(
                name=name, value=repr(value)
            )
        )
    return int(value)


def seq_head_value_(raw: bytes | None) -> int | None:
    """
    提交序号列计数器的原始值转成号：key 不存在是 0；不是 Redis 认的非负整数（被写坏了）是 None。
    解析规则同 INCRBY（`parse_redis_int_`）
    """
    if raw is None:
        return 0
    value = parse_redis_int_(raw)
    return None if value is None or value < 0 else value


class RedisModelClient(BackendClient):
    """
    Redis 数据模型的后端基类（纯逻辑，见模块说明）。子类实现 I/O：读行、查索引、`commit_script_`、
    `direct_set` 的写入。
    """

    # keyspace 通知频道里的 db 号：Redis 取连接的 db，SQLite 固定 0
    dbi: int = 0

    @staticmethod
    def _get_referred_components() -> list[type[BaseComponent]]:
        """获取当前app用到的Component列表"""
        from ...system.definer import SystemClusters

        return list(SystemClusters().get_components())

    def _schema_checking(self, components: Iterable[type[BaseComponent]] | None = None):
        """
        检查Component的schema定义，确保索引字段能编码成可排序的字节。按 dtype.kind 白名单查：
        索引字段只能是整数、浮点、str、bytes，复合索引的分组列只能是整数、str、bytes。黑名单
        漏掉的类型（datetime64 / timedelta64）要到 commit 写索引时才崩
        """
        if components is None:
            components = self._get_referred_components()
        for comp_cls in components:
            for index_name, groups in comp_cls.index_groups_.items():
                for field, kinds in (
                    *((col, "iuUS") for col in groups),
                    (index_name, "iufUS"),
                ):
                    dtype = comp_cls.dtype_map_[field]
                    # 索引不支持复数
                    if np.issubdtype(dtype, np.complexfloating):
                        raise ValueError(
                            _(
                                "Component `{comp_name}` 的索引字段`{field}`"
                                "使用了复数，Redis后端不支持此类型作为索引字段"
                            ).format(comp_name=comp_cls.name_, field=field)
                        )
                    # 其他类型不支持索引
                    elif dtype.kind not in kinds:
                        raise ValueError(
                            _(
                                "Component `{comp_name}` 的索引字段`{field}`"
                                "使用了不可用的类型 `{dtype}`，此类型不支持索引"
                            ).format(comp_name=comp_cls.name_, field=field, dtype=dtype)
                        )

    # ============ key 与频道 ============

    @staticmethod
    def table_prefix(table_ref: TableReference) -> str:
        """获取redis表名前缀"""
        return f"{table_ref.instance_name}:{table_ref.comp_cls.name_}"

    @staticmethod
    def cluster_prefix(table_ref: TableReference) -> str:
        """获取redis表名前缀"""
        return (
            f"{table_ref.instance_name}:{table_ref.comp_cls.name_}:"
            f"{{CLU{table_ref.cluster_id}}}"
        )

    @classmethod
    def all_clusters_prefix(cls, table_ref: TableReference) -> str:
        """
        这张表所有簇的行、索引 key 的共同前缀（到 `{CLU` 为止）。只按 `table_prefix` 匹配会连带
        匹配上名字以本组件名开头的副本组件（`Comp:suffix`，见 `BaseComponent.duplicate`）
        """
        return f"{cls.table_prefix(table_ref)}:{{CLU"

    @classmethod
    def row_key(cls, table_ref: TableReference, row_id: str | int) -> str:
        """获取redis表行的key名"""
        return f"{cls.cluster_prefix(table_ref)}:id:{row_id!s}"

    @classmethod
    def index_key(cls, table_ref: TableReference, index_name: str) -> str:
        """获取redis表索引的key名"""
        return f"{cls.cluster_prefix(table_ref)}:index:{index_name}"

    @classmethod
    def index_zset_key_(cls, idx_key: str, prefix: bytes) -> str:
        """
        内部方法：索引数据所在的 zset key。单列索引（prefix 为空）就是索引 key；复合索引每组一个
        zset，key 是索引 key 加组前缀 P 的 token。token 是摘要时万一碰撞，两组共用一个 key：member
        带着 P，查询、UNIQ、CNT 照样分得开，只是订阅多被叫醒一次
        """
        return f"{idx_key}:{sortable_token(prefix)}" if prefix else idx_key

    @staticmethod
    def seq_counter_key_(
        cluster_prefix: str, comp_cls: type[BaseComponent], prefix: bytes
    ) -> str:
        """
        内部方法：提交序号列（commit_seq）一个组的计数器 key，值是这一组最近发出的号。prefix 是组前缀
        P，不分组时为 b""。

        - 用 P 的完整 hex，不用组 key 的 `sortable_token`：超过 32 字节的 P 会被它取成摘要，摘要碰撞时
          两组共用组 key 无害（member 带着 P），共用计数器就会让两组的号交错、各自出现缺口；
        - 带上分组列名：改了 groupby（比如 owner 改成 guild，都是 int64）之后新组的 P 可能和旧组的 P
          字节相同，不带列名就会接着旧组的号发、开头出现缺口。

        在 `{cluster_prefix}` 下：cluster 下和行同 slot，换簇时跟着搬，易失表 flush 时一起删
        """
        field = comp_cls.commit_seq_
        assert field is not None
        groups = comp_cls.index_groups_[field]
        head = f"{cluster_prefix}:seq:{field}"
        if not groups:
            return head
        return f"{head}:{','.join(groups)}:{prefix.hex()}"

    @classmethod
    def composite_entry_(
        cls, comp_cls: type[BaseComponent], idx_key: str, index_name: str, row: Any
    ) -> tuple[str, bytes]:
        """
        内部方法：一行在复合索引上的 (zset key, 值段)：key 是这一行所在组的，值段是组前缀 P 加本字段
        的编码（同 `index_value_bytes_`）。row 同 `row_group_prefix_`
        """
        prefix = row_group_prefix_(comp_cls, index_name, row)
        value = to_sortable_bytes(comp_cls.dtype_map_[index_name].type(row[index_name]))
        return cls.index_zset_key_(idx_key, prefix), prefix + value

    @override
    def index_channel(
        self,
        table_ref: TableReference,
        index_name: str,
        group: Mapping[str, Any] | None = None,
    ) -> str:
        """
        返回索引的频道名（keyspace 通知），这个 zset 任何 ZADD/ZREM 都会通知到它。复合索引每组一个
        zset，要给出 group，得到的是这一组的频道；单列索引不能给 group
        """
        key = self.index_zset_key_(
            self.index_key(table_ref, index_name),
            self.group_prefix_of_(table_ref, index_name, group),
        )
        return f"__keyspace@{self.dbi}__:{key}"

    @classmethod
    def value_channel_(cls, idx_key: str, sortable: bytes) -> str:
        """`index_value_channel` 的内部形式：commit 里已经算好 sortable bytes 时直接拼，不重复编码"""
        return f"{idx_key}:{sortable_token(sortable)}"

    @override
    def index_value_channel(
        self, table_ref: TableReference, index_name: str, value: Any
    ) -> str:
        """
        返回索引某一个值的频道名（只有声明了 point_sub 的索引才有，否则抛 ValueError）。
        这是 commit lua 脚本主动 PUBLISH 的普通频道（非 keyspace 通知）；名字带 {CLU}
        hash tag，cluster 模式下按 slot 路由。

        Channel of one index value (only for indexes declared with `point_sub`, raises
        `ValueError` otherwise). A plain channel PUBLISHed by the commit Lua script, not
        a keyspace notification; the name carries the {CLU} hash tag, so cluster mode
        routes it by slot.
        """
        comp_cls = table_ref.comp_cls
        if comp_cls.index_groups_.get(index_name):
            # commit 不给复合索引发值频道：算出来的频道永远没人发，订阅方会静默收不到通知
            raise ValueError(
                _(
                    "{comp_name}.{index_name} 是复合索引，没有值频道；按组订阅请用 "
                    "index_channel(…, group=…)"
                ).format(comp_name=comp_cls.name_, index_name=index_name)
            )
        self.require_point_sub_(table_ref, index_name)
        dtype = comp_cls.dtype_map_[index_name]
        return self.value_channel_(
            self.index_key(table_ref, index_name), to_sortable_bytes(dtype.type(value))
        )

    @override
    def row_channel(self, table_ref: TableReference, row_id: int):
        """返回行数据的频道名。如果行有变动，会通知到该频道"""
        return f"__keyspace@{self.dbi}__:{self.row_key(table_ref, row_id)}"

    @override
    def table_channel(self, table_ref: TableReference):
        """
        返回表级变更频道名。这是commit lua脚本主动PUBLISH的普通频道（非keyspace通知），
        只给声明了 table_sub 的组件发；名字带{CLU}hash tag，cluster模式下
        AsyncKeyspacePubSub按slot路由订阅。

        Channel of table-level changes: a plain channel PUBLISHed by the commit Lua
        script (not a keyspace notification), only for components declared with
        `table_sub`. The name carries the {CLU} hash tag, so in cluster mode
        AsyncKeyspacePubSub routes the subscription by slot.
        """
        return f"{self.cluster_prefix(table_ref)}{self.TABLE_CHANNEL_SUFFIX}"

    # 索引 member 的值编码搬到了 base.py（两个后端共用来给索引值频道命名），这里保留同名别名
    to_sortable_bytes = staticmethod(to_sortable_bytes)

    # ============ 行解码 ============

    @overload
    @staticmethod
    def row_decode_(
        comp_cls: type[BaseComponent],
        row: dict[bytes, bytes],
        fmt: Literal[RowFormat.STRUCT],
    ) -> np.record: ...
    @overload
    @staticmethod
    def row_decode_(
        comp_cls: type[BaseComponent],
        row: dict[bytes, bytes],
        fmt: Literal[RowFormat.RAW, RowFormat.TYPED_DICT],
    ) -> dict[str, Any]: ...
    @overload
    @staticmethod
    def row_decode_(
        comp_cls: type[BaseComponent],
        row: dict[bytes, bytes],
        fmt: Literal[RowFormat.ID_LIST],
    ) -> Never: ...
    @staticmethod
    def row_decode_(
        comp_cls: type[BaseComponent], row: dict[bytes, bytes], fmt: RowFormat
    ) -> np.record | dict[str, Any]:
        """将redis获取的行byte数据解码为指定格式"""
        match fmt:
            case RowFormat.STRUCT:
                return RedisModelClient.rows_decode_(comp_cls, (row,))[0]
            case RowFormat.TYPED_DICT:
                struct_row = RedisModelClient.rows_decode_(comp_cls, (row,))[0]
                return comp_cls.struct_to_dict(struct_row)
            case RowFormat.RAW:
                # RAW 一律是 str，bytes 字段也按 utf-8 容错解码
                return {
                    k.decode("utf-8", "ignore"): v.decode("utf-8", "ignore")
                    for k, v in row.items()
                }
            case _:
                raise ValueError(_("不可用的行格式: {fmt}").format(fmt=fmt))

    @staticmethod
    def rows_decode_(
        comp_cls: type[BaseComponent], rows: Iterable[dict[bytes, bytes]]
    ) -> np.recarray:
        """
        把 HGETALL 读回的多行一次解码成 recarray，顺序与传入一致。`row_decode_` 的 STRUCT
        格式就是它的单行特例，解码规则只写在这里。
        """
        # bytes 字段用原始字节：utf-8 解码会丢掉不合法的字节，非 ASCII 的 str 也存不进
        # S 类型。其余字段按 utf-8 容错解码成 str，交给 numpy 按 dtype 转换
        fields = [
            (name.encode(), name in comp_cls.bytes_fields_)
            for name, _prop in comp_cls.properties_
        ]
        values = [
            tuple(
                [
                    row[key] if raw else row[key].decode("utf-8", "ignore")
                    for key, raw in fields
                ]
            )
            for row in rows
        ]
        return np.array(values, dtype=comp_cls.dtypes).view(np.recarray)

    # ============ 索引区间 ============

    @classmethod
    def range_normalize_(
        cls,
        dtype: np.dtype,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None,
        desc: bool,
        prefix: bytes = b"",
    ) -> tuple[bytes, bytes]:
        """
        规范化范围查询的边界，返回 ZRANGE BYLEX 的两端，按扫描顺序排（desc 时上界在前）。
        prefix 是复合索引的组前缀 P（见 `group_prefix_`），加在两端值的前面，单列索引为空。

        left / right 是 (下界, 上界)，desc 时也一样；下界大于上界多半是传反了，报 ValueError。
        整数索引按区间的数学含义收成闭区间（见 `normalize_int_bounds_`），越界、小数照常查。
        区间是空的（两端开区间同值、整数列里没有整数……）时，返回的两端交叉或相等：ZRANGE /
        ZLEXCOUNT 对这样的两端自然得到空 / 0，`zrange_args_` 也就判定为空区间、不去查了。
        """
        if right is None:
            right = left

        # component字段如果是str/bytes类型的索引，不能查询数字
        if issubclass(dtype.type, np.character) and (
            type(left) not in (str, bytes) or type(right) not in (str, bytes)
        ):
            raise ValueError(
                f"字符串类型的查询变量类型必须是str/bytes，你的：left={type(left)}({left}), "
                f"right={type(right)}({right})"
            )

        # 边界值开头的 "(" / "[" 指定开/闭，默认闭区间
        lower, li = peel_bound_(left)
        upper, ui = peel_bound_(right)
        li = True if li is None else li
        ui = True if ui is None else ui

        if issubclass(dtype.type, np.integer):
            # 不按 dtype 转换（会溢出、会截断小数）：先用精确的数判定传反，再收成范围内的闭区间
            lower_num, upper_num = exact_number_(lower), exact_number_(upper)
            if lower_num > upper_num:
                raise inverted_bounds_error_(lower, upper)
            bounds = normalize_int_bounds_(dtype, lower_num, li, upper_num, ui)
            if bounds is None:
                # 区间里没有整数：给交叉的两端 [最大值, 最小值]
                info = np.iinfo(dtype)
                bounds = (info.max, info.min)
            lower_value = to_sortable_bytes(dtype.type(bounds[0]))
            upper_value = to_sortable_bytes(dtype.type(bounds[1]))
            li = ui = True
        else:
            lower_value = to_sortable_bytes(dtype.type(lower))
            upper_value = to_sortable_bytes(dtype.type(upper))
            # 按值判定传反（编码是保序的）。同值时开区间让两端交叉，那是空区间，不是传反
            if upper_value < lower_value:
                raise inverted_bounds_error_(lower, upper)

        # member 是 value\x00id（value 段已对 0x00 转义，见 to_sortable_bytes）。
        # 终止符 b"\x00" = 该 value 的下边界(含最小 id)，b"\x00\xff" = 上边界(含所有 id)
        b_lower = b"[" + prefix + lower_value + (b"\x00" if li else b"\x00\xff")
        b_upper = b"[" + prefix + upper_value + (b"\x00\xff" if ui else b"\x00")
        return (b_upper, b_lower) if desc else (b_lower, b_upper)

    @staticmethod
    def group_prefix_of_(
        table_ref: TableReference, index_name: str, group: Mapping[str, Any] | None
    ) -> bytes:
        """
        内部方法：查询给的分组值 → 组前缀 P，单列索引为 b""。复合索引没给、少给、多给分组值，
        单列索引给了分组值，都抛 ValueError：所有按索引查询的路径都经过这里，入口漏了校验也不会
        用空前缀去查、读到别的组的 member
        """
        comp_cls = table_ref.comp_cls
        values = resolve_group_(comp_cls, index_name, group)
        return group_prefix_(comp_cls, index_name, values) if values else b""

    @staticmethod
    def make_zrange_cmd_(b_left, b_right, desc, limit):
        return {
            "start": b_left,
            "end": b_right,
            "desc": desc,
            "offset": 0,
            "num": limit,
            "bylex": True,
            "byscore": False,
        }

    @classmethod
    def zrange_args_(
        cls,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None,
        desc: bool,
        group: Mapping[str, Any] | None = None,
    ) -> tuple[str, bytes, bytes, bool]:
        """
        按索引区间查询要用的：zset key、ZRANGE BYLEX 的两端（按扫描顺序），以及区间是否为空
        （两端交叉或相等；传反的已在 `range_normalize_` 里报错）。空区间不用去查。复合索引查
        group 那一组的 key，两端也都带着组前缀。
        """
        idx_key = cls.index_key(table_ref, index_name)
        comp_cls = table_ref.comp_cls
        if index_name not in comp_cls.indexes_:
            raise ValueError(f"Component `{comp_cls.name_}` 没有索引 `{index_name}`")
        prefix = cls.group_prefix_of_(table_ref, index_name, group)
        b_left, b_right = cls.range_normalize_(
            comp_cls.dtype_map_[index_name], left, right, desc, prefix
        )
        empty = (b_left <= b_right) if desc else (b_right <= b_left)
        return cls.index_zset_key_(idx_key, prefix), b_left, b_right, empty

    @staticmethod
    def range_observation_(
        index_name: str,
        key: str,
        members: list[bytes],
        b_left: bytes,
        b_right: bytes,
        limit: int,
        desc: bool,
    ) -> tuple[list[int], RangeObservation]:
        """
        `range_read_` 的后半段：由 ZRANGE 读回的原样 member 与规范化后的两端，得到行 id 与这次读取的
        观察（commit 时变成对 key 的 CNT 检查，见 `_range_checks`）。key 是读的那个 zset。
        """
        row_ids = [member_row_id_(vk) for vk in members]
        # 观察区间给 ZLEXCOUNT 用，要按 min, max 排；desc 时 b_left 是上界
        lo, hi = (b_right, b_left) if desc else (b_left, b_right)
        # 两端都是 "[" 开头的闭区间，去掉 "[" 就是 member（排序键）的闭区间
        query = (lo[1:], hi[1:])
        if 0 < limit == len(members):
            # 截断读只看到了前 limit 行，观察区间收到最后一个返回的 member 为止
            if desc:
                lo = b"[" + members[-1]
            else:
                hi = b"[" + members[-1]
        return row_ids, RangeObservation(
            index_name, row_ids, (lo, hi), members, query, key=key
        )

    # ============ 读：两个后端共用，I/O 由子类的两个原语完成 ============

    async def hgetall_many_(
        self, table_ref: TableReference, row_ids: Sequence[int]
    ) -> list[dict[bytes, bytes]]:
        """内部方法：按 row_ids 的顺序批量 HGETALL 这张表的行，读不到的为空 dict。子类实现"""
        raise NotImplementedError

    async def kv_get_many_(self, keys: Sequence[str]) -> list[bytes | None]:
        """
        内部方法：按 keys 的顺序批量读字符串 key（提交序号列的计数器），不存在的为 None，同 MGET。
        子类实现
        """
        raise NotImplementedError

    def zrange_bylex_(
        self, idx_key: str, b_left: bytes, b_right: bytes, desc: bool, limit: int
    ) -> Awaitable[list[bytes]]:
        """
        内部方法：`ZRANGE idx_key b_left b_right BYLEX [REV] LIMIT 0 limit`，返回原样 member
        （value\\x00id）的 awaitable。子类实现；不写成 async，Redis 可以直接交出 redis-py 的
        awaitable，range 路径上少一层协程
        """
        raise NotImplementedError

    async def _zrange_members(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None,
        limit: int,
        desc: bool,
        group: Mapping[str, Any] | None = None,
    ) -> tuple[list[bytes], str, bytes, bytes]:
        """
        按索引区间 ZRANGE，返回原样 member（value\\x00id）、查的 zset key（复合索引是 group 那一组的）
        与规范化后的两个边界
        """
        idx_key, b_left, b_right, empty = self.zrange_args_(
            table_ref, index_name, left, right, desc, group
        )
        # 两端交叉或相等就是空区间（传反的已在 range_normalize_ 里报错），不用去查
        if empty:
            return [], idx_key, b_left, b_right
        members = await self.zrange_bylex_(idx_key, b_left, b_right, desc, limit)
        return members, idx_key, b_left, b_right

    @override
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
        """校验 range 查询的参数（limit、索引存在、边界与分组值合法），见基类"""
        super().check_range_(table_ref, index_name, left, right, limit, desc)
        self.zrange_args_(table_ref, index_name, left, right, desc, group)

    @override
    async def get_many(
        self,
        table_ref: TableReference,
        row_ids: Iterable[int],
        row_format: RowFormat = RowFormat.STRUCT,
    ) -> list[np.record | dict[str, str] | dict[str, Any] | None]:
        """批量获取多行数据，见基类"""
        assert row_format != RowFormat.ID_LIST, "get_many不支持ID_LIST格式"
        comp_cls = table_ref.comp_cls
        if not isinstance(row_ids, (list, tuple)):
            row_ids = list(row_ids)
        raw_rows = await self.hgetall_many_(table_ref, row_ids)
        if row_format is RowFormat.STRUCT:
            batch = self.rows_decode_(comp_cls, [row for row in raw_rows if row])
            records = detach_rows_(batch)
            return [next(records) if row else None for row in raw_rows]
        return [
            self.row_decode_(comp_cls, row, row_format) if row else None
            for row in raw_rows
        ]

    @override
    async def get_many_array_(
        self, table_ref: TableReference, row_ids: list[int]
    ) -> tuple[np.recarray, list[int]]:
        """见基类"""
        raw_rows = await self.hgetall_many_(table_ref, row_ids)
        rows = self.rows_decode_(table_ref.comp_cls, [row for row in raw_rows if row])
        return rows, [row_id for row_id, row in zip(row_ids, raw_rows) if not row]

    @overload
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 100,
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
        limit: int = 100,
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
        limit: int = 100,
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
        limit: int = 100,
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
        limit: int = 100,
        desc: bool = False,
        row_format: RowFormat = ...,
        *,
        group: Mapping[str, Any] | None = None,
    ) -> np.recarray | list[dict[str, str]] | list[dict[str, Any]] | list[int]: ...
    @override
    async def range(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None = None,
        limit: int = 100,
        desc: bool = False,
        row_format=RowFormat.STRUCT,
        *,
        group: Mapping[str, Any] | None = None,
    ) -> list[int] | list[dict[str, Any]] | np.recarray:
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
            查询范围，闭区间。可以在开头加上"["指定闭区间，还是"("开区间。
            如果right不填写，则精确查询等于left的数据。
        limit: int
            限制返回的行数，越少越快。负数表示不限制行数。
        desc: bool
            是否降序排列
        row_format
            返回数据解码格式，见 "Returns"
        group: Mapping[str, Any] | None
            复合索引（groupby）的分组值 {分组列: 值}，必须正好给出全部分组列；单列索引不能给。

        Returns
        -------
        row: np.recarray or list[int] or list[dict]
            根据 `row_format` 参数返回以下格式之一：

            - RowFormat.STRUCT - **默认值**
                返回 `numpy.recarray`，如果没有查询到数据，返回空 `numpy.recarray`。
                `numpy.recarray` 是一种 c-struct array。
            - RowFormat.RAW
                返回无类型的原始数据 (dict[str, str]) 的列表，如果没有查询到数据，返回空list
            - RowFormat.TYPED_DICT
                返回符合Component定义的，有格式的dict类型列表，如果没有查询到数据，返回空list
                此方法性能低于 `RowFormat.STRUCT` ，主要用于json后传递给客户端。
            - RowFormat.ID_LIST
                返回查询到的 row id 列表，如果没有查询到数据，返回空list

        Notes
        -----
        先查索引拿 id，再批量读行，两次之间行可能被删改（读不到的行跳过）。

        如何复合条件查询？
        "某一组里按某字段排序"请声明复合索引（`property_field(groupby=...)`），查询时给出
        `group`。其他组合条件，先在数据库上筛选出最少量的数据，然后本地二次筛选::

            items = client.range(ref, "owner", player_id, limit=100)
            few_items = items[items.amount < 10]

        由于python numpy支持SIMD，比直接在数据库复合查询快。
        """
        members, _key, _b_left, _b_right = await self._zrange_members(
            table_ref, index_name, left, right, limit, desc, group
        )
        row_ids = [member_row_id_(vk) for vk in members]

        if row_format == RowFormat.ID_LIST:
            return row_ids

        comp_cls = table_ref.comp_cls
        raw_rows = [row for row in await self.hgetall_many_(table_ref, row_ids) if row]
        if row_format == RowFormat.STRUCT:
            return self.rows_decode_(comp_cls, raw_rows)
        return [
            cast(dict[str, Any], self.row_decode_(comp_cls, row, row_format))
            for row in raw_rows
        ]

    @override
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
        """见基类。复合索引的值段是组前缀 P 加本字段的编码，去掉 P 再还原"""
        members, _key, _b_left, _b_right = await self._zrange_members(
            table_ref, index_name, left, right, limit, desc, group
        )
        dtype = table_ref.comp_cls.dtype_map_[index_name]
        skip = len(self.group_prefix_of_(table_ref, index_name, group))
        result = []
        for member in members:
            value = member_value_(member)[skip:]
            result.append((from_sortable_bytes(dtype, value), member_row_id_(member)))
        return result

    # ============ 提交序号列的库外消费方：读计数器、增量读 ============

    @staticmethod
    def commit_seq_field_(comp_cls: type[BaseComponent]) -> str:
        """内部方法：组件的提交序号列字段名，没有就抛 ValueError"""
        field = comp_cls.commit_seq_
        if field is None:
            raise ValueError(
                _("组件 `{comp}` 没有提交序号列（commit_seq）").format(
                    comp=comp_cls.name_
                )
            )
        return field

    def _seq_counter_keys(
        self,
        table_ref: TableReference,
        groups: Sequence[Mapping[str, Any] | None],
    ) -> list[str]:
        """各组计数器的 key，顺序同 groups。分组值都在这里校验完，之后才访问数据库"""
        comp_cls = table_ref.comp_cls
        field = self.commit_seq_field_(comp_cls)
        prefix = self.cluster_prefix(table_ref)
        return [
            self.seq_counter_key_(
                prefix, comp_cls, self.group_prefix_of_(table_ref, field, group)
            )
            for group in groups
        ]

    @override
    async def seq_head(
        self, table_ref: TableReference, group: Mapping[str, Any] | None = None
    ) -> int:
        """读一个组的计数器，被写坏了就抛 RuntimeError、带上 key 和值，见基类"""
        (key,) = self._seq_counter_keys(table_ref, [group])
        (raw,) = await self.kv_get_many_([key])
        head = seq_head_value_(raw)
        if head is None:
            raise RuntimeError(
                _(
                    "提交序号列的计数器 {key} 的值不对：{raw}，应该是非负整数。组里还有行时，"
                    "跑 hetu upgrade 重建索引会把它校准回来；没有行时 upgrade 不动它，要手工"
                    "改成不小于所有消费方游标的值，不能直接删"
                ).format(key=key, raw=repr(raw))
            )
        return head

    @override
    async def seq_head_many(
        self,
        table_ref: TableReference,
        groups: Sequence[Mapping[str, Any] | None],
    ) -> list[int | None]:
        """批量读计数器，逐组返回：被写坏了的那一组是 None，不拖住别的组，见基类"""
        keys = self._seq_counter_keys(table_ref, groups)
        if not keys:
            return []
        return [seq_head_value_(raw) for raw in await self.kv_get_many_(keys)]

    @override
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
        """提交序号列的增量读，见基类"""
        comp_cls = table_ref.comp_cls
        field = self.commit_seq_field_(comp_cls)
        after = seq_int_arg_("after", after)
        if not 0 <= after < COMMIT_SEQ_LIMIT:
            raise ValueError(
                _("seq_read 的 after 要在 [0, 2^53) 里，收到：{after}").format(
                    after=after
                )
            )
        limit = seq_int_arg_("limit", limit)
        if limit < 1:
            raise ValueError(
                _("seq_read 的 limit 必须是正整数，收到：{limit}").format(limit=limit)
            )
        if after_id is not None:
            after_id = seq_int_arg_("after_id", after_id)
            if not -(1 << 63) <= after_id < (1 << 63):
                raise ValueError(
                    _("seq_read 的 after_id 不在 int64 范围里：{id}").format(
                        id=after_id
                    )
                )
            if after < 1:
                raise ValueError(_("给了 after_id 时 after 必须 >= 1：0 号没有行"))
        if not isinstance(row_format, RowFormat) or row_format is RowFormat.ID_LIST:
            raise ValueError(
                _("seq_read 不支持 row_format={fmt}").format(fmt=repr(row_format))
            )

        # 1. 一条 ZRANGE：读的那个节点上的原子快照。给了锚点就从 after 读起，多读一个
        anchored = after_id is not None
        idx_key, b_left, b_right, _empty = self.zrange_args_(
            table_ref, field, after if anchored else after + 1, math.inf, False, group
        )
        members = await self.zrange_bylex_(
            idx_key, b_left, b_right, False, limit + 1 if anchored else limit
        )
        # 值段是组前缀 P 加号的 8 字节编码；同号的 member 按 id 排，号是不降的
        dtype = comp_cls.dtype_map_[field]
        entries = [
            (
                int(from_sortable_bytes(dtype, member_value_(member)[-8:])),
                member_row_id_(member),
                member,
            )
            for member in members
        ]

        # 2. 核对序列，只看 member
        name = comp_cls.name_
        start = 0
        if anchored and entries:
            assert after_id is not None
            at_after = list(itertools.takewhile(lambda e: e[0] == after, entries))
            if not at_after:
                # 后面有号、游标那一行却没了：按裁剪规则它不会被正常删掉
                raise CommitSeqDiverged(
                    name, group, after, after_id, None, "anchor_missing"
                )
            others = [row_id for _seq, row_id, _m in at_after if row_id != after_id]
            if others:
                # 锚点还在、另有同号的行是重号；锚点不在了、这一号是别的行是号被复用
                reason = "duplicate" if len(others) < len(at_after) else "reused"
                raise CommitSeqDiverged(name, group, after, after_id, others[0], reason)
            start = len(at_after)
        rest = entries[start : start + limit]
        for (prev_seq, prev_id, _pm), (seq, row_id, _m) in itertools.pairwise(rest):
            if seq == prev_seq:
                raise CommitSeqDiverged(name, group, seq, prev_id, row_id, "duplicate")
        if not rest:
            empty: Any = (
                self.rows_decode_(comp_cls, [])
                if row_format is RowFormat.STRUCT
                else []
            )
            return SeqReadResult(empty, None)

        # 3. 取行，遇到第一个读不到的就停：它不跳过，记为 blocked。取行不钉节点，可能和 ZRANGE
        # 落在不同的节点上（设计稿 §15）：取行的节点落后时只会停下，缺口只按上面那条 ZRANGE 确认
        raw_rows = await self.hgetall_many_(
            table_ref, [row_id for _seq, row_id, _m in rest]
        )
        readable = next((i for i, raw in enumerate(raw_rows) if not raw), len(raw_rows))
        decoded = self.rows_decode_(comp_cls, raw_rows[:readable])
        # 行在索引上的值段（带组前缀）要和 member 的对上，同 `_range_checks`：只比号会放过被挪进
        # 本组同一号的别组的行
        values = index_value_bytes_list_(comp_cls, field, decoded)
        for i, (value, (_seq, _row_id, member)) in enumerate(
            zip(values, rest, strict=False)
        ):
            if value != member_value_(member):
                readable = i
                break
        blocked = None
        if readable < len(rest):
            blocked = (rest[readable][0], rest[readable][1])
        if row_format is RowFormat.STRUCT:
            return SeqReadResult(decoded[:readable], blocked)
        return SeqReadResult(
            [
                self.row_decode_(comp_cls, raw, row_format)
                for raw in raw_rows[:readable]
            ],
            blocked,
        )

    @override
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
        """见基类"""
        members, key, b_left, b_right = await self._zrange_members(
            table_ref, index_name, left, right, limit, desc, group
        )
        return self.range_observation_(
            index_name, key, members, b_left, b_right, limit, desc
        )

    @override
    def index_sort_keys_(
        self, table_ref: TableReference, index_name: str, rows: np.ndarray
    ) -> list[bytes]:
        """
        见基类。排序键就是索引 member：值段（复合索引是组前缀加本字段的编码）+ 0x00 + id 段
        （见 `ROW_ID_TOKEN_LEN`），与 commit 写索引时一致
        """
        values = index_value_bytes_list_(table_ref.comp_cls, index_name, rows)
        return [
            index_member_(value, row_id)
            for value, row_id in zip(values, rows["id"].tolist())
        ]

    @override
    def index_query_keys_(
        self,
        table_ref: TableReference,
        index_name: str,
        left: int | float | str | bytes | bool,
        right: int | float | str | bytes | bool | None,
        *,
        group: Mapping[str, Any] | None = None,
    ) -> tuple[bytes, bytes]:
        """见基类。ZRANGE 的两端都是 "[" 开头的闭区间，去掉 "[" 就是排序键的闭区间"""
        lo, hi = self.range_normalize_(
            table_ref.comp_cls.dtype_map_[index_name],
            left,
            right,
            False,
            self.group_prefix_of_(table_ref, index_name, group),
        )
        return lo[1:], hi[1:]

    @override
    def shrink_observation_(
        self, obs: RangeObservation, last_key: bytes, desc: bool
    ) -> None:
        """见基类。观察区间是 ZLEXCOUNT 的两端，截断的一端（升序是上界、降序是下界）收到 last_key"""
        lo, hi = obs.bounds
        end = b"[" + last_key
        members = obs.members
        if desc:
            if end <= lo:
                return
            obs.bounds = (end, hi)
            keep = sum(1 for member in members if member >= last_key)
        else:
            if end >= hi:
                return
            obs.bounds = (lo, end)
            keep = sum(1 for member in members if member <= last_key)
        del obs.ids[keep:]
        del members[keep:]

    # ============ 提交 ============

    def _range_checks(self, idmap: IdentityMap) -> list[list[str | bytes | int]]:
        """
        把本事务的 range 观察变成 commit 的 CNT 检查（ZLEXCOUNT 观察区间 == 读到的行数）。

        行数不变 + 读到的行 VER 不变，就说明区间里还是这些行；前提是读到的每一行，本事务
        手里的数据（VER 钉住的那个版本）与读取时索引里的 member 一致。ZRANGE 与随后取行
        不是原子的、还可能打到不同节点，中间有行被改走又有行插进来时行数可能不变，所以
        这里先在 worker 上核对，对不上直接判竞态，不去 master。本事务新 insert 的行不核对，
        主键冲突交给 NX 判定（否则盲插已存在的 id 会从 UniqueViolation 变成无限重试）。
        unique 列点查已由 VER / UNIQ 保证不变的，不发 CNT（见 range_observations_to_check）；
        同一查询读了几次的，被另一条包含的也不发（见 _drop_covered）。
        """
        if located := idmap.inconsistent_range():
            raise InconsistentRangeRead(*located)
        for ref, observations in idmap.range_observations().items():
            comp_cls = ref.comp_cls
            # 同一个 member 在几次读里都读到时只核对一次（比如"取一行、删一行"的循环，每次
            # 读都带着之前删掉的整段前缀）
            checked: set[tuple[str, bytes]] = set()
            for obs in observations:
                for row_id, member in zip(obs.ids, obs.members):
                    if (obs.index_name, member) in checked:
                        continue
                    checked.add((obs.index_name, member))
                    row = idmap.db_row(ref, row_id)
                    if row is None:
                        continue
                    # 按整个值段比：复合索引只比本字段的话，读取期间被挪到别的组（本字段没变）
                    # 的行会被放过
                    value = index_value_bytes_(comp_cls, obs.index_name, row)
                    if value != member_value_(member):
                        raise InconsistentRangeRead(
                            comp_cls.name_, obs.index_name, row_id
                        )
        checks: list[list[str | bytes | int]] = []
        for ref, observations in idmap.range_observations_to_check().items():
            for obs in self._drop_covered(observations):
                lo, hi = obs.bounds
                assert obs.key, "range 观察缺 key（应由 range_observation_ 生成）"
                checks.append(
                    [
                        "CNT",
                        # 读的那个 zset：复合索引是这一组的 key
                        obs.key,
                        lo,
                        hi,
                        len(obs.ids),
                        f"{ref.comp_cls.name_}.{obs.index_name}",
                    ]
                )
        return checks

    @staticmethod
    def _covers(outer: RangeObservation, inner: RangeObservation) -> bool:
        """
        outer 的区间包含 inner 的，且 outer 在 inner 区间里读到的 member 就是 inner 读到的：
        outer 的 CNT 加上读到的行的 VER 保证 outer 区间里还是那些行，inner 区间也就没变
        """
        (outer_lo, outer_hi), (inner_lo, inner_hi) = outer.bounds, inner.bounds
        if not (outer_lo <= inner_lo and inner_hi <= outer_hi):
            return False
        # 两端都是 "[" 开头的闭区间
        lo, hi = inner_lo[1:], inner_hi[1:]
        inside = {member for member in outer.members if lo <= member <= hi}
        return inside == set(inner.members)

    @classmethod
    def _drop_covered(
        cls, observations: list[RangeObservation]
    ) -> list[RangeObservation]:
        """
        同一查询读了几次（比如本事务写入前后各读一次，截断读收到的位置不同）：被另一条包含的
        观察不用单独发 CNT。只在同一索引、同一查询的观察之间比，几次读之间库里的行变了的
        （有并发写入）对不上，都留着。
        """
        if len(observations) < 2:
            return observations
        groups: dict[tuple, list[RangeObservation]] = {}
        for obs in observations:
            groups.setdefault((obs.index_name, obs.query), []).append(obs)
        if len(groups) == len(observations):
            return observations
        kept_all: list[RangeObservation] = []
        for group in groups.values():
            kept: list[RangeObservation] = []
            for obs in group:
                if any(cls._covers(other, obs) for other in kept):
                    continue
                kept = [other for other in kept if not cls._covers(obs, other)]
                kept.append(obs)
            kept_all.extend(kept)
        return kept_all

    def build_commit_payload_(self, idmap: IdentityMap) -> tuple[list[str], list]:
        """
        把 IdentityMap 里的修改组装成提交脚本的输入：返回 (keys, payload)，payload 为
        `[checks, pushes, deleted, table_pubs, value_chans, seq_allocs]`（格式见
        `redis/commit_v2.lua`），没有号要发时不带最后的 `seq_allocs`。

        提交序号列（commit_seq）的组件，insert 的 HSET 换成 HSETSEQ：号由提交脚本在检查都通过
        之后发（`seq_allocs` 是这次每个计数器要发几个），写进这一行、拼出这一行在这个索引上的
        member（见 `seq_counter_key_`、设计稿 docs/superpowers/specs/2026-11-09-commit-seq-design.md）。
        """

        def _key_must_not_exist(_key: str, _race: bool, _label: str):
            """
            添加key must not exist的检查（insert 主键、按 id 读空过的行）；_race 表示本事务曾
            get 观察其不存在
            """
            (race_checks if _race else strict_checks).append(
                ["NX", _key, "RACE" if _race else "UNIQUE", _label]
            )

        def _version_must_match(_key: str, _old_version):
            """添加version match的检查，恒为竞态类"""
            race_checks.append(["VER", _key, _old_version])

        def _unique_meet(_idx_key: str, _value: bytes, _race: bool, _label: str):
            """
            添加unique索引检查：_value 是索引值段（复合索引带组前缀），这个值上已有别的行即冲突。
            _race 表示本事务曾观察该值不存在，冲突判竞态(RACE)，否则判确定性冲突(UNIQUE)
            """
            (race_checks if _race else strict_checks).append(
                [
                    "UNIQ",
                    _idx_key,
                    b"[" + _value + b"\x00",
                    b"[" + _value + b"\x00\xff",
                    "RACE" if _race else "UNIQUE",
                    _label,
                ]
            )

        def _index_label(_comp_cls, _index_name: str) -> str:
            """冲突信息里的索引名，复合索引带上分组列。只在发 UNIQ 时才拼"""
            _groups = _comp_cls.index_groups_[_index_name]
            if not _groups:
                return f"{_comp_cls.name_}.{_index_name}"
            return f"{_comp_cls.name_}.{_index_name}({','.join(_groups)})"

        def _enter_channel(_comp_cls, _index_name: str, _idx_key: str, _value: bytes):
            """
            单列 point_sub 索引登记"进入"的值频道（insert 的值、update 后的新值）。离开（delete、
            改走）由订阅者订着的行频道发现，不用发。同一频道一个事务只发一条。复合索引不发：每组
            一个 zset，按组订阅订这一组 key 的 keyspace 通知
            """
            if (
                _index_name in _comp_cls.point_subs_
                and not _comp_cls.index_groups_[_index_name]
            ):
                value_chans[self.value_channel_(_idx_key, _value)] = None

        def _hset_args(_old_version, _update: dict[str, str | bytes]) -> list:
            """hset 的字段参数：_version 加一（到上限回绕，见 MAX_VERSION）在前，接着是要写的字段"""
            _ver = int(_old_version) + 1
            if _ver > MAX_VERSION:
                _ver = 1
            _update.pop("_version", None)  # 无视用户传入的_version字段
            return [
                "_version",
                str(_ver),
                *itertools.chain.from_iterable(_update.items()),
            ]

        def _hset_key(_key, _old_version, _update: dict[str, str | bytes]):
            """添加hset的push命令"""
            pushes.append(["HSET", _key, *_hset_args(_old_version, _update)])

        def _hsetseq_key(
            _key: str,
            _update: dict[str, str | bytes],
            _seq_name: str,
            _counter_key: str,
            _zset_key: str,
            _prefix: bytes,
            _b_row_id: bytes,
        ):
            """
            添加 HSETSEQ：提交脚本按 seq_allocs 的第 _slot 项发号，写进这一行（号放在 HSET 的最后
            一对，中间要是还留着 0 也会被它盖掉），再把 P + 号的编码 + 0x00 + id 段 ZADD 到组 key
            """
            # 下标按第一次出现的顺序从 1 起（Lua 数组从 1 开始），和 seq_counts 的顺序一致
            _slot = seq_slots.setdefault(_counter_key, len(seq_slots) + 1)
            seq_counts[_counter_key] = seq_counts.get(_counter_key, 0) + 1
            _update.pop(_seq_name, None)  # 行里的 0 不写，号由提交脚本填
            _args = _hset_args(random.randrange(FIRST_VERSION_SPAN), _update)
            pushes.append(
                [
                    "HSETSEQ",
                    _slot,
                    _zset_key,
                    _prefix,
                    _b_row_id,
                    _key,
                    *_args,
                    _seq_name,
                    "",
                ]
            )

        def _del_key(_key):
            """添加del的push命令"""
            pushes.append(["DEL", _key])

        dirties, read_versions = idmap.get_commit_rows()
        if not dirties:
            raise ValueError(_("没有脏数据需要提交"))

        first_ref = idmap.first_reference()
        assert first_ref is not None, "typing检查"
        # 本事务曾 get 观察"不存在"的 unique 列：{ref: {row_id: {field}}}，决定冲突判 RACE 还是 UNIQUE
        absent_by_ref = idmap.get_absent_unique_fields()
        # range 读的区间校验（防幻读）；读取本身就不一致的，这里直接抛 RaceCondition
        range_checks = self._range_checks(idmap)

        # 组合成checks/pushes命令表，减少lua脚本的复杂度
        # checks有exists/unique/version/区间行数，分两组：竞态类在前（VER、带 RACE 标记的
        # NX/UNIQ，最后是区间的 CNT），确定性类在后。Lua 首个失败即返回 → 同时存在两类冲突
        # 时 RACE 优先（保住 upsert 锚定列与其他 unique 列同时撞车时"重试后转 update"的
        # 语义；基于过时区间做的决定撞上 unique 也该重试）。CNT 排在其他竞态检查之后，
        # 同时冲突时报出的仍是原来的信息
        # pushes有hset/zadd/zrem/del
        race_checks: list[list[str | bytes]] = []
        strict_checks: list[list[str | bytes]] = []
        pushes: list[list[str | bytes | int]] = []
        # 本事务删掉的行：{id 段: True}，id 段同索引 member 末尾的（见 ROW_ID_TOKEN_LEN）
        deleted: dict[bytes, bool] = {}
        # 主动 PUBLISH 的通知只有两种，都只给声明了的组件/索引发（PUBLISH 很贵，见
        # benchmark/redis_publish_cost_result.md；tests/test_arch_publish.py 守门，别往这里
        # 加新通知、也别往消息里塞内容）：
        # - 表频道 [channel, msgpack(row_id列表)]：table_sub 组件，一个事务一张表一条
        # - 值频道 channel（消息为空串）：point_sub 索引的"进入"，一个事务每个 (索引, 值) 一条
        table_pubs: list[list[str | bytes]] = []
        value_chans: dict[str, None] = {}  # 有序去重
        # 提交序号列这次要发的号：计数器 key → 个数，按第一次出现的顺序，最后变成 payload 的
        # seq_allocs [[计数器 key, 个数], …]；seq_slots 是计数器 key → 在其中的下标（从 1 起），
        # HSETSEQ 按下标取号
        seq_counts: dict[str, int] = {}
        seq_slots: dict[str, int] = {}

        for ref, (inserts, (old_rows, new_rows), deletes) in dirties.items():
            cluster_prefix = self.cluster_prefix(ref)
            id_prefix = cluster_prefix + ":id:"
            idx_prefix = cluster_prefix + ":index:"
            comp_cls = ref.comp_cls
            unique_fields = comp_cls.uniques_
            indexes = comp_cls.indexes_
            column_indexes = comp_cls.column_indexes_
            comp_name = comp_cls.name_
            absent_rows = absent_by_ref.get(ref, {})
            dtype_map = comp_cls.dtype_map_
            index_groups = comp_cls.index_groups_
            seq_name = comp_cls.commit_seq_
            # 索引全部转换为bytes索引（score统一用0，不需要score排序），测试下来lex和score
            # 排序性能是一样的。member 是值段 + 0x00 + id 段（定宽，见 ROW_ID_TOKEN_LEN），值段见
            # index_value_bytes_；单列索引的值段就是本字段的编码，热路径上直接编码。复合索引每组
            # 一个 zset，ZADD / ZREM / UNIQ 都发到行所在组的 key（见 composite_entry_）
            # insert
            for insert in inserts:
                row_id = str(insert["id"])
                key = id_prefix + row_id
                absent = absent_rows.get(int(row_id), set())
                _key_must_not_exist(
                    key, "id" in absent, f"{comp_name}.id id={row_id} insert"
                )
                entries: list[tuple[str, str, bytes]] = []  # (索引名, zset key, 值段)
                for name in indexes:
                    if name == seq_name:
                        continue  # 号由提交脚本发，member 由 HSETSEQ 拼
                    if index_groups[name]:
                        idx_key, value = self.composite_entry_(
                            comp_cls, idx_prefix + name, name, insert
                        )
                    else:
                        idx_key = idx_prefix + name
                        value = to_sortable_bytes(dtype_map[name].type(insert[name]))
                    entries.append((name, idx_key, value))
                for name, idx_key, value in entries:
                    if name in unique_fields:
                        _unique_meet(
                            idx_key,
                            value,
                            name in absent,
                            f"{_index_label(comp_cls, name)} id={row_id} insert",
                        )
                b_row_id = row_id_token_(int(row_id))
                if seq_name is None:
                    # 版本号随机起步（见 FIRST_VERSION_SPAN）
                    _hset_key(key, random.randrange(FIRST_VERSION_SPAN), insert)
                else:
                    seq_prefix = (
                        row_group_prefix_(comp_cls, seq_name, insert)
                        if index_groups[seq_name]
                        else b""
                    )
                    _hsetseq_key(
                        key,
                        insert,
                        seq_name,
                        self.seq_counter_key_(cluster_prefix, comp_cls, seq_prefix),
                        self.index_zset_key_(idx_prefix + seq_name, seq_prefix),
                        seq_prefix,
                        b_row_id,
                    )
                for name, idx_key, value in entries:
                    _enter_channel(comp_cls, name, idx_key, value)
                    pushes.append(["ZADD", idx_key, "0", value + b"\x00" + b_row_id])
            # update：new_row 只含改了的字段。受影响的索引是改动列用到的索引（复合索引改了
            # 分组列也算），值段变了才写：ZREM 旧 member、ZADD 新 member、unique 查新值。按值段
            # 判断而不是按字段判断，NaN 变号这种编码不变的改动不会撞上自己
            for old_row, new_row in zip(old_rows, new_rows):
                row_id = str(old_row["id"])
                key = id_prefix + row_id
                old_version = old_row["_version"]
                _version_must_match(key, old_version)
                touched = {
                    name for col in new_row for name in column_indexes.get(col, ())
                }
                if seq_name in touched and (
                    seq_name in new_row or int(old_row[seq_name]) > 0
                ):
                    # repo 的 update 已经拦过，这里防绕过 repo 的写入：号只能由提交脚本写，有号的
                    # 行搬到别的组会和那一组的号重复。库里号 ≤ 0（没有号）的历史行可以换组
                    raise ValueError(
                        _(
                            "{comp_name}.{field} 是 commit_seq 列：不能改它，有号的行也不能改"
                            "分组列（id={row_id}）"
                        ).format(comp_name=comp_name, field=seq_name, row_id=row_id)
                    )
                # 复合索引的新值段要用合成后的整行（旧值上覆盖改动列），用到时才合成
                merged = new_row
                # (索引名, 旧 zset key, 旧值段, 新 zset key, 新值段)：复合索引换组时新旧 key 不同
                changes: list[tuple[str, str, bytes, str, bytes]] = []
                if touched:
                    for name in indexes:
                        if name not in touched:
                            continue
                        if index_groups[name]:
                            if merged is new_row:
                                merged = {**old_row, **new_row}
                            old_key, old_value = self.composite_entry_(
                                comp_cls, idx_prefix + name, name, old_row
                            )
                            new_key, new_value = self.composite_entry_(
                                comp_cls, idx_prefix + name, name, merged
                            )
                        else:
                            # 单列索引：touched 说明本字段改了，new_row 里有它
                            dtype_type = dtype_map[name].type
                            old_key = new_key = idx_prefix + name
                            old_value = to_sortable_bytes(dtype_type(old_row[name]))
                            new_value = to_sortable_bytes(dtype_type(new_row[name]))
                        if old_value != new_value:
                            changes.append(
                                (name, old_key, old_value, new_key, new_value)
                            )
                absent = absent_rows.get(int(row_id), set())
                for name, _old_key, _old_value, new_key, new_value in changes:
                    if name in unique_fields:
                        _unique_meet(
                            new_key,
                            new_value,
                            name in absent,
                            f"{_index_label(comp_cls, name)} id={row_id} update",
                        )
                _hset_key(key, old_version, new_row)
                b_row_id = row_id_token_(int(row_id))
                for _name, old_key, old_value, _new_key, _new_value in changes:
                    pushes.append(["ZREM", old_key, old_value + b"\x00" + b_row_id])
                for name, _old_key, _old_value, new_key, new_value in changes:
                    _enter_channel(comp_cls, name, new_key, new_value)
                    pushes.append(
                        ["ZADD", new_key, "0", new_value + b"\x00" + b_row_id]
                    )
            # delete：按数据库里的原值清索引
            for delete in deletes:
                row_id = str(delete["id"])
                b_row_id = row_id_token_(int(row_id))
                # 传入删掉的行的 id 段：之后 unique 检查查到的 member 是这些行的，不算冲突
                deleted[b_row_id] = True
                key = id_prefix + row_id
                old_version = delete["_version"]
                _version_must_match(key, old_version)
                for name in indexes:
                    if index_groups[name]:
                        idx_key, value = self.composite_entry_(
                            comp_cls, idx_prefix + name, name, delete
                        )
                    else:
                        idx_key = idx_prefix + name
                        value = to_sortable_bytes(dtype_map[name].type(delete[name]))
                    pushes.append(["ZREM", idx_key, value + b"\x00" + b_row_id])
                _del_key(key)
            # 变动的 row_id 只有表频道要用：没声明 table_sub 的组件（绝大多数）不收集
            if comp_cls.table_sub_:
                updated_ids = [row["id"] for row in old_rows]
                if len(comp_cls.hidden_fields_) > 1:
                    # 只改了 hidden 字段的行客户端看不出变化，不叫醒整表订阅（new_row 只含改了
                    # 的字段）。RLS 判定用的字段例外：改了它行会对订阅者变得可见 / 不可见
                    quiet = set(comp_cls.hidden_fields_)
                    if comp_cls.rls_compare_:
                        quiet.discard(comp_cls.rls_compare_[1])
                    updated_ids = [
                        old_row["id"]
                        for old_row, new_row in zip(old_rows, new_rows)
                        if not new_row.keys() <= quiet
                    ]
                touched_ids = [
                    *(row["id"] for row in inserts),
                    *updated_ids,
                    *(str(row["id"]) for row in deletes),
                ]
                if touched_ids:
                    ids_msg: bytes = msg_packer.pack(touched_ids)  # type: ignore
                    table_pubs.append([self.table_channel(ref), ids_msg])

        # 对纯读行（含改了又改回原值的行）加版本检查，防止事务依赖的陈旧读：
        # 事务读到的某行，在提交前若被其他事务修改，本事务应失败重试。
        for ref, row_versions in read_versions.items():
            clean_id_prefix = self.cluster_prefix(ref) + ":id:"
            for row_id, old_version in row_versions.items():
                _version_must_match(clean_id_prefix + str(row_id), old_version)
        # 按 id 读空过的行（get(id=) / get_by_id）：确认仍然不存在。读空之后被别的事务插入了，
        # 或者读空读到的是落后的副本，判竞态。本事务 insert 了的 id 不在这里（insert 的主键检查
        # 就是这一条）；按 unique 列读空的由区间观察的 CNT 校验
        for ref, absent_ids in idmap.absent_ids_to_check().items():
            absent_id_prefix = self.cluster_prefix(ref) + ":id:"
            comp_name = ref.comp_cls.name_
            for row_id in absent_ids:
                _key_must_not_exist(
                    absent_id_prefix + str(row_id),
                    True,
                    f"{comp_name}.id id={row_id} observed absent",
                )

        checks = race_checks + range_checks + strict_checks
        # 添加一个带cluster id的key，指明lua脚本执行的集群
        keys = [self.row_key(first_ref, 1)]
        payload: list[Any] = [checks, pushes, deleted, table_pubs, list(value_chans)]
        if seq_counts:
            # 没有号要发时不带这一段：提交脚本读到 nil，就不建发号用的表、不进发号的循环
            payload.append([[key, count] for key, count in seq_counts.items()])
        return keys, payload

    @staticmethod
    def raise_for_commit_response_(resp: bytes) -> None:
        """
        提交脚本的返回串 → 异常：`RACE` 可重试，`UNIQUE` 是确定性冲突，`SEQ` 是提交序号列发号失败
        （发出的号会超出 [1, 2^53)：计数器到了上限，或者被手工改成了负数），一个号都没发、什么都
        没写，不重试。计数器上的 Redis 命令出错（比如 key 被写成了别的类型）时，提交脚本把已经
        加上去的计数器减回去，再原样返回 Redis 的错误，由 redis-py 抛出
        """
        text = resp.decode("utf-8")
        if text != "committed":
            if text.startswith("RACE"):
                raise RaceCondition(text)
            elif text.startswith("UNIQUE"):
                # 确定性冲突：本事务从未 get 观察该值不存在，重试无意义
                raise UniqueViolation(text)
            elif text.startswith("SEQ"):
                raise RuntimeError(
                    _("提交序号列发号失败，本次提交没有写入：{resp}").format(resp=text)
                )
            else:
                raise RuntimeError(_("未知的提交错误：{resp}").format(resp=text))

    def commit_script_(self, keys: list[str], args: list[bytes]) -> Awaitable[bytes]:
        """
        原子执行一次提交：`args[0]` 是 msgpack 打包的 payload（见 `build_commit_payload_`），返回
        `b"committed"` 或 `b"RACE: …"` / `b"UNIQUE: …"` 的 awaitable。Redis 是 `commit_v2.lua`，
        SQLite 是它的 Python 版。测试的碰头点、抓 payload 都 patch 这里。
        """
        raise NotImplementedError

    @override
    async def commit(self, idmap: IdentityMap) -> None:
        """
        使用事务，向数据库提交IdentityMap中的所有数据修改

        Exceptions
        --------
        RaceCondition
            数据已被其他事务修改（版本不符）；或主键 / unique 冲突命中了本事务曾 `get`
            观察其不存在的值（基于过期快照）；或本事务 range 读过的区间变了；或本事务按 id
            读空过的行已经存在，可重试
        UniqueViolation
            主键 / unique 值已被占用，且本事务从未观察其不存在：确定性冲突，不重试

        """
        assert not self.is_servant, _("从节点不允许提交事务")
        keys, payload = self.build_commit_payload_(idmap)
        resp = await self.commit_script_(keys, [msg_packer.pack(payload)])  # type: ignore
        if resp != b"committed":
            self.raise_for_commit_response_(resp)

    # ============ 其他 ============

    @staticmethod
    def check_direct_set_(table_ref: TableReference, kwargs: dict[str, str]) -> None:
        """`direct_set` 的参数检查：只能写易失组件的非索引字段，至少一个"""
        assert "id" not in kwargs, "id不允许修改"
        assert table_ref.comp_cls.volatile_, "direct_set只能用于易失数据的Component"
        if not kwargs:
            raise ValueError(_("direct_set 至少要写一个字段"))
        for prop in kwargs:
            # 复合索引的分组列也是索引的一部分：直接写它会把复合索引写坏
            if prop in table_ref.comp_cls.column_indexes_:
                raise ValueError(
                    _("索引字段`{prop}`不允许用direct_set修改").format(prop=prop)
                )
            if prop not in table_ref.comp_cls.prop_idx_map_:
                raise ValueError(
                    _("Component `{comp_name}` 没有字段`{prop}`").format(
                        comp_name=table_ref.comp_name, prop=prop
                    )
                )

    @classmethod
    def rebuild_entry_(
        cls,
        comp_cls: type[BaseComponent],
        idx_key: str,
        index_name: str,
        struct: np.record,
        raws: Mapping[str, bytes],
        row_id: int,
    ) -> tuple[str, bytes]:
        """
        重建索引时，由行里存的原始字节算出 (zset key, 索引 member)：先按 dtype 转换再编码，与 commit
        写索引时一致。复合索引的 key 是这一行所在组的，单列索引就是 idx_key。raws 是该索引各列
        （复合索引含分组列）存的原始字节。bytes 字段用原始字节（同 `rows_decode_`），decode 成 str
        后非 ASCII 的塞不进 S 列。`struct` 是调用方备好的一行，只拿来按 dtype 转值。
        """
        bytes_fields = comp_cls.bytes_fields_
        for col, raw in raws.items():
            struct[col] = raw if col in bytes_fields else raw.decode()
        if comp_cls.index_groups_[index_name]:
            key, value = cls.composite_entry_(comp_cls, idx_key, index_name, struct)
        else:
            key, value = idx_key, index_value_bytes_(comp_cls, index_name, struct)
        return key, index_member_(value, row_id)
