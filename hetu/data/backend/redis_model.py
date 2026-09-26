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
from collections.abc import Awaitable, Iterable
from typing import TYPE_CHECKING, Any, Literal, Never, overload, override

# from msgspec import msgpack  # 不支持关闭bin type，lua 的msgpack库7年没更新了
import msgpack
import numpy as np

from ...i18n import _
from .base import (
    BackendClient,
    InconsistentRangeRead,
    RaceCondition,
    RowFormat,
    UniqueViolation,
    exact_number_,
    inverted_bounds_error_,
    normalize_int_bounds_,
    peel_bound_,
    sortable_token,
    to_sortable_bytes,
)
from .idmap import RangeObservation

if TYPE_CHECKING:
    from ..component import BaseComponent
    from .idmap import IdentityMap
    from .table import TableReference

msg_packer = msgpack.Packer(use_bin_type=False)


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

        return [comp_cls for comp_cls in SystemClusters().get_components().keys()]

    def _schema_checking(self, components: Iterable[type[BaseComponent]] | None = None):
        """检查Component的schema定义，确保索引字段能编码成可排序的字节"""
        if components is None:
            components = self._get_referred_components()
        for comp_cls in components:
            for field, _is_str in comp_cls.indexes_.items():
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
                elif np.issubdtype(dtype, np.object_):
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
    def row_key(cls, table_ref: TableReference, row_id: str | int) -> str:
        """获取redis表行的key名"""
        return f"{cls.cluster_prefix(table_ref)}:id:{str(row_id)}"

    @classmethod
    def index_key(cls, table_ref: TableReference, index_name: str) -> str:
        """获取redis表索引的key名"""
        return f"{cls.cluster_prefix(table_ref)}:index:{index_name}"

    @override
    def index_channel(self, table_ref: TableReference, index_name: str):
        """返回整个索引的频道名（keyspace 通知）。该索引 zset 任何 ZADD/ZREM 都会通知到该频道"""
        return f"__keyspace@{self.dbi}__:{self.index_key(table_ref, index_name)}"

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
        self.require_point_sub_(table_ref, index_name)
        dtype = table_ref.comp_cls.dtype_map_[index_name]
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
    ) -> tuple[bytes, bytes]:
        """
        规范化范围查询的边界，返回 ZRANGE BYLEX 的两端，按扫描顺序排（desc 时上界在前）。

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
        b_lower = b"[" + lower_value + (b"\x00" if li else b"\x00\xff")
        b_upper = b"[" + upper_value + (b"\x00\xff" if ui else b"\x00")
        return (b_upper, b_lower) if desc else (b_lower, b_upper)

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
        left: float | str | bytes | bool,
        right: float | str | bytes | bool | None,
        desc: bool,
    ) -> tuple[str, bytes, bytes, bool]:
        """
        按索引区间查询要用的：索引 key、ZRANGE BYLEX 的两端（按扫描顺序），以及区间是否为空
        （两端交叉或相等；传反的已在 `range_normalize_` 里报错）。空区间不用去查。
        """
        idx_key = cls.index_key(table_ref, index_name)
        comp_cls = table_ref.comp_cls
        if index_name not in comp_cls.indexes_:
            raise ValueError(f"Component `{comp_cls.name_}` 没有索引 `{index_name}`")
        b_left, b_right = cls.range_normalize_(
            comp_cls.dtype_map_[index_name], left, right, desc
        )
        empty = (b_left <= b_right) if desc else (b_right <= b_left)
        return idx_key, b_left, b_right, empty

    @staticmethod
    def range_observation_(
        index_name: str,
        members: list[bytes],
        b_left: bytes,
        b_right: bytes,
        limit: int,
        desc: bool,
    ) -> tuple[list[int], RangeObservation]:
        """
        `range_read_` 的后半段：由 ZRANGE 读回的原样 member 与规范化后的两端，得到行 id 与这次读取的
        观察（commit 时变成 CNT 检查，见 `_range_checks`）。
        """
        row_ids = [int(vk.rsplit(b"\x00", 1)[-1]) for vk in members]
        # 观察区间给 ZLEXCOUNT 用，要按 min, max 排；desc 时 b_left 是上界
        lo, hi = (b_right, b_left) if desc else (b_left, b_right)
        if 0 < limit == len(members):
            # 截断读只看到了前 limit 行，观察区间收到最后一个返回的 member 为止
            if desc:
                lo = b"[" + members[-1]
            else:
                hi = b"[" + members[-1]
        return row_ids, RangeObservation(index_name, row_ids, (lo, hi), members)

    # ============ 提交 ============

    def _range_checks(self, idmap: IdentityMap) -> list[list[str | bytes | int]]:
        """
        把本事务的 range 观察变成 commit 的 CNT 检查（ZLEXCOUNT 观察区间 == 读到的行数）。

        行数不变 + 读到的行 VER 不变，就说明区间里还是这些行；前提是读到的每一行，本事务
        手里的数据（VER 钉住的那个版本）与读取时索引里的 member 一致。ZRANGE 与随后取行
        不是原子的、还可能打到不同节点，中间有行被改走又有行插进来时行数可能不变，所以
        这里先在 worker 上核对，对不上直接判竞态，不去 master。本事务新 insert 的行不核对，
        主键冲突交给 NX 判定（否则盲插已存在的 id 会从 UniqueViolation 变成无限重试）。
        unique 列点查已由 VER / UNIQ 保证不变的，不发 CNT（见 range_observations_to_check）。
        """
        if located := idmap.inconsistent_range():
            raise InconsistentRangeRead(*located)
        for ref, observations in idmap.range_observations().items():
            comp_cls = ref.comp_cls
            for obs in observations:
                dtype = comp_cls.dtype_map_[obs.index_name]
                for row_id, member in zip(obs.ids, obs.members or ()):
                    row = idmap.db_row(ref, row_id)
                    if row is None:
                        continue
                    value = to_sortable_bytes(dtype.type(row[obs.index_name]))
                    if value != member.rsplit(b"\x00", 1)[0]:
                        raise InconsistentRangeRead(
                            comp_cls.name_, obs.index_name, row_id
                        )
        checks: list[list[str | bytes | int]] = []
        for ref, observations in idmap.range_observations_to_check().items():
            for obs in observations:
                lo, hi = obs.bounds
                checks.append(
                    [
                        "CNT",
                        self.index_key(ref, obs.index_name),
                        lo,
                        hi,
                        len(obs.ids),
                        f"{ref.comp_cls.name_}.{obs.index_name}",
                    ]
                )
        return checks

    def build_commit_payload_(self, idmap: IdentityMap) -> tuple[list[str], list]:
        """
        把 IdentityMap 里的修改组装成提交脚本的输入：返回 (keys, payload)，payload 为
        `[checks, pushes, deleted, table_pubs, value_chans]`（格式见 `redis/commit_v2.lua`）。
        """

        def _key_must_not_exist(_key: str, _race: bool, _label: str):
            """添加key must not exist的检查（insert 主键）；_race 表示本事务曾 get 观察其不存在"""
            (race_checks if _race else strict_checks).append(
                ["NX", _key, "RACE" if _race else "UNIQUE", _label]
            )

        def _version_must_match(_key: str, _old_version):
            """添加version match的检查，恒为竞态类"""
            race_checks.append(["VER", _key, _old_version])

        def _unique_meet(
            _unique_fields,
            _dtype_map,
            _idx_prefix,
            _row: dict[str, str | bytes],
            _absent: set[str],
            _comp_name: str,
            _row_id: str,
            _op: str,
        ):
            """添加unique索引检查；_absent 内的列冲突判竞态(RACE)，其余判确定性冲突(UNIQUE)"""
            for _field, _value in _row.items():
                if _field in _unique_fields:
                    _idx_key = _idx_prefix + _field
                    _sortable_value = to_sortable_bytes(_dtype_map[_field].type(_value))
                    _start_val = b"[" + _sortable_value + b"\x00"
                    _end_val = b"[" + _sortable_value + b"\x00\xff"
                    _race = _field in _absent
                    (race_checks if _race else strict_checks).append(
                        [
                            "UNIQ",
                            _idx_key,
                            _start_val,
                            _end_val,
                            "RACE" if _race else "UNIQUE",
                            f"{_comp_name}.{_field} id={_row_id} {_op}",
                        ]
                    )

        def _hset_key(_key, _old_version, _update: dict[str, str | bytes]):
            """添加hset的push命令"""
            # 版本+1
            _ver = int(_old_version) + 1
            _update.pop("_version", None)  # 无视用户传入的_version字段
            # 组合hset, 别忘记写_version
            _kvs = itertools.chain.from_iterable(_update.items())
            pushes.append(["HSET", _key, "_version", str(_ver), *_kvs])

        def _exc_index(
            _indexes, _point_subs, _dtype_map, _idx_prefix, _old, _new, _add
        ):
            """exchange index(zadd/zrem)的push命令"""
            _b_row_id = _old["id"].encode("ascii")
            _values = _new if _add else _old
            for _field in _new.keys():
                if _field in _indexes:
                    _idx_key = _idx_prefix + _field
                    # 索引全部转换为bytes索引，测试下来lex和score排序性能是一样的
                    _sortable_value = to_sortable_bytes(
                        _dtype_map[_field].type(_values[_field])
                    )
                    # 值频道只给声明了 point_sub 的索引、只记"进入"（insert 的值、update 的
                    # 新值）：离开（delete、改走）由订阅者订着的行频道发现，不用发。
                    # 同一 (索引, 值) 一个事务只发一条
                    if _add and _field in _point_subs:
                        value_chans[self.value_channel_(_idx_key, _sortable_value)] = (
                            None
                        )
                    _member = _sortable_value + b"\x00" + _b_row_id
                    if _add:
                        # score统一用0，因为我们不需要score排序功能
                        pushes.append(["ZADD", _idx_key, "0", _member])
                    else:
                        pushes.append(["ZREM", _idx_key, _member])

        def _del_key(_key):
            """添加del的push命令"""
            pushes.append(["DEL", _key])

        dirties = idmap.get_dirty_rows()
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
        pushes: list[list[str | bytes]] = []
        deleted: dict[str, bool] = {}
        # 主动 PUBLISH 的通知只有两种，都只给声明了的组件/索引发（PUBLISH 很贵，见
        # benchmark/redis_publish_cost_result.md；tests/test_arch_publish.py 守门，别往这里
        # 加新通知、也别往消息里塞内容）：
        # - 表频道 [channel, msgpack(row_id列表)]：table_sub 组件，一个事务一张表一条
        # - 值频道 channel（消息为空串）：point_sub 索引的"进入"，一个事务每个 (索引, 值) 一条
        table_pubs: list[list[str | bytes]] = []
        value_chans: dict[str, None] = {}  # 有序去重

        for ref, (inserts, (old_rows, new_rows), deletes) in dirties.items():
            id_prefix = self.cluster_prefix(ref) + ":id:"
            idx_prefix = self.cluster_prefix(ref) + ":index:"
            comp_cls = ref.comp_cls
            unique_fields = comp_cls.uniques_
            indexes = comp_cls.indexes_
            point_subs = comp_cls.point_subs_
            dtype_map = comp_cls.dtype_map_
            comp_name = comp_cls.name_
            absent_rows = absent_by_ref.get(ref, {})
            # insert
            for insert in inserts:
                row_id = str(insert["id"])
                key = id_prefix + row_id
                absent = absent_rows.get(int(row_id), set())
                _key_must_not_exist(
                    key, "id" in absent, f"{comp_name}.id id={row_id} insert"
                )
                _unique_meet(
                    unique_fields,
                    dtype_map,
                    idx_prefix,
                    insert,
                    absent,
                    comp_name,
                    row_id,
                    "insert",
                )
                _hset_key(key, 0, insert)
                _exc_index(
                    indexes, point_subs, dtype_map, idx_prefix, insert, insert, True
                )
            # update
            for old_row, new_row in zip(old_rows, new_rows):
                row_id = str(old_row["id"])
                key = id_prefix + row_id
                old_version = old_row["_version"]
                _version_must_match(key, old_version)
                _unique_meet(
                    unique_fields,
                    dtype_map,
                    idx_prefix,
                    new_row,
                    absent_rows.get(int(row_id), set()),
                    comp_name,
                    row_id,
                    "update",
                )
                _hset_key(key, old_version, new_row)
                _exc_index(
                    indexes, point_subs, dtype_map, idx_prefix, old_row, new_row, False
                )
                _exc_index(
                    indexes, point_subs, dtype_map, idx_prefix, old_row, new_row, True
                )
            # delete
            for delete in deletes:
                # 传入deleted ids，如果之后的unique冲突查到的id在deleted里，就返回false
                deleted[str(delete["id"])] = True
                key = id_prefix + str(delete["id"])
                old_version = delete["_version"]
                _version_must_match(key, old_version)
                _exc_index(
                    indexes, point_subs, dtype_map, idx_prefix, delete, delete, False
                )
                _del_key(key)
            # 变动的 row_id 只有表频道要用：没声明 table_sub 的组件（绝大多数）不收集
            if comp_cls.table_sub_:
                touched_ids = [
                    *(row["id"] for row in inserts),
                    *(row["id"] for row in old_rows),
                    *(str(row["id"]) for row in deletes),
                ]
                if touched_ids:
                    ids_msg: bytes = msg_packer.pack(touched_ids)  # type: ignore
                    table_pubs.append([self.table_channel(ref), ids_msg])

        # 对纯读行加版本检查，防止事务依赖的陈旧读：
        # 事务读到的某行，在提交前若被其他事务修改，本事务应失败重试。
        for ref, row_versions in idmap.get_clean_rows().items():
            clean_id_prefix = self.cluster_prefix(ref) + ":id:"
            for row_id, old_version in row_versions.items():
                _version_must_match(clean_id_prefix + str(row_id), old_version)

        checks = race_checks + range_checks + strict_checks
        # 添加一个带cluster id的key，指明lua脚本执行的集群
        keys = [self.row_key(first_ref, 1)]
        return keys, [checks, pushes, deleted, table_pubs, list(value_chans)]

    @staticmethod
    def raise_for_commit_response_(resp: bytes) -> None:
        """提交脚本的返回串 → 异常：`RACE` 可重试，`UNIQUE` 是确定性冲突"""
        text = resp.decode("utf-8")
        if text != "committed":
            if text.startswith("RACE"):
                raise RaceCondition(text)
            elif text.startswith("UNIQUE"):
                # 确定性冲突：本事务从未 get 观察该值不存在，重试无意义
                raise UniqueViolation(text)
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
            观察其不存在的值（基于过期快照）；或本事务 range 读过的区间变了，可重试
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
            if prop in table_ref.comp_cls.indexes_:
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
    def rebuild_member_(
        cls, struct: np.record, field: str, raw: bytes, is_bytes: bool, row_id: bytes
    ) -> bytes:
        """
        重建索引时，由行里存的原始字节算出索引 member：先按 dtype 转换再编码，与 commit 写索引时
        一致。bytes 字段用原始字节（同 `rows_decode_`），decode 成 str 后非 ASCII 的塞不进 S 列。
        `struct` 是调用方备好的一行，只拿来按 dtype 转值。
        """
        struct[field] = raw if is_bytes else raw.decode()
        return cls.to_sortable_bytes(struct[field]) + b"\x00" + row_id
