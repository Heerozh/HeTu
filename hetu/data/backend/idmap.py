"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, cast

import numpy as np

if TYPE_CHECKING:
    from .table import TableReference

logger = logging.getLogger("HeTu.root")


class RowState(Enum):
    """行状态枚举"""

    CLEAN = 0  # 干净，无变化
    INSERT = 1  # 新插入
    UPDATE = 2  # 需更新数据
    DELETE = 3  # 需从数据库删除


@dataclass(slots=True)
class RangeObservation:
    """
    本事务一次 `range` 读的"观察"：commit 时校验同样的查询现在返回的行没变（防幻读）。
    由后端的 `range_read_` 生成，`bounds` 只有生成它的那个后端看得懂。

    截断读（返回数 == limit）只观察到最后一个返回行为止，区间后面新增的行不在观察里。
    """

    index_name: str
    # 索引快照里读到的 id（按索引顺序），个数即期望行数
    ids: list[int]
    # 后端相关的校验参数
    bounds: tuple
    # Redis：ZRANGE 原样 member，commit 前核对读取一致性；SQL 不用
    members: list[bytes] | None = None
    # 等值点查的值，否则 None
    point: object | None = None
    # 取行时已读不到（ZRANGE 与取行之间被删）的 id
    missing: list[int] = field(default_factory=list)
    # 只保护返回的行、不校验区间（get 命中：约定是"返回一行匹配的"，那一行由 VER 管）。
    # 读取一致性照样核对
    rows_only: bool = False


def _row_to_db(row: np.record, bytes_fields: frozenset[str]) -> dict[str, str | bytes]:
    """整行转成提交用的 {字段: 值}：一律 str()，bytes 字段原样保留字节"""
    assert row.dtype.names  # for type checker, could be removed by python -O
    ret: dict[str, str | bytes] = dict(zip(row.dtype.names, map(str, row.item())))
    for name in bytes_fields:
        ret[name] = bytes(row[name])
    return ret


def changed_fields(new: np.record, old: np.record) -> list[str]:
    """
    同一 dtype 的两行之间值有变化的字段名。先按字节比较，字节相同就算没变，所以没动过
    的 NaN 不算变更（按值比较 NaN != NaN，会让没改的行也发一次更新）；字节不同再比值，
    0.0 与 -0.0 这类值相等的仍算没变，与按值比较一致。
    """
    new_bytes, old_bytes = new.tobytes(), old.tobytes()
    if new_bytes == old_bytes:
        return []
    fields = new.dtype.fields
    assert fields  # for type checker, could be removed by python -O
    return [
        name
        for name, (dt, offset, *_) in fields.items()
        if new_bytes[offset : offset + dt.itemsize]
        != old_bytes[offset : offset + dt.itemsize]
        and new[name] != old[name]
    ]


class IdentityMap:
    """
    用于缓存和管理事务中的对象。
    SessionComponentTable会经由本类来查询和缓存对象。
    BackendSession在提交时可以通过本类，获得脏对象列表，然后想办法合并成事务指令。
    """

    def __init__(self) -> None:
        # 每个Component类型对应一个缓存
        # {TableReference: np.recarray} - 存储行数据
        self._row_cache: dict[TableReference, np.recarray] = {}

        # 储存查询到的行数据初始值，用于对比变更
        self._row_clean: dict[TableReference, dict[int, np.record]] = {}

        # {TableReference: {row_id: RowState}} - 存储每行的状态
        self._row_states: dict[TableReference, dict[int, RowState]] = {}

        # 本事务对“等值精确查询读到不存在”的观察记录（negative observation）。
        # {TableReference: {(index_name, normalized_value), ...}}
        # 用途：commit 时主键 / unique 冲突若命中此集合内的 (列, 值)，说明本事务是基于
        # “它不存在”的过期快照做的决策，应判为 RaceCondition 而非 UniqueViolation
        # （见 get_absent_unique_fields）。
        self._absent: dict[TableReference, set[tuple[str, object]]] = {}

        # 本事务 range 读的观察，commit 时校验区间没变（防幻读），见 RangeObservation；
        # _range_keys 给完全相同的观察去重
        self._ranges: dict[TableReference, list[RangeObservation]] = {}
        self._range_keys: dict[TableReference, set[tuple]] = {}

    @property
    def is_dirty(self) -> bool:
        """检查是否有脏数据"""
        for states in self._row_states.values():
            if any(state != RowState.CLEAN for state in states.values()):
                return True
        return False

    def first_reference(self) -> TableReference | None:
        # range 读空的表只有观察、没有缓存行，也要参与"同一事务组"的判定
        for refs in (self._row_cache, self._ranges):
            if refs:
                return next(iter(refs.keys()))
        return None

    def is_same_txn_group(self, other: TableReference) -> bool:
        first_reference = self.first_reference()
        if first_reference is None:
            return True
        return first_reference.is_same_txn_group(other)

    def _cache(self, table_ref: TableReference):
        if table_ref not in self._row_cache:
            return self._new_cache(table_ref)
        return (
            self._row_cache[table_ref],
            self._row_clean[table_ref],
            self._row_states[table_ref],
        )

    def _new_cache(
        self,
        table_ref: TableReference,
        first_rows: np.record | np.recarray | None = None,
    ):
        """
        新建该表的缓存。给了 first_rows 就直接拷贝，省掉空 recarray 和 np.append；
        单行和范围读取共用此路径，行状态都由调用方标记。
        """
        if first_rows is None:
            cache = np.rec.array(np.empty(0, dtype=table_ref.comp_cls.dtypes))
        else:
            # 必须拷贝：结构化数组的标量下标是源数组的视图，reshape 也不复制
            cache = first_rows.copy().reshape(-1).view(np.recarray)
        clean_cache: dict[int, np.record] = {}
        states: dict[int, RowState] = {}
        self._row_cache[table_ref] = cache
        self._row_clean[table_ref] = clean_cache
        self._row_states[table_ref] = states
        return cache, clean_cache, states

    def add_clean(
        self, table_ref: TableReference, row_s: np.record | np.recarray
    ) -> None:
        """
        添加 一个/多个 查询到的对象到row缓存中。
        如果数据行已存在，则会报错ValueError。
        """
        # 检测新添加数据，和之前的数据是否在同一个实例/集群下
        assert self.is_same_txn_group(table_ref), (
            f"{table_ref} has different transaction context"
        )
        # 检测comp_cls和row格式是否一致
        assert row_s.dtype == table_ref.comp_cls.dtypes, (
            f"row dtype({row_s.dtype}) does not match component class "
            f"({table_ref.comp_cls.name_}, {table_ref.comp_cls.dtypes})"
        )

        single = row_s.ndim == 0
        if table_ref not in self._row_cache:
            # 首批读取直接建立独立缓存，不用先创建空数组再追加。
            _, clean_cache, states = self._new_cache(table_ref, row_s)
        else:
            # 初始化该component的缓存
            cache, clean_cache, states = self._cache(table_ref)

            # 查找是否已存在该ID的行
            if len(cache) > 0:
                existing_idx = np.isin(cache["id"], row_s["id"])
                if np.any(existing_idx):
                    raise ValueError(
                        f"Row with id {cache['id'][existing_idx]} "
                        "already exists in cache"
                    )

            # 添加新行
            self._row_cache[table_ref] = np.rec.array(np.append(cache, row_s))

        # 标记为CLEAN
        if single:
            # 如果是单行数据，直接添加状态
            row_s = cast(np.record, row_s)
            row_id = row_s["id"]
            states[row_id] = RowState.CLEAN
            clean_cache[row_id] = row_s.copy()
        else:
            # 一次复制整批原值；record 只引用这个独立快照，与调用方和工作缓存隔离。
            clean_rows = row_s.copy()
            ids = clean_rows["id"]
            states.update(dict.fromkeys(ids, RowState.CLEAN))
            clean_cache.update(zip(ids, clean_rows))

    def get(
        self, table_ref: TableReference, row_id: int
    ) -> tuple[np.record | None, RowState | None]:
        """
        从缓存中获取指定ID的行。

        Returns
        -------
        如果缓存中有则返回行数据，否则返回None
        """
        if table_ref not in self._row_cache:
            return None, None

        cache = self._row_cache[table_ref]
        if len(cache) == 0:
            return None, None

        # 查找指定ID的行
        idx = np.where(cache["id"] == row_id)[0]
        if len(idx) == 0:
            return None, None

        # 主要提供状态：是否已删除
        states = self._row_states[table_ref]

        # recarray是基于ndarray的，传入参数可以用np.ndarray类型，返回值
        # 应该使用np.recarray类型以保留字段名访问特性(row.field_name)
        # 必须返回拷贝：结构化数组的标量下标是缓存本体的视图，直接交给调用方，调用方改字段
        # 就把缓存里的"旧值"一起改了，之后 update/upsert 拿它与缓存比对会判成"没有变化"。
        return cast(np.record, cache[idx[0]].copy()), states.get(row_id)

    def add_insert(self, table_ref: TableReference, row: np.record) -> None:
        """
        添加一个新插入的对象到缓存，并标记为INSERT状态。
        """
        assert row.ndim == 0, "不能用np.recarry类型，请用np.recarry[0]转换为record"

        # 检测新添加数据，和之前的数据是否在同一个实例/集群下
        assert self.is_same_txn_group(table_ref), (
            f"{table_ref} has different transaction context"
        )
        # 检测comp_cls和row格式是否一致
        assert row.dtype == table_ref.comp_cls.dtypes, (
            f"row dtype({row.dtype}) does not match component class "
            f"({table_ref.comp_cls.name_}, {table_ref.comp_cls.dtypes})"
        )

        assert row["_version"] == 0, f"不得修改_version字段，{row['_version']}"

        # 初始化缓存

        # 添加到缓存
        cache, _, states = self._cache(table_ref)
        # todo np.append可能有性能问题，等py3.15的旁路trace工具再sampling一下优化看看
        self._row_cache[table_ref] = np.rec.array(np.append(cache, row))

        # 标记为INSERT
        states[row["id"]] = RowState.INSERT

    def update(self, table_ref: TableReference, row: np.record) -> None:
        """
        更新一个对象到缓存，并标记为UPDATE状态。
        """
        assert row.ndim == 0, "不能用np.recarry类型，请用np.recarry[0]转换为record"
        # 检测新添加数据，和之前的数据是否在同一个实例/集群下
        assert self.is_same_txn_group(table_ref), (
            f"{table_ref} has different transaction context"
        )
        # 检测comp_cls和row格式是否一致
        assert row.dtype == table_ref.comp_cls.dtypes, (
            f"row dtype({row.dtype}) does not match component class "
            f"({table_ref.comp_cls.name_}, {table_ref.comp_cls.dtypes})"
        )

        if table_ref not in self._row_cache:
            raise ValueError(f"Component {table_ref} not in cache")

        cache, _, states = self._cache(table_ref)

        # 查找并更新行
        row_id = row["id"]
        idx = np.where(cache["id"] == row_id)[0]
        if len(idx) == 0:
            raise ValueError(f"Row with id {row_id} not found in cache")

        assert row["_version"] == cache[idx[0]]["_version"], "不得修改_version字段"

        # 如果是删除状态，不能更新
        if states.get(row_id) == RowState.DELETE:
            raise ValueError(
                f"Row with id {row_id} is marked as DELETE and cannot be updated"
            )

        cache[idx[0]] = row

        # 如果是新插入的行，保持INSERT状态；否则标记为UPDATE
        if states.get(row_id) != RowState.INSERT:
            states[row_id] = RowState.UPDATE

    def mark_deleted(self, table_ref: TableReference, row_id: int) -> None:
        """
        标记指定ID的对象为删除状态。
        """
        if table_ref not in self._row_states:
            raise ValueError(f"Component {table_ref} not in cache")

        cache, _, states = self._cache(table_ref)

        # 查找行必须已存在
        idx = np.where(cache["id"] == row_id)[0]
        if len(idx) == 0:
            raise ValueError(f"Row with id {row_id} not found in cache")

        # 标记为DELETE
        states[row_id] = RowState.DELETE

    def is_deleted(self, table_ref: TableReference, row_id: int) -> bool:
        """
        检查指定ID的对象是否被标记为删除状态。
        """
        if table_ref not in self._row_states:
            return False

        states = self._row_states[table_ref]
        return states.get(row_id) == RowState.DELETE

    @staticmethod
    def _norm_value(value: object) -> object:
        """把np标量归一成python原生值，保证mark/observe两侧key一致。"""
        return value.item() if isinstance(value, np.generic) else value

    def mark_absent(
        self, table_ref: TableReference, index_name: str, value: object
    ) -> None:
        """
        登记一条「本事务等值查询 `index_name==value` 读到了不存在」的观察。
        只应由等值精确查询（按主键或unique索引的get）在读空时调用。
        """
        self._absent.setdefault(table_ref, set()).add(
            (index_name, self._norm_value(value))
        )

    def observed_absent(
        self, table_ref: TableReference, index_name: str, value: object
    ) -> bool:
        """
        本事务是否曾观察到 `index_name==value` 不存在（见 `mark_absent`）。
        """
        absent = self._absent.get(table_ref)
        if not absent:
            return False
        return (index_name, self._norm_value(value)) in absent

    def get_absent_unique_fields(self) -> dict[TableReference, dict[int, set[str]]]:
        """
        对每个待 INSERT / UPDATE 的行，返回其 unique 列（含 id）中本事务曾观察"该值不存在"
        （见 `mark_absent`）的列集合。commit 据此把主键 / unique 冲突判为 `RaceCondition`
        （基于过期快照的乐观并发失败，重试可解）而非 `UniqueViolation`（确定性冲突）。

        Returns
        -------
        {TableReference: {row_id: {field, ...}}}，没有 absent 列的行不出现。
        """
        ret: dict[TableReference, dict[int, set[str]]] = {}
        for table_ref, absent in self._absent.items():
            states = self._row_states.get(table_ref)
            if not absent or not states:
                continue
            dirty_ids = [
                row_id
                for row_id, state in states.items()
                if state == RowState.INSERT or state == RowState.UPDATE
            ]
            # 只查 absent 里出现过的 unique 列，通常 0~2 个
            candidates = table_ref.comp_cls.uniques_ & {field for field, _ in absent}
            if not dirty_ids or not candidates:
                continue
            cache = self._row_cache[table_ref]
            rows_absent: dict[int, set[str]] = {}
            for row in cache[np.isin(cache["id"], dirty_ids)]:
                fields = {
                    field
                    for field in candidates
                    if (field, self._norm_value(row[field])) in absent
                }
                if fields:
                    rows_absent[int(row["id"])] = fields
            if rows_absent:
                ret[table_ref] = rows_absent
        return ret

    def add_range_observation(
        self, table_ref: TableReference, obs: RangeObservation
    ) -> None:
        """登记一次 range 读的观察，commit 时由后端校验。完全相同的观察只留一条。"""
        assert self.is_same_txn_group(table_ref), (
            f"{table_ref} has different transaction context"
        )
        key = (
            obs.index_name,
            obs.bounds,
            tuple(obs.ids),
            tuple(obs.members or ()),
            tuple(obs.missing),
            obs.rows_only,
        )
        seen = self._range_keys.setdefault(table_ref, set())
        if key not in seen:
            seen.add(key)
            self._ranges.setdefault(table_ref, []).append(obs)

    def range_observations(self) -> dict[TableReference, list[RangeObservation]]:
        """本事务全部 range 读的观察 {TableReference: [RangeObservation, ...]}"""
        return self._ranges

    def range_observations_to_check(
        self,
    ) -> dict[TableReference, list[RangeObservation]]:
        """
        需要后端单独校验区间的观察（Redis 用来决定发哪些 CNT）：`range_observations()`
        去掉只保护返回行的（`rows_only`，get 命中），以及 unique 列点查里已由其他检查保证
        不变的：

        - 命中一行，且该行有数据库态：它的 VER 保证它仍是这个值，unique 保证没有第二行；
        - 读空，且本事务把一行写成了这个值（insert，或 update 改成这个值）、又没删掉任何
          数据库态为这个值的行：这个值的 unique 检查带着 RACE 标记（读空时登记过 absent），
          保证提交时除本事务删掉的行外没有这个值，再排除"删掉了这个值的行"就等价于读空。
          删掉过这样的行，说明本事务先看到"没有"、后又读到"有"，读集本身矛盾，要校验区间。
        """
        ret: dict[TableReference, list[RangeObservation]] = {}
        for table_ref, observations in self._ranges.items():
            # 每个 unique 列的写入值 / 删除原值，一次 commit 只扫一遍缓存行
            writes: dict[str, tuple[set[object], set[object]]] = {}
            keep = [
                obs
                for obs in observations
                if not obs.rows_only
                and not self._implied_by_unique(table_ref, obs, writes)
            ]
            if keep:
                ret[table_ref] = keep
        return ret

    def inconsistent_range(self) -> tuple[str, str, int] | None:
        """
        有 range 读在取行时发现索引里的行已被删（读到的不是任何一刻的区间）时，返回
        (组件名, 索引名, 行 id)，否则 None。commit 据此直接判竞态，不用去数据库。
        """
        for table_ref, observations in self._ranges.items():
            for obs in observations:
                if obs.missing:
                    return table_ref.comp_cls.name_, obs.index_name, obs.missing[0]
        return None

    def db_row(self, table_ref: TableReference, row_id: int) -> np.record | None:
        """
        该行在本事务里的数据库态：读取时的原样副本（commit 时 VER 校验的就是它的版本）。
        本事务新 insert 的行、或不在缓存里的行返回 None。
        """
        return self._row_clean.get(table_ref, {}).get(row_id)

    def moved_away(self, table_ref: TableReference, index_name: str) -> set[int]:
        """
        本事务删掉的行、以及改了 `index_name` 列的行的 id。提交前数据库索引里它们还在原来
        的值上，按这个索引查数据库会读到，但本事务眼里它们已经不在那个值上了。本事务新
        insert 的行数据库里没有，不算。
        """
        states = self._row_states.get(table_ref)
        if not states:
            return set()
        clean_rows = self._row_clean[table_ref]
        moved: set[int] = set()
        updated: list[int] = []
        for row_id, state in states.items():
            if row_id not in clean_rows:
                continue
            if state == RowState.DELETE:
                moved.add(int(row_id))
            elif state == RowState.UPDATE:
                updated.append(row_id)
        if updated:
            cache = self._row_cache[table_ref]
            for row in cache[np.isin(cache["id"], updated)]:
                row_id = int(row["id"])
                if row[index_name] != clean_rows[row_id][index_name]:
                    moved.add(row_id)
        return moved

    def _implied_by_unique(
        self,
        table_ref: TableReference,
        obs: RangeObservation,
        writes: dict[str, tuple[set[object], set[object]]],
    ) -> bool:
        """见 range_observations_to_check。writes 是按列缓存的 _unique_writes 结果"""
        index_name = obs.index_name
        if obs.point is None or index_name not in table_ref.comp_cls.uniques_:
            return False
        if len(obs.ids) == 1:
            return self.db_row(table_ref, obs.ids[0]) is not None
        if len(obs.ids) != 0 or not self.observed_absent(
            table_ref, index_name, obs.point
        ):
            return False
        if index_name not in writes:
            writes[index_name] = self._unique_writes(table_ref, index_name)
        written, deleted = writes[index_name]
        point = self._norm_value(obs.point)
        return point in written and point not in deleted

    def _unique_writes(
        self, table_ref: TableReference, index_name: str
    ) -> tuple[set[object], set[object]]:
        """
        本事务在该列上写入的值（insert 的值，update 改成的新值），以及删除的行的原值。
        """
        written: set[object] = set()
        deleted: set[object] = set()
        states = self._row_states.get(table_ref)
        if not states:
            return written, deleted
        clean_rows = self._row_clean[table_ref]
        for row in self._row_cache[table_ref]:
            row_id = int(row["id"])
            state = states.get(row_id)
            clean = clean_rows.get(row_id)
            clean_value = None if clean is None else self._norm_value(clean[index_name])
            if state == RowState.DELETE:
                if clean is not None:
                    deleted.add(clean_value)
            elif state == RowState.INSERT or state == RowState.UPDATE:
                value = self._norm_value(row[index_name])
                if value != clean_value:
                    written.add(value)
        return written, deleted

    def get_clean_rows(self) -> dict["TableReference", dict[int, str]]:
        """
        返回当前仍处于CLEAN状态的行（被读取但未被修改/删除/重新插入），
        以及它们读取时的 `_version`。提交时用于对纯读行做严格的乐观锁检查，
        避免事务依赖的陈旧读（stale read）。

        Returns
        -------
        {TableReference: {row_id: _version_str}}
        """
        ret: dict[TableReference, dict[int, str]] = {}
        for table_ref, states in self._row_states.items():
            clean_cache = self._row_clean.get(table_ref, {})
            row_versions: dict[int, str] = {}
            for row_id, state in states.items():
                if state != RowState.CLEAN:
                    continue
                clean_row = clean_cache.get(row_id)
                if clean_row is None:
                    continue
                row_versions[row_id] = str(clean_row["_version"])
            if row_versions:
                ret[table_ref] = row_versions
        return ret

    def get_dirty_rows(
        self,
    ) -> dict[
        TableReference,
        tuple[
            list[dict[str, str | bytes]],
            tuple[list[dict[str, str | bytes]], list[dict[str, str | bytes]]],
            list[dict[str, str | bytes]],
        ],
    ]:
        """
        返回所有脏对象的列表，用来提交给数据库，按INSERT、UPDATE、DELETE状态分开。
        既然是提交给数据库用，所以返回的数据都是str类型；只有 bytes（S 类型）字段
        是原样的 bytes，str() 会把它变成 "b'...'" 这种 repr 文本

        Returns
        -------
        {TableReference: (inserts, updates, deletes)} 每个Table各3个修改列表，格式如下：
        inserts: [row_dict, ]
        updates: ([old_row_dict, ], [changed_fields_dict, ]}  # changed_fields_dict只包含变更的字段
        deletes: [old_row_dict, ]
        """
        ret = {}

        for table_ref, states in self._row_states.items():
            # 初始化各状态列表
            inserts, updates, deletes = [], ([], []), []

            cache = self._row_cache[table_ref]
            bytes_fields = table_ref.comp_cls.bytes_fields_

            clean_cache = self._row_clean[table_ref]
            old_rows, new_rows = updates
            # 单行事务直接按已有状态分派，避免 np.isin 掩码和 recarray 副本。
            # 多行仍先用 NumPy 筛掉 CLEAN 行，避免大范围读取、少量写入时逐行
            # 创建 record；INSERT/UPDATE/DELETE 共用一次筛选。
            if len(cache) > 1:
                dirty_ids = [
                    row_id
                    for row_id, state in states.items()
                    if state != RowState.CLEAN
                ]
                dirty_rows = cache[np.isin(cache["id"], dirty_ids)] if dirty_ids else ()
            else:
                dirty_rows = cache
            for row in dirty_rows:
                row_id = row["id"]
                state = states[row_id]
                if state == RowState.INSERT:
                    inserts.append(_row_to_db(row, bytes_fields))
                elif state == RowState.UPDATE:
                    old = clean_cache[row_id]
                    # 只写有变化的字段；改回原值的行没有变化，不发空更新
                    if fields := changed_fields(row, old):
                        new_fields: dict[str, str | bytes] = {
                            field: bytes(row[field])
                            if field in bytes_fields
                            else str(row[field])
                            for field in fields
                        }
                        old_rows.append(_row_to_db(old, bytes_fields))
                        new_rows.append(new_fields)
                elif state == RowState.DELETE:
                    # 按数据库里的原值删：Redis 据此清索引，改过的索引列要清的是原值
                    deletes.append(_row_to_db(clean_cache[row_id], bytes_fields))

            ret[table_ref] = (inserts, updates, deletes)

        return ret

    def filter(self, table_ref: TableReference, **kwargs) -> np.recarray:
        """
        选择出idmap中条件符合index=value的行，排除已删除的行。支持多条件过滤。

        示例:
            rows = id_map.filter(Item, index_name=value, ...)

        Returns
        -------
        滤后的行数据，可能只有0行
        """
        if table_ref not in self._row_cache:
            return np.rec.array(np.empty(0, dtype=table_ref.comp_cls.dtypes))

        cache = self._row_cache[table_ref]
        states = self._row_states[table_ref]

        # 构建过滤掩码
        mask = np.ones(len(cache), dtype=bool)
        for index_name, value in kwargs.items():
            mask &= cache[index_name] == value

        # 排除已删除的行
        for i in range(len(cache)):
            row_id = int(cache[i]["id"])
            if states.get(row_id) == RowState.DELETE:
                mask[i] = False

        return cast(np.recarray, cache[mask])
