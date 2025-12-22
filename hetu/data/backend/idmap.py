"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
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
        self._row_clean: dict[TableReference, np.recarray] = {}

        # {TableReference: {row_id: RowState}} - 存储每行的状态
        self._row_states: dict[TableReference, dict[int, RowState]] = {}

        # 范围查询缓存
        # {TableReference: {index_name: [(left, right), ...]}} - 存储已缓存的范围
        # self._range_cache: dict[TableReference, dict[str, list[tuple]]] = {}

    @property
    def is_dirty(self) -> bool:
        """检查是否有脏数据"""
        for states in self._row_states.values():
            if any(state != RowState.CLEAN for state in states.values()):
                return True
        return False

    def first_reference(self) -> TableReference | None:
        if not self._row_cache:
            return None
        return next(iter(self._row_cache.keys()))

    def is_same_txn_group(self, other: TableReference) -> bool:
        first_reference = self.first_reference()
        if first_reference is None:
            return True
        return first_reference.is_same_txn_group(other)

    def _cache(self, table_ref: TableReference):
        if table_ref not in self._row_cache:
            self._row_cache[table_ref] = np.rec.array(
                np.empty(0, dtype=table_ref.comp_cls.dtypes)
            )
            self._row_clean[table_ref] = np.rec.array(
                np.empty(0, dtype=table_ref.comp_cls.dtypes)
            )
            self._row_states[table_ref] = {}
        return (
            self._row_cache[table_ref],
            self._row_clean[table_ref],
            self._row_states[table_ref],
        )

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
            f"({table_ref.comp_cls.component_name_}, {table_ref.comp_cls.dtypes})"
        )

        # 初始化该component的缓存
        cache, clean_cache, states = self._cache(table_ref)

        # 查找是否已存在该ID的行
        if len(cache) > 0:
            existing_idx = np.isin(cache["id"], row_s["id"])
            if np.any(existing_idx):
                raise ValueError(
                    f"Row with id {cache['id'][existing_idx]} already exists in cache"
                )

        # 添加新行
        self._row_cache[table_ref] = np.rec.array(np.append(cache, row_s))
        self._row_clean[table_ref] = np.rec.array(np.append(clean_cache, row_s))
        # 标记为CLEAN
        if row_s.ndim == 0:
            # 如果是单行数据，直接添加状态
            row_s = cast(np.record, row_s)
            states[row_s["id"]] = RowState.CLEAN
        else:
            states.update({key: RowState.CLEAN for key in row_s["id"]})

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
        return cast(np.record, cache[idx[0]]), states.get(row_id)

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
            f"({table_ref.comp_cls.component_name_}, {table_ref.comp_cls.dtypes})"
        )

        assert row["_version"] == 0, f"不得修改_version字段，{row['_version']}"

        # 初始化缓存

        # 添加到缓存
        cache, _, states = self._cache(table_ref)
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
            f"({table_ref.comp_cls.component_name_}, {table_ref.comp_cls.dtypes})"
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

        cache, clean_cache, states = self._cache(table_ref)

        # 查找行必须已存在
        idx = np.where(cache["id"] == row_id)[0]
        if len(idx) == 0:
            raise ValueError(f"Row with id {row_id} not found in cache")

        # 标记为DELETE
        states[row_id] = RowState.DELETE

    def get_dirty_rows(
        self, max_integer_size: int = 6, max_float_size: int = 8
    ) -> dict[str, dict[TableReference, list[dict[str, bytes | str]]]]:
        """
        返回所有脏对象的列表，用来提交给数据库，按INSERT、UPDATE、DELETE状态分开。
        既然是提交给数据库用，所以返回的数据都是str类型

        Returns
        -------
        {
            'insert': [{all_field: value}, ]
            'update': [{changed_fields: value}, ]
            'delete': [{id: xxx, _version: 0}, ]
        }
        """
        result: dict[str, dict[TableReference, list[dict[str, bytes | str]]]] = {
            "insert": {},
            "update": {},
            "delete": {},
        }

        def serialize_value(kv) -> bytes | str:
            """将row值序列化为字符串，如果是索引值，且大于53位整数，则转换为bytes"""
            dtype, data = kv
            # todo 要把int超过53位的，改成>S8的bytes传输，并标记为字符串，否则score索引无法实现
            # todo 要按database_max_integer_size, max_float_size来判断，交给client传入
            # todo 不对，浮点没办法这么搞，因为浮点无法按byte正确排序

            if np.issubdtype(dtype, np.integer) and dtype.itemsize > max_integer_size:
                return data ^ 0x8000_0000_0000_0000

            return str(data)

        for table_ref, states in self._row_states.items():
            cache = self._row_cache[table_ref]

            # 收集各状态的行ID
            insert_ids = [
                row_id for row_id, state in states.items() if state == RowState.INSERT
            ]
            update_ids = [
                row_id for row_id, state in states.items() if state == RowState.UPDATE
            ]
            delete_ids = [
                row_id for row_id, state in states.items() if state == RowState.DELETE
            ]

            # 从缓存中获取对应的行数据
            if insert_ids:
                mask = np.isin(cache["id"], insert_ids)
                result["insert"][table_ref] = [
                    dict(
                        zip(
                            row.dtype.names,
                            map(serialize_value, zip(row.dtype, row.item())),
                        )
                    )
                    for row in cache[mask]
                ]

            if update_ids:
                clean_cache = self._row_clean[table_ref]
                mask = np.isin(cache["id"], update_ids)
                # 只保存变更的数据
                result["update"][table_ref] = []
                for row in cache[mask]:
                    clean_row = clean_cache[np.where(clean_cache["id"] == row.id)[0][0]]
                    changed_fields = {}
                    for field in row.dtype.names:
                        if row[field] != clean_row[field]:
                            changed_fields[field] = str(row[field])
                    if changed_fields:
                        changed_fields["id"] = str(row["id"])
                        changed_fields["_version"] = str(row["_version"])
                        result["update"][table_ref].append(changed_fields)

            if delete_ids:
                # DELETE只需要ID列表
                mask = np.isin(cache["id"], delete_ids)
                result["delete"][table_ref] = [
                    {"id": str(row["id"]), "_version": str(row["_version"])}
                    for row in cache[mask]
                ]

        return result

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
