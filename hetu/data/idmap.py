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
    from .component import BaseComponent

logger = logging.getLogger("HeTu.root")


class RowState(Enum):
    """行状态枚举"""

    CLEAN = 0  # 干净，无需更新
    INSERT = 1  # 新插入
    UPDATE = 2  # 已更新
    DELETE = 3  # 已删除


class IdentityMap:
    """
    用于缓存和管理事务中的对象。
    SessionComponentTable会经由本类来查询和缓存对象。
    BackendSession在提交时可以通过本类，获得脏对象列表，然后想办法合并成事务指令。
    """

    def __init__(self) -> None:
        # 每个Component类型对应一个缓存
        # {component_cls: np.recarray} - 存储行数据
        self._row_cache: dict[type[BaseComponent], np.recarray] = {}

        # {component_cls: {row_id: RowState}} - 存储每行的状态
        self._row_states: dict[type[BaseComponent], dict[int, RowState]] = {}

        # 范围查询缓存
        # {component_cls: {index_name: [(left, right), ...]}} - 存储已缓存的范围
        self._range_cache: dict[type[BaseComponent], dict[str, list[tuple]]] = {}

        # 用于生成新插入行的负ID
        self._next_insert_id = -1

    def add_clean(
        self, comp_cls: type[BaseComponent], row: np.record | np.ndarray
    ) -> None:
        """
        添加一个查询到的对象到row缓存中。
        如果数据行已存在，则会报错ValueError。
        """
        row_id = int(row["id"])

        # 初始化该component的缓存
        if comp_cls not in self._row_cache:
            self._row_cache[comp_cls] = np.rec.array(np.empty(0, dtype=comp_cls.dtypes))
            self._row_states[comp_cls] = {}

        cache = self._row_cache[comp_cls]
        states = self._row_states[comp_cls]

        # 查找是否已存在该ID的行
        if len(cache) > 0:
            existing_idx = np.where(cache["id"] == row_id)[0]
            if len(existing_idx) > 0:
                # # 更新已存在的行
                # cache[existing_idx[0]] = row
                # # 只有在状态为CLEAN时才保持CLEAN，否则保持原状态
                # if row_id not in states:
                #     states[row_id] = RowState.CLEAN
                # return
                raise ValueError(f"Row with id {row_id} already exists in cache")

        # 添加新行
        self._row_cache[comp_cls] = np.rec.array(np.append(cache, row))
        # 标记为CLEAN（除非之前已有其他状态）
        if row_id not in states:
            states[row_id] = RowState.CLEAN

    def get(
        self, comp_cls: type[BaseComponent], row_id: int
    ) -> tuple[np.record | None, RowState | None]:
        """
        从缓存中获取指定ID的行。

        Args:
            comp_cls: Component类
            row_id: 行ID

        Returns:
            如果缓存中有则返回行数据，否则返回None
        """
        if comp_cls not in self._row_cache:
            return None, None

        cache = self._row_cache[comp_cls]
        if len(cache) == 0:
            return None, None

        # 查找指定ID的行
        idx = np.where(cache["id"] == row_id)[0]
        if len(idx) == 0:
            return None, None

        # 主要提供状态：是否已删除
        states = self._row_states[comp_cls]

        # recarray是基于ndarray的，传入参数可以用np.ndarray类型，返回值
        # 应该使用np.recarray类型以保留字段名访问特性(row.field_name)
        return cast(np.record, cache[idx[0]]), states.get(row_id)

    def add_insert(
        self, comp_cls: type[BaseComponent], row: np.record | np.ndarray
    ) -> None:
        """
        添加一个新插入的对象到缓存，并标记为INSERT状态。
        注意此方法会修改传入Row的ID字段，分配一个负数ID。

        Returns:
            分配了临时ID（负数）的行数据
        """
        # 初始化缓存
        if comp_cls not in self._row_cache:
            self._row_cache[comp_cls] = np.rec.array(np.empty(0, dtype=comp_cls.dtypes))
            self._row_states[comp_cls] = {}

        # 分配负ID
        row["id"] = self._next_insert_id
        self._next_insert_id -= 1

        # 添加到缓存
        cache = self._row_cache[comp_cls]
        self._row_cache[comp_cls] = np.rec.array(np.append(cache, row))

        # 标记为INSERT
        self._row_states[comp_cls][int(row["id"])] = RowState.INSERT

    def update(
        self, comp_cls: type[BaseComponent], row: np.record | np.ndarray
    ) -> None:
        """
        更新一个对象到缓存，并标记为UPDATE状态。
        """
        row_id = int(row["id"])

        if comp_cls not in self._row_cache:
            raise ValueError(f"Component {comp_cls} not in cache")

        cache = self._row_cache[comp_cls]
        states = self._row_states[comp_cls]

        # 查找并更新行
        idx = np.where(cache["id"] == row_id)[0]
        if len(idx) == 0:
            raise ValueError(f"Row with id {row_id} not found in cache")

        # 如果是删除状态，不能更新
        if states.get(row_id) == RowState.DELETE:
            raise ValueError(
                f"Row with id {row_id} is marked as DELETE and cannot be updated"
            )

        cache[idx[0]] = row

        # 如果是新插入的行，保持INSERT状态；否则标记为UPDATE
        if states.get(row_id) != RowState.INSERT:
            states[row_id] = RowState.UPDATE

    def mark_deleted(self, comp_cls: type[BaseComponent], row_id: int) -> None:
        """
        标记指定ID的对象为删除状态。

        Args:
            comp_cls: Component类
            row_id: 行ID
        """
        if comp_cls not in self._row_states:
            raise ValueError(f"Component {comp_cls} not in cache")

        states = self._row_states[comp_cls]

        # 标记为DELETE
        states[row_id] = RowState.DELETE

    def get_dirty_rows(self) -> dict[str, dict[type[BaseComponent], np.ndarray]]:
        """
        返回所有脏对象的列表，按INSERT、UPDATE、DELETE状态分开。

        Returns:
            {
                'insert': {comp_cls: np.ndarray, ...},
                'update': {comp_cls: np.ndarray, ...},
                'delete': {comp_cls: np.ndarray, ...}  # 只包含id
            }
        """
        result: dict[str, dict[type[BaseComponent], np.ndarray]] = {"insert": {}, "update": {}, "delete": {}}

        for comp_cls, states in self._row_states.items():
            cache = self._row_cache[comp_cls]

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
                result["insert"][comp_cls] = cache[mask]

            if update_ids:
                mask = np.isin(cache["id"], update_ids)
                result["update"][comp_cls] = cache[mask]

            if delete_ids:
                # DELETE只需要ID列表
                result["delete"][comp_cls] = np.array(delete_ids, dtype=np.int64)

        return result

    def filter(self, comp_cls: type[BaseComponent], **kwargs) -> np.recarray:
        """
        选择出idmap中条件符合index=value的行，排除已删除的行。支持多条件过滤。

        示例:
            rows = id_map.filter(Item, index_name=value, ...)

        Returns:
            过滤后的行数据，可能只有0行
        """
        if comp_cls not in self._row_cache:
            return np.rec.array(np.empty(0, dtype=comp_cls.dtypes))

        cache = self._row_cache[comp_cls]
        states = self._row_states[comp_cls]

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
