"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from typing import TYPE_CHECKING, cast

import numpy as np

from .base import RowFormat
from .table import TableReference
from .idmap import RowState

if TYPE_CHECKING:
    from hetu.data.component import BaseComponent

    from .session import Session

IndexScalar = (
    np.integer
    | np.floating
    | np.str_
    | np.bytes_
    | np.bool_
    | float
    | str
    | bytes
    | bool
)
Int64 = np.int64 | int


class SessionSelect:
    """帮助方法，从数据库查询数据并放入Session缓存。"""

    def __init__(self, session: Session, comp_cls: type[BaseComponent]) -> None:
        self.session = session
        self.ref: TableReference = TableReference(
            comp_cls, session.instance_name, session.cluster_id
        )

    async def get_by_id(self, row_id: Int64) -> np.record | None:
        """
        从数据库获取单行数据，并放入Session缓存。
        本指令如果命中缓存，不会去数据库查询。
        """
        # 主键查询，先查缓存
        row_id = cast(int, row_id)
        row, row_stat = self.session.idmap.get(self.ref, row_id)
        if row_stat is not None:
            if row_stat == RowState.DELETE:
                return None
            else:
                return row

        # 缓存未命中，查询数据库
        row = await self.session.master_or_servant.get(
            self.ref, row_id, RowFormat.STRUCT
        )
        if row is not None:
            self.session.idmap.add_clean(self.ref, row)
        return row

    async def get(self, **kwargs: IndexScalar) -> np.record | None:
        """
        从数据库获取单行数据，并放入Session缓存。
        推荐通过"id"主键查询，这样无须查询索引。否则会执行1-2次查询。

        Parameters
        ----------
        kwargs: dict
            查询字段和值，例如 `id=1234567890`。只能查询一个字段，且该字段必须有索引。

        Examples
        --------
        ::

            item = await session.select(Item).get(id=1234567890)

        Returns
        -------
        row: np.record or None
            如果未查询到匹配数据，则返回 None。如果查询到数据，则返回查询到的第一行数据。
            返回 np.record (c-struct) 格式。
        """
        # 判断kwargs有且只有一个键值对
        assert len(kwargs) == 1, "Only one field can be queried."
        index_name, query_value = next(iter(kwargs.items()))

        comp_cls = self.ref.comp_cls

        # assert np.isscalar(query_value), (
        #     f"查询值必须为标量类型(数字，字符串等), 你的:{type(query_value)}, {query_value}"
        # )
        assert index_name in comp_cls.indexes_, (
            f"{comp_cls.component_name_} 组件没有叫 {index_name} 的索引"
        )

        # if issubclass(type(query_value), np.generic):
        #     value = query_value.item()

        # 如果不是主键，直接用range方法
        if index_name != "id":
            rows = await self.range(
                limit=1, desc=False, **{index_name: (query_value, query_value)}
            )
            return rows[0] if rows.shape[0] > 0 else None
        else:
            return await self.get_by_id(int(query_value))

    async def range(
        self,
        limit: int = 100,
        desc: bool = False,
        **kwargs: tuple[IndexScalar, IndexScalar],
    ) -> np.recarray:
        """
        从数据库查询索引，返回区间内数据，限制 `limit` 条。
        本指令会去数据库执行 1+(limit-缓存命中) 次查询。

        Parameters
        ----------
        kwargs: dict
            查询字段和区间，例如 `level=(1, 10)`。只能查询一个字段，且该字段必须有索引。
            默认闭区间，如果要自定义区间，请转换为字符串并开头指定 `(` 或 `[`。
        limit: int
            限制返回的行数，越少越快
        desc: bool
            是否降序排列

        Returns
        -------
        row: np.recarray
            返回 `numpy.recarray`，如果没有查询到数据，返回空 `numpy.recarray`。
            `numpy.recarray` 是一种 c-struct array。

        Notes
        -----
        如何复合条件查询？
        请利用python的特性，先在数据库上筛选出最少量的数据，然后本地二次筛选::

            items = await session.select(Item).range(level=(10, 20), limit=100)
            few_items = items[items.amount < 10]

        由于python numpy支持SIMD，比直接在数据库复合查询快。
        """
        # 判断kwargs有且只有一个键值对
        assert len(kwargs) == 1, "Only one field can be queried."
        index_name, (left, right) = next(iter(kwargs.items()))

        # assert np.isscalar(left), (
        #     f"left必须为标量类型(数字，字符串等), 你的:{type(left)}, {left}"
        # )
        # assert np.isscalar(right), (
        #     f"right必须为标量类型(数字，字符串等), 你的:{type(right)}, {right}"
        # )

        comp_cls = self.ref.comp_cls

        # 判断index_name存在
        assert index_name in comp_cls.indexes_, (
            f"{comp_cls.component_name_} 组件没有叫 {index_name} 的索引"
        )

        if isinstance(left, np.generic):
            left = left.item()
        if isinstance(right, np.generic):
            right = right.item()

        # 先查询 id 列表
        row_ids = await self.session.master_or_servant.range(
            self.ref, index_name, left, right, limit, desc, RowFormat.ID_LIST
        )

        # 再根据 id 列表查询数据行，可以命中缓存
        rows = []
        for _id in row_ids:
            if row := await self.get_by_id(_id):
                rows.append(row)

        # 转换成 np.recarray 返回
        if len(rows) == 0:
            return np.rec.array(np.empty(0, dtype=comp_cls.dtypes))
        else:
            return np.rec.array(np.stack(rows, dtype=comp_cls.dtypes))

    async def insert(self, row: np.record) -> None:
        """
        向Session中添加一行待插入数据。

        Parameters
        ----------
        row: np.record
            待插入的行数据，必须是 `c-struct` 格式。
        """
        assert row["_version"] == 0, "Insert row's _version must be 0."

        # todo unique check

        self.session.idmap.add_insert(self.ref, row)

    async def update(self, row: np.record) -> None:
        """
        向Session中添加一行待更新数据。

        Parameters
        ----------
        row : np.record
            待更新的行数据，必须是 `c-struct` 格式。
        """
        # todo 检查和cache中的_version一致

        # todo 检查有修改的列

        # TODO unique check

        self.session.idmap.update(self.ref, row)

    def upsert(self, **kwargs: IndexScalar) -> UpsertContext:
        """
        使用async with语法，根据Unique索引，查询并返回一行数据，如果不存在则返回新行数据。
        在退出上下文时，自动插入新行，或是更新已有行。

        Examples
        --------
        ::

            async with session.select(Order).upsert(id=1234567890) as order:
                order.status = "completed"

        Parameters
        ----------
        kwargs: dict
            查询字段和值，例如 `id=1234567890`。只能查询一个字段，且该字段必须为unique索引。
        """
        # 判断kwargs有且只有一个键值对
        assert len(kwargs) == 1, "Only one field can be queried."
        index_name, query_value = next(iter(kwargs.items()))

        # 检查index_name存在且为unique索引
        comp_cls = self.ref.comp_cls
        assert index_name in comp_cls.uniques_, (
            "upsert只能用于unique索引，"
            f"{comp_cls.component_name_}组件的{index_name}不是unique索引"
        )

        return UpsertContext(self, index_name, query_value)

    def delete(self, row_id: int) -> None:
        """
        向Session中添加一行待删除数据。

        Parameters
        ----------
        row_id : int
            待删除行的主键ID。
        """
        # todo 检查和cache中的_version一致

        self.session.idmap.mark_deleted(self.ref, row_id)


class UpsertContext:
    """用于在事务中执行UpdateOrInsert操作的上下文管理器。"""

    def __init__(
        self, selected: SessionSelect, index_name: str, query_value: IndexScalar
    ) -> None:
        self.row_data = None
        self.insert = None
        self.selected = selected
        self.index_name = index_name
        self.query_value = query_value

    async def __aenter__(self):
        existing_row = await self.selected.get(**{self.index_name: self.query_value})
        if existing_row is not None:
            self.row_data = existing_row
            self.insert = False
        else:
            self.row_data = self.selected.ref.comp_cls.new_row()
            self.insert = True
        return self.row_data

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            assert self.row_data is not None
            if self.insert:
                self.selected.insert(self.row_data)
            else:
                self.selected.update(self.row_data)
