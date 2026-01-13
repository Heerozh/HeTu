"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from typing import TYPE_CHECKING, cast

import numpy as np

from .base import RaceCondition, RowFormat, UniqueViolation
from .idmap import RowState
from .table import TableReference

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


class SessionRepository:
    """帮助方法，从数据库查询数据并放入Session缓存。"""

    def __init__(self, session: Session, comp_cls: type[BaseComponent]) -> None:
        self._session = session
        self.ref: TableReference = TableReference(
            comp_cls, session.instance_name, session.cluster_id
        )
        if comp_cls.hosted_:
            assert comp_cls.hosted_.is_same_txn_group(self.ref), (
                "Component 所属的 instance 和 cluster 必须和 Session 一致"
            )

    @property
    def session(self) -> Session:
        """获取所属的Session对象。"""
        return self._session

    def _local_has_unique_conflicts(self, row: np.record, fields: set) -> str | None:
        """
        在Session本地缓存中检查Unique索引冲突。
        """
        idmap = self._session.idmap
        ref = self.ref
        for unique_index in fields:
            value = row[unique_index]
            rows = idmap.filter(ref, **{unique_index: value})
            if len(rows) > 0:
                return unique_index
        return None

    async def remote_has_unique_conflicts_(
        self, row: np.record, fields: set
    ) -> str | None:
        """
        在远程数据库中检查Unique索引冲突。
        """
        session = self._session
        client = session.master_or_servant
        ref = self.ref
        for unique_index in fields:
            value: np.generic = row[unique_index]
            existing_row = await client.range(
                ref,
                unique_index,
                value.item(),
                value.item(),
                1,
                False,
                RowFormat.ID_LIST,
            )
            if len(existing_row) > 0:
                # 如果existing_row的id存在于mark_deleted中，则不算冲突
                if session.idmap.is_deleted(ref, existing_row[0]):
                    continue
                return unique_index
        return None

    def _get_changed_fields(self, row: np.record):
        """
        根据row.id对比缓存，获取修改的字段列表。
        如果id在修改字段中，表示没有找到旧数据。
        """
        idmap = self._session.idmap
        old_row, row_stat = idmap.get(self.ref, row["id"])
        assert row.dtype.names  # for type checker, could be removed by python -O
        if old_row is None or row_stat == RowState.DELETE:
            return set(row.dtype.names)
        else:
            return {key for key in row.dtype.names if old_row[key] != row[key]}

    async def is_unique_conflicts(self, row: np.record, insert=False) -> str | None:
        """
        检查一行数据的Unique索引在本地和远程数据库中是否有冲突。

        Parameters
        ----------
        row : np.record
            待检查的行数据，必须是 `c-struct` 格式。
        insert : bool
            如果为True，表示这是一个插入操作，否则是更新操作，要求之前已获取过旧数据。
        """
        changed_fields = self._get_changed_fields(row)
        if not insert:
            # 如果有id字段，表示没有找到旧数据
            assert "id" not in changed_fields, (
                f"row id({row.id})在session中未寻到，更新操作必须有旧数据。"
            )
        else:
            assert "id" in changed_fields, (
                f"session中已存在该row id({row.id})，插入操作必须没有旧数据。"
            )

        changed_fields = changed_fields & self.ref.comp_cls.uniques_

        if field := self._local_has_unique_conflicts(row, changed_fields):
            return field

        if field := await self.remote_has_unique_conflicts_(row, changed_fields):
            return field

        return None

    async def get_by_id(self, row_id: Int64) -> np.record | None:
        """
        从数据库获取单行数据，并放入Session缓存。
        本指令如果命中缓存，不会去数据库查询。
        """
        idmap = self._session.idmap
        ref = self.ref
        # 主键查询，先查缓存
        row_id = cast(int, row_id)
        row, row_stat = idmap.get(ref, row_id)
        if row_stat is not None:
            if row_stat == RowState.DELETE:
                return None
            else:
                return row

        # 缓存未命中，查询数据库
        row = await self._session.master_or_servant.get(ref, row_id, RowFormat.STRUCT)
        if row is not None:
            idmap.add_clean(ref, row)
        return row

    async def get(
        self,
        index_name: str | None = None,
        query_value: IndexScalar | None = None,
        **kwargs: IndexScalar,
    ) -> np.record | None:
        """
        从数据库获取单行数据，并放入Session缓存。
        推荐通过"id"主键查询，这样无须查询索引，如果缓存命中，不会去数据库查询；否则会执行1-2次查询。

        Parameters
        ----------
        index_name: str | None
            辅助参数，如果不便使用kwargs参数时使用。
        query_value: IndexScalar | None
            辅助参数，如果不便使用kwargs参数时使用。
        kwargs: IndexScalar
            查询字段和值，例如 `id=1234567890`。只能查询一个字段，且该字段必须有索引。

        Examples
        --------
        ::

            item = await session.using(Item).get(id=1234567890)

        Returns
        -------
        row: np.record or None
            如果未查询到匹配数据，则返回 None。如果查询到数据，则返回查询到的第一行数据。
            返回 np.record (c-struct) 格式。
        """
        if index_name is None or query_value is None:
            # 判断kwargs有且只有一个键值对
            assert len(kwargs) == 1, "Only one field can be queried."
            index_name, query_value = next(iter(kwargs.items()))

        comp_cls = self.ref.comp_cls

        assert index_name in comp_cls.indexes_, (
            f"{comp_cls.component_name_} 组件没有叫 {index_name} 的索引"
        )

        # 如果不是主键，直接用range方法
        if index_name != "id":
            # 去cache查询
            idmap = self._session.idmap
            rows = idmap.filter(self.ref, **{index_name: query_value})
            if len(rows) > 0:
                return rows[0]

            # cache未命中，去数据库查询
            rows = await self.range(index_name, query_value, limit=1, desc=False)
            return rows[0] if rows.shape[0] > 0 else None
        else:
            return await self.get_by_id(int(query_value))

    async def range(
        self,
        index_name: str | None = None,
        _left: IndexScalar | None = None,  # 参数前加_防止和用户字段冲突
        _right: IndexScalar | None = None,
        limit: int = 10,
        desc: bool = False,
        **kwargs: tuple[IndexScalar, IndexScalar],
    ) -> np.recarray:
        """
        从数据库查询索引，返回区间内数据，限制 `limit` 条。
        本指令会去数据库执行 1+(limit-缓存命中) 次查询，至少要进行1次数据库查询。

        Parameters
        ----------
        index_name: str | None
            辅助参数，如果不便使用kwargs参数时使用。
        _left: IndexScalar | None
            辅助参数，如果不便使用kwargs参数时使用。
        _right: IndexScalar | None
            辅助参数，如果不便使用kwargs参数时使用。
        kwargs: IndexScalar
            查询字段和区间，例如 `level=(1, 10)`。只能查询一个字段，且该字段必须有索引。
            默认闭区间，如果要自定义区间，请转换为字符串并开头指定 `(` 或 `[`。
            * 如果要查询的字段和参数冲突，请使用辅助参数方式。
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

            items = await session.using(Item).range(level=(10, 20), limit=100)
            few_items = items[items.amount < 10]

        由于python numpy支持SIMD，比直接在数据库复合查询快。
        """
        if index_name is None and _left is None:
            # 判断kwargs有且只有一个键值对
            assert len(kwargs) == 1, "Only one field can be queried."
            index_name, (_left, _right) = next(iter(kwargs.items()))
        else:
            assert index_name, "不使用kwargs形式时，index_name不能为空"
            assert _left is not None, "不使用kwargs形式时，left不能为空"

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

        if isinstance(_left, np.generic):
            _left = _left.item()
        if isinstance(_right, np.generic):
            _right = _right.item()

        # 先查询 id 列表
        row_ids = await self._session.master_or_servant.range(
            self.ref, index_name, _left, _right, limit, desc, RowFormat.ID_LIST
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

        # unique check
        if conflict := await self.is_unique_conflicts(row, insert=True):
            raise UniqueViolation(
                f"Insert failed: row.{conflict} violates a unique index."
            )

        self._session.idmap.add_insert(self.ref, row)

    async def update(self, row: np.record) -> None:
        """
        向Session中添加一行待更新数据。

        Parameters
        ----------
        row : np.record
            待更新的行数据，必须是 `c-struct` 格式。
        """
        changed_fields = self._get_changed_fields(row)
        # 检查row.id在cache中存在
        if "id" in changed_fields:
            raise LookupError("Cannot update: row id not found in cache.")

        # 检查和cache中的_version一致
        if "_version" in changed_fields:
            raise ValueError("Cannot update _version field.")

        # 检查有修改的列
        if len(changed_fields) == 0:
            raise ValueError("No fields changed, cannot update.")

        # unique check
        if conflict := await self.is_unique_conflicts(row):
            raise UniqueViolation(
                f"Update failed: row.{conflict} violates a unique index."
            )

        self._session.idmap.update(self.ref, row)

    def upsert(self, **kwargs: IndexScalar) -> UpsertContext:
        """
        使用async with语法，根据Unique索引，查询并返回一行数据，如果不存在则返回新行数据。
        在退出上下文时，自动插入新行，或是更新已有行。

        Examples
        --------
        ::

            async with session.using(Order).upsert(id=1234567890) as order:
                order.status = "completed"

        Parameters
        ----------
        kwargs: IndexScalar
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
        old_row, row_stat = self._session.idmap.get(self.ref, row_id)
        if old_row is None or row_stat == RowState.DELETE:
            raise LookupError("Row not existing: not in cache or already deleted.")

        self._session.idmap.mark_deleted(self.ref, row_id)


class UpsertContext:
    """用于在事务中执行UpdateOrInsert操作的上下文管理器。"""

    def __init__(
        self, repo: SessionRepository, index_name: str, query_value: IndexScalar
    ) -> None:
        self.clean_data = None
        self.row_data = None
        self.insert = None
        self.repo = repo
        self.index_name = index_name
        self.query_value = query_value

    async def __aenter__(self) -> np.record:
        existing_row = await self.repo.get(self.index_name, self.query_value)
        if existing_row is not None:
            self.row_data = existing_row
            self.clean_data = existing_row.copy()
            self.insert = False
        else:
            self.row_data = self.repo.ref.comp_cls.new_row()
            self.row_data[self.index_name] = self.query_value
            self.insert = True
        return self.row_data

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            assert self.row_data is not None
            if self.insert:
                # 如果是insert，检查upsert的锚定字段是否已存在，
                if await self.repo.remote_has_unique_conflicts_(
                    self.row_data, {self.index_name}
                ):
                    raise RaceCondition(
                        f"Upsert failed: 锚定的index({self.index_name})存在Unique违反，说明竞态insert"
                    )
                await self.repo.insert(self.row_data)
            else:
                if self.row_data == self.clean_data:
                    # 无修改不更新
                    return
                await self.repo.update(self.row_data)
