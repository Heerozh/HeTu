"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from typing import TYPE_CHECKING

import numpy as np

from .base import RowFormat
from .table import TableReference

if TYPE_CHECKING:
    from hetu.data.component import BaseComponent

    from .session import Session


class Select:
    def __init__(self, session: Session, comp_cls: type[BaseComponent]) -> None:
        self.session = session
        self.table_ref: TableReference = TableReference(
            comp_cls, session.instance_name, session.cluster_id
        )

    async def get(self, row_id: int) -> np.record | None:
        """
        从数据库获取单行数据，并放入Session缓存。

        Parameters
        ----------
        row_id: int
            row id主键

        Returns
        -------
        row: np.record or None
            如果未查询到匹配数据，则返回 None。
            返回 np.record (c-struct) 的单行数据
        """
        row = await self.session.master_or_servant.get(
            self.table_ref, row_id, RowFormat.STRUCT
        )
        if row is not None:
            self.session.idmap.add_clean(self.table_ref, row)
        return row

    async def range(
        self,
        index_name: str,
        left: int | float | str,
        right: int | float | str | None = None,
        limit: int = 100,
        desc: bool = False,
    ) -> np.recarray:
        """
        从数据库查询索引，`index_name`，返回在 [`left`, `right`] 闭区间内数据并放入Session缓存。。
        如果 `right` 为 `None`，则查询等于 `left` 的数据，限制 `limit` 条。

        Parameters
        ----------
        index_name: str
            查询Component中的哪条索引
        left, right: str or number
            查询范围，闭区间。字符串查询时，可以在开头指定是[闭区间，还是(开区间。
            如果right不填写，则精确查询等于left的数据。
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
        请利用python的特性，先在数据库上筛选出最少量的数据，然后本地二次筛选：

        >>> items = select(...).range(ref, "owner", player_id, limit=100)  # noqa
        >>> few_items = items[items.amount < 10]

        由于python numpy支持SIMD，比直接在数据库复合查询快。
        """
        client = self.session.master_or_servant
        rows = await client.range(
            self.table_ref, index_name, left, right, limit, desc, RowFormat.STRUCT
        )
        if rows.shape[0] > 0:
            self.session.idmap.add_clean(self.table_ref, rows)

        return rows
