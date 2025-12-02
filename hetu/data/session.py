"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from typing import TYPE_CHECKING

import numpy as np
from numbers import Number
from .idmap import IdentityMap

if TYPE_CHECKING:
    from .backend import RawComponentTable


# 干2个事情：
# 1. 负责get/query所有数据，可以通过repository直接操作
# 2. 通过idmap，获得要操作的数据生成lua，提交给数据库
class Session:
    def __init__(self) -> None:
        self.idmap = IdentityMap()

    def get(self, comp_table: RawComponentTable, id: int):
        """
        通过id获得整行数据。
        """
        if row := self.idmap.get(comp_table.component_cls, id):
            return row
        row = comp_table.master_get(id)
        self.idmap.add_clean(comp_table.component_cls, row)
        return row

    def query_ids(
        self,
        index: str,
        left: Number | str,
        right: Number | str | None,
        limit: int,
        desc: bool = False,
    ) -> list[int]:
        """
        通过query index获得row id列表。
        """
        return self.comp_repo.master_query(index, left, right, limit, desc)

    def commit(self):
        """ """
        dirty_rows = self.idmap.get_dirty_rows()
