"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from typing import TYPE_CHECKING

from .idmap import IdentityMap
from .select import Select

if TYPE_CHECKING:
    from . import Backend, BackendClient
from hetu.data.component import BaseComponent


# 干2个事情：
# 1. 负责get/query所有数据，可以通过repository直接操作
# 2. 通过idmap，获得要操作的数据生成lua，提交给数据库
class Session:
    def __init__(self, backend: Backend, instance_name: str, cluster_id: int) -> None:
        self.instance_name = instance_name
        self.cluster_id = cluster_id
        self._backend = backend

        self.idmap = IdentityMap()

    def select(self, comp_cls: type[BaseComponent]):
        return Select(self, comp_cls)

    @property
    def master(self) -> BackendClient:
        return self._backend.master

    @property
    def master_or_servant(self) -> BackendClient:
        return self._backend.master_or_servant

    async def commit(self) -> None:
        """
        向数据库提交Session中的所有数据修改

        Exceptions
        --------
        RaceCondition
            当提交数据时，发现数据已被其他事务修改，抛出此异常
        """
        if self.idmap.is_dirty:
            await self._backend.master.commit(self.idmap)
