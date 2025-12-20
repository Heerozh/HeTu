"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from typing import TYPE_CHECKING, cast

from .idmap import IdentityMap
from .select import SessionSelect

if TYPE_CHECKING:
    from . import Backend, BackendClient
from hetu.data.component import BaseComponent


class Session:
    """数据库事务类，用于缓存数据修改，并最终提交到数据库。"""

    def __init__(self, backend: Backend, instance_name: str, cluster_id: int) -> None:
        self.instance_name = instance_name
        self.cluster_id = cluster_id
        self._backend = backend
        self._master = backend.master
        self.idmap = cast(IdentityMap, object())

        self.clean()

    def clean(self):
        """清除Session缓存的数据状态"""
        self.idmap = IdentityMap()

    def select(self, comp_cls: type[BaseComponent]):
        return SessionSelect(self, comp_cls)

    @property
    def master(self) -> BackendClient:
        return self._master

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
        # 如果数据库不具备写入通知功能，要在此手动往MQ推送数据变动消息。
        if self.idmap.is_dirty:
            await self._master.commit(self.idmap)
        self.clean()

    async def discard(self) -> None:
        """放弃Session中的所有数据修改"""
        self.clean()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self.commit()
        else:
            await self.discard()
