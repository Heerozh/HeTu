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

    def __init__(self, backend: Backend, instance: str, cluster_id: int) -> None:
        self.instance_name = instance
        self.cluster_id = cluster_id
        self._backend = backend
        self._master = backend.master
        self._idmap = cast(IdentityMap, object())
        self._entered = False
        self.only_master = False

        self.clean()
        # todo 要检测是否在session中又开了一个session，如果是，应该报错，毕竟嵌套session没意义
        #      或者只是在system中不允许开session，因为其他地方他开是明确的，能知道自己嵌套了

    def clean(self):
        """清除Session缓存的数据状态"""
        self._idmap = IdentityMap()
        self._entered = False

    def select(self, comp_cls: type[BaseComponent]):
        # todo systemsession 重载此类，然后此方法限制只能选取system允许的component
        # todo 可能和大部分orm的select命名歧义，这里的select可以update和insert的，不是纯查询，考虑改成crud?
        return SessionSelect(self, comp_cls)

    @property
    def master(self) -> BackendClient:
        assert self._entered, "Session must be used in `async with` block"
        return self._master

    @property
    def master_or_servant(self) -> BackendClient:
        assert self._entered, "Session must be used in `async with` block"
        if self.only_master:
            return self._master
        else:
            return self._backend.master_or_servant

    @property
    def idmap(self) -> IdentityMap:
        assert self._entered, "Session must be used in `async with` block"
        return self._idmap

    async def commit(self) -> None:
        """
        向数据库提交Session中的所有数据修改

        Exceptions
        --------
        RaceCondition
            当提交数据时，发现数据已被其他事务修改，抛出此异常
        """
        # 如果数据库不具备写入通知功能，要在此手动往MQ推送数据变动消息。
        if self._idmap.is_dirty:
            await self._master.commit(self._idmap)
        self.clean()

    def discard(self) -> None:
        """放弃Session中的所有数据修改"""
        self.clean()

    async def __aenter__(self):
        self._entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self.commit()
        else:
            self.discard()
