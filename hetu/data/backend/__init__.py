"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from .base import (
    BackendClient,
    BackendClientFactory,
    RaceCondition,
    RowFormat,
    UniqueViolation,
)
from .redis import RedisBackendClient, RedisTableMaintenance
from .session import Session
from .table import TableReference

__all__ = [
    "RaceCondition",
    "UniqueViolation",
    "RowFormat",
    "BackendClient",
    "Backend",
    "Session",
    "RedisBackendClient",
    "RedisTableMaintenance",
    "TableReference",
]


import asyncio
import random
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ...common.snowflake_id import WorkerKeeper


class Backend:
    """
    管理master, servants连接。
    """

    def __init__(self, config: dict):
        """
        从配置字典初始化Backend，创建master和servants连接。
        config为配置BACKENDS[i]内容。
        """
        clustering = config.get("clustering", False)
        self._master = BackendClientFactory.create(
            config["type"], config["master"], clustering, False
        )
        self._servants = [
            BackendClientFactory.create(config["type"], servant, clustering, True)
            for servant in config.get("servants", [])
        ]
        # master_weight表示选中的权重，每台副本数据库权重固定为1.0
        #   如果master任务不繁重，理论上提高此值可以降低事务冲突概率，因为从副本读取的值可能落后。
        #   反之降低此值减少主数据库读取负载，但提高冲突概率，也许反而会增加master负载。
        self._master_weight = config.get("master_weight", 1.0)
        self._all_clients = self._servants + [self._master]
        self._all_weights = [1.0] * len(self._servants) + [self._master_weight]

    async def close(self):
        await self._master.close()
        for servant in self._servants:
            await servant.close()

    def configure(self):
        """
        启动时检查并配置数据库，减少运维压力的帮助方法，非必须。
        """
        self._master.configure()
        for servant in self._servants:
            servant.configure()

    async def wait_for_synced(self) -> None:
        """
        等待各个savants数据库和master数据库的数据完成同步。
        主要用于test用例。
        """
        while not await self._master.is_synced():
            await asyncio.sleep(0.1)

    def get_worker_keeper(self) -> WorkerKeeper | None:
        """
        获取WorkerKeeper实例，用于雪花ID的worker id管理。
        如果不支持worker id管理，可以返回None
        """
        return self._master.get_worker_keeper()

    @property
    def master(self) -> BackendClient:
        """返回主数据库连接"""
        return self._master

    @property
    def servant(self) -> BackendClient:
        """返回一个从数据库连接，随机选择。如果没有从数据库，则返回主数据库。"""
        if not self._servants:
            return self._master
        return random.choice(self._servants)

    @property
    def master_or_servant(self) -> BackendClient:
        """
        返回主数据库连接或一个从数据库连接，随机选择。
        """
        return random.choices(self._all_clients, self._all_weights)[0]

    def get_table_maintenance(self):
        """
        获取表维护对象，根据不同后端类型返回不同的实现。
        """
        return self._master.get_table_maintenance()
