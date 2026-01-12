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
    MQClient,
    TableMaintenance,
)
from .redis import RedisBackendClient, RedisTableMaintenance
from .session import Session
from .repo import SessionRepository
from .table import TableReference, Table
from .sub import Subscriptions

__all__ = [
    "RaceCondition",
    "UniqueViolation",
    "RowFormat",
    "BackendClient",
    "Backend",
    "Session",
    "SessionRepository",
    "RedisBackendClient",  # todo 整理下不暴露给用户的类
    "RedisTableMaintenance",
    "Table",
    "TableReference",
    "MQClient",
    "TableMaintenance",
    "Subscriptions",
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
        # 如果未填写servants，则将master也作为servant使用(为了api统一)
        servants_urls = config.get("servants", [])
        if not servants_urls:
            servants_urls.append(config["master"])
        # 连接数据库
        self._master = BackendClientFactory.create(
            config["type"], config["master"], clustering, False
        )
        self._servants = [
            BackendClientFactory.create(config["type"], servant, clustering, True)
            for servant in servants_urls
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

    def post_configure(self):
        """
        对数据库做的配置工作放在这，可以做些减少运维压力的工作，或是需要项目加载完成后才能做的初始化工作。
        此项在服务器完全加载完毕后才会执行，在测试环境中，也是最后调用。
        """
        self._master.post_configure()
        for servant in self._servants:
            servant.post_configure()

    async def wait_for_synced(self) -> None:
        """
        sleep等待各个savants数据库和master数据库的数据完成数据同步。防止后续事务获取不到数据。
        只判断调用时间点前的数据，后续新增的数据不做判断，不会出现长时间等待的问题。
        主要用于性能无关的关键节点，比如创建新用户连接。
        """
        synced, checkpoint = await self._master.is_synced(None)
        while not synced:
            await asyncio.sleep(0.1)
            synced, _ = await self._master.is_synced(checkpoint)

    def get_worker_keeper(self, sequence_id: int) -> WorkerKeeper | None:
        """
        获取WorkerKeeper实例，用于雪花ID的worker id管理。
        如果不支持worker id管理，可以返回None
        """
        return self._master.get_worker_keeper(sequence_id)

    @property
    def master(self) -> BackendClient:
        """返回主数据库连接"""
        return self._master

    @property
    def servant(self) -> BackendClient:
        """返回一个从数据库连接，随机选择。如果没有从数据库，则返回主数据库。"""
        return random.choice(self._servants)

    @property
    def master_or_servant(self) -> BackendClient:
        """
        返回主数据库连接或一个从数据库连接，随机选择。
        """
        return random.choices(self._all_clients, self._all_weights)[0]

    def get_table_maintenance(self) -> TableMaintenance:
        """
        获取表维护对象，根据不同后端类型返回不同的实现。
        """
        return self._master.get_table_maintenance()

    def get_mq_client(self) -> MQClient:
        """获取消息队列连接"""
        return self.servant.get_mq_client()

    def session(self, instance: str, cluster_id: int) -> Session:
        """
        创建一个Session对象，负责缓存数据修改，并最终提交事务到数据库。
        """
        return Session(self, instance, cluster_id)
