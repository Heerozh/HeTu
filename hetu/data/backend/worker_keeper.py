"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from time import time
from typing import TYPE_CHECKING, final, override

import numpy as np

from ...common.helper import get_machine_id
from ...common.permission import Permission
from ...common.snowflake_id import MAX_WORKER_ID, WorkerKeeper
from ..component import BaseComponent, define_component, property_field
from .base import RaceCondition, RowFormat

if TYPE_CHECKING:
    from .table import Table

logger = logging.getLogger("HeTu.root")

WORKER_ID_EXPIRE_SEC = 60


@define_component(namespace="core", permission=Permission.ADMIN, volatile=True)
class WorkerLease(BaseComponent):
    """
    通用 Worker ID 租约表（跨后端）。

    说明:
    - `id` 内置主键，直接使用 `worker_id`（0~1023）
    - `expires_at` / `last_timestamp` 设计为非索引字段，便于 direct_set 续租
    """

    node_id: str = property_field("", dtype="<U96")
    expires_at: np.int64 = property_field(0)
    last_timestamp: np.int64 = property_field(0)


@final
class GeneralWorkerKeeper(WorkerKeeper):
    """
    基于通用Backend事务实现的 WorkerKeeper。

    - 使用 `WorkerLease` 表保存租约（id=worker_id）
    - `get_worker_id` 走事务，保证竞争时安全
    - `keep_alive` / `release_worker_id` 使用 `direct_set`

    此类的目的是：
    1. 分配空余worker id，并让宕机的worker id会得到释放
    2. 每几秒就储存每台服务器的时间，来减少重启时，发生时间回拨导致ID重复的风险。

    服务器重启回拨(关闭期间发生时间回拨)的情况很少，但通过以下方式：
    1. 保证机器ntp持续工作，回拨不超过10秒，这样限制每次重启等10秒以上可解决重启回拨问题。
    2. 再加上本类的keep_alive持续记录服务器时间戳，保证记录间隔<=10秒。
    虽然方式1已经可以解决此类问题，但方式2不依赖运维，可以让此问题透明。

    通过
        node_id = f"{get_machine_id()}:{pid}"
        SET snowflake:worker:{worker_id} {node_id} NX EX WORKER_ID_EXPIRE_SEC
    成功：拿到了 WorkerID = {worker_id}
    失败：说明 ID 正在被别的机器占用，如果node_id不符，循环尝试 ID {worker_id + 1}。
         直到1024次失败报错。

    后台设置个5秒的Task持续续约此key
    """

    def __init__(self, pid: int, lease_table: Table):
        """
        初始化 GeneralWorkerKeeper。
        注意PID决定了Worker ID的稳定性，如果每次重启PID都变，则可能每次重启都换一个Worker ID。
        所以动态扩展的服务器建议从Docker中启动，可保证PID都是从1开始。
        """
        super().__init__()
        self.pid = pid
        self.table = lease_table
        self.worker_id = -1
        self.node_id = f"{get_machine_id()}:{pid}"

    @staticmethod
    def _now_ms() -> int:
        return int(time() * 1000)

    @staticmethod
    def _expire_ms() -> int:
        return WORKER_ID_EXPIRE_SEC * 1000

    async def _try_claim_worker_id(self, worker_id: int) -> bool:
        for _ in range(5):
            try:
                async with self.table.session() as session:
                    repo = session.using(WorkerLease)
                    try:
                        row = await repo.get(id=worker_id)
                    except KeyError:  # 可能是direct set导致的副作用，写入了垃圾数据
                        row = None
                    now_ms = self._now_ms()

                    if row is None:
                        lease = WorkerLease.new_row(id_=worker_id)
                        lease.node_id = self.node_id
                        lease.expires_at = now_ms + self._expire_ms()
                        lease.last_timestamp = now_ms
                        await repo.insert(lease)
                        return True

                    if row.node_id == self.node_id or int(row.expires_at) <= now_ms:
                        row.node_id = self.node_id
                        row.expires_at = now_ms + self._expire_ms()
                        row.last_timestamp = max(int(row.last_timestamp), now_ms)
                        await repo.update(row)
                        return True

                    return False
            except RaceCondition:
                continue
        return False

    @override
    async def get_worker_id(self) -> int:
        """
        从Table中获取一个可用的 Worker ID。
        采用从1到1023的顺序尝试获取Worker ID的方式
        """
        if self.worker_id >= 0:
            return self.worker_id

        for worker_id in range(0, MAX_WORKER_ID + 1):
            if not await self._try_claim_worker_id(worker_id):
                continue
            self.worker_id = worker_id
            logger.info(
                f"[❄️ID] [General] 成功获取 Worker ID: {worker_id}, 进程码: {self.node_id}"
            )
            return worker_id

        raise KeyError(
            "无法获取可用的 Worker ID，所有 ID 均被占用。如果有宕机，请等待ID过期重试"
        )

    @override
    async def release_worker_id(self):
        """
        释放当前占用的 Worker ID。
        """
        worker_id = self.worker_id
        if worker_id < 0:
            return
        await self.table.direct_set(worker_id, expires_at="0")
        logger.info(f"[❄️ID] [General] 释放 Worker ID: {worker_id}")

    @override
    async def get_last_timestamp(self) -> int:
        """
        从 Table 中获取上次生成 ID 的时间戳。
        """
        now_ms = self._now_ms()
        worker_id = self.worker_id
        if worker_id < 0:
            return now_ms

        row = await self.table.backend.master.get(
            self.table, worker_id, row_format=RowFormat.STRUCT
        )
        if row is None:
            return now_ms

        return max(int(row.last_timestamp), now_ms)

    @override
    async def keep_alive(self, last_timestamp: int):
        """
        续租 Worker ID 的有效期，并保存雪花ID上次生成用的时间戳。
        续约失败则抛出异常，表示 Worker ID 可能中途被其他实例占用了。
        此方法需要每5秒调用1次，因为回拨误差是10秒。
        """
        worker_id = self.worker_id
        if worker_id < 0:
            worker_id = await self.get_worker_id()

        now_ms = self._now_ms()
        await self.table.direct_set(
            worker_id,
            expires_at=str(now_ms + self._expire_ms()),
            last_timestamp=str(last_timestamp),
        )
