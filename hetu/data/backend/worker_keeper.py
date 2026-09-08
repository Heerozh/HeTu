"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
import os
import re
from typing import TYPE_CHECKING, final, override

import numpy as np

from ...common.permission import Permission
from ...common.snowflake_id import MAX_WORKER_ID, WorkerKeeper
from ...i18n import _
from ..component import BaseComponent, define_component, property_field

if TYPE_CHECKING:
    from . import Backend

logger = logging.getLogger("HeTu.root")


@define_component(namespace="core", permission=Permission.ADMIN, volatile=True)
class WorkerLease(BaseComponent):
    """
    Worker 相关的跨进程状态表。

    说明:
    - `id` 内置主键，直接使用 `worker_id`（0~1023）
    - `last_timestamp` 是雪花ID的时间戳高水位，由 `SnowflakeTimestampKeeper` 读写，
      非索引字段以便 direct_set 直写
    - `node_id` / `expires_at` 是已删除的 GeneralWorkerKeeper（基于本表做租约）留下的字段，
      目前没有任何代码读写。**故意保留**：改字段会让 `check_and_create_new_tables` 判定
      schema_mismatch，逼所有现网部署跑一次 `hetu upgrade`，为一次内部清理付这个代价不值。
      将来若有别的迁移顺路带上即可。
    """

    node_id: str = property_field("", dtype="<U96")
    expires_at: np.int64 = property_field(0)
    last_timestamp: np.int64 = property_field(0)


@final
class FixedWorkerKeeper(WorkerKeeper):
    """开发模式的固定 Worker ID 分配器：直接用进程在本机内的序号，不做任何跨进程协调。

    ## 适用范围

    给 SQL 后端（SQLite/Postgres/MariaDB）用。这些后端在 HeTu 里本来就只推荐开发/调试或
    极低订阅负载场景（订阅表性能不够，见 CONFIG_TEMPLATE 里 BACKENDS 的说明），而开发场景
    的特征是**单机**——单机内 worker 序号天然唯一，不需要租约、不需要续约、不需要所有权
    校验，也就不存在租约被抢导致雪花ID重复那一整类问题。

    生产多机部署请用 Redis 后端，那里有 `RedisWorkerKeeper` 的真正租约。

    ## id 从哪来

    用 Sanic 给每个 worker 进程设的 `SANIC_WORKER_IDENTIFIER`（形如 "Srv 0"、"Srv 1"，
    见 sanic/worker/process.py）。单进程模式下该变量不存在，退化为 0。

    刻意**不做**成配置项：一旦允许手工指定，就会出现"一部分进程指定了、另一部分忘记指定"
    的混用场景，而两种分配方式互相看不见对方占用了哪些 id，会静默撞车产生重复雪花ID——
    那是现有的 CAS / 围栏等所有防护都拦不住的一类错误。

    A dev-mode worker id allocator for SQL backends: it simply uses the process's index
    within the machine, with no cross-process coordination at all. SQL backends are
    dev-only in HeTu, and dev means single machine, where per-process indexes are already
    unique. Use the Redis backend (and its real lease) for multi-machine production.
    """

    def __init__(self):
        super().__init__()
        self.worker_id = -1

    @staticmethod
    def _sanic_worker_index() -> int:
        """从 Sanic 的 worker 标识里取出本进程在本机内的序号"""
        ident = os.environ.get("SANIC_WORKER_IDENTIFIER", "")
        match = re.search(r"(\d+)", ident)
        return int(match.group(1)) if match else 0

    @override
    async def get_worker_id(self) -> int:
        if self.worker_id >= 0:
            return self.worker_id

        worker_id = self._sanic_worker_index()
        if worker_id > MAX_WORKER_ID:
            raise KeyError(
                _("Worker序号 {worker_id} 超出雪花ID上限 {max}").format(
                    worker_id=worker_id, max=MAX_WORKER_ID
                )
            )
        self.worker_id = worker_id
        logger.info(
            _(
                "[❄️ID] [开发模式] 使用本机进程序号作为 Worker ID: {worker_id}。"
                "此模式只保证单机内不重复，多机部署请改用 Redis 后端"
            ).format(worker_id=worker_id)
        )
        return worker_id

    @override
    async def release_worker_id(self):
        """无需释放：没有占用任何跨进程资源"""

    @override
    async def keep_alive(self):
        """无需续约：id 由本机进程序号决定，不会被别人抢走"""


def create_worker_keeper(backend: Backend, pid: int) -> WorkerKeeper:
    """按后端类型选 Worker ID 分配器。

    * Redis 后端 → `RedisWorkerKeeper`，基于 Redis 原生命令的真正租约（多机安全）
    * 其余（SQL 系）→ `FixedWorkerKeeper`，开发模式的本机序号分配（单机安全）

    这里按后端而不是按配置项来选，是为了不给用户留"选错模式"的机会：能多机部署的后端
    自动获得多机安全的分配器，只适合开发的后端自动获得零协调的分配器。
    """
    from .redis.client import RedisBackendClient
    from .redis.worker_keeper import RedisWorkerKeeper

    master = backend.master
    if isinstance(master, RedisBackendClient):
        return RedisWorkerKeeper(pid, master.aio)
    return FixedWorkerKeeper()
