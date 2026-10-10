"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
import os
import re
from time import monotonic
from typing import TYPE_CHECKING, final, override

import numpy as np

from ...common.helper import get_machine_id, lease_owner_exited
from ...common.permission import Permission
from ...common.snowflake_id import (
    FENCE_MARGIN_SEC,
    MAX_WORKER_ID,
    TOOL_NODE_PREFIX,
    TOOL_WORKER_ID_FLOOR,
    WORKER_ID_EXPIRE_SEC,
    WORKER_ID_KEY,
    WorkerKeeper,
)
from ...i18n import _
from ..component import BaseComponent, define_component, property_field

if TYPE_CHECKING:
    from . import Backend
    from .sqlite.client import SQLiteBackendClient

logger = logging.getLogger("HeTu.root")


@define_component(namespace="core", permission=Permission.ADMIN, volatile=True)
class WorkerLease(BaseComponent):
    """
    Worker 相关的跨进程状态表。

    说明:
    - `id` 内置主键，直接使用 `worker_id`（0~1023）
    - `last_timestamp` 是雪花ID的时间戳高水位，由 `SnowflakeTimestampKeeper` 读写，
      非索引字段以便 direct_set 直写
    - worker id 的租约不在本表：Redis 后端存成 Redis 键（见 `RedisWorkerKeeper`），
      SQLite 后端不用租约（见 `FixedWorkerKeeper`）
    """

    last_timestamp: np.int64 = property_field(0)


@final
class FixedWorkerKeeper(WorkerKeeper):
    """开发模式的固定 Worker ID 分配器：直接用进程在本机内的序号，不做任何跨进程协调。

    ## 适用范围

    给 SQLite 后端用。它在 HeTu 里只用于开发/调试（见 CONFIG_TEMPLATE 里 BACKENDS 的说明），
    而开发场景的特征是**单机**——单机内 worker 序号天然唯一，不需要租约、不需要续约、不需要
    所有权校验，也就不存在租约被抢导致雪花ID重复那一整类问题。也不模拟 Redis 的租约：租约的
    发号围栏会让调试时断点停久了就拒绝发号、甚至让 worker 退出。

    生产多机部署请用 Redis 后端，那里有 `RedisWorkerKeeper` 的真正租约。

    ## id 从哪来

    用 Sanic 给每个 worker 进程设的 `SANIC_WORKER_IDENTIFIER`（形如 "Srv 0"、"Srv 1"，
    见 sanic/worker/process.py）。单进程模式下该变量不存在，退化为 0。

    刻意**不做**成配置项：一旦允许手工指定，就会出现"一部分进程指定了、另一部分忘记指定"
    的混用场景，而两种分配方式互相看不见对方占用了哪些 id，会静默撞车产生重复雪花ID——
    那是现有的 CAS / 围栏等所有防护都拦不住的一类错误。

    A dev-mode worker id allocator for the SQLite backend: it simply uses the process's
    index within the machine, with no cross-process coordination at all. SQLite is
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
        # [TOOL_WORKER_ID_FLOOR, MAX_WORKER_ID] 留给 hetu call / shell（SQLiteToolWorkerKeeper）
        if worker_id >= TOOL_WORKER_ID_FLOOR:
            raise KeyError(
                _("Worker序号 {worker_id} 超出雪花ID上限 {max}").format(
                    worker_id=worker_id, max=TOOL_WORKER_ID_FLOOR - 1
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


@final
class SQLiteToolWorkerKeeper(WorkerKeeper):
    """SQLite 后端上工具进程（hetu call / shell）的 Worker ID 租约。

    SQLite 上的服务器 worker 用本机进程序号（`FixedWorkerKeeper`），不做任何协调；工具进程
    不能也用序号（没有 `SANIC_WORKER_IDENTIFIER`，会拿到 0 和服务器撞号），也不能共用一个
    固定 id（大模型常并行跑命令，两个工具进程同 id 照样重号，还违反时间戳水位的单写者前提）。
    所以工具进程在预留段 [TOOL_WORKER_ID_FLOOR, MAX_WORKER_ID] 里用库里的带过期 KV
    （`__hetu_kv`，维护锁也在用）互斥地租，语义与 `RedisWorkerKeeper` 一致：SET NX 抢、
    值相符才续期、值相符才释放，并同样推进发号围栏。

    Worker id lease for tool processes on the SQLite backend, in a reserved id range.
    """

    def __init__(self, client: SQLiteBackendClient, pid: int):
        super().__init__()
        self.client = client
        self.worker_id = -1
        self.node_id = f"{TOOL_NODE_PREFIX}{get_machine_id()}:{pid}"
        self._token = self.node_id.encode()

    @staticmethod
    def _key(worker_id: int) -> str:
        return f"{WORKER_ID_KEY}:{worker_id}"

    async def _expire_if_mine(self) -> bool:
        """原子地"值相符才续期"，成功就推进围栏（用发起前的 monotonic 时刻，见基类）"""
        from .sqlite.store import SQLiteStore

        started_at = monotonic()
        mine = await self.client.run_(
            SQLiteStore.kv_expire_if,
            self._key(self.worker_id),
            self._token,
            WORKER_ID_EXPIRE_SEC,
        )
        if mine:
            self.lease_deadline = started_at + WORKER_ID_EXPIRE_SEC - FENCE_MARGIN_SEC
        return bool(mine)

    @override
    async def get_worker_id(self) -> int:
        from .sqlite.store import SQLiteStore

        if self.worker_id >= 0 and await self._expire_if_mine():
            return self.worker_id
        for worker_id in range(MAX_WORKER_ID, TOOL_WORKER_ID_FLOOR - 1, -1):
            started_at = monotonic()
            if await self.client.run_(
                SQLiteStore.kv_set_nx,
                self._key(worker_id),
                self._token,
                WORKER_ID_EXPIRE_SEC,
            ):
                self.lease_deadline = (
                    started_at + WORKER_ID_EXPIRE_SEC - FENCE_MARGIN_SEC
                )
                self.worker_id = worker_id
                logger.info(
                    _(
                        "[❄️ID] 成功获取 Worker ID: {worker_id}, 进程码: {node_id}"
                    ).format(worker_id=worker_id, node_id=self.node_id)
                )
                return worker_id
        raise KeyError(
            _(
                "并发的 hetu call / shell 进程太多：SQLite 上预留给它们的 {count} 个 "
                "Worker ID 都被占用了"
            ).format(count=MAX_WORKER_ID - TOOL_WORKER_ID_FLOOR + 1)
        )

    @override
    async def keep_alive(self):
        """值相符才续期；租约已不属于本进程（卡住太久被别人接手）就抛 SystemExit"""
        if not await self._expire_if_mine():
            logger.error(
                _(
                    "[❄️ID] 续约 Worker ID {worker_id} 失败: "
                    "租约已不属于本进程（可能因本进程长时间卡住导致租约过期后被其他实例"
                    "接手），继续发号会产生重复雪花ID"
                ).format(worker_id=self.worker_id)
            )
            raise SystemExit(_("Worker ID 续约失败"))

    @override
    async def release_worker_id(self):
        """只删自己那把（compare-and-delete）"""
        from .sqlite.store import SQLiteStore

        if self.worker_id < 0:
            return
        deleted = await self.client.run_(
            SQLiteStore.kv_delete_if, self._key(self.worker_id), self._token
        )
        if deleted:
            logger.info(
                _("[❄️ID] 释放 Worker ID: {worker_id}").format(worker_id=self.worker_id)
            )
        else:
            logger.warning(
                _("[❄️ID] Worker ID {worker_id} 的租约已不属于本进程，跳过释放").format(
                    worker_id=self.worker_id
                )
            )


def create_worker_keeper(
    backend: Backend, pid: int, *, tool: bool = False
) -> WorkerKeeper:
    """按后端类型选 Worker ID 分配器。

    * Redis 后端 → `RedisWorkerKeeper`，基于 Redis 原生命令的真正租约（多机安全）
    * 其余（SQLite）→ `FixedWorkerKeeper`，开发模式的本机序号分配（单机安全）

    这里按后端而不是按配置项来选，是为了不给用户留"选错模式"的机会：能多机部署的后端
    自动获得多机安全的分配器，只适合开发的后端自动获得零协调的分配器。

    tool=True 给 hetu call / shell 这类工具进程用：Redis 用 `RedisWorkerKeeper` 的 tool 模式
    （不做复用扫描、从上往下分配），SQLite 用 `SQLiteToolWorkerKeeper`（预留段里的 KV 租约）。
    """
    from .redis.client import RedisBackendClient
    from .redis.worker_keeper import RedisWorkerKeeper

    master = backend.master
    if isinstance(master, RedisBackendClient):
        return RedisWorkerKeeper(pid, master.aio, tool=tool)
    if tool:
        from .sqlite.client import SQLiteBackendClient

        assert isinstance(master, SQLiteBackendClient)
        return SQLiteToolWorkerKeeper(master, pid)
    return FixedWorkerKeeper()


def live_worker_leases(backend: Backend) -> dict[int, str]:
    """
    这个后端上还持有租约的 worker id → 持有者（node_id；工具进程带 `cli:` 前缀），非空就说明
    有服务器或 hetu call / shell 在跑（或者异常退出后租约还没过期；Windows 上本机已经退出的
    进程留下的不算，见 `hetu.common.helper.lease_owner_exited`）。`hetu upgrade` 据此拒绝在线
    执行。SQLite 后端只看得到工具进程的租约（服务器 worker 没有租约，见 `FixedWorkerKeeper`）。
    """
    from .redis.client import RedisBackendClient

    master = backend.master
    if isinstance(master, RedisBackendClient):
        from .redis.worker_keeper import live_worker_leases as redis_live_worker_leases

        return redis_live_worker_leases(master.io)
    from .sqlite.client import SQLiteBackendClient
    from .sqlite.store import SQLiteStore

    if not isinstance(master, SQLiteBackendClient):
        return {}
    leases: dict[int, str] = {}
    for worker_id in range(TOOL_WORKER_ID_FLOOR, MAX_WORKER_ID + 1):
        owner = master.run_sync_(SQLiteStore.kv_get, f"{WORKER_ID_KEY}:{worker_id}")
        if owner is not None and not lease_owner_exited(owner):
            leases[worker_id] = owner.decode("utf-8", "replace")
    return leases


def live_worker_ids(backend: Backend) -> list[int]:
    """这个后端上还持有租约的 worker id，见 `live_worker_leases`"""
    return list(live_worker_leases(backend))
