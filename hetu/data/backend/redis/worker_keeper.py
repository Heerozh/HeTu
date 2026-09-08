import logging
from typing import TYPE_CHECKING, cast, final, override

import redis.asyncio

from ....common.helper import get_machine_id
from ....common.snowflake_id import MAX_WORKER_ID, WorkerKeeper
from ....i18n import _

if TYPE_CHECKING:
    import redis

logger = logging.getLogger("HeTu.root")

# 回收worker id的时间，超时则认为宕机
WORKER_ID_EXPIRE_SEC = 60


@final
class RedisWorkerKeeper(WorkerKeeper):
    """
    基于 Redis 的 Worker ID 管理器，目前有通用的，未使用此类。
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

    def __init__(
        self,
        pid: int,
        aio: redis.asyncio.Redis | redis.asyncio.RedisCluster,
    ):
        """
        初始化 RedisWorkerKeeper。
        """
        super().__init__()
        self.aio = aio
        self.worker_id_key = "snowflake:worker"
        self.worker_id = -1
        # 机器码+pid组成的node_id。
        # 如果pid为固定值，则可以保证60秒内获取到的worker_id尽可能不变
        # 比如固定每个容器只启动一个worker，则pid是固定的1
        self.node_id = f"{get_machine_id()}:{pid}"

    @override
    async def get_worker_id(self) -> int:
        """
        从Redis中获取一个可用的 Worker ID。
        """
        # 查找之前是否已经分配过自己的worker id
        for worker_id in range(0, MAX_WORKER_ID + 1):
            key = f"{self.worker_id_key}:{worker_id}"
            # 判断node_id是否相同，相同则说明是容器重启，直接使用
            if (existing_node_id := await self.aio.get(key)) is None:
                continue
            existing_node_id = cast(bytes, existing_node_id)
            if existing_node_id.decode("ascii") != self.node_id:
                continue
            if await self.aio.expire(key, WORKER_ID_EXPIRE_SEC) != 1:
                continue
            logger.info(
                _(
                    "[❄️ID] 重新使用已分配的 Worker ID: {worker_id} "
                    "(通过相同进程码 {node_id} )"
                ).format(worker_id=worker_id, node_id=self.node_id)
            )
            self.worker_id = worker_id
            return worker_id

        # 尝试分配新的worker id
        for worker_id in range(0, MAX_WORKER_ID + 1):
            key = f"{self.worker_id_key}:{worker_id}"
            # 尝试设置键，NX 表示仅当键不存在时设置，EX 表示键过期时间
            result = await self.aio.set(
                key, self.node_id, nx=True, ex=WORKER_ID_EXPIRE_SEC
            )
            if result:
                logger.info(
                    _(
                        "[❄️ID] 成功获取 Worker ID: {worker_id}, 进程码: {node_id}"
                    ).format(worker_id=worker_id, node_id=self.node_id)
                )
                self.worker_id = worker_id
                return worker_id

        raise KeyError(
            _(
                "无法获取可用的 Worker ID，所有 ID 均被占用。如果有宕机，请等待ID过期重试"
            )
        )

    @override
    async def release_worker_id(self):
        """
        释放当前占用的 Worker ID。
        """
        if self.worker_id == -1:
            return
        key = f"{self.worker_id_key}:{self.worker_id}"
        await self.aio.delete(key)
        logger.info(
            _("[❄️ID] 释放 Worker ID: {worker_id}").format(worker_id=self.worker_id)
        )

    @override
    async def keep_alive(self):
        """
        续租 Worker ID 的有效期。
        续约失败则抛出异常，表示 Worker ID 可能中途被其他实例占用了。
        此方法需要每5秒调用1次。

        ⚠️ 已知缺陷：`EXPIRE` 只看 key 在不在、**不看 value**，所以它只挡得住"租约过期
        且没人接手"（key没了→返回0），挡不住"租约被别的 worker 用 SET NX 抢走"（key还在，
        值是对方的 node_id → 照样返回1）。后者恰恰是会产生重复雪花ID的那种。
        正确做法是 compare-and-expire，比如一条 `GETEX key EX ttl` 拿回旧值再比 node_id。

        时间戳高水位不再在这里写，已拆给 `SnowflakeTimestampKeeper`：租约要互斥、水位
        只要单调max，两者并发语义相反，捆一起没必要。
        """
        worker_id = self.worker_id
        key = f"{self.worker_id_key}:{worker_id}"
        # 刷新键的过期时间
        resp = await self.aio.expire(key, WORKER_ID_EXPIRE_SEC)
        if resp != 1:
            logger.error(
                _(
                    "[❄️ID] 续约 Worker ID {worker_id} 失败: "
                    "可能已被其他实例占用，也可能是Redis负载过高来不及响应，将重启Worker..."
                ).format(worker_id=worker_id)
            )
            # 关闭Worker
            raise SystemExit(_("Worker ID 续约失败，重启Worker..."))
