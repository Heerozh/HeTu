import logging
import uuid
from datetime import datetime
from time import time
from typing import TYPE_CHECKING, cast, final, override

import redis.asyncio

from ....common.snowflake_id import MAX_WORKER_ID, WorkerKeeper

if TYPE_CHECKING:
    import redis

logger = logging.getLogger("HeTu.root")

# 回收worker id的时间，超时则认为宕机
WORKER_ID_EXPIRE_SEC = 60


@final
class RedisWorkerKeeper(WorkerKeeper):
    """
    基于 Redis 的 Worker ID 管理器。
    此类的目的是：
    1. 分配空余worker id，并让宕机的worker id会得到释放
    2. 每几秒就储存每台服务器的时间，来减少重启时，发生时间回拨导致ID重复的风险。

    服务器重启回拨(关闭期间发生时间回拨)的情况很少，但通过以下方式：
    1. 保证机器ntp持续工作，回拨不超过10秒，这样限制每次重启等10秒以上可解决重启回拨问题。
    2. 再加上本类的keep_alive持续记录服务器时间戳，保证记录间隔<=10秒。
    虽然方式1已经可以解决此类问题，但方式2不依赖运维，可以让此问题透明。

    通过
        node_id = f"{uuid.getnode()}:{pid}"
        SET snowflake:worker:{worker_id} {node_id} NX EX WORKER_ID_EXPIRE_SEC
    成功：拿到了 WorkerID = {worker_id}
    失败：说明 ID 正在被别的机器占用，如果node_id不符，循环尝试 ID {worker_id + 1}。
         直到1024次失败报错。

    后台设置个5秒的Task持续续约此key
    """

    def __init__(
        self,
        pid: int,
        io: redis.Redis | redis.RedisCluster,
        aio: redis.asyncio.Redis | redis.asyncio.RedisCluster,
    ):
        """
        初始化 RedisWorkerKeeper。
        """
        super().__init__()
        self.io = io
        self.aio = aio
        self.worker_id_key = "snowflake:worker"
        self.last_timestamp_key = "snowflake:last_timestamp"
        self.worker_id = -1
        # 机器码+pid组成的node_id。
        # 如果pid为固定值，则可以保证60秒内获取到的worker_id尽可能不变
        # 比如固定每个容器只启动一个worker，则pid是固定的1
        self.node_id = f"{uuid.getnode()}:{pid}"

    @override
    def get_worker_id(self) -> int:
        """
        从Redis中获取一个可用的 Worker ID。
        """
        # 查找之前是否已经分配过自己的worker id
        for worker_id in range(0, MAX_WORKER_ID + 1):
            key = f"{self.worker_id_key}:{worker_id}"
            # 判断node_id是否相同，相同则说明是容器重启，直接使用
            if (existing_node_id := self.io.get(key)) is None:
                continue
            existing_node_id = cast(bytes, existing_node_id)
            if existing_node_id.decode("ascii") != self.node_id:
                continue
            if self.io.expire(key, WORKER_ID_EXPIRE_SEC) != 1:
                continue
            logger.info(
                f"[❄️ID] 重新使用已分配的 Worker ID: {worker_id} "
                f"(通过相同进程码 {self.node_id} )"
            )
            self.worker_id = worker_id
            return worker_id

        # 尝试分配新的worker id
        for worker_id in range(0, MAX_WORKER_ID + 1):
            key = f"{self.worker_id_key}:{worker_id}"
            # 尝试设置键，NX 表示仅当键不存在时设置，EX 表示键过期时间
            result = self.io.set(key, self.node_id, nx=True, ex=WORKER_ID_EXPIRE_SEC)
            if result:
                logger.info(
                    f"[❄️ID] 成功获取 Worker ID: {worker_id}, 进程码: {self.node_id}"
                )
                self.worker_id = worker_id
                return worker_id

        raise KeyError(
            "无法获取可用的 Worker ID，所有 ID 均被占用。如果有宕机，请等待ID过期重试"
        )

    @override
    def release_worker_id(self):
        """
        释放当前占用的 Worker ID。
        """
        if self.worker_id == -1:
            return
        key = f"{self.worker_id_key}:{self.worker_id}"
        self.io.delete(key)
        logger.info(f"[❄️ID] 释放 Worker ID: {self.worker_id}")

    @override
    def get_last_timestamp(self) -> int:
        """
        从 Redis 中获取上次生成 ID 的时间戳。
        """
        key = f"{self.last_timestamp_key}:{self.node_id}"
        last_timestamp = cast(bytes, self.io.get(key))
        if last_timestamp is not None:
            logger.info(
                f"[❄️ID] 成功获取 {self.node_id} 持久化的 last_timestamp: "
                f"{datetime.fromtimestamp(int(last_timestamp) / 1000):%Y-%m-%d %H:%M:%S}"
            )
            # 返回持久化的时间戳和当前时间的最大值，因为这是为了防止回拨，自然要取最大值
            return max(int(last_timestamp), int(time() * 1000))
        else:
            return int(time() * 1000)

    @override
    async def keep_alive(self, last_timestamp: int):
        """
        续租 Worker ID 的有效期，并保存雪花ID上次生成用的时间戳。
        续约失败则抛出异常，表示 Worker ID 可能中途被其他实例占用了。
        此方法需要每5秒调用1次，因为回拨误差是10秒。
        """
        worker_id = self.worker_id
        key = f"{self.worker_id_key}:{worker_id}"
        # 刷新键的过期时间
        resp = await self.aio.expire(key, WORKER_ID_EXPIRE_SEC)
        if resp != 1:
            logger.error(
                f"[❄️ID] 续约 Worker ID {worker_id} 失败: "
                f"redis.expire({key}, {WORKER_ID_EXPIRE_SEC}) == {resp}，"
                f"可能已被其他实例占用。"
            )
            # 关闭Worker
            raise RuntimeError("Worker ID 续约失败，不该出现的错误，系统退出。")
        # 记录last_timestamp到redis，防止重启回拨
        ts_key = f"{self.last_timestamp_key}:{self.node_id}"
        await self.aio.set(ts_key, last_timestamp, ex=86400)
