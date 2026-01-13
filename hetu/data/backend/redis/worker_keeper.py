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


@final
class RedisWorkerKeeper(WorkerKeeper):
    """
    基于 Redis 的 Worker ID 管理器。
    此类的目的是：
    1. 为了尽可能让worker id在重启后不变，防止不同服务器系统时间不同，每次启动雪花ID要Sleep等待过久。
    2. 每几秒就储存每台服务器的时间，来减少重启时，发生时间回拨导致ID重复的风险。
    虽然以上问题都可以通过运维解决，但此类可让此问题透明化，无需运维依赖。

    服务器重启回拨(关闭期间发生时间回拨)的情况很少，但通过以下方式：
    1. 保证机器ntp持续工作，回拨不超过10秒，这样限制每次重启等10秒以上可解决重启回拨问题。
    2. 再加上本类的keep_alive持续记录服务器时间戳，保证记录间隔<=10秒。
    方式2已基本可以99.99%保证重启回拨问题

    通过
        SET snowflake:worker:{worker_id} {uuid.getnode():启动序列号} NX EX 86400
    成功：拿到了 WorkerID = {worker_id}
    失败：说明 ID 正在被别的机器占用，如果node id不符，循环尝试 ID {worker_id + 1}。
         直到1024次失败报错。

    后台设置个5秒的Task持续续约此key
    """

    def __init__(
        self,
        sequence_id: int,
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
        # getnode返回的机器码，为了让worker id重启不变，sequence_id应该用启动进程的顺序id
        self.node_id = f"{uuid.getnode()}:{sequence_id}"

    @override
    def get_worker_id(self) -> int:
        """
        从Redis中获取一个可用的 Worker ID。
        """
        # 查找之前是否已经分配过自己的worker id
        for worker_id in range(0, MAX_WORKER_ID + 1):
            key = f"{self.worker_id_key}:{worker_id}"
            # 判断node_id是否相同，相同则说明是重启，直接使用
            existing_node_id = self.io.get(key)
            if existing_node_id is not None:
                existing_node_id = cast(bytes, existing_node_id)
                if existing_node_id.decode("ascii") == self.node_id:
                    logger.info(
                        f"[❄️ID] 重新使用已分配的 Worker ID: {worker_id} "
                        f"(通过相同进程码 {self.node_id} )"
                    )
                    self.worker_id = worker_id
                    return worker_id

        # 尝试分配新的worker id
        for worker_id in range(0, MAX_WORKER_ID + 1):
            key = f"{self.worker_id_key}:{worker_id}"
            # 尝试设置键，NX 表示仅当键不存在时设置，EX 86400 表示键过期时间为1天
            result = self.io.set(key, self.node_id, nx=True, ex=86400)
            if result:
                logger.info(
                    f"[❄️ID] 成功获取 Worker ID: {worker_id}, 进程码: {self.node_id}"
                )
                self.worker_id = worker_id
                return worker_id

        raise SystemExit("无法获取可用的 Worker ID，所有 ID 均被占用。")

    @override
    def release_worker_id(self):
        """
        释放当前占用的 Worker ID。无需调用，只用于测试。
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
        resp = await self.aio.expire(key, 86400)
        if resp != 1:
            logger.error(
                f"[❄️ID] 续约 Worker ID {worker_id} 失败，可能已被其他实例占用。"
            )
            # 关闭Worker todo 需要测试是否有效
            raise SystemExit("Worker ID 续约失败，不该出现的错误，系统退出。")
        # 记录last_timestamp到redis，防止重启回拨
        key = f"{self.last_timestamp_key}:{self.node_id}"
        await self.aio.set(key, last_timestamp, ex=86400)
