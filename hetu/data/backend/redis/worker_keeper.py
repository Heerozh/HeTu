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

    通过
        SET snowflake:worker:{worker_id} {uuid.getnode()} NX EX 86400
    成功：拿到了 WorkerID = {worker_id}
    失败：说明 ID 正在被别的机器占用，如果node id不符，循环尝试 ID {worker_id + 1}。
         直到1024次失败报错。

    后台设置个5秒的Task持续续约此key
    """

    def __init__(
        self,
        io: redis.Redis | redis.RedisCluster,
        aio: redis.asyncio.Redis | redis.asyncio.RedisCluster,
    ):
        """
        初始化 RedisWorkerKeeper。

        Parameters
        ----------
        redis_client: Any
            已连接的 Redis 客户端实例。
        """
        super().__init__()
        self.io = io
        self.aio = aio
        self.worker_id_key = "snowflake:worker"
        self.last_timestamp_key = "snowflake:last_timestamp"
        self.worker_id = -1
        self.node_id = uuid.getnode()

    @override
    def get_worker_id(self) -> int:
        """
        从Redis中获取一个可用的 Worker ID。
        """
        for worker_id in range(0, MAX_WORKER_ID + 1):
            key = f"{self.worker_id_key}:{worker_id}"
            # 尝试设置键，NX 表示仅当键不存在时设置，EX 86400 表示键过期时间为1天
            result = self.io.set(key, self.node_id, nx=True, ex=86400)
            if result:
                logger.info(
                    f"[❄️ID] 成功获取 Worker ID: {worker_id}, 机器码: {self.node_id}"
                )
                self.worker_id = worker_id
                return worker_id
            else:
                # 判断node_id是否相同，相同则说明是重启，直接使用
                existing_node_id = cast(bytes, self.io.get(key))
                if (
                    existing_node_id is not None
                    and int(existing_node_id) == self.node_id
                ):
                    logger.info(
                        f"[❄️ID] 重新使用已分配的 Worker ID: {worker_id} "
                        f"(通过相同机器码 {self.node_id} )"
                    )
                    self.worker_id = worker_id
                    return worker_id
        raise SystemExit("无法获取可用的 Worker ID，所有 ID 均被占用。")

    @override
    def get_last_timestamp(self) -> int:
        """
        从 Redis 中获取上次生成 ID 的时间戳。
        """
        key = f"{self.last_timestamp_key}:{self.node_id}"
        last_timestamp = cast(bytes, self.io.get(key))
        # 返回当前时间加10秒，防止重启回拨
        if last_timestamp is not None:
            logger.info(
                f"[❄️ID] 成功获取 {self.node_id} 持久化的 last_timestamp: "
                f"{datetime.fromtimestamp(int(last_timestamp) / 1000):%Y-%m-%d %H:%M:%S}"
            )
            return int(last_timestamp) + 10000
        else:
            return int(time() * 1000) + 10000

    @override
    async def keep_alive(self, last_timestamp: int):
        """
        续租 Worker ID 的有效期，
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
