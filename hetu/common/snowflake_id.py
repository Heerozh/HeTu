"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
from datetime import datetime
from time import time, sleep
from typing import TYPE_CHECKING
import uuid

import redis.asyncio

from hetu.common.singleton import Singleton

if TYPE_CHECKING:
    import redis

logger = logging.getLogger("HeTu.root")

# 初始时间截 (2025-12-18 03:33:20 +0800)
TW_EPOCH = 1766000000000

# 各部分占用位数
WORKER_ID_BITS = 10
SEQUENCE_BITS = 12

# 各部分最大值 (通过位移计算)
MAX_WORKER_ID = -1 ^ (-1 << WORKER_ID_BITS)

# 序列号掩码 (4095)
SEQUENCE_MASK = -1 ^ (-1 << SEQUENCE_BITS)

# 位移量
# WORKER_ID_SHIFT = SEQUENCE_BITS
# DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS
# TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS


class SnowflakeID(metaclass=Singleton):
    """
    标准的雪花算法 (Snowflake) 实现，针对单线程多进程环境。

    结构 (64位):
    1位符号 | 41位时间戳 (毫秒) | 0位数据中心ID | 10位Worker ID | 12位序列号

    雪花ID必须保证服务器ntp同步准确，不会发生10秒以上的时钟回拨。
    类会记录最后的时间，如果发生回拨，会sleep等待，时间过长会卡死服务器。

    重启回拨问题
    ---------
    重启时如果服务器有时间回拨会导致ID重复。
    - 为尽可能减少这个情况，需要持久化最后的时间戳(精度<=n秒，重启不可能n秒内完成)，来防止此情况。
    - 使用方要尽可能保证worker id不变，不然使用了别的机器的id，而重启后机器间有时间差异也会导致ID重复。

    以上2点已通过WorkerKeeper类实现。
    """

    def __init__(self):
        self.worker_id = -1
        self.datacenter_id = -1
        self.sequence = 0
        self.last_timestamp = -1

    def init(self, worker_id: int, last_timestamp: int = int(time() * 1000) + 10000):
        """
        初始化雪花生成器。

        worker_id必须唯一，请在数据库保存自己的worker_id。

        Parameters
        ----------
        worker_id: int
            工作ID (0-1023)，每个进程一个
        last_timestamp: int
            上次生成ID的时间戳 (毫秒)，用于防止重启时时间发生回拨造成的id重复.
            如果持久化的时间戳精度为10秒，建议传入时加上10000。
        """
        if worker_id > MAX_WORKER_ID or worker_id < 0:
            raise ValueError(
                f"Worker ID can't be greater than {MAX_WORKER_ID} or less than 0"
            )

        self.worker_id = worker_id

        self.sequence = 0
        self.last_timestamp = last_timestamp
        logger.info(
            f"[❄️ID] 雪花ID生成器初始化完成，Worker ID: {worker_id}, last_timestamp: {datetime.fromtimestamp(last_timestamp / 1000):%Y-%m-%d %H:%M:%S}"
        )

    def _next_id(self) -> int | None:
        """
        生成下一个 ID，超标时返回 None。
        """
        worker_id = self.worker_id
        assert worker_id >= 0, "SnowflakeID 未初始化，请先调用 init() 方法。"

        timestamp = int(time() * 1000)
        last_timestamp = self.last_timestamp

        # 如果时钟回拨，使用最后的时间
        if timestamp < last_timestamp:
            # 警告：时钟回拨发生。
            logger.warning(f"[❄️ID] 时钟回拨了 {last_timestamp - timestamp} 毫秒。")
            # 策略：假装时间没有倒流，继续使用 last_timestamp
            # 这会导致我们在“过去”的时间里消耗序列号，直到系统时间追上来
            timestamp = last_timestamp

        # 如果是同一毫秒内生成的
        if last_timestamp == timestamp:
            # 序列号自增，并与掩码进行与运算，保证不溢出
            next_sequence = (self.sequence + 1) & SEQUENCE_MASK

            # 如果序列号溢出 (变成0)，说明该毫秒内的 4096 个 ID 已用完
            if next_sequence == 0:
                # 如果序列用完，asyncio.sleep 下一毫秒
                return None
        else:
            # 如果是新的毫秒，序列号重置
            next_sequence = 0
        self.sequence = next_sequence

        # 更新最后生成时间
        self.last_timestamp = timestamp

        # 移位并通过或运算拼凑 64 位 ID
        new_id = ((timestamp - TW_EPOCH) << 22) | (worker_id << 12) | next_sequence

        return new_id

    def next_id(self) -> int:
        """
        生成下一个 ID，同步方法。
        如果在同一毫秒内生成的 ID 超过 4096 个，会sleep到下一毫秒继续生成。
        """
        new_id = self._next_id()
        while new_id is None:
            logger.debug("[❄️ID] 每毫秒只能生成有限的ID，需要休眠 1 ms")
            # 等待到下一毫秒
            sleep(0.001)
            new_id = self._next_id()
        return new_id

    async def next_id_async(self) -> int:
        """
        生成下一个 ID，异步方法。
        如果在同一毫秒内生成的 ID 超过 4096 个，会await sleep到下一毫秒继续生成。

        注意：此方法基本不需要！！
        除非发生时间回拨，不然基本不可能发生sleep，因为单线程Call/ms到不了4096。
        """
        new_id = self._next_id()
        while new_id is None:
            logger.debug("[❄️ID] 每毫秒只能生成有限的ID，需要休眠 1 ms")
            # 等待到下一毫秒
            await asyncio.sleep(0.001)
            new_id = self._next_id()
        return new_id


class WorkerKeeper:
    subclasses = []

    def __init_subclass__(cls, **kwargs):
        """让继承子类自动注册alias"""
        super().__init_subclass__()
        cls.subclasses.append(cls)

    def __init__(self):
        pass

    def get_worker_id(self) -> int:
        # 初始化方法，不能async
        raise NotImplementedError

    def get_last_timestamp(self) -> int:
        # 初始化方法，不能async
        raise NotImplementedError

    async def keep_alive(self, last_timestamp: int):
        raise NotImplementedError


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
                existing_node_id = self.io.get(key)
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

    def get_last_timestamp(self) -> int:
        """
        从 Redis 中获取上次生成 ID 的时间戳。
        """
        key = f"{self.last_timestamp_key}:{self.node_id}"
        last_timestamp = self.io.get(key)
        # 返回当前时间加10秒，防止重启回拨
        if last_timestamp is not None:
            logger.info(
                f"[❄️ID] 成功获取 {self.node_id} 持久化的 last_timestamp: "
                f"{datetime.fromtimestamp(int(last_timestamp) / 1000):%Y-%m-%d %H:%M:%S}"
            )
            return int(last_timestamp) + 10000
        else:
            return int(time() * 1000) + 10000

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



# 后期如果需要其他数据库的实现，再放到新的文件中
