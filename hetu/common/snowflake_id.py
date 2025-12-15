"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import time
import asyncio
import logging

from hetu.common.singleton import Singleton

logger = logging.getLogger("HeTu.root")


class SnowflakeID(metaclass=Singleton):
    """
    标准的雪花算法 (Snowflake) 实现，针对 Python 异步环境优化。

    结构 (64位):
    0 - 0000000000 0000000000 0000000000 0000000000 0 - 00000 - 00000 - 000000000000
    1位标识 | 41位时间戳 (毫秒) | 0位数据中心ID | 10位Worker ID | 12位序列号
    """

    # 初始时间截 (2025-12-18 03:33:20 +0800)
    TW_EPOCH = 1766000000000

    # 各部分占用位数
    WORKER_ID_BITS = 10
    DATACENTER_ID_BITS = 0
    SEQUENCE_BITS = 12

    # 各部分最大值 (通过位移计算)
    MAX_WORKER_ID = -1 ^ (-1 << WORKER_ID_BITS)
    MAX_DATACENTER_ID = -1 ^ (-1 << DATACENTER_ID_BITS)

    # 序列号掩码 (4095)
    SEQUENCE_MASK = -1 ^ (-1 << SEQUENCE_BITS)

    # 位移量
    WORKER_ID_SHIFT = SEQUENCE_BITS
    DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS
    TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS

    def __init__(self):
        self.worker_id = -1
        self.datacenter_id = -1
        self.sequence = 0
        self.last_timestamp = -1

    def init(self, worker_id: int, datacenter_id: int = 0):
        """
        初始化雪花生成器

        Parameters
        ----------
        worker_id: int
            工作ID (0-1023)
        datacenter_id: int
            数据中心ID，废弃 (0-0)
        """
        if worker_id > self.MAX_WORKER_ID or worker_id < 0:
            raise ValueError(
                f"Worker ID can't be greater than {self.MAX_WORKER_ID} or less than 0"
            )
        if datacenter_id > self.MAX_DATACENTER_ID or datacenter_id < 0:
            raise ValueError(
                f"Datacenter ID can't be greater than {self.MAX_DATACENTER_ID} or less than 0"
            )

        self.worker_id = worker_id
        self.datacenter_id = datacenter_id

        self.sequence = 0
        self.last_timestamp = -1

    @staticmethod
    def _current_timestamp() -> int:
        return int(time.time() * 1000)

    async def next_id(self) -> int:
        """
        生成下一个 ID (异步方法)
        """
        assert self.worker_id >= 0
        # 如果这里加锁，虽然能防止大量协程都进入sleep发生切换，但平日性能会下降6倍
        # 所以还是用while方式
        while True:
            timestamp = self._current_timestamp()

            # 如果时钟回拨，使用最后的时间
            if timestamp < self.last_timestamp:
                # 警告：时钟回拨发生。
                logger.warning(
                    f"[❄️ID] 时钟回拨了 {self.last_timestamp - timestamp} 毫秒。"
                )
                # 策略：假装时间没有倒流，继续使用 last_timestamp
                # 这会导致我们在“过去”的时间里消耗序列号，直到系统时间追上来
                timestamp = self.last_timestamp

            # 如果是同一毫秒内生成的
            if self.last_timestamp == timestamp:
                # 序列号自增，并与掩码进行与运算，保证不溢出
                next_sequence = (self.sequence + 1) & self.SEQUENCE_MASK

                # 如果序列号溢出 (变成0)，说明该毫秒内的 4096 个 ID 已用完
                if next_sequence == 0:
                    # 如果序列用完，asyncio.sleep 下一毫秒
                    await asyncio.sleep(0.001)
                    # 重新loop
                    continue

                self.sequence = next_sequence
            else:
                # 如果是新的毫秒，序列号重置
                self.sequence = 0

            # 更新最后生成时间
            self.last_timestamp = timestamp

            # 移位并通过或运算拼凑 64 位 ID
            new_id = (
                ((timestamp - self.TW_EPOCH) << self.TIMESTAMP_LEFT_SHIFT)
                | (self.datacenter_id << self.DATACENTER_ID_SHIFT)
                | (self.worker_id << self.WORKER_ID_SHIFT)
                | self.sequence
            )

            return new_id
        raise RuntimeError("disable return none warning")
