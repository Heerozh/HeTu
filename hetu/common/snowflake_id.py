"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
from datetime import datetime
from time import monotonic, sleep, time
from typing import final

from hetu.common.singleton import Singleton
from hetu.i18n import _

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

TIME_ROLLBACK_TOLERANCE_MS = 10_000  # 允许的时间回拨容忍度，单位毫秒


class WorkerLeaseExpired(Exception):
    """Worker ID 租约已超出安全期，继续发号可能产生重复雪花ID，因此拒绝发号。

    **故意不继承 `RaceCondition`**：`SystemCaller` 只重试 `RaceCondition`，而围栏跳闸后
    重试多少次都还是跳闸，只会空转到 max_retry 才失败。这个异常应当直接向上冒泡，让本次
    System 调用明确失败。

    Raised instead of minting a snowflake id whose worker id may no longer be exclusively
    ours. Deliberately not a RaceCondition: retrying can't clear the fence.
    """


@final
class SnowflakeID(metaclass=Singleton):
    """
    标准的雪花算法 (Snowflake) 实现，针对单线程多进程环境。

    结构 (64位):
    1位符号 | 41位时间戳 (毫秒) | 0位数据中心ID | 10位Worker ID | 12位序列号

    可支持TW_EPOCH开始69年，1024个进程，每毫秒4096个ID

    雪花ID必须保证服务器ntp同步准确，不会发生10秒以上的时钟回拨。
    类会记录最后的时间，如果发生回拨，会sleep等待，时间过长会卡死服务器。

    重启回拨问题
    ---------
    重启时如果服务器有时间回拨会导致ID重复。
    - 为尽可能减少这个情况，需要持久化最后的时间戳(精度要<=容忍度秒)，重启后再等待容忍度秒，来防止此情况。
    - 使用方要尽可能保证worker id不变，或者用别的worker id时，也用别人的最后时间戳。

    以上2点已通过WorkerKeeper类实现。
    """

    def __init__(self):
        self.worker_id = -1
        self.datacenter_id = -1
        self.sequence = 0
        self.last_timestamp = -1
        self.lease: WorkerKeeper | None = None

    def init(
        self,
        worker_id: int,
        last_timestamp: int = -1,
        lease: WorkerKeeper | None = None,
    ):
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
        lease: WorkerKeeper | None
            分配了 worker_id 的 keeper，用作发号围栏（见 `_next_id`）。传 None 或传一个
            不提供租约的 keeper（如开发模式的 FixedWorkerKeeper），围栏就不启用。
        """
        if worker_id > MAX_WORKER_ID or worker_id < 0:
            raise ValueError(
                _("Worker ID不能大于{max}或小于0").format(max=MAX_WORKER_ID)
            )

        if last_timestamp < 0:
            last_timestamp = (
                int(time() * 1000) + TIME_ROLLBACK_TOLERANCE_MS
            )  # 默认加10秒，防止重启回拨

        self.worker_id = worker_id
        self.sequence = 0
        self.last_timestamp = last_timestamp
        # 每次init都重置围栏来源，避免单件在测试/嵌入场景里残留上一次的租约
        self.lease = lease

        logger.info(
            _(
                "[❄️ID] 雪花ID生成器初始化完成，Worker ID: {worker_id}, last_timestamp: {ts}"
            ).format(
                worker_id=worker_id,
                ts=f"{datetime.fromtimestamp(last_timestamp / 1000):%Y-%m-%d %H:%M:%S}",
            )
        )

    def _check_lease_fence(self) -> None:
        """发号围栏：租约超出安全期就拒绝发号，而不是继续发可能重复的ID。

        为什么需要它：WorkerKeeper 的续约检测是**事后**的。worker 长时间卡住（事件循环
        被堵死、GC、网络分区）导致租约过期、被别的 worker 抢走之后，它苏醒过来到下一次
        keep_alive 跑到之间还有最长一个续约周期，这期间它会拿着已经不属于自己的 worker_id
        继续发号，产出和抢占方**字节相同**的雪花ID（sequence 每毫秒重置为0，两边各发第一个
        就撞）。雪花ID是所有Component的主键，后果是 insert 撞主键、以及按id去重的
        FutureCalls 被静默吞掉。

        围栏把这个窗口关掉：只在"最近一次续约成功后的安全期内"才肯发号。安全期由
        `WorkerKeeper.lease_deadline` 给出，算法见那里。keeper 不提供租约（开发模式）时
        `lease_deadline` 恒为 None，围栏不启用。

        这不引入新的故障模式：续约失败意味着后端master写不进去，而每一次 insert/update
        都要写master——那时服务器本来就已经干不了活了。围栏只是把"静默产生重复主键"换成
        "明确报错"。
        """
        lease = self.lease
        if lease is None:
            return
        deadline = lease.lease_deadline
        if deadline is None or monotonic() <= deadline:
            return
        raise WorkerLeaseExpired(
            _(
                "[❄️ID] Worker ID {worker_id} 的租约已超出安全期 {overdue:.1f} 秒，"
                "拒绝发号以防产生重复ID。通常意味着本进程长时间卡住或后端连不上，"
                "Worker 会在下一次续约时重启"
            ).format(worker_id=self.worker_id, overdue=monotonic() - deadline)
        )

    def _next_id(self) -> int | None:
        """
        生成下一个 ID，超标时返回 None。租约超出安全期时抛 `WorkerLeaseExpired`。
        """
        worker_id = self.worker_id
        assert worker_id >= 0, _("SnowflakeID 未初始化，请先调用 init() 方法。")
        self._check_lease_fence()

        timestamp = int(time() * 1000)
        last_timestamp = self.last_timestamp

        # 如果时钟回拨，使用最后的时间
        if timestamp < last_timestamp:
            # 警告：时钟回拨发生。
            logger.warning(
                _("[❄️ID] 时钟回拨了 {ms} 毫秒。").format(ms=last_timestamp - timestamp)
            )
            # 策略：假装时间没有倒流，继续使用 last_timestamp
            # 这会导致我们在"过去"的时间里消耗序列号，直到系统时间追上来
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
            logger.debug(_("[❄️ID] 每毫秒只能生成有限的ID，需要休眠 1 ms"))
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
            logger.debug(_("[❄️ID] 每毫秒只能生成有限的ID，需要休眠 1 ms"))
            # 等待到下一毫秒
            await asyncio.sleep(0.001)
            new_id = self._next_id()
        return new_id


class WorkerKeeper:
    subclasses: list[type[WorkerKeeper]] = []

    def __init_subclass__(cls, **_):
        """让继承子类自动注册alias"""
        super().__init_subclass__()
        cls.subclasses.append(cls)

    def __init__(self):
        # 租约安全期的截止时刻（time.monotonic() 秒）。在这之前可以确信没人能抢走本进程的
        # worker_id，因此发号是安全的；超过它 `SnowflakeID` 就拒绝发号（见 _check_lease_fence）。
        #
        # None 表示"本分配器不提供租约"，围栏随之停用——开发模式的 FixedWorkerKeeper 用
        # 本机进程序号，根本不存在被抢的概念，不需要也无法武装围栏。
        #
        # 算法（由提供租约的子类维护）：
        #     lease_deadline = 发起续约前的 monotonic 时刻 + TTL - 余量
        # 必须用**发起前**而不是完成后的时刻：请求在网络上飞的这段时间，数据库那边的
        # 过期时间已经开始走了，用完成时刻算会高估自己的安全期。余量再覆盖两边时钟漂移。
        self.lease_deadline: float | None = None

    async def get_worker_id(self) -> int:
        raise NotImplementedError

    async def release_worker_id(self):
        """
        释放当前占用的 Worker ID。
        """
        raise NotImplementedError

    async def keep_alive(self) -> None:
        """续租 Worker ID。

        注意：雪花ID的时间戳高水位（防重启回拨）**不在**这里，它由
        `hetu.data.backend.snowflake_timestamp.SnowflakeTimestampKeeper` 独立负责。
        两者的并发语义相反——租约要互斥、水位只要单调max——捆在一起会让一个零协调需求
        背上强协调需求的复杂度，详见那个类的文档。
        """
        raise NotImplementedError
