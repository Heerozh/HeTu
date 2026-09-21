"""
雪花ID时间戳高水位记录器
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from time import time
from typing import TYPE_CHECKING

from ...i18n import _
from .base import RowFormat

if TYPE_CHECKING:
    from .table import Table

logger = logging.getLogger("HeTu.root")

# 高水位的写入间隔（秒）。它决定了"重启回拨"最多能有多大：关服前最后一次写入到真正关服
# 之间的这段时间是保护不到的，所以这个值必须小于运维保证的最大时钟回拨量（NTP正常工作时
# 通常远小于1秒）。5秒是留了足够余量的取值。
TIMESTAMP_SAVE_INTERVAL = 5


class SnowflakeTimestampKeeper:
    """记录雪花ID用过的时间戳高水位，防止服务器重启期间时钟回拨导致ID重复。

    ## 为什么它和 WorkerKeeper 拆开

    这两件事被 `keep_alive` 捆在一起过，但它们的并发语义完全相反：

    * **worker id 分配**需要**互斥**——同一时刻只能一个进程持有，冲突的后果是重复ID，
      致命。所以它需要 CAS / 所有权校验那一整套。
    * **时间戳高水位**需要的只是**单调 max()**——它是个"只能抬高"的水位标记，`max` 天然
      幂等且可交换。两个进程同时写没有任何问题，写晚了、写丢了也只是保护力度下降。它需要
      的协调量是**零**。

    捆在一起等于让一个零协调的需求背上了强协调需求的复杂度，也让 worker id 无法改成由部署
    侧直接指定（那样就不需要租约了，但时间戳保护还是要的）。拆开后本类只依赖一个 `Table`
    和一个 worker_id，不关心这个 id 是抢来的还是配置里写死的。

    存储直接复用 `WorkerLease` 表的 `last_timestamp` 字段（它本来就以 worker_id 为主键，
    语义正好对上），省掉一张新表和它的迁移；本类只碰这一个字段，和租约的
    `node_id`/`expires_at` 互不干涉。

    ## 防的是哪一种回拨

    时钟回拨有两种，只有第二种需要持久化：

    1. **运行中回拨**（进程活着时时钟被拨回）：`SnowflakeID._next_id` 用纯内存的
       `last_timestamp` 钳住时间不倒流，继续消耗 sequence，不会产生重复ID。**不需要数据库**。
    2. **重启后回拨**（关服期间时钟被拨回）：新进程内存里的 `last_timestamp` 是空的，会拿
       回拨后的时间重新发号，撞上关服前已经用过的时间戳 → 重复ID。**这是本类唯一要解决的**，
       而它每 TIMESTAMP_SAVE_INTERVAL 秒一次的粗粒度就够了。

    ## 单写者假设

    `save` 是无条件写（last-writer-wins），不是原子的 max。正常情况下同一个 worker_id 只有
    一个写者，而单个写者写出的 `SnowflakeID.last_timestamp` 本身就是单调递增的，所以存储里
    的值也是单调的。只有在"两个进程拿着同一个 worker_id"时水位才可能被写低——而那个场景本身
    已经在产生重复ID了，是 WorkerKeeper 那边要解决的问题，不该由本类兜底。

    Persists the high-water mark of timestamps consumed by the snowflake ID generator, so
    a clock that went backwards while the server was down can't cause ID reuse. Kept
    deliberately separate from WorkerKeeper: a worker id lease needs mutual exclusion,
    whereas this watermark only needs a monotone max() and therefore needs no coordination
    at all.
    """

    def __init__(self, table: Table, worker_id: int):
        self.table = table
        self.worker_id = worker_id
        self._write_verified = False

    @staticmethod
    def _now_ms() -> int:
        return int(time() * 1000)

    async def load(self) -> int:
        """读回高水位，返回可直接传给 `SnowflakeID.init` 的起始时间戳。

        三种情况：

        * **读到了有效水位** → `max(水位 + 写入间隔, 当前时间)`。取 max 是因为水位只是个
          下界：正常情况下当前时间早就超过它了，只有真的发生重启回拨时水位才更大，那时宁可
          让ID的时间戳"超前"也不能重复。**必须加上一个写入间隔**：水位每
          TIMESTAMP_SAVE_INTERVAL 秒才写一次，崩溃时最后那一个间隔内发出去的ID其时间戳
          已经超过了记录值，不补这一段就会把它们再发一遍。
        * **确认没有记录**（行不存在，或水位为0）→ 返回当前时间，**不做任何钳制**。这个
          worker_id 名下从没发出过ID，也就没有可重复的时间戳，不需要保护。这里绝不能退化成
          "未知"去用兜底值：那会让每次全新开服都白白背上一个几秒的降级窗口——时间戳被钳在
          同一毫秒，总容量只剩4096个ID，超了就1ms一睡地空转，还每发一个ID刷一条回拨警告。
        * **读不出来（后端异常）** → 返回 -1，交给 `SnowflakeID.init` 自己的
          `TIME_ROLLBACK_TOLERANCE_MS` 兜底。注意"读到了空"和"读不出来"是两回事：前者是
          确定的信息（没发过号），后者才是真正"可能发过号但不知道发到哪"，值得付降级窗口
          的代价。这里返回当前时间会把 init 的兜底废掉。

        已知取舍：`hetu upgrade` 会 flush 掉 volatile 的 WorkerLease 表，水位丢失后也表现为
        0，从数据上无法和"首次开服"区分，此时会走不钳制的分支。选这一边是因为首次开服每次
        新部署、每次开发都会发生，而"维护期间恰好又发生时钟回拨"是罕见组合，且有运维侧
        "NTP只slew不step"兜底。
        """
        now_ms = self._now_ms()
        try:
            row = await self.table.backend.master.get(
                self.table, self.worker_id, row_format=RowFormat.STRUCT
            )
        except Exception as e:  # 开服阶段不能因为读不到水位就起不来
            logger.warning(
                _("[❄️ID] 读取时间戳高水位失败，退化为固定容忍度: {err}").format(
                    err=f"{type(e).__name__}:{e}"
                )
            )
            return -1

        # 行不存在 和 水位为0 是同一件事：确认没有记录过，不需要保护
        stored = int(row.last_timestamp) if row is not None else 0
        if stored <= 0:
            return now_ms

        watermark = stored + TIMESTAMP_SAVE_INTERVAL * 1000
        if watermark > now_ms:
            logger.warning(
                _(
                    "[❄️ID] 检测到重启期间时钟回拨了 {ms} 毫秒，"
                    "已按记录的高水位继续发号，避免ID重复"
                ).format(ms=watermark - now_ms)
            )
        return max(watermark, now_ms)

    async def save(self, last_timestamp: int) -> None:
        """把当前用到的时间戳写成高水位。无条件写，不做任何所有权校验（见类文档）。

        `direct_set` 在两种后端上行为不一致：Redis 是 `HSET`，键不存在会顺手建；SQL 是
        `UPDATE ... WHERE id=?`，行不存在就**静默无效**。以前 SQL 那边靠
        GeneralWorkerKeeper 抢租约时把行建出来，那个类已经删了，现在没有任何人替本类建行，
        所以首次写入后回读确认，缺行就自己补一次插入（只在进程内做一次）。
        """
        await self.table.direct_set(self.worker_id, last_timestamp=str(last_timestamp))
        if self._write_verified:
            return
        self._write_verified = True
        if await self._row_exists():
            return
        await self._create_row(last_timestamp)

    async def _row_exists(self) -> bool:
        row = await self.table.backend.master.get(
            self.table, self.worker_id, row_format=RowFormat.STRUCT
        )
        return row is not None

    async def _create_row(self, last_timestamp: int) -> None:
        """补建本 worker_id 的行。整个进程只会走一次，用事务无所谓开销。"""
        from .worker_keeper import WorkerLease

        try:
            async with self.table.session() as session:
                repo = session.using(WorkerLease)
                row = WorkerLease.new_row(id_=self.worker_id)
                row.last_timestamp = last_timestamp
                await repo.insert(row)
        except Exception as e:
            # 并发下别的进程可能刚好也在补建（撞主键），或后端异常；两种都不致命——
            # 最坏是这一轮水位没写上，下个周期 direct_set 就能生效了
            logger.warning(
                _("[❄️ID] 补建时间戳高水位行失败（下个周期会重试）: {err}").format(
                    err=f"{type(e).__name__}:{e}"
                )
            )
            self._write_verified = False
