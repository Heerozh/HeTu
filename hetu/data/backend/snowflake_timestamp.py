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

        返回值有两种：

        * 读到了 → `max(水位 + 写入间隔, 当前时间)`。取 max 是因为水位只是个下界：正常情况
          下当前时间早就超过它了，只有真的发生重启回拨时水位才更大，那时宁可让ID的时间戳
          "超前"也不能重复。**必须加上一个写入间隔**：水位每
          TIMESTAMP_SAVE_INTERVAL 秒才写一次，崩溃时最后那一个间隔内发出去的ID其时间戳
          已经超过了记录值，不补这一段就会把它们再发一遍。
        * 读不到（首次开服 / 表被清过 / 后端暂时异常）→ 返回 -1，交给
          `SnowflakeID.init` 自己的 `TIME_ROLLBACK_TOLERANCE_MS` 兜底（它会用
          当前时间+10秒）。注意不能返回当前时间——那等于把 init 的兜底废掉，退化成完全
          没有回拨保护。
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
        if row is None:
            return -1

        watermark = int(row.last_timestamp) + TIMESTAMP_SAVE_INTERVAL * 1000
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

        ⚠️ 行必须已经存在：`direct_set` 在两种后端上行为不一致——Redis 是 `HSET`，键不存在
        会顺手建；SQL 是 `UPDATE ... WHERE id=?`，行不存在就**静默无效**。目前这行总是由
        `WorkerKeeper.get_worker_id()` 抢租约时先建出来，所以实际跑起来没问题。但这是本类
        对租约仅剩的一点隐含依赖，将来 worker_id 改成部署侧直接指定、不再有租约行时必须
        处理（要么让 SQL 的 direct_set 变成 upsert，要么开服时补一次插入）。
        为了不让它悄无声息地失效，首次写入后会回读确认一次。
        """
        await self.table.direct_set(self.worker_id, last_timestamp=str(last_timestamp))
        if self._write_verified:
            return
        self._write_verified = True
        row = await self.table.backend.master.get(
            self.table, self.worker_id, row_format=RowFormat.STRUCT
        )
        if row is None:
            logger.error(
                _(
                    "[❄️ID] 时间戳高水位写入未生效（worker_id={worker_id} 的行不存在），"
                    "重启期间发生时钟回拨将无法防止ID重复"
                ).format(worker_id=self.worker_id)
            )
