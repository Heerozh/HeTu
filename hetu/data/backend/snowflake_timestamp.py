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

# 高水位的写入间隔（秒）。周期写入每次都往前预留一个间隔（见 SnowflakeTimestampKeeper），
# 所以它也是崩溃后重启最长要背的钳制窗口：窗口内时间戳钳在同一毫秒，只能发4096个ID。
# 在写master的频率和这个窗口之间折中，5秒两头都不贵。正常关服写的是精确值，不受它影响。
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

    ## 预留值与精确值

    存储里的水位有两种来源，读回时一视同仁地取 `max(水位, 当前时间)`：

    * **预留值**（`reserve`，开服发号前写一次，之后每 TIMESTAMP_SAVE_INTERVAL 秒一次）：
      `max(last_timestamp, 当前时间) + 一个写入间隔`。进程随时可能崩溃，崩溃前最后那段
      发出去的ID来不及记录，靠的就是这个"下次写入前不会超过它"的上界。
    * **精确值**（`save`，正常关服时）：之后不再发号，最后用到的时间戳就是真实上界，
      下次开服从这里接着发，不用再背预留的那一段。

    补偿只能放在写端，因为只有写的时候知道这个值是哪一种。以前放在读端，一律补一个写入
    间隔：正常关服后5秒内重启也被钳在未来，关服又把钳住的值原样写回，连续快速重启就一次
    推5秒地越推越远。

    ## 单写者假设

    `save`/`reserve` 都是无条件写（last-writer-wins），不是原子的 max。正常情况下同一个
    worker_id 只有一个写者，而它每次写入的值都不低于此前发出过的所有ID的时间戳（精确值就是
    `last_timestamp`，预留值还往前多留了一段），所以后写的覆盖先写的不会漏掉任何已发出的
    ID，关服那次从预留值落回精确值也是如此。只有在"两个进程拿着同一个 worker_id"时水位才
    可能被写低——而那个场景本身已经在产生重复ID了，是 WorkerKeeper 那边要解决的问题，不该
    由本类兜底。

    Persists the high-water mark of timestamps consumed by the snowflake ID generator, so
    a clock that went backwards while the server was down can't cause ID reuse. Kept
    deliberately separate from WorkerKeeper: a worker id lease needs mutual exclusion,
    whereas this watermark only needs a monotone max() and therefore needs no coordination
    at all.
    """

    def __init__(self, table: Table, worker_id: int):
        self.table = table
        self.worker_id = worker_id
        # 已确认本 worker_id 的行存在，之后 save 只需 direct_set（它发现行没了就重置）
        self._row_ready = False

    @staticmethod
    def _now_ms() -> int:
        return int(time() * 1000)

    async def load(self) -> int:
        """读回高水位，返回可直接传给 `SnowflakeID.init` 的起始时间戳。

        三种情况：

        * **读到了有效水位** → `max(水位, 当前时间)`。水位只是个下界：正常情况下当前时间
          早就超过它了，只有上个进程没正常关服（它预留的那一段还没过完）或者重启期间时钟被
          拨回时水位才更大，那时宁可让ID的时间戳"超前"也不能重复。这里**不再**补写入间隔，
          补偿在写端做：读端分不出水位是预留值还是精确值（见类文档）。
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
            # 按 RAW 读：旧版本在 Redis 上留下过缺 id 的残缺行（见 save），
            # 按 STRUCT 读会 KeyError；它的 last_timestamp 照样是有效的水位
            row = await self.table.backend.master.get(
                self.table, self.worker_id, row_format=RowFormat.RAW
            )
            # 行不存在 和 水位为0 是同一件事：确认没有记录过，不需要保护
            stored = int(row.get("last_timestamp") or 0) if row is not None else 0
        except Exception as e:  # 开服阶段不能因为读不到水位就起不来
            logger.warning(
                _("[❄️ID] 读取时间戳高水位失败，退化为固定容忍度: {err}").format(
                    err=f"{type(e).__name__}:{e}"
                )
            )
            return -1

        if stored <= 0:
            return now_ms

        if stored > now_ms:
            logger.warning(
                _(
                    "[❄️ID] 记录的高水位比当前时间超前 {ms} 毫秒（上次未正常关服，"
                    "或重启期间时钟回拨），已从高水位继续发号，避免ID重复"
                ).format(ms=stored - now_ms)
            )
        return max(stored, now_ms)

    async def reserve(self, last_timestamp: int) -> None:
        """往前预留一段写成水位：`max(last_timestamp, 当前时间) + 一个写入间隔`。

        开服发号前写一次，之后每 TIMESTAMP_SAVE_INTERVAL 秒写一次，所以两次写入之间发出的
        ID都不会超过它（事件循环卡住、写入晚到的那一小段除外），进程在这期间崩溃，下次开服
        从这里接着发就不会重复。取 max 是因为空闲时 `last_timestamp` 可能早就落后于当前
        时间，只按它预留，盖不住接下来发出的ID。
        """
        reserved = max(last_timestamp, self._now_ms()) + TIMESTAMP_SAVE_INTERVAL * 1000
        await self.save(reserved)

    async def save(self, last_timestamp: int) -> None:
        """把 `last_timestamp` 原样写成水位（精确值），用于正常关服：调用方保证之后不会再
        发出时间戳更大的ID，周期写入要用 `reserve`。无条件写，不做任何所有权校验（见类文档）。

        `direct_set` 只改已存在的行，缺行时什么都不写、返回 False，所以行得先有人建。以前靠
        GeneralWorkerKeeper 抢租约时把行建出来，那个类已经删了，现在由本类自己补建：首次写入前
        确认一次行在不在；之后 direct_set 返回 False（运行中行被删了，比如开着服跑了
        `hetu upgrade`，它会清空易失表）也重新补建，不然水位从此静默地写不进去。

        旧版本的 direct_set 是 `HSET`，缺行时建出过只有 `last_timestamp`、缺 `id` 的残缺行，
        已部署的库里可能还留着。它有 `last_timestamp` 字段，direct_set 照样能写（load 也按
        RAW 读它）。
        """
        if not self._row_ready:
            if not await self._row_exists():
                await self._create_row(last_timestamp)
                return
            self._row_ready = True
        if not await self.table.direct_set(
            self.worker_id, last_timestamp=str(last_timestamp)
        ):
            self._row_ready = False
            await self._create_row(last_timestamp)

    async def _row_exists(self) -> bool:
        # 按 RAW 读：旧版本留下的残缺行（缺 id）按 STRUCT 读会 KeyError。它照样能存水位
        # （load 也按 RAW 读），算作已存在，接着 direct_set 就行
        row = await self.table.backend.master.get(
            self.table, self.worker_id, row_format=RowFormat.RAW
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
            # 最坏是这一轮水位没写上，下个周期重新确认：行已被别人建好就直接 direct_set
            logger.warning(
                _("[❄️ID] 补建时间戳高水位行失败（下个周期会重试）: {err}").format(
                    err=f"{type(e).__name__}:{e}"
                )
            )
            return
        self._row_ready = True
