"""
雪花ID发号租约的完整生命周期，服务器 worker 与 hetu call / shell 共用。
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Self

from redis.exceptions import ConnectionError as RedisConnectionError

from ...common.snowflake_id import SnowflakeID, WorkerKeeper
from ...i18n import _
from .snowflake_timestamp import TIMESTAMP_SAVE_INTERVAL, SnowflakeTimestampKeeper

if TYPE_CHECKING:
    from .table import Table

logger = logging.getLogger("HeTu.root")

# 续约间隔（秒）。租约 TTL 60 秒、围栏余量 15 秒，5 秒一次留足了重试的余地
RENEW_INTERVAL = 5


class SnowflakeLease:
    """
    一个进程的雪花发号租约：worker id + 时间戳高水位 + 续约 / 预留水位两个后台循环。

    完整流程（缺一步都可能发出重复ID，见各步说明）：

    1. `acquire`：租 worker id → 读时间戳高水位 → `SnowflakeID.init(lease=keeper)`（武装发号
       围栏）→ 先预留一段水位再发号；
    2. 运行期：`renew_forever` 每 5 秒续约，`reserve_forever` 每 TIMESTAMP_SAVE_INTERVAL
       秒预留水位。两个循环刻意分开，见 `SnowflakeTimestampKeeper` 的文档；
    3. `release`：先写精确水位（之后不再发号），再释放 worker id。

    只租 id、不接水位的进程正常退出并释放租约后，下一个拿到同一 id 的进程读到的是旧水位，
    会落在它用过的毫秒上；跨机器还有时钟差。所以水位这一步不能省。

    服务器在 `start_backends` / `close_backends` 与两个 `add_task` 循环里分步调用；工具进程
    用 ``async with`` 一次拿全（`__aenter__` 起两个循环任务，`__aexit__` 停掉并释放）。

    Lifecycle of a process's snowflake lease (worker id + timestamp watermark + the two
    background loops). Shared by server workers and the hetu call / shell tool commands.
    """

    def __init__(self, keeper: WorkerKeeper, lease_tbl: Table) -> None:
        """
        keeper: worker id 分配器（见 `create_worker_keeper`）。
        lease_tbl: 存时间戳高水位的 `WorkerLease` 表。必须和服务器用同一张：服务器固定用
            ``INSTANCES[0]`` 的那张，否则别的进程拿到同一 id 时读不到这里写的水位。
        """
        self.keeper = keeper
        self.lease_tbl = lease_tbl
        self.worker_id = -1
        self.ts_keeper: SnowflakeTimestampKeeper | None = None
        self._tasks: list[asyncio.Task] = []
        self.on_lost: Callable[[], None] | None = None
        """工具进程用：``async with`` 起的续约循环发现租约丢失时调用"""

    async def acquire(self, *, wait: bool) -> int:
        """
        租 worker id、接上水位、初始化发号器，返回 worker id。

        wait: 所有 id 都被占（KeyError）时，True 每秒重试直到拿到（服务器：反复宕机会把 id
            占满，要等它们过期）；False 直接抛出（工具进程）。
        """
        keeper = self.keeper
        while True:
            try:
                worker_id = await keeper.get_worker_id()
                break
            except KeyError as e:
                if not wait:
                    raise
                logger.exception(e)
                logger.info(
                    _(
                        "⌚ [📡Server] Worker ID分配失败，可能是反复宕机导致Worker ID分配满了，"
                        "等待1秒后重试..."
                    )
                )
                await asyncio.sleep(1)
        self.worker_id = worker_id

        # 时间戳高水位（防重启期间时钟回拨）和租约是两码事，独立取：租约要互斥、水位只要单调
        # max，捆在一起会让零协调的需求背上强协调的复杂度。详见 SnowflakeTimestampKeeper。
        self.ts_keeper = ts_keeper = SnowflakeTimestampKeeper(self.lease_tbl, worker_id)
        last_timestamp = await ts_keeper.load()

        # 初始化雪花id生成器。传入keeper作为发号围栏：租约超出安全期就拒绝发号，防止本进程
        # 卡住导致租约被抢走后还在用旧worker_id发出重复ID。见 SnowflakeID._check_lease_fence
        SnowflakeID().init(worker_id, last_timestamp, lease=keeper)
        # 发号前先预留一段水位：不然在第一次周期写入之前崩溃，这期间发出的ID没有任何记录
        try:
            await ts_keeper.reserve(SnowflakeID().last_timestamp)
        except Exception as e:  # noqa: BLE001 写不进去只是少一层重启回拨保护，不该挡住开服
            logger.warning(
                _("[❄️ID] 写入时间戳高水位失败，将重试: {err}").format(
                    err=f"{type(e).__name__}:{e}"
                )
            )
        return worker_id

    async def renew_forever(self, on_lost: Callable[[], None]) -> None:
        """每 RENEW_INTERVAL 秒续约一次，直到被取消；租约丢失时调用 ``on_lost()`` 后退出。"""
        while True:
            await asyncio.sleep(RENEW_INTERVAL)
            # sanic bug: 它windows下共享sock句柄方法不对，其他worker的task会被暂停，导致续约失败
            try:
                await self.keeper.keep_alive()
            except RedisConnectionError as e:
                logger.error(
                    _("❌ [📡WorkerKeeper] 续约失败，将重试: {err}").format(
                        err=f"{type(e).__name__}:{e}"
                    )
                )
                continue
            except SystemExit:
                on_lost()
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                # 这个循环是发号围栏的心跳来源：它一旦静默死掉，围栏会在安全期后永久跳闸，
                # 进程活着但再也发不出ID。而 app.add_task 注册的task被sanic一直持有引用，
                # 连asyncio那句"Task exception was never retrieved"都不会打印。所以这里必须
                # 兜住所有异常、打出来、继续下一轮
                logger.exception(
                    _("❌ [📡WorkerKeeper] 续约异常，将重试: {err}").format(
                        err=f"{type(e).__name__}:{e}"
                    )
                )

    async def reserve_forever(self) -> None:
        """周期性预留时间戳高水位（见 `SnowflakeTimestampKeeper.reserve`），直到被取消。

        刻意和 `renew_forever` 分成两个循环：租约续约是强协调操作（失败=正确性事故），
        水位写入是零协调操作（失败=保护力度暂时下降，无需任何动作），两者的存活互不牵连。
        """
        assert self.ts_keeper is not None, "acquire() first"
        while True:
            await asyncio.sleep(TIMESTAMP_SAVE_INTERVAL)
            try:
                await self.ts_keeper.reserve(SnowflakeID().last_timestamp)
            except asyncio.CancelledError:
                break
            except Exception as e:  # noqa: BLE001
                # 写不进去只是少一层重启回拨保护，不值得打断服务，下个周期再试
                logger.warning(
                    _("[❄️ID] 写入时间戳高水位失败，将重试: {err}").format(
                        err=f"{type(e).__name__}:{e}"
                    )
                )

    async def release(self) -> None:
        """写精确水位，再释放 worker id。调用方保证之后不再发号。"""
        # 精确水位：不会再发号了，最后用到的时间戳就是真实上界。下次拿到这个 id 的进程从这里
        # 接着发，不用背周期写入预留的那一段。失败不能挡住退出流程，下次就按最近一次的预留值
        if self.ts_keeper is not None:
            try:
                await self.ts_keeper.save(SnowflakeID().last_timestamp)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    _("[❄️ID] 关服时写入时间戳高水位失败: {err}").format(
                        err=f"{type(e).__name__}:{e}"
                    )
                )
        await self.keeper.release_worker_id()

    def _lost(self) -> None:
        if self.on_lost is not None:
            self.on_lost()

    async def __aenter__(self) -> Self:
        await self.acquire(wait=False)
        # 两个循环在调用方设置提交观察钩子（hetu.data.backend.session.commit_observer）之前
        # 创建，复制的上下文里没有它：水位写入不会被 dry-run / 写保护拦下或记进写集
        self._tasks = [
            asyncio.create_task(self.renew_forever(self._lost)),
            asyncio.create_task(self.reserve_forever()),
        ]
        return self

    async def __aexit__(self, *exc: object) -> None:
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.release()
