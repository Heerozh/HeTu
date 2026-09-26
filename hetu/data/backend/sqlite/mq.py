"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import contextlib
import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, final, override

import msgpack

from ....i18n import _
from ..base import HubMQClient, MQClient, MQHub
from .store import SQLiteStore

if TYPE_CHECKING:
    from .client import SQLiteBackendClient

logger = logging.getLogger("HeTu.root")
PULL_BATCH_SIZE = 256
# 轮询通知表失败后的退避区间（与 Redis pubsub 节点失效后的重订阅一致）
POLL_BACKOFF_MIN = 0.5
POLL_BACKOFF_MAX = 5.0
# 订阅的频道超过这么多就不在查询里按频道过滤（IN 参数太多），改成按 id 扫描后本地过滤
MAX_CHANNELS_IN_FILTER = 500


class SQLiteNotifyHub(MQHub):
    """
    每个进程（每个 servant 客户端）一个：唯一的通知表轮询任务 + "频道 → 本进程内订阅了它的连接"
    分发表。commit 把要发的通知和数据在同一个写事务里写进通知表；SQLite 只有一个写者，id 顺序就是
    提交顺序，按 id 消费不会漏。

    与 Redis pubsub 语义对齐，只消费订阅之后产生的通知：每个频道加入本进程时记下当时发过的最后一个
    通知 id 作为水位，之前的通知不算它的。没有任何订阅时不轮询。

    取水位和轮询互斥（同一把锁），水位读回来到记下之间轮询不会把这期间的通知消费掉；同一频道先登记
    再取水位，并发的后来者看到已登记就不再取，先到者的水位说了算。

    通知只保留一段时间（见 `SQLiteBackendClient.NOTIFY_TTL_SECONDS`）。游标之后的通知已经被清理
    （进程卡住太久）时，按 Redis pubsub 断线重订的语义给仍订阅的频道补发一次（`MQHub.resync_`）。
    """

    def __init__(self, client: SQLiteBackendClient):
        super().__init__()
        self._client = client
        # 频道加入 hub 时发过的最后一个通知 id：id 不大于它的通知不属于该频道的订阅者。
        # 已登记但还没取到水位的频道不在这里，轮询遇到它的通知先跳过
        self._since: dict[str, int] = {}
        self._last_notify_id = 0
        # 取水位与轮询互斥
        self._lock = asyncio.Lock()
        # 正在取水位的频道 → 取水位的任务：后来登记同一频道的连接等它，不再自己取
        self._placing: dict[str, asyncio.Task] = {}
        self._large_sub_warned = False
        self._task: asyncio.Task | None = None

    async def _get_current_notify_id(self) -> int:
        # 查不到就让异常抛给 add() 的调用方：游标退回 0 会把整张通知表重放一遍
        return await self._client.run_(SQLiteStore.notify_tail)

    def _polling(self) -> bool:
        return self._task is not None and not self._task.done()

    async def _place_watermark(self, channels: list[str]) -> None:
        """给刚登记的频道取水位；轮询没在跑（此前本进程没人订阅）时把游标放到表尾并起轮询"""
        try:
            async with self._lock:
                watermark = await self._get_current_notify_id()
                for channel in channels:
                    if channel in self._subs:  # 等水位期间订阅者可能已经全走了
                        self._since[channel] = watermark
                if not self._polling() and not self._closed:
                    self._last_notify_id = watermark
                    self._task = asyncio.create_task(self._run())
        finally:
            for channel in channels:
                self._placing.pop(channel, None)

    @override
    async def add(self, mq: MQClient, channels: Iterable[str]) -> None:
        """登记 mq 对这些频道的订阅，返回时水位已经记好（别人正在取的也等到取完）"""
        if self._closed:
            raise ConnectionError(_("连接已关闭，已调用过close"))
        # 先同步登记再取水位：取水位的 await 期间别的连接 add 同一频道时看到的是"已有人订"，
        # 不会各自再查一次表尾、把先到者的水位抬高而漏掉中间发生的通知
        fresh: list[str] = []
        registered: list[str] = []
        waits: set[asyncio.Task] = set()
        for channel in channels:
            subs = self._subs.get(channel)
            if subs is None:
                self._subs[channel] = {mq}
                fresh.append(channel)
                registered.append(channel)
                continue
            if mq not in subs:
                subs.add(mq)
                registered.append(channel)
            placing = self._placing.get(channel)
            if placing is not None:
                waits.add(placing)  # 先到者正在取，等它
        if fresh:
            # 取水位放进独立 task：本调用方中途被取消（连接断了）也得把水位记下，
            # 搭车登记的其他连接还等着它；调用方只撤自己的登记
            task = self._spawn(self._place_watermark(fresh))
            for channel in fresh:
                self._placing[channel] = task
            waits.add(task)
        if waits:
            try:
                # asyncio.wait 只旁观：本调用方被取消不会连带取消共享的取水位任务
                done, _pending = await asyncio.wait(waits)
                for task in done:
                    task.result()
            except BaseException:
                self._release(mq, registered)
                raise
        elif not self._polling() and self._subs:
            self._task = asyncio.create_task(self._run())  # 轮询意外退出了的兜底

    @override
    def _on_channel_gone(self, channel: str) -> None:
        self._since.pop(channel, None)

    @override
    async def remove(self, mq: MQClient, channels: Iterable[str]) -> None:
        """撤销 mq 对这些频道的订阅。本进程没人订阅了轮询任务会自己退出，别空转"""
        self._release(mq, channels)

    async def _stop_task(self) -> None:
        if self._task is None:
            return
        task, self._task = self._task, None
        if task is not asyncio.current_task():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    @staticmethod
    def _should_use_channel_in_filter(subscribed_count: int) -> bool:
        return subscribed_count <= MAX_CHANNELS_IN_FILTER

    async def poll_once(self) -> tuple[int, int]:
        """
        查一批游标之后的通知并分发给订阅者。返回 (本批查到的行数, 其中命中本进程订阅的条数)。
        不按频道过滤时可能整批都是无关频道，命中为 0。
        """
        async with self._lock:
            channels = list(self._subs)
            use_channel_filter = self._should_use_channel_in_filter(len(channels))
            if not use_channel_filter and not self._large_sub_warned:
                logger.warning(
                    "⚠️ [💾SQLite] 订阅频道过多，轮询切换为按id扫描后本地过滤模式，"
                    f"当前订阅数={len(channels)}，阈值={MAX_CHANNELS_IN_FILTER}"
                )
                self._large_sub_warned = True
            cursor = self._last_notify_id
            rows, min_id, tail = await self._client.run_(
                SQLiteStore.notify_fetch,
                cursor,
                channels if use_channel_filter else None,
                PULL_BATCH_SIZE,
            )

            # 游标之后的通知已经被清理掉了（进程卡住太久）：这段时间的变更不可知，同 Redis
            # pubsub 断线重订，给已经有水位的频道补发一次
            lost = min_id > cursor + 1 if min_id is not None else tail > cursor
            if lost:
                logger.warning(
                    _(
                        "⚠️ [💾SQLite] 通知游标 {cursor} 之后的通知已被清理，"
                        "给订阅的频道补发一次重读"
                    ).format(cursor=cursor)
                )
                self._warn_dropped(
                    self.resync_([ch for ch in channels if ch in self._since])
                )

            hits = 0
            for msg_id, channel_name, payload in rows:
                self._last_notify_id = max(self._last_notify_id, msg_id)
                since = self._since.get(channel_name)
                # 没水位 = 刚登记还没取到（取水位和本轮询互斥，它取回的表尾只会 >= 本行）
                if since is None or msg_id <= since or channel_name not in self._subs:
                    continue
                hits += 1
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        _("🔔 [💾SQLite] 收到订阅更新通知: {channel_name}").format(
                            channel_name=channel_name
                        )
                    )
                ids = msgpack.unpackb(payload) if payload else None
                self._warn_dropped(self._dispatch(channel_name, ids))
            if len(rows) < PULL_BATCH_SIZE:
                # 同一个快照里，游标之后与本进程订阅有关的通知已经全部取回：游标直接推到表尾，
                # 别让它因为没有相关通知而一直落在后面（落到清理范围里会误判成丢了通知）
                self._last_notify_id = max(self._last_notify_id, tail)
        return len(rows), hits

    @staticmethod
    def _warn_dropped(dropped: int) -> None:
        if dropped:
            logger.warning(
                _(
                    "⚠️ [💾SQLite] 订阅更新通知来不及处理，"
                    "丢弃了{seconds}秒前的消息共{count}条"
                ).format(seconds=MQClient.DROP_AFTER, count=dropped)
            )

    async def _run(self) -> None:
        interval = 1 / MQClient.UPDATE_FREQUENCY
        failures = 0
        while not self._closed:
            if not self._subs:
                # 没人订阅了就退出；下次 add() 看到任务已结束会重新起（此处到 return 无 await）
                self._task = None
                return
            try:
                fetched, _hits = await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # 库文件不可用时按轮询节奏重试等于每秒几十条带栈日志，每个 worker 都这样：
                # 指数退避，栈只在首次记，之后一行一条
                failures += 1
                backoff = min(POLL_BACKOFF_MIN * 2 ** (failures - 1), POLL_BACKOFF_MAX)
                if failures == 1:
                    logger.exception(
                        _("❌ [💾SQLite] 轮询通知表失败，{backoff}s 后重试").format(
                            backoff=backoff
                        )
                    )
                else:
                    logger.error(
                        _(
                            "❌ [💾SQLite] 轮询通知表连续失败 {count} 次，{backoff}s 后重试：{err}"
                        ).format(
                            count=failures,
                            backoff=backoff,
                            err=f"{type(e).__name__}:{e}",
                        )
                    )
                await asyncio.sleep(backoff)
                continue
            if failures:
                logger.info(
                    _("✅ [💾SQLite] 轮询通知表恢复，期间失败 {count} 次").format(
                        count=failures
                    )
                )
                failures = 0
            if fetched < PULL_BATCH_SIZE:
                # 追平了（没查满一批）才歇一下；查满一批说明还有积压，立刻接着查
                await asyncio.sleep(interval / 2)

    async def close(self) -> None:
        self._closed = True
        self._subs.clear()
        self._since.clear()
        await self._cancel_tasks()
        await self._stop_task()


@final
class SQLiteMQClient(HubMQClient):
    """
    每个用户连接一个实例：只是本连接订阅集合 + 本地消息队列，
    通知表的轮询由本进程共享的 `SQLiteNotifyHub` 负责。
    """

    LOG_TAG = "💾SQLite"
