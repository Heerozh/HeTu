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
from typing import TYPE_CHECKING, final

import msgpack
import sqlalchemy as sa

from ....i18n import _
from ..base import HubMQClient, MQClient

if TYPE_CHECKING:
    from .client import SQLBackendClient

logger = logging.getLogger("HeTu.root")
PULL_BATCH_SIZE = 256
# 轮询通知表失败后的退避区间（与 Redis pubsub 节点失效后的重订阅一致）
POLL_BACKOFF_MIN = 0.5
POLL_BACKOFF_MAX = 5.0
# 避免SQLite等数据库在IN参数过多时触发参数上限/编译开销问题。
MAX_CHANNELS_IN_FILTER = 500


class SQLNotifyHub:
    """
    每个进程（每个 SQLBackendClient）一个：唯一的通知表轮询任务 + "频道 → 本进程内订阅了
    它的连接" 分发表。以前是每个连接各自轮询同一张通知表，DB 负载与在线人数成正比。

    与 Redis pubsub 语义对齐，只消费订阅之后产生的通知：每个频道加入本进程时记下当时通知表的
    最大 id 作为水位，之前的通知不算它的（游标只随命中订阅频道的行前进，可能落后于表尾，
    新频道加入时不能把旧通知重放给它）。没有任何订阅时不轮询。

    取水位和轮询互斥（同一把锁），水位读回来到记下之间轮询不会把这期间的通知消费掉；
    同一频道先登记再取水位，并发的后来者看到已登记就不再取，先到者的水位说了算。
    已知局限：max(id) 不是提交序（PostgreSQL/MariaDB 的自增 id 在事务提交前就分配），
    先分配了较小 id、后于水位提交的通知会被当成旧通知，游标本身也有同样的窗口。
    """

    def __init__(self, client: SQLBackendClient):
        self._client = client
        self._subs: dict[str, set[MQClient]] = {}
        # 频道加入 hub 时通知表的最大 id：id 不大于它的通知不属于该频道的订阅者。
        # 已登记但还没取到水位的频道不在这里，轮询遇到它的通知先跳过
        self._since: dict[str, int] = {}
        self._last_notify_id = 0
        # 取水位与轮询互斥
        self._lock = asyncio.Lock()
        # 正在取水位的频道 → 取水位的任务：后来登记同一频道的连接等它，不再自己取；
        # 任务不随调用方一起取消，close 时统一取消
        self._placing: dict[str, asyncio.Task] = {}
        self._tasks: set[asyncio.Task] = set()
        self._large_sub_warned = False
        self._task: asyncio.Task | None = None
        self._closed = False

    @property
    def channels(self) -> set[str]:
        """本进程当前订阅了的频道"""
        return set(self._subs)

    def subscriber_count(self, channel: str) -> int:
        """某频道在本进程内的订阅连接数，测试用"""
        return len(self._subs.get(channel, ()))

    async def _get_current_notify_id(self) -> int:
        # 查不到就让异常抛给 add() 的调用方：游标退回 0 会把整张通知表重放一遍
        table = self._client.notify_table()
        async with self._client.aio.connect() as conn:
            latest = (await conn.execute(sa.select(sa.func.max(table.c.id)))).scalar()
        return int(latest or 0)

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
            task = asyncio.create_task(self._place_watermark(fresh))
            task.add_done_callback(self._tasks.discard)
            self._tasks.add(task)
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

    def _release(self, mq: MQClient, channels: Iterable[str]) -> None:
        for channel in channels:
            subs = self._subs.get(channel)
            if subs is None:
                continue
            subs.discard(mq)
            if not subs:
                del self._subs[channel]
                self._since.pop(channel, None)

    async def remove(self, mq: MQClient, channels: Iterable[str]) -> None:
        """撤销 mq 对这些频道的订阅。本进程没人订阅了轮询任务会自己退出，别空转打 DB"""
        self._release(mq, channels)

    async def _stop_task(self) -> None:
        # 只在 close 时取消：在 SQL 语句执行中途取消会弄坏池里的连接
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
        回退到按 id 扫描模式时可能整批都是无关频道，命中为 0。
        """
        notify = self._client.notify_table()
        async with self._lock, self._client.aio.connect() as conn:
            channels = list(self._subs)
            use_channel_filter = self._should_use_channel_in_filter(len(channels))
            if not use_channel_filter and not self._large_sub_warned:
                logger.warning(
                    "⚠️ [💾SQL] 订阅频道过多，pull切换为按id扫描后本地过滤模式，"
                    f"当前订阅数={len(channels)}，阈值={MAX_CHANNELS_IN_FILTER}"
                )
                self._large_sub_warned = True

            stmt = sa.select(notify.c.id, notify.c.channel, notify.c.payload).where(
                notify.c.id > self._last_notify_id
            )
            if use_channel_filter:
                stmt = stmt.where(notify.c.channel.in_(channels))
            stmt = stmt.order_by(notify.c.id.asc()).limit(PULL_BATCH_SIZE)
            rows = (await conn.execute(stmt)).mappings().all()

        hits = 0
        for row in rows:
            msg_id = int(row["id"])
            if msg_id > self._last_notify_id:
                self._last_notify_id = msg_id
            channel_name = str(row["channel"])
            subs = self._subs.get(channel_name)
            since = self._since.get(channel_name)
            # 没水位 = 刚登记还没取到（取水位和本轮询互斥，它取回的 max(id) 只会 >= 本行）
            if not subs or since is None or msg_id <= since:
                continue
            hits += 1
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    _("🔔 [💾SQL] 收到订阅更新通知: {channel_name}").format(
                        channel_name=channel_name
                    )
                )
            payload = row.get("payload")
            ids = msgpack.unpackb(payload) if payload else None
            dropped = 0
            for mq in subs:
                dropped += mq.push_pulled_(channel_name, ids)
            if dropped:
                logger.warning(
                    _(
                        "⚠️ [💾SQL] 订阅更新通知来不及处理，"
                        "丢弃了{seconds}秒前的消息共{count}条"
                    ).format(seconds=MQClient.DROP_AFTER, count=dropped)
                )
        return len(rows), hits

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
                # 数据库不可用时按轮询节奏重试等于每秒几十次重连 + 几十条带栈日志，
                # 每个 worker 都这样：指数退避，栈只在首次记，之后一行一条
                failures += 1
                backoff = min(POLL_BACKOFF_MIN * 2 ** (failures - 1), POLL_BACKOFF_MAX)
                if failures == 1:
                    logger.exception(
                        _("❌ [💾SQL] 轮询通知表失败，{backoff}s 后重试").format(
                            backoff=backoff
                        )
                    )
                else:
                    logger.error(
                        _(
                            "❌ [💾SQL] 轮询通知表连续失败 {count} 次，{backoff}s 后重试：{err}"
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
                    _("✅ [💾SQL] 轮询通知表恢复，期间失败 {count} 次").format(
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
        tasks = [t for t in self._tasks if not t.done()]
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        await self._stop_task()


@final
class SQLMQClient(HubMQClient):
    """
    每个用户连接一个实例：只是本连接订阅集合 + 本地消息队列，
    通知表的轮询由本进程共享的 `SQLNotifyHub` 负责。
    """

    LOG_TAG = "💾SQL"
