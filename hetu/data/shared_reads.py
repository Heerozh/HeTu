"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import time
import weakref
from collections import Counter
from collections.abc import Awaitable, Hashable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, cast

from hetu.data.backend import MQClient, RowFormat

if TYPE_CHECKING:
    from hetu.data.backend import Backend, TableReference


class _Read:
    """一次读（可能被共享）：发出时刻、跑读的 task；批量读行时 index 是这一行在批里的位置"""

    __slots__ = ("index", "issued", "task")

    def __init__(self, issued: float, task: asyncio.Future, index: int | None = None):
        self.issued = issued
        self.task = task
        self.index = index

    async def result(self) -> Any:
        """等这次读完。只旁观：等待方被取消不会取消这次读，别人还在等它"""
        value = await asyncio.shield(self.task)
        return value if self.index is None else value[self.index]


class SharedReads:
    """
    订阅读合并：同一 worker 内（同一个 `Backend`），订阅因通知而发的读按 key 共用——行按
    (表, row_id)，范围按 (表, 查询参数)。

    判据（设计稿 docs/superpowers/specs/2026-09-28-sub-shared-reads-design.md §3）：一次读的
    发出时刻 r 不早于覆盖时刻 c + interval，就能给覆盖时刻为 c 的订阅用。尾随重读的保证正是
    "通知之后至少隔一个 interval 发出的读读得到它"，订阅自己去读也不过是在 c + interval 之后
    发一次，所以共享不引入新的假设。覆盖时刻见 `MQClient`。

    只给订阅用，事务读不走这里。共享出去的行、id 列表只读，调用方要改就先拷贝。每个 key 只留
    最近一次读（更晚的读满足的覆盖时刻只多不少）；发出超过 `HORIZON_INTERVALS` 个 interval
    的条目惰性清掉，只为控内存，清掉无非多读一次。

    Coalesces the reads that subscriptions issue after notifications within one worker: a
    read sent at r serves any subscription whose cover time c satisfies r >= c + interval.
    Results are shared and must be treated as read-only.
    """

    # 发出超过这么多个 interval 的条目清掉
    HORIZON_INTERVALS = 10

    _registry: ClassVar[weakref.WeakKeyDictionary[Any, SharedReads]] = (
        weakref.WeakKeyDictionary()
    )

    @classmethod
    def of(cls, backend: Backend) -> SharedReads:
        """该 backend 的共享读层，同一个 backend 只有一个。不能弱引用的替身（测试）各用各的"""
        try:
            reads = cls._registry.get(backend)
        except TypeError:
            return cls(backend)
        if reads is None:
            reads = cls._registry[backend] = cls(backend)
        return reads

    def __init__(self, backend: Backend, share: bool = True):
        """share=False 时每次都自己读、不登记（测试、压测对比用）"""
        self._backend = backend
        self._share = share
        self._entries: dict[Hashable, _Read] = {}
        # 在跑的读：保存引用免得被 gc（发起方被取消、条目被更晚的读替换后，可能还有人等它）
        self._inflight: set[asyncio.Future] = set()
        self._loop: object | None = None
        self._next_sweep = 0.0
        # rows_issued / rows_shared / ranges_issued / ranges_shared / fallbacks
        self.stats: Counter[str] = Counter()

    async def rows(
        self, table_ref: TableReference, wants: Sequence[tuple[int, float]]
    ) -> list[tuple[dict[str, Any] | None, float]]:
        """
        按 row_id 读原始行（`TYPED_DICT`，含 `_version`），wants 为 [(row_id, 覆盖时刻), ...]。
        返回同序的 [(行，不存在为 None, 这份结果那次读的发出时刻), ...]。能共用的等别人的读
        （在途的也等），其余的并成一次 get_many。
        """
        results: list[tuple[dict[str, Any] | None, float] | None] = [None] * len(wants)
        riding: list[tuple[int, _Read]] = []
        missing: list[int] = []
        if self._share:
            self._maintain()
            interval = 1 / MQClient.UPDATE_FREQUENCY
            entries = self._entries
            for pos, (row_id, cover) in enumerate(wants):
                entry = entries.get((table_ref, row_id))
                if entry is not None and entry.issued >= cover + interval:
                    riding.append((pos, entry))
                else:
                    missing.append(pos)
        else:
            missing = list(range(len(wants)))

        own: _Read | None = None
        if missing:
            ids = [wants[pos][0] for pos in missing]
            keys = [(table_ref, row_id) for row_id in ids] if self._share else []
            own = self._issue(
                self._backend.servant.get_many(table_ref, ids, RowFormat.TYPED_DICT),
                keys,
            )
            self.stats["rows_issued"] += len(ids)

        failed: list[int] = []
        for pos, entry in riding:
            try:
                results[pos] = (await entry.result(), entry.issued)
            except Exception:  # noqa: BLE001 别人发的读失败了，下面自己读
                failed.append(pos)
        self.stats["rows_shared"] += len(riding) - len(failed)

        if own is not None:
            rows = await asyncio.shield(own.task)  # 自己发的读失败就照抛，同今天
            for i, pos in enumerate(missing):
                results[pos] = (rows[i], own.issued)
        if failed:
            # 搭车的读失败了：各自读一次（不登记）。否则一次偶发的读错误会让共享它的连接一起
            # 断开，今天只断发起的那一个
            self.stats["fallbacks"] += len(failed)
            issued = time.monotonic()
            rows = await self._backend.servant.get_many(
                table_ref, [wants[pos][0] for pos in failed], RowFormat.TYPED_DICT
            )
            for pos, row in zip(failed, rows):
                results[pos] = (cast(dict[str, Any] | None, row), issued)
        return cast(list[tuple[dict[str, Any] | None, float]], results)

    async def range_ids(
        self, table_ref: TableReference, query: Mapping[str, Any], cover: float
    ) -> tuple[list[int], float]:
        """
        按查询参数（index_name / left / right / limit / desc，同 `BackendClient.range`）读
        id 列表（`ID_LIST`）。返回 (id 列表, 这份结果那次读的发出时刻)，列表是共享的，别改。
        """
        key: Hashable | None = None
        if self._share:
            self._maintain()
            key = (
                "range",
                table_ref,
                query["index_name"],
                query["left"],
                query["right"],
                query["limit"],
                query["desc"],
            )
            try:
                entry = self._entries.get(key)
            except TypeError:  # 边界值不能 hash：不共享
                key = entry = None
            if (
                entry is not None
                and entry.issued >= cover + 1 / MQClient.UPDATE_FREQUENCY
            ):
                try:
                    ids = await entry.result()
                except Exception:  # noqa: BLE001 别人发的读失败了，下面自己读（不登记）
                    self.stats["fallbacks"] += 1
                    key = None
                else:
                    self.stats["ranges_shared"] += 1
                    return ids, entry.issued
        own = self._issue(
            self._backend.servant.range(
                table_ref, **query, row_format=RowFormat.ID_LIST
            ),
            [key] if key is not None else [],
            indexed=False,
        )
        self.stats["ranges_issued"] += 1
        return await asyncio.shield(own.task), own.issued

    def _issue(
        self, read: Awaitable[Any], keys: Sequence[Hashable], indexed: bool = True
    ) -> _Read:
        """发一次读并登记到这些 key 下（indexed 时第 i 个 key 取结果的第 i 项）"""
        issued = time.monotonic()  # 取在真正发出之前，只会偏早，偏保守
        task = asyncio.ensure_future(read)
        self._inflight.add(task)
        registered: list[tuple[Hashable, _Read]] = []
        for index, key in enumerate(keys):
            entry = _Read(issued, task, index if indexed else None)
            self._entries[key] = entry
            registered.append((key, entry))

        def done(finished: asyncio.Future) -> None:
            self._inflight.discard(finished)
            # 取一下异常，免得没人等时报 "exception was never retrieved"；失败的读不留
            if finished.cancelled() or finished.exception() is not None:
                for key, entry in registered:
                    if self._entries.get(key) is entry:
                        del self._entries[key]

        task.add_done_callback(done)
        return _Read(issued, task)

    def _maintain(self) -> None:
        """换了事件循环就清空（future 不能跨 loop，测试按模块换 loop）；定期清掉太老的条目"""
        loop = asyncio.get_running_loop()
        if loop is not self._loop:
            self._loop = loop
            self._entries.clear()
            self._inflight.clear()
        now = time.monotonic()
        if now < self._next_sweep:
            return
        horizon = self.HORIZON_INTERVALS / MQClient.UPDATE_FREQUENCY
        self._next_sweep = now + horizon
        cutoff = now - horizon
        stale = [
            key
            for key, entry in self._entries.items()
            if entry.issued < cutoff and entry.task.done()
        ]
        for key in stale:
            del self._entries[key]
