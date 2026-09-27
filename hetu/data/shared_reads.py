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
from collections.abc import Hashable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from hetu.data.backend import MQClient, RowFormat

if TYPE_CHECKING:
    from hetu.data.backend import Backend, BackendClient, TableReference


class _Abandoned(Exception):
    """发起共享读的连接被取消了，读没有结果：搭车的各自重读"""


class _Batch:
    """
    一次读（可能被共享）：发出时刻与结果。搭车的来了才建 future，没人搭车时（大多数订阅）
    和直接读一样，不多建 future、不多一层协程
    """

    __slots__ = ("done", "error", "future", "issued", "value")

    def __init__(self, issued: float):
        self.issued = issued
        self.done = False
        self.value: Any = None
        self.error: Exception | None = None
        self.future: asyncio.Future | None = None

    def finish(self, value: Any = None, error: Exception | None = None) -> None:
        """发起方读完（或失败）：记下结果，叫醒搭车的"""
        self.done = True
        self.value = value
        self.error = error
        future = self.future
        if future is not None and not future.done():
            if error is None:
                future.set_result(None)
            else:
                future.set_exception(error)
                future.exception()  # 搭车的都被取消了时，别在 gc 时报 never retrieved

    async def wait(self) -> Any:
        """搭车：等发起方读完。只旁观，搭车的被取消不会取消这次读。读失败则抛出"""
        if not self.done:
            if self.future is None:
                self.future = asyncio.get_running_loop().create_future()
            await asyncio.shield(self.future)
        if self.error is not None:
            raise self.error
        return self.value


class SharedReads:
    """
    订阅读合并：同一 worker 内（同一个 `Backend`），订阅因通知而发的读按 key 共用——行按
    (表, row_id)，范围按 (表, 查询参数)。

    判据（设计稿 docs/superpowers/specs/2026-09-28-sub-shared-reads-design.md §3）：一次读的
    发出时刻 r 不早于覆盖时刻 c + interval，就能给覆盖时刻为 c 的订阅用。尾随重读的保证正是
    "通知之后至少隔一个 interval 发出的读读得到它"，订阅自己去读也不过是在 c + interval 之后
    发一次，所以共享不引入新的假设。覆盖时刻见 `MQClient`。

    读由发起的连接自己等，用它自己的 servant（与以前各读各的时一样）；搭车的来了才建 future。
    发起方的读失败或被取消（连接断了），搭车的各自重读，不跟着一起断开。

    只给订阅用，事务读不走这里。共享出去的行、id 列表只读，调用方要改就先拷贝。每个 key 只留
    最近一次读（更晚的读满足的覆盖时刻只多不少）；发出超过 `HORIZON_INTERVALS` 个 interval
    的条目惰性清掉，只为控内存，清掉无非多读一次。

    同一个连接一个 tick 里几个订阅读同一行也靠它合并：tick 开头批量预读登记，各订阅再用
    `peek_row` 同步取到。给连接一个自己的实例，就只在本连接内合并（测试、压测对比用）。

    Coalesces the reads that subscriptions issue after notifications within one worker: a
    read sent at r serves any subscription whose cover time c satisfies r >= c + interval.
    Results are shared and must be treated as read-only.
    """

    # 发出超过这么多个 interval 的条目清掉
    HORIZON_INTERVALS = 10

    # 值不引用 backend，登记表不会反过来钉住它
    _registry: ClassVar[weakref.WeakKeyDictionary[Any, SharedReads]] = (
        weakref.WeakKeyDictionary()
    )

    @classmethod
    def of(cls, backend: Backend) -> SharedReads:
        """该 backend 的共享读层，同一个 backend 只有一个。不能弱引用的替身（测试）各用各的"""
        try:
            reads = cls._registry.get(backend)
        except TypeError:
            return cls()
        if reads is None:
            reads = cls._registry[backend] = cls()
        return reads

    def __init__(self) -> None:
        # 行：表 → {row_id: (那次读, 在结果里的位置)}；按表分两层，一批行只 hash 一次表
        self._rows: dict[TableReference, dict[int, tuple[_Batch, int]]] = {}
        # 范围：(表, 查询参数) → 那次读
        self._ranges: dict[Hashable, _Batch] = {}
        self._loop: object | None = None
        self._next_sweep = 0.0
        # rows_issued / rows_shared / ranges_issued / ranges_shared / fallbacks
        self.stats: Counter[str] = Counter()

    def peek_row(
        self, table_ref: TableReference, row_id: int, cover: float
    ) -> tuple[dict[str, Any] | None, float] | None:
        """
        同步查表：这一行有已经读完、满足判据的共享读，就直接返回 (行, 发出时刻)；在途的、
        不满足判据的、没有的返回 None，调用方再走 `rows`。订阅的热路径：tick 开头批量预读
        登记过的行，各订阅来取时不用再 await。读完的结果与事件循环无关，不用查换没换 loop
        """
        table = self._rows.get(table_ref)
        hit = None if table is None else table.get(row_id)
        if hit is None:
            return None
        batch, index = hit
        if (
            not batch.done
            or batch.error is not None
            or batch.issued < cover + 1 / MQClient.UPDATE_FREQUENCY
        ):
            return None
        return batch.value[index], batch.issued

    async def rows(
        self,
        servant: BackendClient,
        table_ref: TableReference,
        wants: Sequence[tuple[int, float]],
    ) -> list[tuple[dict[str, Any] | None, float]]:
        """
        按 row_id 读原始行（`TYPED_DICT`，含 `_version`），wants 为 [(row_id, 覆盖时刻), ...]。
        返回同序的 [(行，不存在为 None, 这份结果那次读的发出时刻), ...]。能共用的等别人的读
        （在途的也等），其余的用 servant 并成一次 get_many。
        """
        self._maintain()
        interval = 1 / MQClient.UPDATE_FREQUENCY
        table = self._rows.get(table_ref)
        if table is None:
            table = self._rows[table_ref] = {}
        # 热路径上不用 cast：`dict[str, Any] | None` 这类写法运行时每次都要现造类型对象
        results: list[Any] = [None] * len(wants)
        riding: list[tuple[int, _Batch, int]] = []
        missing: list[int] = []
        for pos, (row_id, cover) in enumerate(wants):
            hit = table.get(row_id)
            if hit is not None and hit[0].issued >= cover + interval:
                riding.append((pos, *hit))
            else:
                missing.append(pos)

        if missing:
            ids = [wants[pos][0] for pos in missing]
            self.stats["rows_issued"] += len(ids)
            batch = _Batch(time.monotonic())  # 取在真正发出之前，只会偏早，偏保守
            for index, row_id in enumerate(ids):
                table[row_id] = (batch, index)
            try:
                rows = await servant.get_many(table_ref, ids, RowFormat.TYPED_DICT)
            except BaseException as e:
                for row_id in ids:  # 失败的读不留
                    if table.get(row_id, (None,))[0] is batch:
                        del table[row_id]
                # 搭车的会以为是自己被取消了，不能把 CancelledError 交给它们
                batch.finish(error=e if isinstance(e, Exception) else _Abandoned())
                raise  # 自己发的读失败就照抛，同以前
            batch.finish(rows)
            issued = batch.issued
            for index, pos in enumerate(missing):
                results[pos] = (rows[index], issued)
        if not riding:
            return results

        failed: list[int] = []
        for pos, batch, index in riding:
            try:
                results[pos] = ((await batch.wait())[index], batch.issued)
            except Exception:  # noqa: BLE001 别人发的读失败了，下面自己读
                failed.append(pos)
        self.stats["rows_shared"] += len(riding) - len(failed)
        if failed:
            # 搭车的读失败了：各自读一次（不登记）。否则一次偶发的读错误会让共享它的连接一起
            # 断开，以前只断发起的那一个
            self.stats["fallbacks"] += len(failed)
            issued = time.monotonic()
            rows = await servant.get_many(
                table_ref, [wants[pos][0] for pos in failed], RowFormat.TYPED_DICT
            )
            for pos, row in zip(failed, rows):
                results[pos] = (row, issued)
        return results

    async def range_ids(
        self,
        servant: BackendClient,
        table_ref: TableReference,
        query: Mapping[str, Any],
        cover: float,
    ) -> tuple[list[int], float]:
        """
        按查询参数（index_name / left / right / limit / desc，同 `BackendClient.range`）读
        id 列表（`ID_LIST`）。返回 (id 列表, 这份结果那次读的发出时刻)，列表是共享的，别改。
        """
        self._maintain()
        key: Hashable | None = (
            table_ref,
            query["index_name"],
            query["left"],
            query["right"],
            query["limit"],
            query["desc"],
        )
        try:
            batch = self._ranges.get(key)
        except TypeError:  # 边界值不能 hash：不共享
            key = batch = None
        if batch is not None and batch.issued >= cover + 1 / MQClient.UPDATE_FREQUENCY:
            try:
                ids = await batch.wait()
            except Exception:  # noqa: BLE001 别人发的读失败了，下面自己读（不登记）
                self.stats["fallbacks"] += 1
                key = None
            else:
                self.stats["ranges_shared"] += 1
                return ids, batch.issued
        self.stats["ranges_issued"] += 1
        batch = _Batch(time.monotonic())
        if key is not None:
            self._ranges[key] = batch
        try:
            ids = await servant.range(table_ref, **query, row_format=RowFormat.ID_LIST)
        except BaseException as e:
            if key is not None and self._ranges.get(key) is batch:
                del self._ranges[key]
            batch.finish(error=e if isinstance(e, Exception) else _Abandoned())
            raise
        batch.finish(ids)
        return ids, batch.issued

    def _maintain(self) -> None:
        """换了事件循环就清空（在途的读不跨 loop，测试按模块换 loop）；定期清掉太老的条目"""
        loop = asyncio.get_running_loop()
        if loop is not self._loop:
            self._loop = loop
            self._rows.clear()
            self._ranges.clear()
        now = time.monotonic()
        if now < self._next_sweep:
            return
        horizon = self.HORIZON_INTERVALS / MQClient.UPDATE_FREQUENCY
        self._next_sweep = now + horizon
        cutoff = now - horizon
        for table_ref in list(self._rows):
            table = self._rows[table_ref]
            stale = [
                row_id
                for row_id, (batch, _index) in table.items()
                if batch.done and batch.issued < cutoff
            ]
            for row_id in stale:
                del table[row_id]
            if not table:
                del self._rows[table_ref]
        stale_ranges = [
            key
            for key, batch in self._ranges.items()
            if batch.done and batch.issued < cutoff
        ]
        for key in stale_ranges:
            del self._ranges[key]
