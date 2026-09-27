"""
订阅读合并的共享读层（`hetu/data/shared_reads.py`，设计稿 2026-09-28 §4.3、§4.4）：
发出时刻 >= 覆盖时刻 + interval 的读才共用；在途的也共用；发起方被取消不影响搭车的；
读失败时发起方照抛、搭车的各自重读；订阅用到的读只进不退。不连数据库。
"""

import asyncio
import time
from types import SimpleNamespace

import pytest

from hetu.data.backend import RowFormat
from hetu.data.backend.base import MQClient
from hetu.data.shared_reads import SharedReads
from hetu.data.sub import RowSubscription

INTERVAL = 1 / MQClient.UPDATE_FREQUENCY
REF = "tbl"  # 共享层只拿它当 key、原样传给 servant


class FakeServant:
    """记录读调用；gate 可以卡住读，fail 让下一次读抛错；行里带 version 区分是哪次读的"""

    def __init__(self):
        self.calls: list[tuple] = []
        self.gate = asyncio.Event()
        self.gate.set()
        self.fail: Exception | None = None
        self.version = 1

    async def _read(self):
        await self.gate.wait()
        if self.fail is not None:
            exc, self.fail = self.fail, None
            raise exc

    async def get_many(self, ref, row_ids, row_format=RowFormat.STRUCT):
        self.calls.append(("get_many", ref, list(row_ids)))
        await self._read()
        return [{"id": i, "_version": self.version} for i in row_ids]

    async def range(
        self,
        ref,
        index_name,
        left,
        right=None,
        limit=100,
        desc=False,
        row_format=RowFormat.STRUCT,
    ):
        self.calls.append(("range", ref, index_name, left, right, limit, desc))
        await self._read()
        return [1, 2, 3]


def make_reads(**kwargs) -> tuple[SharedReads, FakeServant]:
    servant = FakeServant()
    return SharedReads(SimpleNamespace(servant=servant), **kwargs), servant


def old_cover() -> float:
    """足够早的覆盖时刻：现在发出的读一定满足它"""
    return time.monotonic() - INTERVAL * 2


async def test_reuses_read_issued_late_enough():
    reads, servant = make_reads()
    cover = old_cover()
    [(row, issued)] = await reads.rows(REF, [(1, cover)])
    assert issued >= cover + INTERVAL
    assert await reads.rows(REF, [(1, cover)]) == [(row, issued)]
    assert len(servant.calls) == 1


async def test_does_not_reuse_read_issued_too_early():
    """覆盖时刻之后不足 interval 发出的读，可能落在还没应用那条通知的副本上：不能用，新发"""
    reads, servant = make_reads()
    [(_row, issued)] = await reads.rows(REF, [(1, old_cover())])
    servant.version = 2
    [(row, issued2)] = await reads.rows(REF, [(1, issued)])
    assert row is not None and row["_version"] == 2
    assert issued2 > issued
    assert len(servant.calls) == 2


async def test_inflight_read_is_shared():
    reads, servant = make_reads()
    servant.gate.clear()
    cover = old_cover()
    first = asyncio.create_task(reads.rows(REF, [(1, cover)]))
    await asyncio.sleep(0)
    second = asyncio.create_task(reads.rows(REF, [(1, cover)]))
    await asyncio.sleep(0)
    servant.gate.set()
    async with asyncio.timeout(1):
        assert await first == await second
    assert len(servant.calls) == 1


async def test_partial_hits_fetch_only_missing_in_one_batch():
    reads, servant = make_reads()
    cover = old_cover()
    await reads.rows(REF, [(1, cover), (2, cover)])
    rows = await reads.rows(REF, [(1, cover), (2, cover), (3, cover)])
    assert [row["id"] for row, _issued in rows if row] == [1, 2, 3]
    assert servant.calls == [("get_many", REF, [1, 2]), ("get_many", REF, [3])]


async def test_issuer_cancelled_riders_still_get_result():
    """发起读的连接断开（被取消）：它的读跟着取消，搭车的不跟着被取消，各自重读，照常拿到
    结果"""
    reads, servant = make_reads()
    servant.gate.clear()
    cover = old_cover()
    issuer = asyncio.create_task(reads.rows(REF, [(1, cover)]))
    await asyncio.sleep(0)
    rider = asyncio.create_task(reads.rows(REF, [(1, cover)]))
    await asyncio.sleep(0)
    issuer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await issuer
    servant.gate.set()
    async with asyncio.timeout(1):
        [(row, _issued)] = await rider
    assert row is not None and row["id"] == 1
    assert len(servant.calls) == 2  # 发起方那次（被取消）+ 搭车的自己重读


async def test_failed_read_raises_for_issuer_riders_read_themselves():
    """共享的读失败：发起方照抛（同今天）；搭车的各自再读一次，不跟着一起断开。失败的
    读不留，之后要重新发"""
    reads, servant = make_reads()
    servant.gate.clear()
    servant.fail = ConnectionError("read failed")
    cover = old_cover()
    issuer = asyncio.create_task(reads.rows(REF, [(1, cover)]))
    await asyncio.sleep(0)
    rider = asyncio.create_task(reads.rows(REF, [(1, cover)]))
    await asyncio.sleep(0)
    servant.gate.set()
    async with asyncio.timeout(1):
        with pytest.raises(ConnectionError, match="read failed"):
            await issuer
        [(row, _issued)] = await rider
    assert row is not None and row["id"] == 1
    assert len(servant.calls) == 2
    await reads.rows(REF, [(1, cover)])
    assert len(servant.calls) == 3, "失败的读不能留着给后来的人用"


async def test_range_ids_shared_per_query():
    reads, servant = make_reads()
    query = {"index_name": "id", "left": 0, "right": 10, "limit": 5, "desc": True}
    cover = old_cover()
    ids, issued = await reads.range_ids(REF, query, cover)
    assert ids == [1, 2, 3]
    assert await reads.range_ids(REF, dict(query), cover) == (ids, issued)
    assert servant.calls == [("range", REF, "id", 0, 10, 5, True)]
    await reads.range_ids(REF, dict(query, limit=6), cover)
    assert len(servant.calls) == 2, "不同查询不能共享"


async def test_entries_expire_after_horizon(monkeypatch):
    """发出超过 HORIZON 的读清掉（控内存），之后同样的覆盖时刻也要新发"""
    monkeypatch.setattr(MQClient, "UPDATE_FREQUENCY", 1000)  # interval 1ms
    reads, servant = make_reads()
    cover = time.monotonic() - 1
    await reads.rows(REF, [(1, cover)])
    await asyncio.sleep(SharedReads.HORIZON_INTERVALS / 1000 * 3)
    await reads.rows(REF, [(1, cover)])
    assert len(servant.calls) == 2


async def test_event_loop_change_drops_entries():
    """future 不能跨事件循环（测试按模块换 loop）：换了 loop 就清空"""
    reads, servant = make_reads()
    cover = old_cover()
    await reads.rows(REF, [(1, cover)])
    reads._loop = object()  # type: ignore[assignment]  模拟上一个模块的 loop
    await reads.rows(REF, [(1, cover)])
    assert len(servant.calls) == 2


async def test_share_disabled_reads_every_time():
    reads, servant = make_reads(share=False)
    cover = old_cover()
    await reads.rows(REF, [(1, cover)])
    await reads.rows(REF, [(1, cover)])
    assert len(servant.calls) == 2


def test_of_returns_one_instance_per_backend():
    class FakeBackend:
        servant = None

    backend = FakeBackend()
    assert SharedReads.of(backend) is SharedReads.of(backend)
    assert SharedReads.of(FakeBackend()) is not SharedReads.of(backend)
    # 不能弱引用的替身（如 SimpleNamespace）：不报错，各用各的
    namespace = SimpleNamespace(servant=None)
    assert SharedReads.of(namespace) is not SharedReads.of(namespace)


class _Comp:
    @staticmethod
    def is_rls() -> bool:
        return False


class _Ref:
    """当 table_ref 用：能 hash，带 comp_cls"""

    comp_cls = _Comp


async def test_row_subscription_never_uses_older_read():
    """订阅用到的读只进不退：它已经用过更晚发出的读时，积压的旧通知（覆盖时刻早）不能
    拿到比那次更早的共享读，否则客户端会短暂倒退"""
    reads, servant = make_reads()
    ref = _Ref()
    cover = old_cover()
    [(_row, early)] = await reads.rows(ref, [(1, cover)])
    sub = RowSubscription(ref, servant, reads, None, "ch", 1)  # type: ignore[arg-type]
    sub.read_at = early + 0.001  # 它已经用过一次更晚发出的读
    RowSubscription.reset_cache_()
    await asyncio.sleep(0.005)  # 下面新发的读晚于 read_at
    await sub.read_("ch", cover)
    assert len(servant.calls) == 2, "用上了比它已用过的更早的读"
    assert sub.read_at > early + 0.001
