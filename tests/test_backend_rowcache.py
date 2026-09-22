"""RowCache / CachedRowReader 的纯单元测试：不需要任何数据库，用假客户端计数调用"""

from typing import cast

import numpy as np
import pytest

from hetu.data.backend import BackendClient, RowFormat, TableReference
from hetu.data.backend.rowcache import (
    DELETED,
    UNKNOWN,
    CachedRowReader,
    RowCache,
)


def _row(model, row_id: int, version: int, **fields):
    row = model.new_row(row_id)
    row._version = version
    for k, v in fields.items():
        setattr(row, k, v)
    return row


class FakeClient:
    """假 BackendClient：dict 存行，计数每种读的调用次数；可注入"副本滞后"的旧行"""

    def __init__(self, model, rows):
        self.model = model
        self.rows = {int(r.id): r for r in rows}
        self.stale: dict[int, np.record] = {}  # get/get_many 优先返回这里的旧行
        self.calls = {
            "get": 0,
            "get_many": 0,
            "get_authoritative": 0,
            "get_many_authoritative": 0,
        }

    def row_channel(self, ref, row_id):
        return f"{ref.comp_cls.name_}:id:{row_id}"

    def _replica(self, row_id):
        row = self.stale.get(row_id, self.rows.get(row_id))
        return row.copy() if row is not None else None

    async def get(self, ref, row_id, row_format=RowFormat.STRUCT):
        self.calls["get"] += 1
        return self._replica(int(row_id))

    async def get_many(self, ref, row_ids, row_format=RowFormat.STRUCT):
        self.calls["get_many"] += 1
        return [self._replica(int(i)) for i in row_ids]

    async def get_authoritative(self, ref, row_id):
        self.calls["get_authoritative"] += 1
        row = self.rows.get(int(row_id))
        return row.copy() if row is not None else None

    async def get_many_authoritative(self, ref, row_ids):
        self.calls["get_many_authoritative"] += 1
        return [
            self.rows[int(i)].copy() if int(i) in self.rows else None for i in row_ids
        ]


class FakeBackend:
    def __init__(self, row_cache, master):
        self.row_cache = row_cache
        self.master = master


@pytest.fixture
def item_ref(mod_item_model):
    return TableReference(mod_item_model, "pytest", 1)


@pytest.fixture
def volatile_ref(mod_rls_test_model):
    return TableReference(mod_rls_test_model, "pytest", 1)


# ---------------------------------------------------------------- RowCache


def test_activate_lease_floor(mod_item_model, item_ref):
    cache = RowCache()
    ch = "Item:id:1"
    assert cache.lease(ch) is None
    assert not cache.is_active(ch)
    cache.activate(ch, "hubA")
    lease = cache.lease(ch)
    assert lease is not None and lease.channel == ch
    assert cache.is_active(ch)
    assert cache.floor(ch) is UNKNOWN
    # 未激活频道的 floor 视为未知
    assert cache.floor("Item:id:999") is UNKNOWN


def test_fill_rules(mod_item_model):
    cache = RowCache()
    ch = "Item:id:1"
    cache.activate(ch, "hubA")
    lease = cache.lease(ch)
    assert lease is not None
    row_v3 = _row(mod_item_model, 1, 3, qty=3)

    # floor 未知：副本读不能填，权威读才能
    assert cache.fill(lease, row_v3, authoritative=False) is False
    assert cache.get(ch) is None
    assert cache.fill(lease, row_v3, authoritative=True) is True
    assert cache.floor(ch) == 3
    got = cache.get(ch)
    assert got is not None and got.qty == 3

    # floor 已知：低于 floor 拒、不低于收并抬 floor
    cache.evict(ch)
    assert cache.fill(lease, _row(mod_item_model, 1, 2), authoritative=False) is False
    assert cache.get(ch) is None
    assert cache.fill(lease, _row(mod_item_model, 1, 3), authoritative=False) is True
    assert cache.fill(lease, _row(mod_item_model, 1, 5), authoritative=False) is True
    assert cache.floor(ch) == 5
    # 权威读也不能低于 floor（读发生在更新的通知之前）
    assert cache.fill(lease, _row(mod_item_model, 1, 4), authoritative=True) is False
    got = cache.get(ch)
    assert got is not None and got._version == 5

    # DELETED：副本读拒（副本可能还没删），权威读回的行收（重插 / 乱序）
    cache.notify(ch, 0)
    assert cache.get(ch) is None
    assert cache.floor(ch) is DELETED and cache.replica_floor(ch) is None
    assert cache.fill(lease, _row(mod_item_model, 1, 5), authoritative=False) is False
    assert cache.fill(lease, _row(mod_item_model, 1, 1), authoritative=True) is True
    assert cache.floor(ch) == 1
    # 见过删除通知后本次激活周期内只信权威读：重插后版本从 1 重来，滞后副本上删除前的
    # 旧行（v5）版本反而更高，floor 挡不住，副本读一律不收
    cache.notify(ch, 2)
    assert cache.get(ch) is None and cache.floor(ch) == 2
    assert cache.replica_floor(ch) is None
    assert cache.fill(lease, _row(mod_item_model, 1, 5), authoritative=False) is False
    assert cache.fill(lease, _row(mod_item_model, 1, 2), authoritative=False) is False
    assert cache.fill(lease, _row(mod_item_model, 1, 2), authoritative=True) is True
    # 权威读仍受 floor 约束（读发生在更新的通知之前）
    cache.notify(ch, 3)
    assert cache.fill(lease, _row(mod_item_model, 1, 2), authoritative=True) is False

    # 失活再激活：旧 lease 的代次过期，"只信权威读"也随之清掉
    cache.deactivate(ch, "hubA")
    cache.activate(ch, "hubA")
    assert cache.fill(lease, _row(mod_item_model, 1, 9), authoritative=True) is False
    new_lease = cache.lease(ch)
    assert new_lease is not None and new_lease.epoch != lease.epoch
    assert cache.fill(new_lease, _row(mod_item_model, 1, 9), authoritative=True)
    assert cache.replica_floor(ch) == 9
    assert cache.fill(new_lease, _row(mod_item_model, 1, 10), authoritative=False)


def test_notify_rules(mod_item_model):
    cache = RowCache()
    ch = "Item:id:1"
    # 未激活频道的通知无副作用
    cache.notify(ch, 7)
    assert cache.floor(ch) is UNKNOWN and len(cache) == 0

    cache.activate(ch, "hubA")
    lease = cache.lease(ch)
    assert lease is not None
    assert cache.fill(lease, _row(mod_item_model, 1, 3), authoritative=True)

    # 低于 / 等于缓存行版本的通知：不逐出、floor 不降
    cache.notify(ch, 2)
    assert cache.get(ch) is not None and cache.floor(ch) == 3
    cache.notify(ch, 3)
    assert cache.get(ch) is not None and cache.floor(ch) == 3
    # 更高版本：逐出 + 抬 floor
    cache.notify(ch, 4)
    assert cache.get(ch) is None and cache.floor(ch) == 4
    # 乱序到达的旧通知不降 floor
    cache.notify(ch, 2)
    assert cache.floor(ch) == 4
    # 删除 → DELETED；之后的插入通知重置 floor，但副本读到本次激活结束前都不再可信
    cache.notify(ch, 0)
    assert cache.floor(ch) is DELETED
    cache.notify(ch, 1)
    assert cache.floor(ch) == 1 and cache.replica_floor(ch) is None
    cache.clear()
    cache.activate(ch, "hubA")
    cache.notify(ch, 1)
    assert cache.replica_floor(ch) == 1


def test_put_committed(mod_item_model):
    cache = RowCache()
    ch = "Item:id:1"
    # 未激活：提交前取不到凭据，无从写穿
    assert cache.lease(ch) is None

    cache.activate(ch, "hubA")
    lease = cache.lease(ch)
    assert lease is not None
    assert cache.put_committed(lease, _row(mod_item_model, 1, 1, qty=1)) is True
    assert cache.floor(ch) == 1
    got = cache.get(ch)
    assert got is not None and got.qty == 1
    # 自己那条通知（版本相等）不逐出
    cache.notify(ch, 1)
    assert cache.get(ch) is not None
    # 别人更高版本的通知先到：低版本的写穿不覆盖
    cache.notify(ch, 5)
    assert cache.put_committed(lease, _row(mod_item_model, 1, 4)) is False
    assert cache.get(ch) is None and cache.floor(ch) == 5
    # 删除后重插：DELETED 也能被写穿
    cache.notify(ch, 0)
    assert cache.put_committed(lease, _row(mod_item_model, 1, 1)) is True
    assert cache.floor(ch) == 1

    # 提交往返期间失活过（最后一个订阅者退订、又有人订回来）：那段时间别人的写入没有
    # 通知到本进程，提交前取的凭据代次已过期，写穿丢弃，留给下次的权威读
    cache.deactivate(ch, "hubA")
    cache.activate(ch, "hubA")
    assert cache.put_committed(lease, _row(mod_item_model, 1, 2)) is False
    assert cache.get(ch) is None and cache.floor(ch) is UNKNOWN


def test_get_returns_copy_and_lru(mod_item_model):
    cache = RowCache(max_rows=2)
    for i in (1, 2, 3):
        cache.activate(f"Item:id:{i}", "hubA")
    lease1 = cache.lease("Item:id:1")
    assert lease1 is not None
    assert cache.fill(lease1, _row(mod_item_model, 1, 1, qty=1), authoritative=True)
    got = cache.get("Item:id:1")
    assert got is not None
    got.qty = 99
    again = cache.get("Item:id:1")
    assert again is not None and again.qty == 1  # 改返回值不影响缓存

    for i in (2, 3):
        lease = cache.lease(f"Item:id:{i}")
        assert lease is not None
        assert cache.fill(lease, _row(mod_item_model, i, 1), authoritative=True)
    # 超过上限：最早的行被丢，但激活 / floor 仍在
    assert len(cache) == 2
    assert cache.get("Item:id:1") is None
    assert cache.is_active("Item:id:1") and cache.floor("Item:id:1") == 1
    assert cache.get("Item:id:3") is not None


def test_multiple_owners_and_clear(mod_item_model):
    cache = RowCache()
    ch = "Item:id:1"
    cache.activate(ch, "hubA")
    cache.activate(ch, "hubB")
    lease = cache.lease(ch)
    assert lease is not None
    assert cache.fill(lease, _row(mod_item_model, 1, 1), authoritative=True)
    # 一个 owner 失活不影响另一个：行还在、lease 仍可填
    cache.deactivate(ch, "hubA")
    assert cache.is_active(ch) and cache.get(ch) is not None
    assert cache.fill(lease, _row(mod_item_model, 1, 2), authoritative=False)
    # 最后一个失活：行、floor 都清
    cache.deactivate(ch, "hubB")
    assert not cache.is_active(ch)
    assert cache.get(ch) is None and cache.floor(ch) is UNKNOWN
    assert len(cache) == 0

    cache.activate(ch, "hubA")
    new_lease = cache.lease(ch)
    assert new_lease is not None
    assert cache.fill(new_lease, _row(mod_item_model, 1, 1), authoritative=True)
    cache.clear()
    assert len(cache) == 0 and not cache.is_active(ch)


def test_stats(mod_item_model):
    cache = RowCache()
    ch = "Item:id:1"
    cache.activate(ch, "hubA")
    assert cache.get(ch) is None
    lease = cache.lease(ch)
    assert lease is not None
    cache.fill(lease, _row(mod_item_model, 1, 1), authoritative=True)
    assert cache.get(ch) is not None
    assert cache.stats.hits == 1 and cache.stats.misses == 1
    assert cache.stats.size == 1


# --------------------------------------------------------- CachedRowReader


async def test_reader_bypass_paths(mod_item_model, item_ref, volatile_ref):
    rows = [_row(mod_item_model, 1, 1)]
    master = FakeClient(mod_item_model, rows)
    fallback = FakeClient(mod_item_model, rows)
    fb = cast(BackendClient, fallback)

    # 没有缓存对象：只走 fallback
    reader = CachedRowReader(FakeBackend(None, master))
    got = await reader.get(item_ref, 1, fb)
    assert got is not None and fallback.calls["get"] == 1
    assert master.calls["get_authoritative"] == 0

    # 有缓存但易失组件：只走 fallback
    cache = RowCache()
    reader = CachedRowReader(FakeBackend(cache, master))
    cache.activate(master.row_channel(volatile_ref, 1), "hub")
    await reader.get(volatile_ref, 1, fb)
    assert fallback.calls["get"] == 2 and master.calls["get_authoritative"] == 0
    assert len(cache) == 0

    # 有缓存但未激活：只走 fallback、不填充
    got = await reader.get(item_ref, 1, fb)
    assert got is not None
    assert fallback.calls["get"] == 3 and len(cache) == 0


async def test_reader_get(mod_item_model, item_ref):
    rows = [_row(mod_item_model, 1, 3, qty=3)]
    master = FakeClient(mod_item_model, rows)
    fallback = FakeClient(mod_item_model, rows)
    fb = cast(BackendClient, fallback)
    cache = RowCache()
    reader = CachedRowReader(FakeBackend(cache, master))
    ch = master.row_channel(item_ref, 1)
    cache.activate(ch, "hub")

    # 首次（floor 未知）：只走权威读并填充
    got = await reader.get(item_ref, 1, fb)
    assert got is not None and got.qty == 3
    assert master.calls["get_authoritative"] == 1 and fallback.calls["get"] == 0
    assert cache.floor(ch) == 3
    # 命中：零调用，且是副本
    got = await reader.get(item_ref, 1, fb)
    assert got is not None
    got.qty = 100
    assert master.calls["get_authoritative"] == 1 and fallback.calls["get"] == 0
    assert (await reader.get(item_ref, 1, fb)).qty == 3  # type: ignore[union-attr]

    # 通知到达（版本 4）：逐出；副本仍是旧行 → 识破滞后，改权威读
    rows[0]._version = 4
    rows[0].qty = 4
    master.rows[1] = rows[0]
    fallback.stale[1] = _row(mod_item_model, 1, 3, qty=3)
    cache.notify(ch, 4)
    got = await reader.get(item_ref, 1, fb)
    assert got is not None and got.qty == 4
    assert fallback.calls["get"] == 1 and master.calls["get_authoritative"] == 2
    assert cache.floor(ch) == 4
    cached = cache.get(ch)
    assert cached is not None and cached.qty == 4

    # 副本追上：副本读直接填充，不碰权威读
    cache.evict(ch)
    fallback.stale.pop(1)
    fallback.rows[1] = rows[0]
    got = await reader.get(item_ref, 1, fb)
    assert got is not None and got.qty == 4
    assert fallback.calls["get"] == 2 and master.calls["get_authoritative"] == 2

    # 删除通知：已知它不存在，直接返回 None，一次库都不打（副本还读得到旧行也无所谓）
    cache.notify(ch, 0)
    master.rows.pop(1)
    got = await reader.get(item_ref, 1, fb)
    assert got is None
    assert master.calls["get_authoritative"] == 2 and cache.get(ch) is None


async def test_reader_delete_reinsert_stale_replica(mod_item_model, item_ref):
    """同 id 删除后重插：版本从 1 重来，滞后副本上删除前的旧行（v7）比新一代（v2）版本高，
    floor 挡不住。见过删除通知的频道本次激活周期内只走权威读，旧行进不了缓存也到不了调用方"""
    old = _row(mod_item_model, 1, 7, qty=7)
    master = FakeClient(mod_item_model, [old])
    fallback = FakeClient(mod_item_model, [old])
    fb = cast(BackendClient, fallback)
    cache = RowCache()
    reader = CachedRowReader(FakeBackend(cache, master))
    ch = master.row_channel(item_ref, 1)
    cache.activate(ch, "hub")
    got = await reader.get(item_ref, 1, fb)
    assert got is not None and got.qty == 7 and cache.floor(ch) == 7

    # 别的进程：删除 → 重插(v1) → 更新(v2)；副本一直没追上，还是删除前的 v7
    cache.notify(ch, 0)
    new = _row(mod_item_model, 1, 2, qty=2)
    master.rows[1] = new
    cache.notify(ch, 1)
    cache.notify(ch, 2)
    fallback.stale[1] = old
    assert cache.replica_floor(ch) is None
    got = await reader.get(item_ref, 1, fb)
    assert got is not None and got.qty == 2
    assert fallback.calls["get"] == 0 and master.calls["get_authoritative"] == 2
    cached = cache.get(ch)
    assert cached is not None and cached.qty == 2 and cache.floor(ch) == 2
    # 再更新(v3)：逐出后仍不碰副本，副本的 v7 旧行永远进不来
    new._version = 3
    new.qty = 3
    cache.notify(ch, 3)
    got = await reader.get(item_ref, 1, fb)
    assert got is not None and got.qty == 3
    assert fallback.calls["get"] == 0 and master.calls["get_authoritative"] == 3
    # get_many 同样只走权威批读
    cache.evict(ch)
    got_many = await reader.get_many(item_ref, [1], fb)
    assert got_many[0] is not None and got_many[0].qty == 3
    assert fallback.calls["get_many"] == 0
    assert master.calls["get_many_authoritative"] == 1

    # 最后一个订阅者退订、下次激活：重新信副本（floor 又从未知开始）
    cache.deactivate(ch, "hub")
    cache.activate(ch, "hub")
    fallback.stale.pop(1)
    fallback.rows[1] = new
    await reader.get(item_ref, 1, fb)  # 首次权威读
    cache.notify(ch, 4)
    new._version = 4
    got = await reader.get(item_ref, 1, fb)
    assert got is not None and got._version == 4
    assert fallback.calls["get"] == 1 and master.calls["get_authoritative"] == 4


async def test_reader_get_many(mod_item_model, item_ref):
    rows = [_row(mod_item_model, i, 2, qty=i) for i in range(1, 7)]
    master = FakeClient(mod_item_model, rows)
    fallback = FakeClient(mod_item_model, rows)
    fb = cast(BackendClient, fallback)
    cache = RowCache()
    reader = CachedRowReader(FakeBackend(cache, master))
    chans = {i: master.row_channel(item_ref, i) for i in range(1, 7)}

    # 1：命中；2：激活 + floor 已知；3：激活 + floor 已知但副本滞后；4：激活 + floor 未知；
    # 5：未激活；6：激活 + 已删除（副本还有旧行）
    for i in (1, 2, 3, 4, 6):
        cache.activate(chans[i], "hub")
    lease1 = cache.lease(chans[1])
    assert lease1 is not None
    cache.fill(lease1, rows[0], authoritative=True)
    cache.notify(chans[2], 2)
    cache.notify(chans[3], 2)
    fallback.stale[3] = _row(mod_item_model, 3, 1, qty=-3)
    cache.notify(chans[6], 0)
    master.rows.pop(6)

    got = await reader.get_many(item_ref, [1, 2, 3, 4, 5, 6], fb)
    assert [r.qty if r is not None else None for r in got] == [1, 2, 3, 4, 5, None]
    # 副本批读一次（2、3、5），权威批读：首轮（4、6）+ 滞后补读（3）
    assert fallback.calls["get_many"] == 1
    assert master.calls["get_many_authoritative"] == 2
    assert fallback.calls["get"] == 0 and master.calls["get_authoritative"] == 0
    # 填充结果：2、3、4 入缓存，5 未激活不入，6 不存在
    for i in (2, 3, 4):
        cached = cache.get(chans[i])
        assert cached is not None and cached.qty == i
    assert cache.get(chans[5]) is None and cache.get(chans[6]) is None
    assert cache.floor(chans[3]) == 2


async def test_reader_absent_row_needs_no_read(mod_item_model, item_ref):
    """订阅中的行被删除后：删除通知本身就是权威的，之后再读它不该打任何一次库——
    以前每次读都是一发 master 权威读（还读不到东西），服务端逻辑每 tick 读一次就每 tick 一发"""
    rows = [_row(mod_item_model, i, 1, qty=i) for i in (1, 2)]
    master = FakeClient(mod_item_model, rows)
    fallback = FakeClient(mod_item_model, rows)
    fb = cast(BackendClient, fallback)
    cache = RowCache()
    reader = CachedRowReader(FakeBackend(cache, master))
    ch1 = master.row_channel(item_ref, 1)
    ch2 = master.row_channel(item_ref, 2)
    for ch in (ch1, ch2):
        cache.activate(ch, "hub")

    # 行 1 被删除：通知到达，缓存记下"它不存在"
    cache.notify(ch1, 0)
    master.rows.pop(1)
    before = dict(master.calls)

    for _ in range(3):
        assert await reader.get(item_ref, 1, fb) is None
    assert dict(master.calls) == before, (
        f"读已知不存在的行不该打 master：{master.calls}"
    )
    assert fallback.calls["get"] == 0, "也不该打副本"

    # 批读：已知不存在的行不进任何一组，其余照常
    got = await reader.get_many(item_ref, [1, 2], fb)
    assert got[0] is None and got[1] is not None
    assert master.calls["get_many_authoritative"] == 1, "只该为行 2 读一次"

    # 重新插入的通知到来后恢复正常（不再当它不存在）
    cache.notify(ch1, 1)
    master.rows[1] = _row(mod_item_model, 1, 1, qty=11)
    got1 = await reader.get(item_ref, 1, fb)
    assert got1 is not None and got1.qty == 11
