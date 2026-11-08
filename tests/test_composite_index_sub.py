"""
复合索引（groupby）的订阅与线协议：按组订阅（声明了 point_sub 时只订这一组的频道）、离开与补位、
组内换序、退化与警告、sub_id 与共享键、分组值校验、hidden 分组列，以及 receiver 按位置转发 group。
设计稿见 docs/superpowers/specs/2026-11-08-composite-index-design.md §6、§7。
"""

import asyncio
import logging
from collections.abc import AsyncGenerator, Callable
from contextvars import ContextVar
from types import SimpleNamespace
from typing import cast

import numpy as np
import pytest
from fixtures.testdata import create_ref

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, TableReference
from hetu.data.sub import (
    IndexSubscription,
    RowSubscription,
    SubscriptionBroker,
    SubscriptionHub,
)

SnowflakeID().init(1, 0)


def def_rank():
    from hetu.data import BaseComponent, Permission, define_component, property_field

    global Rank

    @define_component(namespace="pytest", permission=Permission.USER, force=True)
    class Rank(BaseComponent):
        zone: np.int32 = property_field(0)
        # 按 zone 分组、组内按 level 排序，按组发频道
        level: np.int32 = property_field(0, groupby="zone", point_sub=True)
        # 没声明 point_sub：按组订阅退化为订整个索引的频道
        score: np.int32 = property_field(0, groupby="zone")
        secret: np.int32 = property_field(0, hidden=True)
        # 分组列是 hidden：客户端不能按组订阅
        power: np.int32 = property_field(0, groupby="secret")
        qty: np.int32 = property_field(0)

    return Rank


@pytest.fixture
async def rank_ref(new_component_env, mod_auto_backend) -> TableReference:
    return create_ref(def_rank(), mod_auto_backend())


@pytest.fixture
async def broker(mod_auto_backend) -> AsyncGenerator[SubscriptionBroker]:
    """独享 hub 的手动模式订阅门面（同 test_backend_sub.py）"""
    RowSubscription._RowSubscription__cache = ContextVar("user_row_cache")  # type: ignore
    backend = mod_auto_backend("main")
    broker = SubscriptionBroker(backend, hub=SubscriptionHub(backend, autostart=False))
    yield broker
    await broker.close()
    await broker._hub.close()


async def updates_until(
    broker: SubscriptionBroker,
    check: Callable[[dict[str, dict]], object],
    timeout: float = 10.0,
) -> dict[str, dict]:
    """反复 get_updates 合并结果，直到 check 里的断言通过（同 test_backend_sub.py）"""
    merged: dict[str, dict] = {}
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            check(merged)
            return merged
        except AssertionError, KeyError, TypeError:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise
        for sub_id, rows in (await broker.get_updates(timeout=remaining)).items():
            merged.setdefault(sub_id, {}).update(rows)


async def _insert(backend: Backend, comp, *rows: dict) -> list[int]:
    ids = []
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        for fields in rows:
            row = comp.new_row()
            for key, value in fields.items():
                row[key] = value
            await repo.insert(row)
            ids.append(int(row.id))
    await backend.wait_for_synced()
    return ids


async def test_group_subscription_uses_group_channel(broker, rank_ref, admin_ctx):
    """声明了 point_sub 的复合索引按组订阅：只订这一组的频道，本组的插入照样推送"""
    backend = broker._backend
    comp = rank_ref.comp_cls
    await _insert(backend, comp, {"zone": 1, "level": 5}, {"zone": 2, "level": 6})
    sub_id, rows = await broker.subscribe_range(
        rank_ref, admin_ctx, "level", 0, 100, limit=10, group={"zone": 1}
    )
    assert sub_id and [r["level"] for r in rows] == [5]
    idx_sub = cast(IndexSubscription, broker._subs[sub_id])
    assert idx_sub.enter_only
    servant = backend.servant
    assert idx_sub.index_channel == servant.index_group_channel(
        rank_ref, "level", {"zone": 1}
    )
    assert idx_sub.index_channel != servant.index_group_channel(
        rank_ref, "level", {"zone": 2}
    )
    (new_id,) = await _insert(backend, comp, {"zone": 1, "level": 7})
    await updates_until(broker, lambda u: u[sub_id][new_id]["level"] == 7)


@pytest.mark.parametrize("desc", [False, True])
async def test_group_subscription_leave_backfills(broker, rank_ref, admin_ctx, desc):
    """
    组频道只报进入：结果里的行换组、被删，由它们的行频道发现，推 None 并补进被 limit 截在外面
    的行（同单列值频道的 test_point_query_leave_backfills_limit）
    """
    backend = broker._backend
    comp = rank_ref.comp_cls
    ids = await _insert(
        backend, comp, *({"zone": 1, "level": lv} for lv in range(1, 7))
    )
    by_level = dict(zip(range(1, 7), ids))
    sub_id, rows = await broker.subscribe_range(
        rank_ref, admin_ctx, "level", 0, 100, limit=3, desc=desc, group={"zone": 1}
    )
    first = [6, 5, 4] if desc else [1, 2, 3]
    assert [r["level"] for r in rows] == first
    moved, deleted = by_level[first[0]], by_level[first[1]]
    backfill = [by_level[lv] for lv in ([3, 2] if desc else [4, 5])]

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(id=moved)
        row.zone = 2
        await repo.update(row)
        assert await repo.get(id=deleted) is not None
        repo.delete(deleted)

    def left_and_backfilled(updates):
        got = updates[sub_id]
        assert got[moved] is None and got[deleted] is None
        assert all(got[row_id]["zone"] == 1 for row_id in backfill)
        idx_sub = cast(IndexSubscription, broker._subs[sub_id])
        assert idx_sub.last_range_result == {by_level[first[2]], *backfill}

    await updates_until(broker, left_and_backfilled)


async def test_group_subscription_sees_reorder_within_group(
    broker, rank_ref, admin_ctx
):
    """组内本字段变化让截断窗口外的行挤进来：commit 发组频道，订阅重跑比对，被挤出的推 None"""
    backend = broker._backend
    comp = rank_ref.comp_cls
    low, mid, high = await _insert(
        backend, comp, *({"zone": 1, "level": lv} for lv in (10, 20, 30))
    )
    sub_id, rows = await broker.subscribe_range(
        rank_ref, admin_ctx, "level", 0, 100, limit=2, group={"zone": 1}
    )
    assert [r["level"] for r in rows] == [10, 20]
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(id=high)
        row.level = 5
        await repo.update(row)

    def reordered(updates):
        got = updates[sub_id]
        assert got[high]["level"] == 5 and got[mid] is None
        idx_sub = cast(IndexSubscription, broker._subs[sub_id])
        assert idx_sub.order == [high, low]

    await updates_until(broker, reordered)


async def test_group_subscription_without_point_sub(
    broker, rank_ref, admin_ctx, caplog
):
    """没声明 point_sub 的复合索引按组订阅：退化为订整个索引的频道，警告一次，照样能推送"""
    backend = broker._backend
    comp = rank_ref.comp_cls
    with caplog.at_level(logging.WARNING, logger="HeTu.root"):
        sub_id, _rows = await broker.subscribe_range(
            rank_ref, admin_ctx, "score", 0, 100, group={"zone": 1}
        )
    assert "复合索引但没有声明" in caplog.text
    idx_sub = cast(IndexSubscription, broker._subs[sub_id])
    assert not idx_sub.enter_only
    assert idx_sub.index_channel == backend.servant.index_channel(rank_ref, "score")
    (new_id,) = await _insert(backend, comp, {"zone": 1, "score": 3})
    await updates_until(broker, lambda u: u[sub_id][new_id]["score"] == 3)


async def test_group_sub_id_and_share_key(broker, rank_ref, admin_ctx):
    """
    sub_id 与共享键都用分组值的原值：等价但类型不同的值（1 与 True）是两个订阅，同一连接订两次
    两边都收得到推送、退订一个不影响另一个；单列查询的 sub_id 格式不变
    """
    backend = broker._backend
    comp = rank_ref.comp_cls
    await _insert(backend, comp, {"zone": 1, "level": 5})
    a, _rows = await broker.subscribe_range(
        rank_ref, admin_ctx, "level", 0, 100, group={"zone": 1}
    )
    b, _rows = await broker.subscribe_range(
        rank_ref, admin_ctx, "level", 0, 100, group={"zone": True}
    )
    assert a and b and a != b
    assert a.endswith("{zone=1}") and b.endswith("{zone=True}")
    (new_id,) = await _insert(backend, comp, {"zone": 1, "level": 6})
    await updates_until(broker, lambda u: u[a][new_id] and u[b][new_id])
    await broker.unsubscribe(a)
    (newer,) = await _insert(backend, comp, {"zone": 1, "level": 7})
    await updates_until(broker, lambda u: u[b][newer]["level"] == 7)
    # 单列查询
    assert (
        SubscriptionBroker.make_query_id_(rank_ref, "id", 1, None, 1, False)
        == f"{comp.name_}.id[1:None:1][:1]"
    )
    # 字符串分组值用 repr：值里有 , = } 也不会拼出别的查询的 sub_id
    sid1 = SubscriptionBroker.make_query_id_(
        rank_ref, "level", 0, 1, 1, False, {"a": "x,b=y", "b": "z"}
    )
    sid2 = SubscriptionBroker.make_query_id_(
        rank_ref, "level", 0, 1, 1, False, {"a": "x", "b": "y,b=z"}
    )
    assert sid1 != sid2


async def test_group_subscription_rejects(broker, rank_ref, admin_ctx):
    """
    分组值不合法当场抛 ValueError（接收协程里抛出，按非法请求断开 4002）；分组列是 hidden 的
    按组订阅被拒（返回 None，同按 hidden 字段订阅）
    """
    bad = [
        ({}, "分组"),
        ({"group": {"zone": 1, "qty": 2}}, "正好给出"),
        ({"group": {"zone": 2**40}}, "超出"),
        ({"group": {"zone": "1"}}, "只收数字"),
        ({"group": {1: 1}}, "正好给出"),
    ]
    for kwargs, match in bad:
        with pytest.raises(ValueError, match=match):
            await broker.subscribe_range(rank_ref, admin_ctx, "level", 0, 100, **kwargs)
    with pytest.raises(ValueError, match="不是复合索引"):
        await broker.subscribe_range(
            rank_ref, admin_ctx, "id", 0, 100, group={"zone": 1}
        )
    with pytest.raises(ValueError, match="id"):
        await broker.subscribe_get(rank_ref, admin_ctx, "id", 1, group={"zone": 1})
    with pytest.raises(ValueError, match="分组"):
        await broker.subscribe_get(rank_ref, admin_ctx, "level", 1)
    assert await broker.subscribe_range(
        rank_ref, admin_ctx, "power", 0, 10, group={"secret": 1}
    ) == (None, [])
    assert await broker.subscribe_get(
        rank_ref, admin_ctx, "power", 1, group={"secret": 1}
    ) == (None, None)
    assert broker._subs == {}


async def test_group_subscription_owner_permission(
    broker, new_component_env, mod_auto_backend, user_id10_ctx
):
    """OWNER 组件按组订阅：按连接各自过滤，只看得到自己的行；别人进组不推，自己的照推"""
    from hetu.data import BaseComponent, Permission, define_component, property_field

    @define_component(namespace="pytest", permission=Permission.OWNER, force=True)
    class Bag(BaseComponent):
        owner: np.int64 = property_field(0, index=True)
        zone: np.int32 = property_field(0)
        level: np.int32 = property_field(0, groupby="zone", point_sub=True)

    backend = broker._backend
    ref = create_ref(Bag, backend)
    mine, _theirs = await _insert(
        backend,
        Bag,
        {"owner": 10, "zone": 1, "level": 1},
        {"owner": 11, "zone": 1, "level": 2},
    )
    sub_id, rows = await broker.subscribe_range(
        ref, user_id10_ctx, "level", 0, 100, group={"zone": 1}
    )
    assert sub_id and [r["id"] for r in rows] == [mine]
    await _insert(backend, Bag, {"owner": 11, "zone": 1, "level": 3})
    assert await broker.get_updates(timeout=0.5) == {}
    (new_id,) = await _insert(backend, Bag, {"owner": 10, "zone": 1, "level": 4})
    await updates_until(broker, lambda u: u[sub_id][new_id]["level"] == 4)


async def test_group_subscribe_get(broker, rank_ref, admin_ctx):
    """按复合索引订单行：在给定的组里找"""
    backend = broker._backend
    comp = rank_ref.comp_cls
    (row_id,) = await _insert(backend, comp, {"zone": 3, "level": 9})
    sub_id, row = await broker.subscribe_get(
        rank_ref, admin_ctx, "level", 9, group={"zone": 3}
    )
    assert sub_id and row and row["id"] == row_id
    assert await broker.subscribe_get(
        rank_ref, admin_ctx, "level", 9, group={"zone": 4}
    ) == (None, None)


async def test_sub_call_forwards_group():
    """receiver 按位置把 group 转发给订阅门面：range 第 10 项、get 第 6 项；再多就是非法消息"""
    from hetu.server.receiver import sub_call

    class Recorder:
        def __init__(self):
            self.calls: list[tuple] = []

        async def begin_subscribe_range(self, table, ctx, *args):
            self.calls.append(("range", args))

            async def finish():
                return "S", []

            return finish()

        async def begin_subscribe_get(self, table, ctx, *args):
            self.calls.append(("get", args))

            async def finish():
                return "S", None

            return finish()

        def count(self):
            return 0, 0, 0

    limits = SimpleNamespace(max_row_sub=100, max_index_sub=100, max_table_sub=100)
    executor = SimpleNamespace(
        context=limits, tbl_mgr=SimpleNamespace(get_table=lambda name: object())
    )
    broker = Recorder()
    queue: asyncio.Queue = asyncio.Queue()
    deferred: set[asyncio.Task] = set()
    group = {"zone": 3}
    sub = ["sub", "Rank", "range", "level", 0, 10, 5, False, True, group]
    assert await sub_call(sub, executor, broker, queue, deferred)  # type: ignore[arg-type]
    assert broker.calls[-1] == ("range", ("level", 0, 10, 5, False, True, group))
    get = ["sub", "Rank", "get", "level", 3, group]
    assert await sub_call(get, executor, broker, queue, deferred)  # type: ignore[arg-type]
    assert broker.calls[-1] == ("get", ("level", 3, group))
    # 旧格式照旧
    old = ["sub", "Rank", "range", "level", 0, 10, 5, False, True]
    assert await sub_call(old, executor, broker, queue, deferred)  # type: ignore[arg-type]
    assert broker.calls[-1] == ("range", ("level", 0, 10, 5, False, True))
    for too_long in ([*sub, "x"], [*get, "x"]):
        with pytest.raises(ValueError, match="Invalid"):
            await sub_call(too_long, executor, broker, queue, deferred)  # type: ignore[arg-type]
    await asyncio.gather(*deferred)
