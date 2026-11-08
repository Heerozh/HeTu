"""
复合索引（groupby）的订阅与线协议：按组订阅（每组一个 zset，订这一组 key 的 keyspace 频道）、离开
与补位、组内换序、频道 API、sub_id 与共享键、分组值校验、hidden 分组列、OWNER / RLS 组件（RLS 的本字段
是浮点），以及 receiver 按位置转发 group。
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
        # 按 zone 分组、组内按 level / score 排序
        level: np.int32 = property_field(0, groupby="zone")
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


async def test_group_subscription_uses_group_key_channel(
    broker, rank_ref, admin_ctx, caplog
):
    """
    复合索引每组一个 zset，按组订阅订这一组 key 的 keyspace 频道：别的组的写入叫不醒它，本组的
    插入照样推送。不需要声明 point_sub，不退化、不警告
    """
    backend = broker._backend
    comp = rank_ref.comp_cls
    index_name = "level"
    await _insert(backend, comp, {"zone": 1, index_name: 5}, {"zone": 2, index_name: 6})
    with caplog.at_level(logging.WARNING, logger="HeTu.root"):
        sub_id, rows = await broker.subscribe_range(
            rank_ref, admin_ctx, index_name, 0, 100, limit=10, group={"zone": 1}
        )
    assert "point_sub" not in caplog.text
    assert sub_id and [r[index_name] for r in rows] == [5]
    idx_sub = cast(IndexSubscription, broker._subs[sub_id])
    assert not idx_sub.enter_only  # 组 key 的通知进出都报，不靠行频道判离开
    servant = backend.servant
    assert idx_sub.index_channel == servant.index_channel(
        rank_ref, index_name, group={"zone": 1}
    )
    assert idx_sub.index_channel != servant.index_channel(
        rank_ref, index_name, group={"zone": 2}
    )
    await _insert(backend, comp, {"zone": 2, index_name: 7})
    assert await broker.get_updates(timeout=0.5) == {}
    (new_id,) = await _insert(backend, comp, {"zone": 1, index_name: 8})
    await updates_until(broker, lambda u: u[sub_id][new_id][index_name] == 8)


async def test_group_subscription_numeric_string(broker, rank_ref, admin_ctx):
    """
    整数分组列收数字字符串（同单列索引）：订的是同一组的频道，推送照常。sub_id 按原值算，和整数
    写法是两个订阅（结果一样，只是不共享）
    """
    backend = broker._backend
    comp = rank_ref.comp_cls
    await _insert(backend, comp, {"zone": 1, "level": 5}, {"zone": 2, "level": 6})
    sub_id, rows = await broker.subscribe_range(
        rank_ref, admin_ctx, "level", 0, 100, limit=10, group={"zone": "1"}
    )
    assert sub_id and [r["level"] for r in rows] == [5]
    idx_sub = cast(IndexSubscription, broker._subs[sub_id])
    assert idx_sub.index_channel == backend.servant.index_channel(
        rank_ref, "level", group={"zone": 1}
    )
    int_sub_id, int_rows = await broker.subscribe_range(
        rank_ref, admin_ctx, "level", 0, 100, limit=10, group={"zone": 1}
    )
    assert int_sub_id and int_sub_id != sub_id and int_rows == rows
    get_id, row = await broker.subscribe_get(
        rank_ref, admin_ctx, "level", 5, group={"zone": "1"}
    )
    assert get_id and row and row["level"] == 5
    (new_id,) = await _insert(backend, comp, {"zone": 1, "level": 7})
    await updates_until(broker, lambda u: u[sub_id][new_id]["level"] == 7)


@pytest.mark.parametrize("desc", [False, True])
async def test_group_subscription_leave_backfills(broker, rank_ref, admin_ctx, desc):
    """
    结果里的行换组、被删：这一组 key 上的 ZREM 叫醒订阅，推 None 并补进被 limit 截在外面的行
    （同单列值频道的 test_point_query_leave_backfills_limit，那边靠行频道发现离开）
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
    """组内本字段变化让截断窗口外的行挤进来：这一组 key 上有 ZREM / ZADD，订阅重跑比对，被挤出的推 None"""
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


async def test_group_channel_api(rank_ref, mod_auto_backend):
    """
    index_channel：复合索引要给全分组值，单列索引不能给 group；复合索引没有值频道（commit 不给
    它发），调用报错并指向 index_channel
    """
    servant = mod_auto_backend().servant
    with pytest.raises(ValueError, match="分组"):
        servant.index_channel(rank_ref, "level")
    with pytest.raises(ValueError, match="正好给出"):
        servant.index_channel(rank_ref, "level", group={"zone": 1, "qty": 2})
    with pytest.raises(ValueError, match="不是复合索引"):
        servant.index_channel(rank_ref, "id", group={"zone": 1})
    with pytest.raises(ValueError, match="index_channel"):
        servant.index_value_channel(rank_ref, "level", 3)
    assert servant.index_channel(rank_ref, "level", group={"zone": 1}) == (
        servant.index_channel(rank_ref, "level", group={"zone": "1"})
    )


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
        ({"group": {"zone": "x"}}, "不是数字"),
        ({"group": {"zone": None}}, "只收数字"),
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
        level: np.int32 = property_field(0, groupby="zone")

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


def def_team_score():
    """RLS 组件：同队可见（team == ctx.user_data["team"]），按 zone 分组、组内按浮点 score 排序"""
    from hetu.data import BaseComponent, Permission, define_component, property_field

    @define_component(
        namespace="pytest",
        permission=Permission.RLS,
        rls_compare=("eq", "team", "team"),
        force=True,
    )
    class TeamScore(BaseComponent):
        team: np.int32 = property_field(0)
        zone: np.int32 = property_field(0)
        score: np.float32 = property_field(0.0, groupby="zone")
        qty: np.int32 = property_field(0)

    return TeamScore


def _team_ctx(team: int):
    from fixtures.contexts import make_ctx

    ctx = make_ctx(caller=10)
    ctx.user_data["team"] = team
    return ctx


async def _set(backend: Backend, comp, row_id: int, **fields) -> None:
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(id=row_id)
        assert row is not None
        for key, value in fields.items():
            row[key] = value
        await repo.update(row)


async def test_group_subscription_rls_float_field(
    broker, new_component_env, mod_auto_backend
):
    """
    RLS 组件按组订阅、本字段是 float32：订阅按连接私有；初始行只有同队的，按浮点值排（负数在前），
    闭区间上界 0.1 按 float32 取整才命中存进去的 0.1。补读后同组里别队的行也订上行频道但不推；
    它改成同队（不经索引）时推出来，同队的行改成别队时推 None；本字段改动让行进入区间时推出来、
    按浮点值排进结果；别的组的写入、本组别队的行插入都不推
    """
    backend = broker._backend
    comp = def_team_score()
    ref = create_ref(comp, backend)
    a, other_team, c, d, _zone2 = await _insert(
        backend,
        comp,
        {"team": 1, "zone": 1, "score": -1.5},
        {"team": 2, "zone": 1, "score": -0.5},
        {"team": 1, "zone": 1, "score": 0.1},
        {"team": 1, "zone": 1, "score": 2.75},
        {"team": 1, "zone": 2, "score": 0.0},
    )
    ctx = _team_ctx(1)
    sub_id, rows = await broker.subscribe_range(
        ref, ctx, "score", -10, 0.1, limit=10, group={"zone": 1}
    )
    assert sub_id and [r["id"] for r in rows] == [a, c]
    assert rows[1]["score"] == pytest.approx(0.1)
    idx_sub = cast(IndexSubscription, broker._subs[sub_id])
    assert idx_sub.rls_ctx is ctx
    assert idx_sub.index_channel == backend.servant.index_channel(
        ref, "score", group={"zone": 1}
    )
    # 补读过后：别队的行也在结果里、订上了行频道，但不推给客户端
    assert await broker.get_updates(timeout=0.5) == {}
    assert idx_sub.last_range_result == {a, other_team, c}
    assert backend.servant.row_channel(ref, other_team) in idx_sub.row_subs

    # 别的组的写入叫不醒它；本组插入别队的行叫醒了也不推
    await _insert(backend, comp, {"team": 1, "zone": 2, "score": -1.0})
    (hidden_new,) = await _insert(backend, comp, {"team": 2, "zone": 1, "score": -3.0})
    assert await broker.get_updates(timeout=0.5) == {}
    assert hidden_new in idx_sub.last_range_result

    # 不经索引得失 RLS：改 team，从行频道推出来 / 推 None
    await _set(backend, comp, other_team, team=1)
    await _set(backend, comp, c, team=2)

    def rls_changed(updates):
        got = updates[sub_id]
        assert got[other_team]["score"] == pytest.approx(-0.5)
        assert got[c] is None

    await updates_until(broker, rls_changed)

    # 本字段改动：2.75 → -2.0 进入区间，排在 -3.0（别队）与 -1.5 之间
    await _set(backend, comp, d, score=-2.0)
    await updates_until(broker, lambda u: u[sub_id][d]["score"] == -2.0)
    assert idx_sub.order == [hidden_new, d, a, other_team, c]


async def test_group_subscribe_get_rls_float_field(
    broker, new_component_env, mod_auto_backend
):
    """RLS 组件按组订单行、本字段是 float32：按 float32 取整后点查，别队的行订不到"""
    backend = broker._backend
    comp = def_team_score()
    ref = create_ref(comp, backend)
    mine, _theirs = await _insert(
        backend,
        comp,
        {"team": 1, "zone": 1, "score": 0.1},
        {"team": 2, "zone": 1, "score": 0.3},
    )
    ctx = _team_ctx(1)
    sub_id, row = await broker.subscribe_get(ref, ctx, "score", 0.1, group={"zone": 1})
    assert sub_id and row and row["id"] == mine
    assert await broker.subscribe_get(ref, ctx, "score", 0.3, group={"zone": 1}) == (
        None,
        None,
    )
    assert await broker.subscribe_get(ref, ctx, "score", 0.1, group={"zone": 2}) == (
        None,
        None,
    )
    await _set(backend, comp, mine, qty=5)
    await updates_until(broker, lambda u: u[sub_id][mine]["qty"] == 5)


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
