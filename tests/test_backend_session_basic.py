#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

from typing import Callable

import numpy as np
import pytest
from fixtures.backends import use_redis_family_backend_only
from redis.asyncio.cluster import RedisCluster

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RaceCondition, UniqueViolation
from hetu.data.backend.session import Session

SnowflakeID().init(1, 0)


async def test_repo_outside(mod_item_model):
    """测试SessionRepo在未进入Session上下文时抛出异常。"""
    backend = Backend.__new__(Backend)
    backend._master = None  # type: ignore
    session = Session(backend, "pytest", 1)
    async with session as session:
        item_repo = session.using(mod_item_model)

    with pytest.raises(AssertionError, match="Session"):
        await item_repo.range(limit=10, id=(10, 5))


async def test_basic_crud(item_ref, mod_auto_backend: Callable[..., Backend]):
    """测试基本的CRUD操作。"""
    backend = mod_auto_backend()
    # 测试插入数据
    row_ids = []
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        # 插入3行数据
        item_repo = session.using(item_ref.comp_cls)
        row = item_ref.comp_cls.new_row()
        row.name = "Item1"
        row.owner = 1
        row.time = 1
        row_ids.append(row.id)
        await item_repo.insert(row)

        row = item_ref.comp_cls.new_row()
        row.name = "Item2"
        row.owner = 1
        row.time = 2
        row_ids.append(row.id)
        await item_repo.insert(row)

        row = item_ref.comp_cls.new_row()
        row.name = "Item3"
        row.owner = 2
        row.time = 3
        row_ids.append(row.id)
        await item_repo.insert(row)

        # 测试刚添加的缓存
        np.testing.assert_array_equal((await item_repo.get(id=row_ids[2])).time, 3)  # type: ignore

        # 测试提前commit
        await session.commit()

        # 测试Session退出后的repo报错
        with pytest.raises(AssertionError, match="Session"):
            await item_repo.range(limit=10, id=(10, 5))

    # 测试基本的range和get
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_repo = session.using(item_ref.comp_cls)
        result = await item_repo.range(limit=10, id=(-np.inf, +np.inf))
        np.testing.assert_array_equal(result.id, row_ids)
        assert (await item_repo.get(id=row_ids[0])).name == "Item1"  # type: ignore
        # 测试第一行dict get不出来的历史bug
        assert (await item_repo.get(name="Item1")).name == "Item1"  # type: ignore
        assert type(await item_repo.get(name="Item1")) is not np.recarray

    # 测试update是否正确
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_repo = session.using(item_ref.comp_cls)
        row = await item_repo.get(id=row_ids[0])
        assert row
        row.qty = 2
        await item_repo.update(row)
        # 测试刚update的缓存
        np.testing.assert_array_equal((await item_repo.get(id=row_ids[0])).qty, 2)  # type: ignore
    # 测试写入后
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_repo = session.using(item_ref.comp_cls)
        np.testing.assert_array_equal((await item_repo.get(id=row_ids[0])).qty, 2)  # type: ignore

    # 测试delete是否正确
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_repo = session.using(item_ref.comp_cls)
        with pytest.raises(LookupError):
            item_repo.delete(row_ids[1])

        await item_repo.get(id=row_ids[1])  # 确保缓存中有数据
        item_repo.delete(row_ids[1])
        # 测试刚delete的缓存
        result = await item_repo.range(limit=10, id=(-np.inf, +np.inf))
        np.testing.assert_array_equal(result.id, [row_ids[0], row_ids[2]])


async def test_insert_unique(item_ref, mod_auto_backend: Callable[..., Backend]):
    """测试插入Unique重复数据"""
    from hetu.data.backend import UniqueViolation

    backend = mod_auto_backend()
    row_ids = []

    # 测试本地缓存中unique违反：
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_repo = session.using(item_ref.comp_cls)
        row = item_ref.comp_cls.new_row()
        row.name = "Item1"
        row.owner = 1
        row.time = 1
        row_ids.append(row.id)
        await item_repo.insert(row)

        row = item_ref.comp_cls.new_row()
        row.name = "Item2"
        row.owner = 2
        row.time = 2
        row_ids.append(row.id)
        await item_repo.insert(row)

        # 同事务内重复：本地 IdentityMap 检查，在 insert()/update() 处立即抛
        # update 重复time
        row.name = "Item2"
        row.owner = 2
        row.time = 1
        with pytest.raises(UniqueViolation, match="time"):
            await item_repo.update(row)

        # insert 重复name
        row = item_ref.comp_cls.new_row()
        row.name = "Item1"
        row.owner = 3
        row.time = 3
        with pytest.raises(UniqueViolation, match="name"):
            await item_repo.insert(row)
        # insert 重复time
        row.name = "Item3"
        row.owner = 3
        row.time = 1
        with pytest.raises(UniqueViolation, match="time"):
            await item_repo.insert(row)

    # 与库中既有数据冲突：insert()/update() 不再预检，改由 commit（async with 退出）原子判定；
    # 本事务未曾 get 观察其不存在 → 确定性 UniqueViolation，从 async with 退出处抛
    with pytest.raises(UniqueViolation, match="name"):
        async with backend.session("pytest", 1) as session:
            session.only_master = True
            item_repo = session.using(item_ref.comp_cls)
            row = item_ref.comp_cls.new_row()
            row.name = "Item2"
            row.owner = 2
            row.time = 999
            await item_repo.insert(row)  # 此处不再抛

    with pytest.raises(UniqueViolation, match="time"):
        async with backend.session("pytest", 1) as session:
            session.only_master = True
            item_repo = session.using(item_ref.comp_cls)
            row = item_ref.comp_cls.new_row()
            row.name = "Item4"
            row.time = 2
            await item_repo.insert(row)

    # update 改成既有 unique 值，同理在 commit 处抛
    with pytest.raises(UniqueViolation, match="time"):
        async with backend.session("pytest", 1) as session:
            session.only_master = True
            item_repo = session.using(item_ref.comp_cls)
            row = await item_repo.get(name="Item2")
            assert row
            row.time = 1
            await item_repo.update(row)


async def test_unique_blind_insert_is_violation_no_retry(item_ref, mod_auto_backend):
    """规则1：盲 insert 既有 unique 值 → commit 抛 UniqueViolation；Session.retry 不重试"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    async with backend.session("pytest", 1) as s:
        r = comp.new_row()
        r.name, r.time = "taken", 1
        await s.using(comp).insert(r)
    await backend.wait_for_synced()

    attempts = 0
    with pytest.raises(UniqueViolation, match="name"):
        async for attempt in backend.session("pytest", 1).retry(3):
            async with attempt as s:
                attempts += 1
                r = comp.new_row()
                r.name, r.time = "taken", 2
                await s.using(comp).insert(r)  # 不在此抛
    assert attempts == 1


async def test_unique_after_get_none_is_race_and_retries(item_ref, mod_auto_backend):
    """规则2：get(name=x) 读空 → 另一 session 插入 x → insert x：commit 判 RaceCondition；
    Session.retry 包住则重试后 get 命中转 update，整体成功。"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    async def intrude(name: str, time: int):
        async with backend.session("pytest", 1) as s:
            r = comp.new_row()
            r.name, r.time, r.qty = name, time, 1
            await s.using(comp).insert(r)

    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as s:
            s.only_master = True
            repo = s.using(comp)
            assert await repo.get(name="x") is None
            await intrude("x", 5001)
            r = comp.new_row()
            r.name, r.time = "x", 5002
            await repo.insert(r)  # 不在此抛，commit 判 RACE

    attempts = 0
    async for attempt in backend.session("pytest", 1).retry(3):
        async with attempt as s:
            s.only_master = True
            attempts += 1
            repo = s.using(comp)
            row = await repo.get(name="y")
            if row is None:
                await intrude("y", 5003)  # 只在第 1 轮抢先
                row = comp.new_row()
                row.name, row.time = "y", 5004
                await repo.insert(row)
            else:
                row.qty = row.qty + 1
                await repo.update(row)
    assert attempts == 2
    async with backend.session("pytest", 1) as s:
        s.only_master = True
        row = await s.using(comp).get(name="y")
        assert row is not None and row.qty == 2


async def test_unique_after_range_none_is_race(item_ref, mod_auto_backend):
    """等值 range 读空与 get 一样登记 absent：撞车判 RaceCondition；区间 range 不登记"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    async def intrude(name: str, time: int):
        async with backend.session("pytest", 1) as s:
            r = comp.new_row()
            r.name, r.time = name, time
            await s.using(comp).insert(r)

    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as s:
            s.only_master = True
            repo = s.using(comp)
            assert (await repo.range(name=("x", "x"), limit=1)).shape[0] == 0
            await intrude("x", 6001)
            r = comp.new_row()
            r.name, r.time = "x", 6002
            await repo.insert(r)

    # 对照：区间查询读空不算 absent 观察，盲写撞车是确定性冲突（关掉区间校验单看这条规则）
    with pytest.raises(UniqueViolation, match="time"):
        async with backend.session("pytest", 1) as s:
            s.only_master = True
            repo = s.using(comp)
            rows = await repo.range(time=(0, 1000), limit=10, phantom_check=False)
            assert rows.shape[0] == 0
            await intrude("z", 5)
            r = comp.new_row()
            r.name, r.time = "w", 5
            await repo.insert(r)

    # 默认开着区间校验：读过的区间被插入了，先判竞态（重试后区间一致，才轮到确定性冲突）
    with pytest.raises(RaceCondition, match="Range"):
        async with backend.session("pytest", 1) as s:
            s.only_master = True
            repo = s.using(comp)
            assert (await repo.range(time=(2000, 3000), limit=10)).shape[0] == 0
            await intrude("y", 2005)
            r = comp.new_row()
            r.name, r.time = "v", 2005
            await repo.insert(r)


async def test_unique_race_has_priority_over_violation(item_ref, mod_auto_backend):
    """规则3：锚定列 absent 冲突 + 其他 unique 列既有值同时冲突 → RaceCondition；
    对照：只有非 absent 列冲突 → UniqueViolation。"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    async with backend.session("pytest", 1) as s:  # 既有 time=100
        r = comp.new_row()
        r.name, r.time = "a", 100
        await s.using(comp).insert(r)
    await backend.wait_for_synced()

    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as s:
            s.only_master = True
            repo = s.using(comp)
            assert await repo.get(name="anchor") is None
            async with backend.session("pytest", 1) as s2:  # 并发插入 anchor
                r2 = comp.new_row()
                r2.name, r2.time = "anchor", 101
                await s2.using(comp).insert(r2)
            r = comp.new_row()
            r.name, r.time = "anchor", 100  # name 竞态 + time 确定性冲突
            await repo.insert(r)

    with pytest.raises(UniqueViolation, match="time"):
        async with backend.session("pytest", 1) as s:
            r = comp.new_row()
            r.name, r.time = "fresh", 100
            await s.using(comp).insert(r)


async def test_unique_race_on_updated_row_has_priority(item_ref, mod_auto_backend):
    """规则3 补充：本事务 update 的行在提交前被别人改过（版本过期）+ 盲 insert 撞既有 unique
    值同时发生 → RaceCondition（重跑事务体可能就不写那个值了），而不是确定性 UniqueViolation"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    async with backend.session("pytest", 1) as s:  # 既有 time=100；p 是待 update 的行
        r = comp.new_row()
        r.name, r.time = "a", 100
        await s.using(comp).insert(r)
        p = comp.new_row()
        p.name, p.time = "p", 200
        await s.using(comp).insert(p)
    await backend.wait_for_synced()

    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as s:
            s.only_master = True
            repo = s.using(comp)
            p = await repo.get(name="p")
            assert p is not None
            async with backend.session("pytest", 1) as s2:  # 并发改了 p
                repo2 = s2.using(comp)
                p2 = await repo2.get(name="p")
                assert p2 is not None
                p2.qty = 7
                await repo2.update(p2)
            r = comp.new_row()
            r.name, r.time = "fresh", 100  # time 确定性冲突
            await repo.insert(r)
            p.qty = 8
            await repo.update(p)  # p 的版本已过期


@pytest.mark.parametrize("backend_name", ["sqlite"], indirect=True)
async def test_unique_ci_collation_multi_candidate_is_deterministic(
    monkeypatch, new_component_env, mod_auto_backend
):
    """SQL 后端遇到大小写不敏感 collation（MariaDB 默认；这里用 SQLite 的 NOCASE 模拟）：
    一个事务盲 insert 两个不同的 name，其中一个按数据库的相等语义撞上既有行（'Alice' vs
    'alice'）→ 必须是确定性 UniqueViolation、只跑一次；不能因为查回的值对不上本地候选就漏判，
    交给 UNIQUE 约束报错后被当成 RaceCondition 反复重试"""
    import sqlalchemy as sa
    from fixtures.testdata import create_ref

    from hetu.data import BaseComponent, Permission, define_component, property_field
    from hetu.data.backend.sql import client as sql_client

    real_type = sql_client._numpy_to_sqla_type

    def ci_type(dtype):
        col_type = real_type(dtype)
        if isinstance(col_type, sa.String):
            return sa.String(length=col_type.length, collation="NOCASE")
        return col_type

    monkeypatch.setattr(sql_client, "_numpy_to_sqla_type", ci_type)

    @define_component(namespace="pytest", permission=Permission.ADMIN)
    class CIName(BaseComponent):
        name: "U8" = property_field("", unique=True, index=True)  # type: ignore  # noqa
        time: np.int64 = property_field(0, unique=True, index=True)

    backend: Backend = mod_auto_backend()
    ref = create_ref(CIName, backend)  # 表在打了 collation 补丁之后建
    async with backend.session("pytest", 1) as s:
        r = CIName.new_row()
        r.name, r.time = "alice", 300
        await s.using(CIName).insert(r)

    attempts = 0
    with pytest.raises(UniqueViolation, match="name"):
        async for attempt in backend.session("pytest", 1).retry(3):
            async with attempt as s:
                attempts += 1
                repo = s.using(ref.comp_cls)
                for name, t in (("Alice", 301), ("Bob", 302)):
                    r = CIName.new_row()
                    r.name, r.time = name, t
                    await repo.insert(r)
    assert attempts == 1


async def test_unique_explicit_id_pk_conflict(item_ref, mod_auto_backend):
    """规则4（headless）：显式 id 撞主键，无 get → UniqueViolation；先 get(id=) 读空再撞 → RaceCondition"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    async with backend.session("pytest", 1) as s:
        s.explicit_ids_only = True
        r = comp.new_row(id_=-77)
        r.name, r.time = "pk", 1
        await s.using(comp).insert(r)
    await backend.wait_for_synced()

    with pytest.raises(UniqueViolation, match=r"Item\.id"):
        async with backend.session("pytest", 1) as s:
            s.explicit_ids_only = True
            r = comp.new_row(id_=-77)
            r.name, r.time = "pk2", 2
            await s.using(comp).insert(r)

    with pytest.raises(RaceCondition, match=r"Item\.id"):
        async with backend.session("pytest", 1) as s:
            s.explicit_ids_only = True
            s.only_master = True
            repo = s.using(comp)
            assert await repo.get(id=-78) is None
            async with backend.session("pytest", 1) as s2:  # 并发插入 -78
                s2.explicit_ids_only = True
                r2 = comp.new_row(id_=-78)
                r2.name, r2.time = "pk3", 3
                await s2.using(comp).insert(r2)
            r = comp.new_row(id_=-78)
            r.name, r.time = "pk4", 4
            await repo.insert(r)


async def test_get_negative_cache(item_ref, mod_auto_backend):
    """get 读空后登记 absent，同一事务内再 get 同一值不再打远程；本地新 insert 的行优先；
    非 unique 索引不登记；upsert 内部的二次 get 因此省一次往返"""
    from unittest.mock import patch

    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    master = backend.master

    async with backend.session("pytest", 1) as session:
        session.only_master = True
        repo = session.using(comp)
        with (
            patch.object(master, "range_read_", wraps=master.range_read_) as m_range,
            patch.object(master, "get", wraps=master.get) as m_get,
        ):
            # unique 列读空：第一次打远程，第二次命中 negative cache
            assert await repo.get(name="nope") is None
            assert m_range.call_count == 1
            assert await repo.get(name="nope") is None
            assert m_range.call_count == 1
            # 主键同理
            assert await repo.get(id=424242) is None
            assert m_get.call_count == 1
            assert await repo.get(id=424242) is None
            assert m_get.call_count == 1
            # 非 unique 索引读空不登记，每次都查
            assert await repo.get(owner=777) is None
            assert await repo.get(owner=777) is None
            assert m_range.call_count == 3

            # 本事务内 insert 曾观察不存在的值后，get 能读到（本地缓存先于 negative cache）
            row = comp.new_row(id_=424242)
            row.name, row.time = "nope", 1
            await repo.insert(row)
            got = await repo.get(name="nope")
            assert got is not None and got.id == 424242
            got = await repo.get(id=424242)
            assert got is not None and got.name == "nope"
            assert m_range.call_count == 3 and m_get.call_count == 1

            # SystemLock 式：get 读空 + upsert 同一锚定值，只打一次远程
            assert await repo.get(name="lock1") is None
            async with repo.upsert(name="lock1") as lock:
                lock.time = 2
            assert m_range.call_count == 4

    # 提交后的数据正确
    async with backend.session("pytest", 1) as session:
        session.only_master = True
        repo = session.using(comp)
        assert (await repo.get(name="lock1")) is not None
        assert (await repo.get(id=424242)) is not None


async def test_upsert(item_ref, mod_auto_backend: Callable[..., Backend]):
    """测试upsert操作"""
    backend = mod_auto_backend()

    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_repo = session.using(item_ref.comp_cls)

        async with item_repo.upsert(name="item1") as row:
            row.time = 1

        async with item_repo.upsert(name="items4") as row:
            row.time = 4

    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_repo = session.using(item_ref.comp_cls)

        async with item_repo.upsert(name="item1") as row:
            assert row.time == 1

        async with item_repo.upsert(name="items4") as row:
            assert row.time == 4


async def test_upsert_by_id_does_not_mint_snowflake(
    item_ref, mod_auto_backend: Callable[..., Backend]
):
    """锚定 id 的 upsert 未命中时直接用锚定值建行，不消耗雪花号（默认模式下也如此）。"""
    backend = mod_auto_backend()
    before = (SnowflakeID().last_timestamp, SnowflakeID().sequence)
    async with backend.session("pytest", 1) as session:
        session.only_master = True
        item_repo = session.using(item_ref.comp_cls)
        async with item_repo.upsert(id=-42) as row:
            row.name = "byid"
            row.time = 42
    assert (SnowflakeID().last_timestamp, SnowflakeID().sequence) == before
    row = await backend.master.get(item_ref, -42)
    assert row is not None and row.name == "byid"


async def test_explicit_ids_only(item_ref, mod_auto_backend: Callable[..., Backend]):
    """explicit_ids_only=True（headless）：不发号——insert 必须带非零 id，
    只有锚定 id 的 upsert 允许新建，锚定其它 unique 字段未命中就报错。"""
    backend = mod_auto_backend()
    comp = item_ref.comp_cls

    async with backend.session("pytest", 1) as session:
        session.only_master = True
        session.explicit_ids_only = True
        item_repo = session.using(comp)

        # insert：id == 0 明确报错，而不是静默发号
        with pytest.raises(ValueError, match="id"):
            await item_repo.insert(comp.new_row(id_=0))
        # 显式 id 正常
        row = comp.new_row(id_=-7)
        row.name = "exp7"
        row.time = 7
        await item_repo.insert(row)

        # upsert 锚定 id：未命中允许新建（不发号）
        async with item_repo.upsert(id=-8) as row:
            row.name = "exp8"
            row.time = 8
        # upsert 锚定 id == 0 也算没给 id
        with pytest.raises(ValueError, match="id"):
            async with item_repo.upsert(id=0) as row:
                row.name = "zero"

        # upsert 锚定其它 unique 字段：未命中 → 报错，命中 → 正常 update
        with pytest.raises(LookupError):
            async with item_repo.upsert(name="nope") as row:
                row.time = 99
        async with item_repo.upsert(name="exp7") as row:
            row.time = 77

    row7 = await backend.master.get(item_ref, -7)
    row8 = await backend.master.get(item_ref, -8)
    assert row7 is not None and row7.time == 77
    assert row8 is not None and row8.name == "exp8"

    # 默认（服务器 / Sandbox）行为不变：upsert 其它 unique 字段未命中会发号新建
    async with backend.session("pytest", 1) as session:
        session.only_master = True
        assert session.explicit_ids_only is False
        async with session.using(comp).upsert(name="auto") as row:
            row.time = 100
            assert row.id > 0


async def test_cache_hit_row_is_a_copy(
    item_ref, mod_auto_backend: Callable[..., Backend]
):
    """同一事务内第二次访问同一行会命中 IdentityMap 缓存，拿到的必须是拷贝而不是缓存视图，
    否则改它等于改缓存里的"旧值"，update / upsert 会误报 No fields changed。
    覆盖 get(id=) / get_by_id / upsert(id=) 三条命中缓存的路径。"""
    backend = mod_auto_backend()
    comp = item_ref.comp_cls
    async with backend.session("pytest", 1) as session:
        session.only_master = True
        repo = session.using(comp)
        row = comp.new_row()
        row.name = "alias"
        row.time = 900
        row.qty = 1
        await repo.insert(row)
        rid = int(row.id)

    async with backend.session("pytest", 1) as session:
        session.only_master = True
        repo = session.using(comp)
        first = await repo.get(name="alias")  # 缓存未命中
        assert first is not None and int(first.id) == rid

        # get_by_id 命中缓存：改了不 update，缓存不能被污染
        hit = await repo.get_by_id(rid)
        assert hit is not None
        hit.qty = 99
        cached, _ = session.idmap.get(repo.ref, rid)
        assert cached is not None and cached.qty == 1

        # get(id=) 命中缓存 → 改 → update 要能判出变化
        row = await repo.get(id=rid)
        assert row is not None
        row.qty = 2
        await repo.update(row)
        reread = await repo.get_by_id(rid)
        assert reread is not None and reread.qty == 2

        # upsert(id=) 命中缓存（内部走 get_by_id）→ 改 → 退出时 update
        async with repo.upsert(id=rid) as r:
            r.qty = 3

    final = await backend.master.get(item_ref, rid)
    assert final is not None and final.qty == 3 and final._version == 2


async def test_range_batches_row_reads(filled_item_ref, mod_auto_backend):
    """range 拿到 id 列表后，缓存未命中的行一次 get_many 批量读回（不逐行 get）；
    命中缓存的行不再读、本事务修改可见、已删除的行排除；结果顺序与索引一致"""
    from unittest.mock import patch

    backend: Backend = mod_auto_backend()
    comp = filled_item_ref.comp_cls
    master = backend.master

    async with backend.session("pytest", 1) as session:
        session.only_master = True
        repo = session.using(comp)
        with (
            patch.object(master, "get_many", wraps=master.get_many) as m_many,
            patch.object(master, "get", wraps=master.get) as m_get,
        ):
            # 25 行全部未命中：1 次 get_many、0 次 get
            rows = await repo.range(owner=(10, 10), limit=100)
            assert rows.shape[0] == 25
            assert m_many.call_count == 1 and m_get.call_count == 0
            assert len(m_many.call_args.args[1]) == 25
            assert list(rows.time) == sorted(rows.time)  # 与索引顺序一致

            # 全部命中缓存：不再读远程
            rows = await repo.range(owner=(10, 10), limit=100)
            assert rows.shape[0] == 25
            assert m_many.call_count == 1

            # 本事务的修改在结果里可见；删除的行被排除
            first = rows[0]
            first.level = 99
            await repo.update(first)
            repo.delete(int(rows[1].id))
            rows = await repo.range(owner=(10, 10), limit=100)
            assert rows.shape[0] == 24
            assert rows[rows.id == first.id].level[0] == 99
            assert m_many.call_count == 1

    # 部分命中：只批量读未命中的那些（上面删掉的是 time=111，这里避开）
    async with backend.session("pytest", 1) as session:
        session.only_master = True
        repo = session.using(comp)
        cached = await repo.range(time=(113, 115), limit=10)
        assert cached.shape[0] == 3
        with patch.object(master, "get_many", wraps=master.get_many) as m_many:
            rows = await repo.range(time=(113, 122), limit=10)
            assert rows.shape[0] == 10
            assert m_many.call_count == 1
            assert len(m_many.call_args.args[1]) == 7


async def test_range_interval(filled_item_ref, mod_auto_backend):
    """测试开闭区间"""
    backend: Backend = mod_auto_backend()

    # 测试range的区间是否正确，表内值参考test_data.py的filled_item_ref夹具
    # time范围为110-134，共25个
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        # 默认查询区间检测
        np.testing.assert_array_equal(
            (await item_repo.range(time=(110, 115))).time, range(110, 116)
        )
        # 左闭右开
        np.testing.assert_array_equal(
            (await item_repo.range(time=("[110", "(115"))).time, range(110, 115)
        )
        # 左开右闭
        np.testing.assert_array_equal(
            (await item_repo.range(time=("(110", "[115"))).time, range(111, 116)
        )
        # 左开右开
        np.testing.assert_array_equal(
            (await item_repo.range(time=("(110", "(115"))).time, range(111, 115)
        )


async def test_range_interval_desc(filled_item_ref, mod_auto_backend):
    """降序的开闭区间与升序相同、只是顺序相反：两端开闭不同时不能互换"""
    backend: Backend = mod_auto_backend()

    # time范围为110-134，name为Itm10-Itm34，见test_data.py的filled_item_ref夹具
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        np.testing.assert_array_equal(
            (await item_repo.range(time=(110, 115), desc=True)).time,
            range(115, 109, -1),
        )
        # 左闭右开
        np.testing.assert_array_equal(
            (await item_repo.range(time=("[110", "(115"), desc=True)).time,
            range(114, 109, -1),
        )
        # 左开右闭
        np.testing.assert_array_equal(
            (await item_repo.range(time=("(110", "[115"), desc=True)).time,
            range(115, 110, -1),
        )
        # 左开右开
        np.testing.assert_array_equal(
            (await item_repo.range(time=("(110", "(115"), desc=True)).time,
            range(114, 110, -1),
        )
        # 字符串索引同理
        rows = await item_repo.range(name=("(Itm10", "Itm12"), desc=True)
        assert list(rows.name) == ["Itm12", "Itm11"]


async def test_range_infinite(filled_item_ref, mod_auto_backend):
    """测试np.inf作为范围值"""
    backend: Backend = mod_auto_backend()

    # 测试range的区间是否正确，表内值参考test_data.py的filled_item_ref夹具
    # 测试int索引的inf范围，time范围为110-134，共25个
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        # 左无限右有限
        np.testing.assert_array_equal(
            (await item_repo.range(time=(-np.inf, 115))).time, range(110, 116)
        )
        # 左有限右无限
        np.testing.assert_array_equal(
            (await item_repo.range(time=(120, np.inf))).time, range(120, 130)
        )
        # 左无限右无限
        np.testing.assert_array_equal(
            (await item_repo.range(time=(-np.inf, np.inf), limit=100)).time,
            range(110, 135),
        )

    # 测试float索引的inf范围，model范围为0.0-2.4，共25个
    # （MySQL/MariaDB 不接受 inf 绑定参数，后端须把 float 列的 ±inf 钳到 dtype 极值）
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        np.testing.assert_array_almost_equal(
            (await item_repo.range(time=(-np.inf, np.inf), limit=99)).model,
            np.arange(0, 2.5, 0.1),
        )
        np.testing.assert_array_almost_equal(
            (await item_repo.range(model=(1.05, np.inf), limit=99)).model,
            np.arange(1.1, 2.5, 0.1),
        )
        np.testing.assert_array_almost_equal(
            (await item_repo.range(model=(-np.inf, 0.45), limit=99)).model,
            np.arange(0, 0.5, 0.1),
        )
        np.testing.assert_array_almost_equal(
            (await item_repo.range(model=(-np.inf, np.inf), limit=99)).model,
            np.arange(0, 2.5, 0.1),
        )

    # 测试字符串类型的无限不允许
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        with pytest.raises(ValueError, match="str"):
            np.testing.assert_array_equal(
                (await item_repo.range(name=(-np.inf, "Itm15"))).time, range(110, 116)
            )


async def test_range_number_index(filled_item_ref, mod_auto_backend):
    """测试number类型索引的各种range查询"""
    backend: Backend = mod_auto_backend()

    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        ids = (await item_repo.range(id=(-np.inf, np.inf), limit=999)).id

    # 测试各种query是否正确，表内值参考test_data.py的filled_item_ref夹具
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        # time范围为110-134，共25个
        np.testing.assert_array_equal(
            (await item_repo.range(time=(110, 115))).time, range(110, 116)
        )
        np.testing.assert_array_equal(
            (await item_repo.range(time=(110, 115), desc=True)).time,
            range(115, 109, -1),
        )
        # range owner单项和limit
        assert (await item_repo.range(owner=(10, 10))).shape[0] == 10
        assert (await item_repo.range(owner=(10, 10), limit=30)).shape[0] == 25
        assert (await item_repo.range(owner=(10, 10), limit=8)).shape[0] == 8
        assert (await item_repo.range(owner=(11, 11))).shape[0] == 0
        # range id
        np.testing.assert_array_equal(
            (await item_repo.range(id=(ids[5], ids[10]), limit=999)).id, ids[5:11]
        )
        # 测试range的方向反了
        with pytest.raises(ValueError, match="下界大于上界"):
            await item_repo.range(time=(115, 110))
        # 测试float类型索引
        np.testing.assert_array_equal(
            (await item_repo.range(model=(1.1, 2.3), limit=99)).time,
            range(121, 134),
        )


async def test_query_string_index(filled_item_ref, mod_auto_backend):
    """测试string类型索引的各种query查询"""
    backend: Backend = mod_auto_backend()

    # 测试各种query是否正确，表内值参考test_data.py的filled_item_ref夹具
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        # range string with int
        with pytest.raises(ValueError, match="str"):
            assert (await item_repo.range(name=(11, 11))).shape[0] == 0
        # range on str typed unique
        assert (await item_repo.range(name=("11", "11"))).shape[0] == 0
        assert (row11 := await item_repo.range(name=("Itm11", "Itm11"))).shape[0] == 1
        assert (await item_repo.range(name=("Itm11", "Itm11"))).time == 111
        # get on name index
        row11_13 = await item_repo.range(name=("Itm11", "Itm13"))
        assert (await item_repo.get(name="Itm11")).id == row11.id  # type: ignore
        assert (await item_repo.get(name="Itm13")).id == row11_13.id[-1]  # type: ignore
        np.testing.assert_array_equal(
            (await item_repo.range(name=("Itm11", "Itm12"))).time, [111, 112]
        )
        # reverse range one row
        assert (await item_repo.range(time=(111, 111))).name == ["Itm11"]
        assert len((await item_repo.range(time=(111, 111))).name) == 1


async def test_string_index_colon_no_collision(item_ref, mod_auto_backend):
    """发现2回归：字符串索引曾用 ':' 分隔 value/row_id，value 内含 ':' 会串扰。
    查询 value=='boss' 不应命中 'boss:1' 等；unique 也不应被 ':' 串扰。"""
    backend = mod_auto_backend()

    async with backend.session("pytest", 1) as session:
        repo = session.using(item_ref.comp_cls)
        for i, nm in enumerate(["boss", "boss:1", "boss:99", "bossX"]):
            row = item_ref.comp_cls.new_row()
            row.name = nm
            row.time = 300 + i  # time 也是 unique，给不同值
            await repo.insert(row)
    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        repo = session.using(item_ref.comp_cls)
        # 精确查询只能命中自己，不能带出 'boss:1'/'boss:99'
        assert set(
            map(str, (await repo.range(name=("boss", "boss"), limit=99)).name)
        ) == {"boss"}
        assert str((await repo.get(name="boss")).name) == "boss"
        # 含 ':' 的值本身也能被精确查询
        assert set(
            map(str, (await repo.range(name=("boss:1", "boss:1"), limit=99)).name)
        ) == {"boss:1"}

    # unique 串扰：已存在 'zzz:1' 时插入 'zzz' 不应被误判为唯一冲突
    async with backend.session("pytest", 1) as session:
        repo = session.using(item_ref.comp_cls)
        row = item_ref.comp_cls.new_row()
        row.name = "zzz:1"
        row.time = 400
        await repo.insert(row)
    async with backend.session("pytest", 1) as session:
        repo = session.using(item_ref.comp_cls)
        row = item_ref.comp_cls.new_row()
        row.name = "zzz"
        row.time = 401
        await repo.insert(row)  # 旧编码会在此误报 UniqueViolation
    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        repo = session.using(item_ref.comp_cls)
        assert str((await repo.get(name="zzz")).name) == "zzz"
        assert str((await repo.get(name="zzz:1")).name) == "zzz:1"


async def test_string_index_variable_length_ordering(item_ref, mod_auto_backend):
    """潜伏 bug 回归：变长字符串索引的 range 查询必须保持字符串字典序。
    旧 ':'(0x3A) 分隔符会让『更短的前缀』排到『后接 <0x3A 字符(如数字)的更长值』之后，
    导致 range 漏查/乱序（例如 'a' 会排到 'a0' 之后，range('a','ab') 会漏掉 'a0'/'a9'）。"""
    backend = mod_auto_backend()

    # 'a' 是 'a0'/'a9'/'ab' 的前缀；'0'(0x30)、'9'(0x39) 都 < ':'(0x3A)
    names = ["a", "a0", "a9", "ab"]
    async with backend.session("pytest", 1) as session:
        repo = session.using(item_ref.comp_cls)
        for i, nm in enumerate(names):
            row = item_ref.comp_cls.new_row()
            row.name = nm
            row.time = 500 + i  # time 也是 unique
            await repo.insert(row)
    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        repo = session.using(item_ref.comp_cls)
        # 升序：必须按字符串序返回且不漏（旧 ':' 方案会漏 'a0'/'a9'）
        rows = await repo.range(name=("a", "ab"), limit=99)
        assert [str(n) for n in rows.name] == ["a", "a0", "a9", "ab"]
        # 降序：必须是严格逆序
        rows_desc = await repo.range(name=("a", "ab"), limit=99, desc=True)
        assert [str(n) for n in rows_desc.name] == ["ab", "a9", "a0", "a"]


async def test_query_bool(filled_item_ref, mod_auto_backend):
    """测试bool类型索引的各种query查询"""
    backend: Backend = mod_auto_backend()

    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        row1 = await item_repo.get(time=115)
        assert row1
        row1.used = True
        await item_repo.update(row1)
        row2 = await item_repo.get(time=117)
        assert row2
        row2.used = True
        await item_repo.update(row2)

    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_repo = session.using(filled_item_ref.comp_cls)
        assert set((await item_repo.range(used=(True, True))).id) == {
            row1.id,
            row2.id,
        }
        np.testing.assert_array_equal(
            (await item_repo.range(used=(False, False), limit=99)).time,
            sorted(set(range(110, 135)) - {115, 117}),
        )
        np.testing.assert_array_equal(
            (await item_repo.range(used=(0, 1), limit=99)).time,
            sorted(set(range(110, 135)) - {115, 117}) + [115, 117],  # 等与1的排后面
        )
        np.testing.assert_array_equal(
            (await item_repo.range(used=(False, True), limit=99)).time,
            sorted(set(range(110, 135)) - {115, 117}) + [115, 117],  # 等与1的排后面
        )

    # delete
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        _ = await item_repo.range(used=(True, True))  # 必须get获得乐观锁
        item_repo.delete(row1.id)
        item_repo.delete(row2.id)

    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_repo = session.using(filled_item_ref.comp_cls)
        assert set((await item_repo.range(used=(True, True))).id) == set()


async def test_string_length_cutoff(filled_item_ref, mod_auto_backend):
    """测试字符串长度截断功能"""
    backend: Backend = mod_auto_backend()

    # 测试插入的字符串超出长度是否截断
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        row = filled_item_ref.comp_cls.new_row()
        row.name = "reinsert2"  # 超出U8长度会被截断
        await item_repo.insert(row)

    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_repo = session.using(filled_item_ref.comp_cls)
        assert (await item_repo.get(name="reinsert")) is not None, (
            "超出U8长度应该要被截断，这里没索引出来说明没截断"
        )

        assert (await item_repo.get(name="reinsert")).id == row.id  # type: ignore
        assert len(await item_repo.range("id", -np.inf, +np.inf, limit=999)) == 26


async def test_batch_delete(filled_item_ref, mod_auto_backend):
    """测试批量删除功能"""
    backend: Backend = mod_auto_backend()

    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        rows = await item_repo.range(id=(-np.inf, +np.inf), limit=999)
        for i in reversed(rows.id[4:-1]):  # 保留前4个和最后一个
            item_repo.delete(i)  # 再删掉

    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_repo = session.using(filled_item_ref.comp_cls)
        np.testing.assert_array_equal(
            (await item_repo.range("id", rows.id[0], rows.id[-1], limit=999)).id,
            rows.id[:4].tolist() + rows.id[-1:].tolist(),
        )
        assert len(await item_repo.range("id", -np.inf, +np.inf, limit=999)) == 5

    # 测试get=None是否正常
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        x = await item_repo.get(id=999)
        assert x is None
        # 不再设计is_exist/exist方法，因为这样无法利用缓存和乐观锁
        x = await item_repo.get("id", rows.id[5])
        assert x is None


async def test_unique_table(new_component_env, mod_auto_backend):
    """另一个unique table测试， 忘记测试啥了"""
    backend: Backend = mod_auto_backend()

    from hetu.data import BaseComponent, define_component, property_field
    from hetu.data.backend import RaceCondition, TableReference

    @define_component(namespace="pytest")
    class UniqueTest(BaseComponent):
        name: "U8" = property_field("", unique=True, index=True)  # noqa # type: ignore
        timestamp: float = property_field(0, unique=False, index=True)

    # 测试连接数据库并创建表
    model_ref = TableReference(UniqueTest, "pytest", 1)
    table_maint = backend.get_table_maintenance()
    try:
        table_maint.create_table(model_ref)
    except RaceCondition:
        table_maint.flush(model_ref, force=True)

    # 测试insert是否正确
    async with backend.session("pytest", 1) as session:
        ut_repo = session.using(UniqueTest)
        row = UniqueTest.new_row()
        first_row_id = row.id
        assert type(row) is not np.ndarray
        await ut_repo.insert(row)

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        ut_repo = session.using(UniqueTest)
        result = await ut_repo.range("id", -np.inf, np.inf)
        assert result.shape[0] == 1

    await backend.wait_for_synced()

    # 测试可用update_or_insert
    async with backend.session("pytest", 1) as session:
        ut_repo = session.using(UniqueTest)
        async with ut_repo.upsert(name="test") as row:
            assert row.name == "test"
            last_row_id = row.id
        async with ut_repo.upsert(name="") as row:
            assert row.id == first_row_id

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        ut_repo = session.using(UniqueTest)
        result = await ut_repo.range("name", "test", "test")
        assert result.id[0] == last_row_id


async def test_upsert_limit(mod_item_model):
    """测试upsert不能用于非unique字段"""
    backend = Backend.__new__(Backend)
    backend._master = None  # type: ignore
    with pytest.raises(AssertionError, match="unique"):
        async with backend.session("pytest", 1) as session:
            item_repo = session.using(mod_item_model)
            async with item_repo.upsert(used=True) as _:
                pass


async def test_session_exception(item_ref, mod_auto_backend):
    """测试Session中途抛出异常时回滚"""
    backend: Backend = mod_auto_backend()

    try:
        async with backend.session("pytest", 1) as session:
            item_repo = session.using(item_ref.comp_cls)
            row = item_ref.comp_cls.new_row()
            row.owner = 123
            await item_repo.insert(row)

            raise Exception("测试异常回滚")

    except Exception as _:  # noqa
        pass

    await backend.wait_for_synced()

    # 验证数据没有被提交
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(item_ref.comp_cls)
        row = await item_repo.range(id=(-np.inf, +np.inf), limit=999)
        assert len(row) == 0


@use_redis_family_backend_only
async def test_redis_empty_index(filled_item_ref, mod_auto_backend, backend_name):
    """测试Redis后端删除所有key后，index key应该为空"""
    backend: Backend = mod_auto_backend()

    # 测试更新name后再把所有key删除后index是否正常为空
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        row = await item_repo.get(time=115)
        assert row
        row.name = "TST1"
        await item_repo.update(row)

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        rows = await item_repo.range("id", -np.inf, +np.inf, limit=999)
        for row in rows:
            item_repo.delete(row.id)

    # time.sleep(1)  # 等待部分key过期
    assert (
        backend.master.io.keys("pytest:Item:{CLU*", target_nodes=RedisCluster.PRIMARIES)  # type: ignore
        == []
    )  # type: ignore


async def test_unique_batch_add_in_same_session_bug(item_ref, mod_auto_backend):
    """测试同事务中插入多个重复Unique数据应该报错（和test_unique重复了，但这是最早版本的bug）"""
    backend: Backend = mod_auto_backend()

    # 同事务中插入多个重复Unique数据应该失败
    with pytest.raises(UniqueViolation, match="name"):
        async with backend.session("pytest", 1) as session:
            item_repo = session.using(item_ref.comp_cls)

            row = item_ref.comp_cls.new_row()
            row.name = "Item1"
            row.time = 1
            await item_repo.insert(row)

            row = item_ref.comp_cls.new_row()
            row.name = "Item1"
            row.time = 2
            await item_repo.insert(row)

    with pytest.raises(UniqueViolation, match="time"):
        async with backend.session("pytest", 1) as session:
            item_repo = session.using(item_ref.comp_cls)

            row = item_ref.comp_cls.new_row()
            row.name = "Item1"
            row.time = 1
            await item_repo.insert(row)

            row = item_ref.comp_cls.new_row()
            row.name = "Item2"
            row.time = 2
            await item_repo.insert(row)

            row = item_ref.comp_cls.new_row()
            row.name = "Item3"
            row.time = 2
            await item_repo.insert(row)


async def test_unique_batch_upsert_in_same_session_bug(item_ref, mod_auto_backend):
    """测试同事务中upsert多个重复Unique数据应该报错的bug"""
    backend: Backend = mod_auto_backend()

    # 同事务中upsert多个重复Unique数据时，应该成功
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(item_ref.comp_cls)

        async with item_repo.upsert(name="Item1") as row:
            row.time = 1
            last_row_id = row.id

        async with item_repo.upsert(name="Item1") as row:
            row.time = 2
            assert row.id == last_row_id


async def test_unique_remove_then_add_bug(item_ref, mod_auto_backend):
    """测试删除Unique数据后再插入相同Unique数据是否成功"""
    backend: Backend = mod_auto_backend()

    # 删除Unique数据后再插入相同Unique数据应该成功
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(item_ref.comp_cls)

        row = item_ref.comp_cls.new_row()
        row.name = "Item1"
        row.time = 1
        await item_repo.insert(row)

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        item_repo = session.using(item_ref.comp_cls)
        fetched_row = await item_repo.get(name="Item1")
        assert fetched_row is not None
        item_repo.delete(fetched_row.id)

        row = item_ref.comp_cls.new_row()
        row.name = "Item1"
        row.time = 2
        await item_repo.insert(row)


async def test_session_insert_then_upsert(item_ref, mod_auto_backend):
    """测试在同一Session中insert后立即upsert同一Unique字段的数据"""
    backend: Backend = mod_auto_backend()

    async with backend.session("pytest", 1) as session:
        item_repo = session.using(item_ref.comp_cls)

        row = item_ref.comp_cls.new_row()
        row.name = "Item1"
        row.time = 1
        await item_repo.insert(row)

        context = item_repo.upsert(name="Item1")

        async with context as upserted_row:
            assert upserted_row.id == row.id
            assert upserted_row.time == 1
            assert context.insert is False


# ============ is_unique_conflicts：可选的提前 unique 检查 ============


async def test_is_unique_conflicts_local_and_remote(item_ref, mod_auto_backend):
    """同事务内已有同值行、库里已有同值行，都是确定性冲突 (field, False)；
    都不撞时返回 (None, False)"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    async with backend.session("pytest", 1) as s:
        r = comp.new_row()
        r.name, r.time = "remote", 1
        await s.using(comp).insert(r)
    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as s:
        repo = s.using(comp)
        local = comp.new_row()
        local.name, local.time = "local", 2
        await repo.insert(local)

        row = comp.new_row()
        row.name, row.time = "local", 3  # 撞本事务刚 insert 的行
        assert await repo.is_unique_conflicts(row, insert=True) == ("name", False)
        row.name, row.time = "fresh", 1  # 撞库里既有的行
        assert await repo.is_unique_conflicts(row, insert=True) == ("time", False)
        row.name, row.time = "fresh", 4
        assert await repo.is_unique_conflicts(row, insert=True) == (None, False)


async def test_is_unique_conflicts_observed_absent_is_race(item_ref, mod_auto_backend):
    """本事务 get 观察到某值不存在、之后被别人写入：判为竞态 (field, True)；另一列同时
    是确定性冲突时也优先判竞态。与 commit 时的判定一致"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    async with backend.session("pytest", 1) as s:  # 既有 time=100
        r = comp.new_row()
        r.name, r.time = "a", 100
        await s.using(comp).insert(r)
    await backend.wait_for_synced()

    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as s:
            s.only_master = True
            repo = s.using(comp)
            assert await repo.get(name="anchor") is None
            async with backend.session("pytest", 1) as s2:  # 并发插入 anchor
                r2 = comp.new_row()
                r2.name, r2.time = "anchor", 101
                await s2.using(comp).insert(r2)

            row = comp.new_row()
            row.name, row.time = "anchor", 102
            assert await repo.is_unique_conflicts(row, insert=True) == ("name", True)
            row.time = 100  # time 也撞既有行（确定性），仍优先判竞态
            assert await repo.is_unique_conflicts(row, insert=True) == ("name", True)
            await repo.insert(row)  # commit 也判 RaceCondition


async def test_is_unique_conflicts_ignores_rows_deleted_in_session(
    item_ref, mod_auto_backend
):
    """本事务已 delete 的行占着的 unique 值不算冲突（本地、远程都不算），之后照常插入"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    async with backend.session("pytest", 1) as s:
        r = comp.new_row()
        r.name, r.time = "old", 20
        await s.using(comp).insert(r)
    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as s:
        repo = s.using(comp)
        old = await repo.get(name="old")
        assert old is not None
        repo.delete(old.id)

        row = comp.new_row()
        row.name, row.time = "old", 20
        assert await repo.is_unique_conflicts(row, insert=True) == (None, False)
        await repo.insert(row)
    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as s:
        got = await s.using(comp).get(name="old")
        assert got is not None and got.id == row.id


async def test_is_unique_conflicts_update(item_ref, mod_auto_backend):
    """update 只检查改动了的 unique 列；insert 参数与行是否已在 Session 中不符时断言失败"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    async with backend.session("pytest", 1) as s:
        for name, time in (("a", 1), ("b", 2)):
            r = comp.new_row()
            r.name, r.time = name, time
            await s.using(comp).insert(r)
    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as s:
        repo = s.using(comp)
        row = await repo.get(name="b")
        assert row is not None
        row.qty = 5  # 只改非 unique 列：没有要查的列
        assert await repo.is_unique_conflicts(row) == (None, False)
        row.time = 1  # 改成 a 占着的值
        assert await repo.is_unique_conflicts(row) == ("time", False)

        # 行已在 Session 中却当插入检查、新行却当更新检查
        with pytest.raises(AssertionError):
            await repo.is_unique_conflicts(row, insert=True)
        with pytest.raises(AssertionError):
            await repo.is_unique_conflicts(comp.new_row())


# ============ Session.retry：遇到竞态自动重试 ============


def _no_db_session() -> Session:
    """不连数据库的 Session：事务体不读写时，retry 的控制流与后端无关"""
    backend = Backend.__new__(Backend)
    backend._master = None  # type: ignore
    return Session(backend, "pytest", 1)


@pytest.fixture
def backoff_sleeps(monkeypatch) -> list[float]:
    """记下 retry 每次退避要 sleep 的秒数，不真睡。只替换 session 模块里的 asyncio，
    别的协程照常 sleep"""
    from types import SimpleNamespace

    from hetu.data.backend import session as session_mod

    delays: list[float] = []

    async def fake_sleep(delay):
        delays.append(delay)

    monkeypatch.setattr(session_mod, "asyncio", SimpleNamespace(sleep=fake_sleep))
    return delays


async def test_retry_body_race_exhausted(backoff_sleeps):
    """事务体一直抛 RaceCondition：前几次被吞掉、按指数退避重试，最后一次原样抛出"""
    counts = []
    with pytest.raises(RaceCondition, match="body"):
        async for attempt in _no_db_session().retry(3):
            async with attempt:
                counts.append(attempt.count)
                raise RaceCondition("body")
    assert counts == [1, 2, 3]
    assert backoff_sleeps == [0.1, 0.2]


async def test_retry_stops_after_success(backoff_sleeps):
    """某次成功提交后不再重试"""
    attempts = 0
    async for attempt in _no_db_session().retry(5):
        async with attempt:
            attempts += 1
            if attempts < 3:
                raise RaceCondition("body")
    assert attempts == 3
    assert backoff_sleeps == [0.1, 0.2]


async def test_retry_other_exception_not_retried(backoff_sleeps):
    """RaceCondition 以外的异常不重试，直接抛出"""
    attempts = 0
    with pytest.raises(ValueError, match="boom"):
        async for attempt in _no_db_session().retry(3):
            async with attempt:
                attempts += 1
                raise ValueError("boom")
    assert attempts == 1
    assert backoff_sleeps == []


async def test_retry_backoff_options(backoff_sleeps):
    """次数必须 > 0；backoff 为 None 或返回 0 时重试前不 sleep"""
    from hetu.data.backend.session import AsyncSessionRetryGenerator

    with pytest.raises(ValueError, match="times"):
        _no_db_session().retry(0)

    for backoff in (None, lambda _i: 0):
        attempts = 0
        retry = AsyncSessionRetryGenerator(
            session=_no_db_session(), times=2, backoff=backoff
        )
        async for attempt in retry:
            async with attempt:
                attempts += 1
                if attempts == 1:
                    raise RaceCondition("body")
        assert attempts == 2
    assert backoff_sleeps == []


async def test_retry_commit_race_exhausted(item_ref, mod_auto_backend, backoff_sleeps):
    """每次提交前都被别人抢先改了同一行，commit 一直 RaceCondition：重试耗尽后抛
    RuntimeError，__cause__ 是最后一次的 RaceCondition"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    async with backend.session("pytest", 1) as s:
        r = comp.new_row()
        r.name, r.time = "p", 1
        await s.using(comp).insert(r)
    await backend.wait_for_synced()

    attempts = 0
    with pytest.raises(RuntimeError, match="Exceeded maximum retry") as exc_info:
        async for attempt in backend.session("pytest", 1).retry(3):
            async with attempt as s:
                attempts += 1
                repo = s.using(comp)
                row = await repo.get(name="p")
                assert row is not None
                async with backend.session("pytest", 1) as s2:  # 抢先改掉同一行
                    s2.only_master = True
                    repo2 = s2.using(comp)
                    row2 = await repo2.get(name="p")
                    assert row2 is not None
                    row2.qty = row2.qty + 1
                    await repo2.update(row2)
                row.qty = 100
                await repo.update(row)
    assert attempts == 3
    assert isinstance(exc_info.value.__cause__, RaceCondition)
    assert backoff_sleeps == [0.1, 0.2]


# ============ ctx.session_discard：System 内提前放弃事务 ============


async def test_system_session_discard(
    mod_auto_backend, new_component_env, new_clusters_env
):
    """System 里写入后调用 `ctx.session_discard()`：写入全部放弃不落库，`ctx.repo` 被清空"""
    from hetu.data import BaseComponent, define_component, property_field
    from hetu.manager import ComponentTableManager
    from hetu.system import SystemClusters, SystemContext, define_system
    from hetu.system.caller import SystemCaller

    @define_component(namespace="pytest", force=True)
    class DiscardComp(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)

    repo_after_discard = []

    @define_system(namespace="pytest", components=(DiscardComp,))
    async def write_then_discard(ctx, owner):
        row = DiscardComp.new_row()
        row.owner = owner
        await ctx.repo[DiscardComp].insert(row)
        await ctx.session_discard()
        repo_after_discard.append(dict(ctx.repo))
        return "done"

    SystemClusters().build_clusters("pytest")
    backend = mod_auto_backend()
    tbl_mgr = ComponentTableManager("pytest", "server1", {"default": backend})
    tbl_mgr._flush_all(force=True)

    ctx = SystemContext(
        caller=0,
        connection_id=0,
        address="NotSet",
        group="",
        user_data={},
        timestamp=0,
        request=None,  # type: ignore
        systems=None,  # type: ignore
    )
    ctx.systems = SystemCaller("pytest", tbl_mgr, ctx)
    sys_def = SystemClusters().get_system("write_then_discard", namespace="pytest")
    assert sys_def is not None
    assert await ctx.systems.call_(sys_def, 7) == "done"
    assert repo_after_discard == [{}]

    await backend.wait_for_synced()
    tbl = tbl_mgr.get_table(DiscardComp)
    assert tbl is not None
    async with tbl.session() as session:
        assert await session.using(DiscardComp).get(owner=7) is None
