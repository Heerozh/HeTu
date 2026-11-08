"""
同一事务里在行之间挪 unique 值：提交脚本的 UNIQ 查的是写入前的库，查到的 member 如果本次提交会
ZREM 掉（值被挪到了别的行，比如换槽位），就不算冲突。被挪走的行都有 VER 钉住，库里它的 member
就是本次要 ZREM 的那个。组内 unique（复合索引）的用例见 test_composite_index.py。
"""

import numpy as np
import pytest
from fixtures.backends import raw_index_members
from fixtures.testdata import create_ref

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, TableReference, UniqueViolation

SnowflakeID().init(1, 0)


def def_pair():
    from hetu.data import BaseComponent, Permission, define_component, property_field

    global Pair

    @define_component(namespace="pytest", permission=Permission.USER, force=True)
    class Pair(BaseComponent):
        a: np.int32 = property_field(0, unique=True)
        b: np.int32 = property_field(0, unique=True)
        f: np.float64 = property_field(0.0, unique=True)

    return Pair


@pytest.fixture
async def pair_ref(new_component_env, mod_auto_backend) -> TableReference:
    return create_ref(def_pair(), mod_auto_backend())


async def _insert(backend: Backend, comp, *values: tuple) -> list[int]:
    """values 是 (a, b, f) 元组，每个一行"""
    rows = []
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        for a, b, f in values:
            row = comp.new_row()
            row.a, row.b, row.f = a, b, f
            await repo.insert(row)
            rows.append(int(row.id))
    await backend.wait_for_synced()
    return rows


async def test_move_unique_value_within_txn(pair_ref, mod_auto_backend):
    """挪走之后补位、借中间值互换都能提交；挪到别的行占着、这次又没挪走的值照样 UniqueViolation"""
    backend: Backend = mod_auto_backend()
    comp = pair_ref.comp_cls
    p1, p2 = await _insert(backend, comp, (1, 101, 1.0), (2, 102, 2.0))
    # 挪走之后补位
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(id=p1)
        row.a = 3
        await repo.update(row)
        new = comp.new_row()
        new.a, new.b, new.f = 1, 103, 3.0
        await repo.insert(new)
    # 借中间值互换
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        one, two = await repo.get(id=p1), await repo.get(id=p2)
        one.a = 99
        await repo.update(one)
        two.a = 3
        await repo.update(two)
        one.a = 2
        await repo.update(one)
    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        assert (await repo.get(id=p1)).a == 2
        assert (await repo.get(id=p2)).a == 3
    # p2 不在本事务里，本地拦不住，提交时判冲突
    with pytest.raises(UniqueViolation):
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            row = await repo.get(id=p1)
            row.a = 3
            await repo.update(row)


async def test_move_exemption_is_per_index(pair_ref, mod_auto_backend):
    """
    同一行两个 unique 列同值时 member 字节一样：挪走 a 列的 5，不能把 b 列的 5 也豁免掉。
    先插入、后改那一行，本地检查拦不住，要靠提交脚本按 (索引 key, member) 豁免
    """
    backend: Backend = mod_auto_backend()
    comp = pair_ref.comp_cls
    (r_id,) = await _insert(backend, comp, (5, 5, 5.0))
    with pytest.raises(UniqueViolation):
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            other = comp.new_row()
            other.a, other.b, other.f = 7, 5, 7.0
            await repo.insert(other)
            mine = await repo.get(id=r_id)
            mine.a = 6
            await repo.update(mine)


async def test_nan_sign_update_no_self_collision(pair_ref, mod_auto_backend):
    """unique 浮点列存着 NaN，改成 -NaN：编码相同（NaN 的符号位统一了），不该撞上自己"""
    backend: Backend = mod_auto_backend()
    comp = pair_ref.comp_cls
    (r_id,) = await _insert(backend, comp, (11, 11, np.nan))
    before = raw_index_members(backend, pair_ref, "f")
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        mine = await repo.get(id=r_id)
        mine.f = -np.nan
        await repo.update(mine)
    assert raw_index_members(backend, pair_ref, "f") == before
