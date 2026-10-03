"""
range 读到本事务自己的写入（读己之写）：本事务 insert 的行、update 后落进区间的行出现在结果
里，和库里的行按索引顺序排在一起；删掉的、改走了的行不在结果里，也不占 limit 名额。
"""

import numpy as np
import pytest

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RaceCondition

SnowflakeID().init(1, 0)


def _item(comp, *, time: int, name: str, owner: int = 0, level: int = 1, **fields):
    """造一行 Item。time / name 是 unique 列，每行要给不同的值（name 最多 8 个字符）"""
    row = comp.new_row(id_=fields.pop("id_", None))
    row.owner, row.time, row.name, row.level = owner, time, name, level
    for key, value in fields.items():
        row[key] = value
    return row


async def _insert_rows(backend: Backend, comp, *rows) -> None:
    async with backend.session("pytest", 1) as session:
        for row in rows:
            await session.using(comp).insert(row)
    await backend.wait_for_synced()


async def _master_range(backend: Backend, comp, *, desc: bool = False, **query):
    async with backend.session("pytest", 1) as session:
        session.only_master = True
        return await session.using(comp).range(limit=-1, desc=desc, **query)


def _timed(comp, *times: int) -> list:
    """time 各不相同的几行（time、name 都是 unique 列）"""
    return [_item(comp, time=t, name=f"t{t}") for t in times]


@pytest.mark.parametrize("desc", [False, True])
async def test_range_sees_own_inserts(item_ref, mod_auto_backend, desc):
    """本事务 insert 的行落在区间里就出现在结果中，和库里的行按索引顺序排在一起；
    limit 按合并后的顺序截断，开区间、点查同样按区间语义匹配"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, *_timed(comp, 10, 20, 30))

    def order(times):
        return sorted(times, reverse=desc)

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        for row in _timed(comp, 5, 25, 40, 200):  # 200 在区间外
            await repo.insert(row)
        rows = await repo.range(time=(0, 100), limit=-1, desc=desc)
        assert list(rows.time) == order([5, 10, 20, 25, 30, 40])
        rows = await repo.range(time=(0, 100), limit=3, desc=desc)
        assert list(rows.time) == order([5, 10, 20, 25, 30, 40])[:3]
        rows = await repo.range(time=("(5", "[25"), limit=-1, desc=desc)
        assert list(rows.time) == order([10, 20, 25])
        assert list((await repo.range(time=(25, 25))).time) == [25]
        # 主键索引上也一样
        rows = await repo.range(id=(-np.inf, np.inf), limit=-1, desc=desc)
        assert len(rows) == 7
    # 提交后库里读到的和事务里看到的一致
    rows = await _master_range(backend, comp, time=(0, 100), desc=desc)
    assert list(rows.time) == order([5, 10, 20, 25, 30, 40])


async def test_range_check_then_insert_twice_in_txn(item_ref, mod_auto_backend):
    """同一事务里两次调用"range 为空就发一件"：第二次要看到第一次插入的行，只发一件"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    granted: list[int] = []

    async def grant_if_none(repo, time: int):
        rows = await repo.range(owner=(7, 7), limit=-1)
        if len(rows) == 0:
            await repo.insert(_item(comp, owner=7, time=time, name=f"g{time}"))
            granted.append(time)

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        await grant_if_none(repo, 1)
        await grant_if_none(repo, 2)
    assert granted == [1]
    assert len(await _master_range(backend, comp, owner=(7, 7))) == 1


@pytest.mark.parametrize("desc", [False, True])
async def test_range_follows_own_updates(item_ref, mod_auto_backend, desc):
    """本事务改了索引列的行按新值匹配、排在新位置：改出区间的不再返回，改进区间的出现；
    只改了别的列的行留在原位，带着改后的值"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, *_timed(comp, 10, 20, 30, 40))

    def order(times):
        return sorted(times, reverse=desc)

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        rows = await repo.range(time=(0, 35), limit=-1, desc=desc)
        assert list(rows.time) == order([10, 20, 30])
        moved_out = rows[rows.time == 20][0]
        moved_out.time = 99
        await repo.update(moved_out)
        touched = rows[rows.time == 30][0]
        touched.level = 7
        await repo.update(touched)
        moved_in = await repo.get(time=40)  # 库里在区间外
        assert moved_in is not None
        moved_in.time = 15
        await repo.update(moved_in)

        rows = await repo.range(time=(0, 35), limit=-1, desc=desc)
        assert list(rows.time) == order([10, 15, 30])
        assert rows[rows.time == 30].level[0] == 7

        # 区间内换位置：10 改成 33
        first = rows[rows.time == 10][0]
        first.time = 33
        await repo.update(first)
        rows = await repo.range(time=(0, 35), limit=2, desc=desc)
        assert list(rows.time) == order([15, 30, 33])[:2]
    rows = await _master_range(backend, comp, time=(0, 100), desc=desc)
    assert list(rows.time) == order([15, 30, 33, 99])


@pytest.mark.parametrize("desc", [False, True])
async def test_range_own_deletes_do_not_take_limit(item_ref, mod_auto_backend, desc):
    """本事务删掉、改走的行不占 limit 名额：截断读照样返回 limit 行"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    times = [10, 20, 30, 40, 50]
    await _insert_rows(backend, comp, *_timed(comp, *times))
    expect = sorted(times, reverse=desc)

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        rows = await repo.range(time=(0, 100), limit=2, desc=desc)
        assert list(rows.time) == expect[:2]
        repo.delete(int(rows[0].id))
        moved = rows[1]
        moved.time = 500
        await repo.update(moved)
        rows = await repo.range(time=(0, 100), limit=2, desc=desc)
        assert list(rows.time) == expect[2:4]


async def test_range_own_writes_with_phantom_check_off(item_ref, mod_auto_backend):
    """phantom_check=False 只是不校验区间，照样读到本事务的写入；insert 了又删掉的行不出现"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, *_timed(comp, 10, 20, 30))

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        await repo.insert(_timed(comp, 5)[0])
        dropped = _timed(comp, 25)[0]
        await repo.insert(dropped)
        repo.delete(int(dropped.id))
        first = await repo.get(time=10)
        assert first is not None
        repo.delete(int(first.id))
        rows = await repo.range(time=(0, 100), limit=-1, phantom_check=False)
        assert list(rows.time) == [5, 20, 30]
        rows = await repo.range(time=(0, 100), limit=2, phantom_check=False)
        assert list(rows.time) == [5, 20]


@pytest.mark.parametrize("desc", [False, True])
async def test_range_own_inserts_ordered_like_db(item_ref, mod_auto_backend, desc):
    """同一个索引值上，本事务插入的行和库里的行按数据库的顺序（索引里值之后是 id）排：事务里
    看到的顺序与提交后读到的一致。显式给的 id 按字节序排，不按数值"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    existing = _item(comp, owner=5, time=1, name="db")
    await _insert_rows(backend, comp, existing)

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        for i, id_ in enumerate((int(existing.id) - 1, 7, None, None)):
            await repo.insert(_item(comp, owner=5, time=10 + i, name=f"n{i}", id_=id_))
        seen = list((await repo.range(owner=(5, 5), limit=-1, desc=desc)).id)
    committed = list((await _master_range(backend, comp, owner=(5, 5), desc=desc)).id)
    assert len(seen) == 5
    assert seen == committed


async def test_range_unique_point_hits_local_without_db_read(
    item_ref, mod_auto_backend
):
    """unique 列点查：本事务缓存里已经有这个值的行（insert 的、update 改成这个值的、读过的），
    直接返回、不去数据库（同 get）；参数照样校验"""
    from unittest.mock import patch

    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, *_timed(comp, 1, 2, 3))
    master = backend.master

    async with backend.session("pytest", 1) as session:
        session.only_master = True
        repo = session.using(comp)
        await repo.insert(_timed(comp, 4)[0])
        moved = await repo.get(name="t2")
        assert moved is not None
        moved.name = "moved"
        await repo.update(moved)
        assert await repo.get(name="t3") is not None
        await repo.insert(_item(comp, time=11, name="11"))

        with (
            patch.object(master, "range_read_", wraps=master.range_read_) as m_read,
            patch.object(master, "range", wraps=master.range) as m_range,
        ):
            for name, time in (("t4", 4), ("moved", 2), ("t3", 3)):
                assert list((await repo.range(name=(name, name))).time) == [time]
                rows = await repo.range("name", name, desc=True, phantom_check=False)
                assert list(rows.time) == [time]
            assert list((await repo.range(time=(3, 3))).name) == ["t3"]
            moved_id = int(moved.id)
            assert list((await repo.range(id=(moved_id, moved_id))).name) == ["moved"]
            # 字符串列拿数字查照样报错，不因为本地有 name="11" 的行就放过
            with pytest.raises(ValueError, match="str"):
                await repo.range(name=(11, 11))
            assert m_read.call_count == 0 and m_range.call_count == 0
        # 改走了的旧值本地没有：去数据库读到的那行已经不是这个值了
        assert len(await repo.range(name=("t2", "t2"))) == 0


@pytest.mark.parametrize("desc", [False, True])
@pytest.mark.parametrize("where", ["outside", "inside"])
async def test_range_truncated_with_own_rows_observes_visible_only(
    item_ref, mod_auto_backend, desc, where
):
    """合并了本事务的行再截断：只取、只观察到看到的最后一行为止。库里排在后面、没看到的行
    不取也不校验——并发插在看到的末尾之外、改了没看到的行都不算冲突；插进看到的范围里照样
    判竞态"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    a = _item(comp, owner=1, time=1, name="a")
    b = _item(comp, owner=2, time=2, name="b")
    c = _item(comp, owner=3, time=3, name="c")
    await _insert_rows(backend, comp, a, b, c)
    # 升序看到 x(0)、a、b，没看到 c；降序看到 x(4)、c、b，没看到 a
    unseen = a if desc else c
    if where == "outside":
        # 和 b 同值、排在 b 外侧（升序 id 更大，降序 id 更小；a、b、c 的雪花号连号，
        # 减 1 会撞上 a）
        intruder = _item(
            comp, owner=2, time=9, name="i", id_=int(b.id) - 1000 if desc else None
        )
    else:
        intruder = _item(comp, owner=3 if desc else 1, time=9, name="i")

    async def read_then_commit():
        async with backend.session("pytest", 1) as s1:
            repo = s1.using(comp)
            await repo.insert(_item(comp, owner=4 if desc else 0, time=10, name="x"))
            rows = await repo.range(owner=(0, 10), limit=3, desc=desc)
            assert list(rows.name) == (["x", "c", "b"] if desc else ["x", "a", "b"])
            async with backend.session("pytest", 1) as s2:
                repo2 = s2.using(comp)
                await repo2.insert(intruder)
                if where == "outside":
                    row = await repo2.get(id=int(unseen.id))
                    assert row is not None
                    row.level = 9
                    await repo2.update(row)

    if where == "outside":
        await read_then_commit()
    else:
        with pytest.raises(RaceCondition, match="Range"):
            await read_then_commit()


def _spy_cnt_checks(backend: Backend):
    """包住 master.commit_script_，记下每次提交里的 CNT 检查（区间校验）"""
    from unittest.mock import patch

    import msgpack

    master = backend.master
    captured: list[list] = []
    orig_commit_script = master.commit_script_

    async def spy(keys, args):
        checks = msgpack.unpackb(args[0], raw=True)[0]
        captured.append([chk for chk in checks if chk[0] == b"CNT"])
        return await orig_commit_script(keys, args)

    return captured, patch.object(master, "commit_script_", new=spy)


@pytest.mark.parametrize("write", ["insert", "delete"])
async def test_range_reread_after_own_write_sends_one_check(
    item_ref, mod_auto_backend, write
):
    """同一区间在本事务写入前后各截断读一次：两次观察到的库里的行一致（一个包含另一个），
    提交时只发一条区间校验，同一批行不多占一次 master 调用"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, *_timed(comp, 10, 20, 30, 40, 50))
    captured, spy = _spy_cnt_checks(backend)

    with spy:
        async with backend.session("pytest", 1) as session:
            session.only_master = True
            repo = session.using(comp)
            rows = await repo.range(time=(0, 100), limit=3)
            assert list(rows.time) == [10, 20, 30]
            if write == "insert":
                await repo.insert(_timed(comp, 15)[0])
                expect, count = [10, 15, 20], 3  # 第二次只看到 20，被第一次的观察包含
            else:
                repo.delete(int(rows[0].id))
                expect, count = [20, 30, 40], 4  # 第二次多读到 40，包含第一次的观察
            rows = await repo.range(time=(0, 100), limit=3)
            assert list(rows.time) == expect
    [checks] = captured
    assert [chk[4] for chk in checks] == [count]


async def test_range_truncated_reread_after_phantom_is_race(item_ref, mod_auto_backend):
    """同一区间截断读两次、中间被并发插入：两次读到的库里的行对不上，两条校验都要留，判竞态"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, *_timed(comp, 10, 20, 30))

    with pytest.raises(RaceCondition, match="Range"):
        async with backend.session("pytest", 1) as s1:
            s1.only_master = True
            repo = s1.using(comp)
            assert list((await repo.range(time=(0, 100), limit=2)).time) == [10, 20]
            async with backend.session("pytest", 1) as s2:
                await s2.using(comp).insert(_timed(comp, 15)[0])
            assert list((await repo.range(time=(0, 100), limit=2)).time) == [10, 15]
            await repo.insert(_timed(comp, 500)[0])
