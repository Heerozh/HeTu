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


async def test_range_unique_point_hits_db_row_without_db_read(
    item_ref, mod_auto_backend
):
    """unique 列点查：本事务从库里读过、这一列没改过的行就是结果，直接返回、不去数据库
    （同 get），提交时它的版本校验加 unique 保证这个值上仍只有它。本事务写成这个值的行
    （insert 的、改成这个值的）提交前可能又离开这个值，库里也可能本来就有同值的行，照样去
    数据库读。参数照样校验"""
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
        touched = await repo.get(name="t3")
        assert touched is not None
        touched.level = 9  # 改的是别的列
        await repo.update(touched)
        await repo.insert(_item(comp, time=11, name="11"))

        with (
            patch.object(master, "range_read_", wraps=master.range_read_) as m_read,
            patch.object(master, "range", wraps=master.range) as m_range,
        ):
            assert list((await repo.range(name=("t3", "t3"))).time) == [3]
            rows = await repo.range("name", "t3", desc=True, phantom_check=False)
            assert list(rows.time) == [3]
            assert list((await repo.range(time=(3, 3))).name) == ["t3"]
            moved_id = int(moved.id)
            assert list((await repo.range(id=(moved_id, moved_id))).name) == ["moved"]
            assert m_read.call_count == 0 and m_range.call_count == 0
            # 本事务写成这个值的行：去数据库读，合并进结果
            for name, time in (("t4", 4), ("moved", 2)):
                assert list((await repo.range(name=(name, name))).time) == [time]
            assert m_read.call_count == 2
        # 字符串列拿数字查照样报错，不因为本地有 name="11" 的行就放过
        with pytest.raises(ValueError, match="str"):
            await repo.range(name=(11, 11))
        # 改走了的旧值本地没有：去数据库读到的那行已经不是这个值了
        assert len(await repo.range(name=("t2", "t2"))) == 0


@pytest.mark.parametrize("leave", ["delete", "move"])
async def test_range_unique_point_own_row_left_value_is_race(
    item_ref, mod_auto_backend, leave
):
    """unique 列点查读到本事务写成这个值的行（insert 的、改成这个值的），之后它又离开了这个值
    （删掉、改走）：提交时这个值上没有本事务的行兜着了，这期间别的事务插进来的同值行要判竞态"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, _item(comp, time=1, name="w"))

    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as s1:
            s1.only_master = True
            repo = s1.using(comp)
            if leave == "delete":
                mine = _item(comp, time=2, name="x")
                await repo.insert(mine)
            else:
                mine = await repo.get(name="w")
                assert mine is not None
                mine.name = "x"
                await repo.update(mine)
            assert list((await repo.range(name=("x", "x"))).id) == [mine.id]
            async with backend.session("pytest", 1) as s2:
                await s2.using(comp).insert(_item(comp, time=3, name="x"))
            if leave == "delete":
                repo.delete(int(mine.id))
            else:
                mine.name = "z"
                await repo.update(mine)
            await repo.insert(_item(comp, time=4, name="other"))


async def test_range_unique_point_blind_insert_sees_db_row(item_ref, mod_auto_backend):
    """本事务盲插了库里已有的 unique 值：点查连库里那行一起返回，不能只看本地的行（提交时会撞
    unique，但事务里的逻辑在那之前看到的应该是真实情况）"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    existing = _item(comp, time=1, name="x")
    await _insert_rows(backend, comp, existing)

    async with backend.session("pytest", 1) as session:
        session.only_master = True
        repo = session.using(comp)
        mine = _item(comp, time=2, name="x")
        await repo.insert(mine)
        rows = await repo.range(name=("x", "x"))
        assert sorted(rows.id) == sorted([existing.id, mine.id])
        session.discard()


async def test_range_unique_point_reverted_row_is_race(item_ref, mod_auto_backend):
    """unique 列点查直接返回读过的行，靠的是这一行提交时校验版本：改了又改回原值的行也要
    校验，它被别的事务改走、同值又插进新行时判竞态"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    r = _item(comp, time=1, name="a")
    await _insert_rows(backend, comp, r)

    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as s1:
            s1.only_master = True
            repo = s1.using(comp)
            row = await repo.get(id=int(r.id))
            assert row is not None
            row.level = 5
            await repo.update(row)
            row.level = 1
            await repo.update(row)  # 改回原值
            async with backend.session("pytest", 1) as s2:
                repo2 = s2.using(comp)
                renamed = await repo2.get(id=int(r.id))
                assert renamed is not None
                renamed.name = "z"
                await repo2.update(renamed)
            async with backend.session("pytest", 1) as s3:
                await s3.using(comp).insert(_item(comp, time=3, name="a"))
            assert list((await repo.range(name=("a", "a"))).id) == [r.id]
            await repo.insert(_item(comp, time=4, name="u"))


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


async def test_range_merged_row_deleted_while_reading(item_ref, mod_auto_backend):
    """有本事务写入时照样核对读取一致性：读索引之后、取行之前被删的行不在结果里，写事务
    提交时判竞态（同没有写入时的 range）"""
    from unittest.mock import patch

    from hetu.data.backend import InconsistentRangeRead

    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    a = _item(comp, owner=6, time=1, name="a")
    b = _item(comp, owner=6, time=2, name="b")
    await _insert_rows(backend, comp, a, b)
    master = backend.master
    orig_fetch = master.get_many_array_
    fired = False

    async def get_many_array_(*args, **kwargs):
        nonlocal fired
        if not fired:
            fired = True
            async with backend.session("pytest", 1) as intruder:
                intruder.only_master = True
                repo2 = intruder.using(comp)
                assert await repo2.get(id=int(b.id)) is not None
                repo2.delete(int(b.id))
        return await orig_fetch(*args, **kwargs)

    with pytest.raises(InconsistentRangeRead):
        async with backend.session("pytest", 1) as s1:
            s1.only_master = True
            repo = s1.using(comp)
            await repo.insert(_item(comp, owner=6, time=3, name="mine"))
            with patch.object(master, "get_many_array_", new=get_many_array_):
                rows = await repo.range(owner=(6, 6), limit=-1)
            assert sorted(rows.name) == ["a", "mine"]


async def test_range_merged_unique_point_read_empty_is_race(item_ref, mod_auto_backend):
    """有本事务写入时，unique 列点查读空照样登记"观察到不存在"：并发插入同值后本事务再插，
    判竞态（重试），不是确定性的 UniqueViolation。关掉区间校验，单看这条规则"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as s1:
            s1.only_master = True
            repo = s1.using(comp)
            await repo.insert(_item(comp, time=1, name="other"))
            rows = await repo.range(name=("v", "v"), limit=1, phantom_check=False)
            assert len(rows) == 0
            async with backend.session("pytest", 1) as s2:
                await s2.using(comp).insert(_item(comp, time=2, name="v"))
            await repo.insert(_item(comp, time=3, name="v"))


async def test_range_covered_reread_with_reverted_row_is_race(
    item_ref, mod_auto_backend
):
    """同一区间截断读、全读各一次，后一次的观察包含前一次：前一次的区间校验能省，前提是后一次
    读到的行提交时都校验版本。改了又改回原值的行也要校验，否则它被并发改动、挪出前一次的
    区间就没人发现"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    r = _item(comp, time=10, name="r")
    await _insert_rows(backend, comp, r, _item(comp, time=50, name="s"))

    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as s1:
            s1.only_master = True
            repo = s1.using(comp)
            assert list((await repo.range(time=(0, 100), limit=1)).name) == ["r"]
            rows = await repo.range(time=(0, 100), limit=-1)
            assert list(rows.name) == ["r", "s"]
            row = rows[0]
            row.level = 5
            await repo.update(row)
            row.level = 1
            await repo.update(row)  # 改回原值
            await repo.insert(_item(comp, time=500, name="x"))
            async with backend.session("pytest", 1) as s2:
                repo2 = s2.using(comp)
                moved = await repo2.get(id=int(r.id))
                assert moved is not None
                moved.time = 60  # r 不再是区间里的第一行
                await repo2.update(moved)


async def test_range_own_nan_ordered_like_committed(item_ref, mod_auto_backend):
    """浮点索引上本事务写入的 NaN：不论符号位，排序键都和提交后数据库里的一致（提交时 NaN
    一律存成正的），事务里看到的结果和提交后读到的一样"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, _item(comp, time=1, name="a", model=1.0))
    negative_nan = np.copysign(np.float32(np.nan), np.float32(-1))

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        await repo.insert(_item(comp, time=2, name="n", model=negative_nan))
        whole = list((await repo.range(model=(-np.inf, np.inf), limit=-1)).name)
        unit = list((await repo.range(model=(0, 1), limit=-1)).name)
    assert len(whole) == 2
    assert whole == list(
        (await _master_range(backend, comp, model=(-np.inf, np.inf))).name
    )
    assert unit == list((await _master_range(backend, comp, model=(0, 1))).name)


async def test_get_string_value_sees_own_insert(item_ref, mod_auto_backend):
    """数值索引拿字符串值查（如 "10"）：get 和 range 一样按列类型换算后匹配，都看得到本事务
    insert 的行（"get 为空就发一件"调两次只发一件）"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        mine = _item(comp, time=10, name="ten", owner=3)
        await repo.insert(mine)
        for field, value in (("time", "10"), ("owner", "3")):
            got = await repo.get(**{field: value})
            assert got is not None and got.id == mine.id, field
            rows = await repo.range(**{field: (value, value)})
            assert list(rows.id) == [mine.id], field
        session.discard()


@pytest.mark.parametrize("path", ["db", "merged", "cached_point"])
async def test_range_limit_checked_the_same_on_every_path(
    item_ref, mod_auto_backend, path
):
    """limit 的校验不随走哪条路变：numpy 整数照常用，小数、bool 一律报 TypeError"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, *_timed(comp, 1, 2))

    # 区间查询走数据库（本事务有写入时是合并路径），unique 点查命中读过的行时不去数据库
    query = {"time": (0, 10)}
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        if path == "merged":
            await repo.insert(_timed(comp, 3)[0])
        elif path == "cached_point":
            assert await repo.get(name="t1") is not None
            query = {"name": ("t1", "t1")}
        for bad in (1.0, True):
            with pytest.raises(TypeError, match="limit"):
                await repo.range(limit=bad, **query)  # type: ignore
        rows = await repo.range(limit=np.int64(1), **query)  # type: ignore
        assert list(rows.time) == [1]
        session.discard()


async def test_range_huge_limit_after_own_delete(item_ref, mod_auto_backend):
    """limit 用很大的数表示不限（如 sys.maxsize）：本事务删过行、要多读几行时也不溢出"""
    import sys

    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, *_timed(comp, 1, 2, 3))

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        rows = await repo.range(time=(0, 10), limit=sys.maxsize)
        assert list(rows.time) == [1, 2, 3]
        repo.delete(int(rows[0].id))
        rows = await repo.range(time=(0, 10), limit=sys.maxsize)
        assert list(rows.time) == [2, 3]
        session.discard()


async def test_range_merged_phantom_check_off_skips_range_check(
    item_ref, mod_auto_backend
):
    """有本事务写入时 phantom_check=False 同样不校验区间：区间里并发插进来的行不算冲突"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    async with backend.session("pytest", 1) as s1:
        s1.only_master = True
        repo = s1.using(comp)
        await repo.insert(_item(comp, time=1, name="mine"))
        rows = await repo.range(owner=(7, 7), limit=-1, phantom_check=False)
        assert len(rows) == 0
        async with backend.session("pytest", 1) as s2:
            await s2.using(comp).insert(_item(comp, owner=7, time=2, name="other"))
    assert len(await _master_range(backend, comp, owner=(7, 7))) == 1


@pytest.mark.xfail(
    strict=True,
    reason="多读按整张表删改的库里行数算。细算哪些原值在区间里要逐行取快照字段（每行约 "
    "0.7µs，比 master 上多读一个索引项贵约 20 倍），得等按索引增量维护删改行的排序键后再做",
)
async def test_range_reads_extra_only_for_own_rows_in_range(item_ref, mod_auto_backend):
    """本事务删掉、改走的行，只有原值落在查询区间里的才在数据库结果里占位置。删改的行多了，
    多读也只多读这几行，不按整张表删改了多少行去读"""
    from unittest.mock import patch

    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    owners = {1: 1, 2: 1, 3: 1} | {t: 2 for t in range(100, 120)}  # time -> owner
    await _insert_rows(
        backend,
        comp,
        *(_item(comp, owner=o, time=t, name=f"n{t}") for t, o in owners.items()),
    )
    master = backend.master

    async with backend.session("pytest", 1) as session:
        session.only_master = True
        repo = session.using(comp)
        for row in await repo.range(owner=(2, 2), limit=-1):
            repo.delete(int(row.id))
        rows = await repo.range(owner=(1, 1), limit=-1)
        assert list(rows.time) == [1, 2, 3]
        repo.delete(int(rows[0].id))
        moved = rows[1]
        moved.owner = 9  # 原值在 owner=1 上
        await repo.update(moved)
        with patch.object(master, "range_read_", wraps=master.range_read_) as m_read:
            rows = await repo.range(owner=(1, 1), limit=1)
            assert list(rows.time) == [3]
            rows = await repo.range(owner=(1, 1), limit=1, desc=True)
            assert list(rows.time) == [3]
        # 1 行 + owner=1 上删掉、改走的 2 行；owner=2 上删掉的 20 行不在区间里
        assert [call.args[4] for call in m_read.call_args_list] == [3, 3]
        session.discard()
