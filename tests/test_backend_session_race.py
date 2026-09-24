from unittest.mock import patch

import pytest

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RaceCondition, UniqueViolation

SnowflakeID().init(1, 0)


async def _read_master(backend: Backend, comp, name: str):
    """从 master 读一行（不受副本滞后影响），用来核对嵌套的两个事务谁提交成功了"""
    async with backend.session("pytest", 1) as session:
        session.only_master = True
        row = await session.using(comp).get(name=name)
        assert row is not None
        return row


async def test_version_race(item_ref, mod_auto_backend):
    from hetu.data.backend import RaceCondition

    # 测试竞态。两个事务的先后用嵌套 session 排定：外层先读，内层在其间整个提交，外层最后
    # 提交。不靠两个协程的 sleep 时间差——长 GC 冻住事件循环时先后会乱（见 test_unique_commit_race）
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    # 数据准备
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(comp)
        row = comp.new_row()
        row.owner = 65535
        row.name = "Self"
        row.time = 233874
        await item_repo.insert(row)
        row.id = SnowflakeID().next_id()
        row.name = "ForUpdt"
        row.time += 1
        await item_repo.insert(row)
        row.id = SnowflakeID().next_id()
        row.name = "ForDel"
        row.time += 1
        await item_repo.insert(row)

    await backend.wait_for_synced()

    async def bump_owner(session, name):
        repo = session.using(comp)
        row = await repo.get(name=name)
        assert row
        row.owner = row.owner + 1  # type: ignore
        await repo.update(row)

    # 测试update_owner和read_only不应该激发RaceCondition：只读事务读过的行被别人改了也不报错
    async with backend.session("pytest", 1) as s1:
        assert len(await s1.using(comp).range("owner", 65535)) > 0
        async with backend.session("pytest", 1) as s2:
            await bump_owner(s2, "Self")
    assert (await _read_master(backend, comp, "Self")).owner == 65536

    # 测试update和del竞态是否激发race condition
    await backend.wait_for_synced()
    with pytest.raises(RaceCondition, match="Version"):
        async with backend.session("pytest", 1) as s1:
            repo = s1.using(comp)
            row = await repo.get(name="ForDel")
            assert row
            repo.delete(row.id)
            async with backend.session("pytest", 1) as s2:
                await bump_owner(s2, "ForDel")
    # 内层的修改提交了，外层的删除没有生效
    assert (await _read_master(backend, comp, "ForDel")).owner == 65536

    # 测试update和update竞态是否激发race condition
    await backend.wait_for_synced()
    with pytest.raises(RaceCondition, match="Version"):
        async with backend.session("pytest", 1) as s1:
            await bump_owner(s1, "ForUpdt")
            async with backend.session("pytest", 1) as s2:
                await bump_owner(s2, "ForUpdt")
    assert (await _read_master(backend, comp, "ForUpdt")).owner == 65536

    # 测试update和不同行update不应该冲突
    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as s1:
        await bump_owner(s1, "ForUpdt")
        async with backend.session("pytest", 1) as s2:
            await bump_owner(s2, "Self")
    assert (await _read_master(backend, comp, "ForUpdt")).owner == 65537
    assert (await _read_master(backend, comp, "Self")).owner == 65537


async def test_stale_read_race(item_ref, mod_auto_backend):
    """
    测试陈旧读（stale read）场景：
    事务1先读行A的值，再用读到的值更新行B；
    在事务1提交前，事务2修改了行A。

    依据严格的事务语义（Snapshot/Serializable Isolation），
    事务1此时持有的是A的陈旧快照，提交时应抛出 RaceCondition。
    """
    from hetu.data.backend import RaceCondition

    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    # 数据准备：插入源行A 与目标行B
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(comp)
        row_a = comp.new_row()
        row_a.owner = 1
        row_a.name = "SourceA"
        row_a.time = 100
        row_a.qty = 1
        await item_repo.insert(row_a)

        row_b = comp.new_row()
        row_b.id = SnowflakeID().next_id()
        row_b.owner = 2
        row_b.name = "TargetB"
        row_b.time = 101
        row_b.qty = 0
        await item_repo.insert(row_b)

    await backend.wait_for_synced()

    # 事务1（外层）先读A；事务2（内层）在此期间修改A并提交；然后事务1把A的旧值写入B。
    # 先后用嵌套 session 排定，不靠 sleep 时间差（见 test_version_race）
    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as s1:
            repo1 = s1.using(comp)
            a = await repo1.get(name="SourceA")
            assert a is not None
            stale_qty = int(a.qty)
            async with backend.session("pytest", 1) as s2:
                repo2 = s2.using(comp)
                a2 = await repo2.get(name="SourceA")
                assert a2 is not None
                a2.qty = a2.qty + 99  # type: ignore
                await repo2.update(a2)
            b = await repo1.get(name="TargetB")
            assert b is not None
            b.qty = stale_qty  # type: ignore
            await repo1.update(b)
    # 事务2对A的修改提交了，事务1写B被拒
    assert (await _read_master(backend, comp, "SourceA")).qty == 100
    assert (await _read_master(backend, comp, "TargetB")).qty == 0


async def test_unique_commit_race(item_ref, mod_auto_backend):
    """
    提交时的 unique 冲突判定：盲写（本事务未曾 get 观察其不存在）撞上并发已提交的同值
    → 确定性 UniqueViolation，不重试（重跑事务体只会再写同一个值）。
    """
    from hetu.data.backend import UniqueViolation

    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    # 两个事务的先后用嵌套 session 排定：外层先开事务写入，内层整个提交完，外层退出时才
    # 提交。不能用两个协程靠 sleep 时间差排序——一次长 GC 冻住事件循环几百毫秒，醒来后
    # 两边会几乎同时进入提交，外层的唯一性 SELECT 看不到内层还没提交完的行，只能撞上
    # UNIQUE 约束变成 RaceCondition
    async def insert_row(session, uni_val):
        row = comp.new_row()
        row.owner = 874233
        row.name = str(uni_val)
        row.time = uni_val
        await session.using(comp).insert(row)

    # 测试insert不同的值应该没有竞态
    async with backend.session("pytest", 1) as s1:
        await insert_row(s1, 111111)
        async with backend.session("pytest", 1) as s2:
            await insert_row(s2, 111112)

    # 相同的time：后提交者是确定性冲突
    with pytest.raises(UniqueViolation, match="UNIQUE"):
        async with backend.session("pytest", 1) as s1:
            await insert_row(s1, 222222)
            async with backend.session("pytest", 1) as s2:
                await insert_row(s2, 222222)

    # 测试update提交时把不同行改成同一个unique值：后提交者是确定性冲突
    await backend.wait_for_synced()

    async def update_row(session, name):
        repo = session.using(comp)
        row = await repo.get(name=str(name))
        assert row
        row.time = 874233
        await repo.update(row)

    with pytest.raises(UniqueViolation, match="UNIQUE"):
        async with backend.session("pytest", 1) as s1:
            await update_row(s1, 111111)
            async with backend.session("pytest", 1) as s2:
                await update_row(s2, 111112)


async def test_update_to_value_set_by_concurrent_self_update_is_race(
    item_ref, mod_auto_backend
):
    """
    并发把本行改成了目标值：commit 的 unique 检查命中的是自身行，不能误判 UniqueViolation，
    应由版本检查报 RaceCondition（Redis：VER 排在 UNIQ 前；SQL：SELECT 跳过自身 → UPDATE
    rowcount 为 0）。
    """
    from hetu.data.backend import RaceCondition

    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    async with backend.session("pytest", 1) as s:
        r = comp.new_row()
        r.name, r.time = "self", 1
        await s.using(comp).insert(r)
    await backend.wait_for_synced()

    with pytest.raises(RaceCondition, match="Version"):
        async with backend.session("pytest", 1) as s:
            s.only_master = True
            row = await s.using(comp).get(name="self")
            assert row is not None
            # 并发：另一事务先把同一行的 time 改成 5 并提交
            async with backend.session("pytest", 1) as s2:
                s2.only_master = True
                r2 = await s2.using(comp).get(name="self")
                assert r2 is not None
                r2.time = 5
                await s2.using(comp).update(r2)
            row.time = 5
            await s.using(comp).update(row)


async def test_update_or_insert_race(item_ref, mod_auto_backend):
    from hetu.data.backend import RaceCondition

    backend = mod_auto_backend()
    comp = item_ref.comp_cls

    # 测试update_or_insert UniqueViolation是否转化为了RaceCondition
    # 这其实是本地unique违反，但为了语义上正确，应该换成RaceCondition。
    # upsert 观察到不存在之后，另一个事务（内层）插入同名行并整个提交完，外层才提交
    # （先后用嵌套 session 排定，不靠 sleep 时间差，见 test_version_race）
    with pytest.raises(RaceCondition):
        async with (
            backend.session("pytest", 1) as s1,
            s1.using(comp).upsert(name="uni_vio") as row,
        ):
            async with backend.session("pytest", 1) as s2:
                intruder = comp.new_row()
                intruder.name = "uni_vio"
                intruder.qty = 7
                await s2.using(comp).insert(intruder)
            row.qty = 1
    assert (await _read_master(backend, comp, "uni_vio")).qty == 7


async def test_insert_after_get_none_is_race(item_ref, mod_auto_backend):
    """
    泛化自 upsert 的特例：若本事务先 `get(unique)` 观察到该值不存在，之后 `commit`
    时却发现远程已被并发插入，应判为 `RaceCondition`（基于过期快照的乐观并发失败），
    而非不可重试的 `UniqueViolation`。

    对照 `test_insert_unique`：未先 get、直接 insert 已存在数据，仍是确定性
    `UniqueViolation`。
    """
    from hetu.data.backend import RaceCondition

    backend = mod_auto_backend()
    comp = item_ref.comp_cls

    # 观察者（外层）先 get 到不存在；入侵者（内层）在其间插入并整个提交；观察者再插入
    # （先后用嵌套 session 排定，不靠 sleep 时间差，见 test_version_race）
    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as observer:
            observer.only_master = True  # 强制 master 读，避免 replica 延迟
            repo = observer.using(comp)
            # 观察到 name="zrc" 不存在
            assert await repo.get(name="zrc") is None
            async with backend.session("pytest", 1) as intruder:
                intruder.only_master = True
                row = comp.new_row()
                row.name = "zrc"
                row.time = 7770001  # time 取不同值，确保冲突只发生在 name 上
                await intruder.using(comp).insert(row)
            row = comp.new_row()
            row.name = "zrc"
            row.time = 7770002
            # 远程已被占用且本事务曾观察其不存在 → commit 时判 Race
            await repo.insert(row)
    assert (await _read_master(backend, comp, "zrc")).time == 7770001


async def test_retry_generator(item_ref, mod_auto_backend):
    import asyncio

    backend = mod_auto_backend()
    retry = 0

    async def write_task(name, sleep):
        async for attempt in backend.session("pytest", 1).retry(50):
            nonlocal retry
            retry += 1
            async with attempt as _session:
                _item_repo = _session.using(item_ref.comp_cls)
                async with _item_repo.upsert(name=name) as _row:
                    await asyncio.sleep(sleep)
                    _row.qty = (_row.qty or 1) + 1
                    _row.time = _row.id

    await asyncio.gather(
        write_task("a", 0.1),
        write_task("a", 0.2),
        write_task("a", 0.3),
        write_task("a", 0.4),
    )

    # 检查结果
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(item_ref.comp_cls)
        row = await item_repo.get(name="a")
        assert row.qty == 5

    print("test_retry_generator retry:", retry)
    assert retry > 4  # 应该有重试发生


# ---------------------------------------------------------------------------
# range 的区间校验（防幻读）：写事务里 range 读过的区间，提交时若"同样的查询现在会返回
# 不同的行"就判竞态。见 docs/superpowers/specs/2026-09-24-range-phantom-check-design.md
# ---------------------------------------------------------------------------


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


async def _master_range(backend: Backend, comp, **query):
    async with backend.session("pytest", 1) as session:
        session.only_master = True
        return await session.using(comp).range(limit=-1, **query)


async def test_range_phantom_insert_is_race(item_ref, mod_auto_backend):
    """range 读空后决定插入，提交前别的事务往同一区间插了一行 → 本事务判竞态"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    with pytest.raises(RaceCondition, match="Range"):
        async with backend.session("pytest", 1) as s1:
            repo = s1.using(comp)
            assert len(await repo.range(owner=(7, 7), limit=-1)) == 0
            async with backend.session("pytest", 1) as s2:
                await s2.using(comp).insert(_item(comp, owner=7, time=1, name="other"))
            await repo.insert(_item(comp, owner=7, time=2, name="mine"))
    assert len(await _master_range(backend, comp, owner=(7, 7))) == 1


async def test_range_phantom_retry_converges(item_ref, mod_auto_backend):
    """朴素的"查不到就插、查到就加数量"：中途被并发插入同模板的行，重试后改走 update，
    最终只有一行、数量是两次之和"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    intruded = False
    async for attempt in backend.session("pytest", 1).retry(3):
        async with attempt as session:
            session.only_master = True  # 重试要读到内层刚提交的行，不等副本同步
            repo = session.using(comp)
            items = await repo.range(owner=(7, 7), limit=-1)
            hit = items[items.level == 3]  # level 当道具模板
            if not intruded:
                intruded = True
                async with backend.session("pytest", 1) as s2:
                    first = _item(comp, owner=7, level=3, qty=5, time=1, name="first")
                    await s2.using(comp).insert(first)
            if len(hit) == 0:
                second = _item(comp, owner=7, level=3, qty=2, time=2, name="second")
                await repo.insert(second)
            else:
                row = hit[0]
                row.qty += 2
                await repo.update(row)

    rows = await _master_range(backend, comp, owner=(7, 7))
    assert len(rows) == 1 and rows[0].qty == 7


async def test_range_insert_outside_no_race(item_ref, mod_auto_backend):
    """并发插入落在查询区间外，不算冲突"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    async with backend.session("pytest", 1) as s1:
        repo = s1.using(comp)
        assert len(await repo.range(owner=(7, 7), limit=-1)) == 0
        async with backend.session("pytest", 1) as s2:
            await s2.using(comp).insert(_item(comp, owner=8, time=1, name="other"))
        await repo.insert(_item(comp, owner=7, time=2, name="mine"))
    assert len(await _master_range(backend, comp, owner=(7, 7))) == 1


@pytest.mark.parametrize("desc", [False, True])
async def test_range_truncated_observes_returned_rows_only(
    item_ref, mod_auto_backend, desc
):
    """截断读（返回数 == limit）只观察到最后一个返回行为止：插在它外面不算冲突，插在里面算"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(
        backend, comp, *(_item(comp, time=t, name=f"t{t}") for t in (10, 20, 30, 40))
    )
    # 升序读到 10、20，观察 [0, 20]；降序读到 40、30，观察 [30, 100]
    outside, inside = (25, 35) if desc else (25, 15)

    async def read_then_insert(intruder_time: int):
        async with backend.session("pytest", 1) as s1:
            repo = s1.using(comp)
            rows = await repo.range(time=(0, 100), limit=2, desc=desc)
            assert list(rows.time) == ([40, 30] if desc else [10, 20])
            async with backend.session("pytest", 1) as s2:
                intruder = _item(comp, time=intruder_time, name=f"t{intruder_time}")
                await s2.using(comp).insert(intruder)
            mine = _item(comp, time=1000 + intruder_time, name=f"m{intruder_time}")
            await repo.insert(mine)

    await read_then_insert(outside)
    await backend.wait_for_synced()
    with pytest.raises(RaceCondition, match="Range"):
        await read_then_insert(inside)


async def test_range_own_writes_no_false_race(item_ref, mod_auto_backend):
    """本事务删掉 / 改走 range 返回的行、再往区间里插入：校验看的是写入前的状态，不误判"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(
        backend,
        comp,
        _item(comp, owner=5, time=1, name="a"),
        _item(comp, owner=5, time=2, name="b"),
    )

    async with backend.session("pytest", 1) as s1:
        repo = s1.using(comp)
        rows = await repo.range(owner=(5, 5), limit=-1)
        assert len(rows) == 2
        repo.delete(int(rows[0].id))
        moved = rows[1]
        moved.owner = 6
        await repo.update(moved)
        await repo.insert(_item(comp, owner=5, time=3, name="c"))
    assert list((await _master_range(backend, comp, owner=(5, 5))).name) == ["c"]


async def test_range_phantom_check_off(item_ref, mod_auto_backend):
    """phantom_check=False：只校验返回的行，不管区间里新增的行（同改动前的行为）"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    async with backend.session("pytest", 1) as s1:
        repo = s1.using(comp)
        rows = await repo.range(owner=(7, 7), limit=-1, phantom_check=False)
        assert len(rows) == 0
        async with backend.session("pytest", 1) as s2:
            await s2.using(comp).insert(_item(comp, owner=7, time=1, name="other"))
        await repo.insert(_item(comp, owner=7, time=2, name="mine"))
    assert len(await _master_range(backend, comp, owner=(7, 7))) == 2


async def test_range_read_only_unaffected(item_ref, mod_auto_backend):
    """只读事务不提交，区间被插入也不抛"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    async with backend.session("pytest", 1) as s1:
        repo = s1.using(comp)
        assert len(await repo.range(owner=(7, 7), limit=-1)) == 0
        async with backend.session("pytest", 1) as s2:
            await s2.using(comp).insert(_item(comp, owner=7, time=1, name="other"))


def _intrude_before_get_many(backend: Backend, intrude):
    """包住 master.get_many：range 拿到 id 列表之后、取行之前，先跑一次 intrude(repo) 并提交。
    用来构造"ZRANGE 与取行之间有行被改"（调用方的 session 要 only_master）"""
    master = backend.master
    orig_get_many = master.get_many
    fired = False

    async def get_many(*args, **kwargs):
        nonlocal fired
        if not fired:
            fired = True
            async with backend.session("pytest", 1) as intruder:
                intruder.only_master = True
                await intrude(intruder)
        return await orig_get_many(*args, **kwargs)

    return patch.object(master, "get_many", new=get_many)


@pytest.mark.parametrize("write", [True, False])
async def test_range_row_deleted_while_reading(item_ref, mod_auto_backend, write):
    """索引读到、取行时已被删的行：读到的不是任何一刻的区间，写事务判竞态；只读事务照旧"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    a = _item(comp, owner=6, time=1, name="a")
    b = _item(comp, owner=6, time=2, name="b")
    await _insert_rows(backend, comp, a, b)

    async def delete_b(session):
        repo = session.using(comp)
        assert await repo.get(id=int(b.id)) is not None
        repo.delete(int(b.id))

    async def read(then_write: bool):
        async with backend.session("pytest", 1) as s1:
            s1.only_master = True
            repo = s1.using(comp)
            with _intrude_before_get_many(backend, delete_b):
                rows = await repo.range(owner=(6, 6), limit=-1)
            assert list(rows.id) == [a.id]
            if then_write:
                await repo.insert(_item(comp, owner=9, time=3, name="c"))

    if write:
        with pytest.raises(RaceCondition, match="Inconsistent"):
            await read(True)
    else:
        await read(False)


async def test_range_row_swapped_while_reading(item_ref, mod_auto_backend):
    """取行之前一行被改出区间、同时另一行插进来（行数不变）：写事务也要判竞态"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    a = _item(comp, owner=6, time=1, name="a")
    b = _item(comp, owner=6, time=2, name="b")
    await _insert_rows(backend, comp, a, b)

    async def swap(session):
        repo = session.using(comp)
        row = await repo.get(id=int(a.id))
        assert row is not None
        row.owner = 60
        await repo.update(row)
        await repo.insert(_item(comp, owner=6, time=3, name="c"))

    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as s1:
            s1.only_master = True
            repo = s1.using(comp)
            with _intrude_before_get_many(backend, swap):
                assert len(await repo.range(owner=(6, 6), limit=-1)) == 2
            await repo.insert(_item(comp, owner=9, time=4, name="d"))


async def test_range_reads_ids_and_rows_on_same_node(item_ref, mod_auto_backend):
    """range 查 id 与取行要落在同一个节点：两次各自随机选节点的话，节点间复制进度不同，
    取行时读不到或读到旧版本，会被读取一致性核对误判成竞态"""
    import itertools
    from unittest.mock import PropertyMock

    from hetu.data.backend.session import Session

    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, _item(comp, owner=6, time=1, name="a"))

    reads: list[tuple[str, str]] = []

    class Node:
        """代理 master 的一个"节点"，记下在它上面做了哪种读"""

        def __init__(self, name: str):
            self.name = name

        def __getattr__(self, attr):
            target = getattr(backend.master, attr)
            if attr not in ("range_read_", "range", "get_many"):
                return target

            async def read(*args, **kwargs):
                reads.append((self.name, attr))
                return await target(*args, **kwargs)

            return read

    nodes = itertools.cycle([Node("A"), Node("B")])
    with patch.object(
        Session, "master_or_servant", new_callable=PropertyMock, side_effect=nodes
    ):
        async with backend.session("pytest", 1) as s1:
            rows = await s1.using(comp).range(owner=(6, 6), limit=-1)
    assert len(rows) == 1
    assert {name for name, _ in reads} == {"A"}, reads


async def test_range_reread_after_phantom_is_race(item_ref, mod_auto_backend):
    """同一事务两次读同一区间、中间被插入：两次结果不同，写事务判竞态"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as s1:
            s1.only_master = True
            repo = s1.using(comp)
            assert len(await repo.range(owner=(3, 3), limit=-1)) == 0
            async with backend.session("pytest", 1) as s2:
                await s2.using(comp).insert(_item(comp, owner=3, time=1, name="other"))
            assert len(await repo.range(owner=(3, 3), limit=-1)) == 1
            await repo.insert(_item(comp, owner=99, time=2, name="mine"))


async def test_nonunique_get_none_then_insert_is_race(item_ref, mod_auto_backend):
    """非 unique 列 get 读空后插入，被并发插入同值的行 → 判竞态"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    with pytest.raises(RaceCondition, match="Range"):
        async with backend.session("pytest", 1) as s1:
            repo = s1.using(comp)
            assert await repo.get(owner=9) is None
            async with backend.session("pytest", 1) as s2:
                await s2.using(comp).insert(_item(comp, owner=9, time=1, name="other"))
            await repo.insert(_item(comp, owner=9, time=2, name="mine"))


async def test_nonunique_get_skips_row_deleted_in_txn(item_ref, mod_auto_backend):
    """非 unique 列 get：索引里排在前面的同值行已被本事务删掉，要返回后面那行匹配的。
    返回 None 的话，"get 为 None 就 insert"会插出重复"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(
        backend,
        comp,
        _item(comp, owner=5, time=1, name="x"),
        _item(comp, owner=5, time=2, name="y"),
    )

    async with backend.session("pytest", 1) as s1:
        s1.only_master = True
        repo = s1.using(comp)
        first = await repo.get(owner=5)
        assert first is not None
        repo.delete(int(first.id))
        second = await repo.get(owner=5)
        assert second is not None and second.id != first.id


async def test_get_skips_row_moved_in_txn(item_ref, mod_auto_backend):
    """本事务把命中的行改走了索引值：再 get 旧值不能返回它（它已经不匹配了），要返回别的
    匹配行；unique 列没有别的匹配行，读空"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(
        backend,
        comp,
        _item(comp, owner=5, time=1, name="x"),
        _item(comp, owner=5, time=2, name="y"),
    )

    async with backend.session("pytest", 1) as s1:
        s1.only_master = True
        repo = s1.using(comp)
        first = await repo.get(owner=5)
        assert first is not None
        old_name = str(first.name)
        first.owner, first.name = 6, "moved"
        await repo.update(first)
        second = await repo.get(owner=5)
        assert second is not None and second.owner == 5 and second.id != first.id
        assert await repo.get(name=old_name) is None


async def test_nonunique_get_none_after_deleting_all_then_insert_is_race(
    item_ref, mod_auto_backend
):
    """本事务删掉了这个值上仅有的一行，get 读空后插入，被并发插入同值的行 → 判竞态。
    读空要校验整个值上没有别的行，不能只看到被删的那一行为止"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, _item(comp, owner=5, time=1, name="x"))

    with pytest.raises(RaceCondition, match="Range"):
        async with backend.session("pytest", 1) as s1:
            s1.only_master = True
            repo = s1.using(comp)
            row = await repo.get(owner=5)
            assert row is not None
            repo.delete(int(row.id))
            assert await repo.get(owner=5) is None
            async with backend.session("pytest", 1) as s2:
                await s2.using(comp).insert(_item(comp, owner=5, time=2, name="z"))
            await repo.insert(_item(comp, owner=5, time=3, name="w"))


async def test_nonunique_get_hit_then_insert_after_no_race(item_ref, mod_auto_backend):
    """非 unique 列 get 命中是 limit=1 的截断读：新插入的同值行（id 更大）排在它后面，不算冲突"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, _item(comp, owner=2, time=1, name="a"))

    async with backend.session("pytest", 1) as s1:
        repo = s1.using(comp)
        row = await repo.get(owner=2)
        assert row is not None
        async with backend.session("pytest", 1) as s2:
            await s2.using(comp).insert(_item(comp, owner=2, time=2, name="b"))
        row.qty = 3
        await repo.update(row)


async def test_nonunique_get_hit_ignores_rows_sorting_before(
    item_ref, mod_auto_backend
):
    """get 命中只保护返回的那一行：之后插入的同值行即使排在它前面（id 更小），也不算冲突。
    get 的约定是"返回一行匹配的"，不是"第一行"；省掉这次区间校验（每次约 1.3µs master）"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    hit = _item(comp, owner=2, time=1, name="hit")
    await _insert_rows(backend, comp, hit)

    async with backend.session("pytest", 1) as s1:
        repo = s1.using(comp)
        row = await repo.get(owner=2)
        assert row is not None and row.id == hit.id
        async with backend.session("pytest", 1) as s2:
            # 位数相同、数值更小：两个后端都排在命中行前面
            before = _item(comp, owner=2, time=2, name="before", id_=int(hit.id) - 1)
            await s2.using(comp).insert(before)
        row.qty = 3
        await repo.update(row)


async def test_nonunique_get_hit_moved_while_reading_is_race(
    item_ref, mod_auto_backend
):
    """get 命中仍要核对读取一致性：取行前命中的行被改走（读回的行已不满足查询），写事务判竞态"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    a = _item(comp, owner=2, time=1, name="a")
    await _insert_rows(backend, comp, a)

    async def move_a(session):
        repo = session.using(comp)
        row = await repo.get(id=int(a.id))
        assert row is not None
        row.owner = 20
        await repo.update(row)

    with pytest.raises(RaceCondition, match="Inconsistent|Range"):
        async with backend.session("pytest", 1) as s1:
            s1.only_master = True
            repo = s1.using(comp)
            with _intrude_before_get_many(backend, move_a):
                row = await repo.get(owner=2)
            assert row is not None and row.owner == 20  # 读回的已经被改走了
            await repo.insert(_item(comp, owner=9, time=3, name="c"))


@pytest.mark.parametrize("backend_name", ["sqlite"], indirect=True)
async def test_get_hit_ci_collation_commits(
    monkeypatch, new_component_env, mod_auto_backend
):
    """SQL 后端 get 命中的行是数据库按自身相等语义找到的（MariaDB 默认大小写不敏感的
    collation；这里用 SQLite 的 NOCASE 模拟）：get(name="Alice") 命中 "alice" 后写入要能提交，
    不能因为 Python 里 "alice" != "Alice" 判竞态——重试每次读到的都一样，会一直重试到上限"""
    import numpy as np
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
        qty: np.int16 = property_field(0)

    backend: Backend = mod_auto_backend()
    create_ref(CIName, backend)  # 表在打了 collation 补丁之后建
    async with backend.session("pytest", 1) as s:
        r = CIName.new_row()
        r.name = "alice"
        await s.using(CIName).insert(r)

    async with backend.session("pytest", 1) as s:
        repo = s.using(CIName)
        row = await repo.get(name="Alice")
        assert row is not None and row.name == "alice"
        row.qty = 5
        await repo.update(row)

    async with backend.session("pytest", 1) as s:
        row = await s.using(CIName).get(name="alice")
        assert row is not None and row.qty == 5


async def test_range_float_index_truncated_commits(item_ref, mod_auto_backend):
    """float32 索引上的截断读，没有并发写时一次提交成功（不能拿读回的浮点值做等值比较，
    MariaDB 的单精度 FLOAT 会对不上，变成永远失败的重试）"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(
        backend,
        comp,
        *(
            _item(comp, time=i, name=f"f{i}", model=m)
            for i, m in enumerate((0.1, 0.2, 0.3), 1)
        ),
    )

    async with backend.session("pytest", 1) as s1:
        repo = s1.using(comp)
        assert len(await repo.range(model=(0.0, 1.0), limit=2)) == 2
        await repo.insert(_item(comp, time=10, name="x", model=5.0))


async def test_range_blind_insert_existing_id_is_violation(item_ref, mod_auto_backend):
    """盲插一个已存在的显式 id、之后 range 又读到它：仍是确定性的 UniqueViolation，
    不能被区间校验变成竞态（重跑事务体只会再插同一个 id，无限重试）"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    existing = _item(comp, owner=4, time=1, name="x")
    await _insert_rows(backend, comp, existing)

    with pytest.raises(UniqueViolation):
        async with backend.session("pytest", 1) as s1:
            repo = s1.using(comp)
            dup = _item(comp, owner=4, time=2, name="dup", id_=int(existing.id))
            await repo.insert(dup)
            assert len(await repo.range(owner=(4, 4), limit=-1)) == 1


async def test_unique_absent_then_delete_same_value_is_race(item_ref, mod_auto_backend):
    """get(name=v) 读空，之后又读到并删掉一行 name=v 的行、插入自己的 v：读集前后矛盾，判竞态。
    unique 检查不算本事务删掉的行，所以这种情况不能省掉区间校验"""
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls

    with pytest.raises(RaceCondition):
        async with backend.session("pytest", 1) as s1:
            s1.only_master = True
            repo = s1.using(comp)
            assert await repo.get(name="v") is None
            theirs = _item(comp, time=1, name="v")
            async with backend.session("pytest", 1) as s2:
                await s2.using(comp).insert(theirs)
            assert await repo.get(id=int(theirs.id)) is not None
            repo.delete(int(theirs.id))
            await repo.insert(_item(comp, time=2, name="v"))
