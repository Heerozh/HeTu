import pytest

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend

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
