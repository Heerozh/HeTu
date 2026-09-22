import pytest

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend

SnowflakeID().init(1, 0)


async def test_version_race(item_ref, mod_auto_backend):
    import asyncio

    from hetu.data.backend import RaceCondition

    # 测试竞态，通过2个协程来测试
    backend: Backend = mod_auto_backend()

    # 数据准备
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(item_ref.comp_cls)
        row = item_ref.comp_cls.new_row()
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

    async def read_owner(value):
        async with backend.session("pytest", 1) as _session:
            _item_repo = _session.using(item_ref.comp_cls)
            rows = await _item_repo.range("owner", value)
            assert len(rows) > 0
            await asyncio.sleep(0.2)

    async def del_row(name, sleep):
        async with backend.session("pytest", 1) as _session:
            _item_repo = _session.using(item_ref.comp_cls)
            _row = await _item_repo.get(name=name)
            await asyncio.sleep(sleep)
            _item_repo.delete(_row.id)  # type: ignore

    async def update_owner(name, sleep):
        async with backend.session("pytest", 1) as _session:
            _item_repo = _session.using(item_ref.comp_cls)
            _row = await _item_repo.get(name=name)
            assert _row
            _row.owner = _row.owner + 1  # type: ignore
            await asyncio.sleep(sleep)
            await _item_repo.update(_row)

    # 测试update_owner和read_only不应该激发RaceCondition
    task1 = asyncio.create_task(read_owner(65535))
    task2 = asyncio.create_task(update_owner("Self", 0.2))
    await asyncio.gather(task1, task2)

    # 测试update和del竞态是否激发race condition
    task1 = asyncio.create_task(del_row("ForDel", 0.2))
    task2 = asyncio.create_task(update_owner("ForDel", 0.01))
    await task2
    with pytest.raises(RaceCondition, match="Version"):
        await task1

    # 测试update和update竞态是否激发race condition
    task1 = asyncio.create_task(update_owner("ForUpdt", 0.2))
    task2 = asyncio.create_task(update_owner("ForUpdt", 0.2))
    with pytest.raises(RaceCondition, match="Version"):
        await asyncio.gather(task1, task2)

    # 测试update和不同行update不应该冲突
    task1 = asyncio.create_task(update_owner("ForUpdt", 0.2))
    task2 = asyncio.create_task(update_owner("Self", 0.2))
    await asyncio.gather(task1, task2)


async def test_stale_read_race(item_ref, mod_auto_backend):
    """
    测试陈旧读（stale read）场景：
    事务1先读行A的值，再用读到的值更新行B；
    在事务1提交前，事务2修改了行A。

    依据严格的事务语义（Snapshot/Serializable Isolation），
    事务1此时持有的是A的陈旧快照，提交时应抛出 RaceCondition。
    """
    import asyncio

    from hetu.data.backend import RaceCondition

    backend: Backend = mod_auto_backend()

    # 数据准备：插入源行A 与目标行B
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(item_ref.comp_cls)
        row_a = item_ref.comp_cls.new_row()
        row_a.owner = 1
        row_a.name = "SourceA"
        row_a.time = 100
        row_a.qty = 1
        await item_repo.insert(row_a)

        row_b = item_ref.comp_cls.new_row()
        row_b.id = SnowflakeID().next_id()
        row_b.owner = 2
        row_b.name = "TargetB"
        row_b.time = 101
        row_b.qty = 0
        await item_repo.insert(row_b)

    await backend.wait_for_synced()

    async def copy_a_to_b(sleep):
        """读A，把读到的qty写到B"""
        async with backend.session("pytest", 1) as _session:
            _item_repo = _session.using(item_ref.comp_cls)
            _a = await _item_repo.get(name="SourceA")
            assert _a is not None
            stale_qty = int(_a.qty)
            await asyncio.sleep(sleep)  # 期间A被task2改掉
            _b = await _item_repo.get(name="TargetB")
            assert _b is not None
            _b.qty = stale_qty  # type: ignore
            await _item_repo.update(_b)

    async def modify_a(sleep):
        """修改A的qty"""
        async with backend.session("pytest", 1) as _session:
            _item_repo = _session.using(item_ref.comp_cls)
            _a = await _item_repo.get(name="SourceA")
            assert _a is not None
            await asyncio.sleep(sleep)
            _a.qty = _a.qty + 99  # type: ignore
            await _item_repo.update(_a)

    # task1先读A并等待，task2在此期间修改A并先提交，然后task1把A的旧值写入B
    task1 = asyncio.create_task(copy_a_to_b(0.2))
    task2 = asyncio.create_task(modify_a(0.05))
    await task2  # 先等task2完成（A已被改）
    with pytest.raises(RaceCondition):
        await task1


async def test_unique_commit_race(item_ref, mod_auto_backend):
    """
    提交时的 unique 冲突判定：盲写（本事务未曾 get 观察其不存在）撞上并发已提交的同值
    → 确定性 UniqueViolation，不重试（重跑事务体只会再写同一个值）。
    """
    import asyncio

    from hetu.data.backend import UniqueViolation

    backend: Backend = mod_auto_backend()

    # 测试insert提交时unique的确定性冲突
    async def insert_and_sleep(uni_val, sleep):
        async with backend.session("pytest", 1) as _session:
            _item_repo = _session.using(item_ref.comp_cls)
            _row = item_ref.comp_cls.new_row()
            _row.owner = 874233
            _row.name = str(uni_val)
            _row.time = uni_val
            await _item_repo.insert(_row)
            await asyncio.sleep(sleep)

    # 测试insert不同的值应该没有竞态
    task1 = asyncio.create_task(insert_and_sleep(111111, 0.1))
    task2 = asyncio.create_task(insert_and_sleep(111112, 0.01))
    await asyncio.gather(task1, task2)

    # 相同的time：后提交者是确定性冲突
    task1 = asyncio.create_task(insert_and_sleep(222222, 0.1))
    task2 = asyncio.create_task(insert_and_sleep(222222, 0.01))
    await task2
    with pytest.raises(UniqueViolation, match="UNIQUE"):
        await task1

    # 测试update提交时把不同行改成同一个unique值：后提交者是确定性冲突
    async def update_and_sleep(name, sleep):
        async with backend.session("pytest", 1) as _session:
            _item_repo = _session.using(item_ref.comp_cls)
            _row = await _item_repo.get(name=str(name))
            assert _row
            _row.time = 874233
            await _item_repo.update(_row)
            await asyncio.sleep(sleep)

    task1 = asyncio.create_task(update_and_sleep(111111, 0.1))
    task2 = asyncio.create_task(update_and_sleep(111112, 0.02))
    await task2
    with pytest.raises(UniqueViolation, match="UNIQUE"):
        await task1


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
    import asyncio

    from hetu.data.backend import RaceCondition

    backend = mod_auto_backend()

    # 测试update_or_insert UniqueViolation是否转化为了RaceCondition
    # 这其实是本地unique违反，但为了语义上正确，应该换成RaceCondition
    async def main_task():
        async with backend.session("pytest", 1) as session:
            item_repo = session.using(item_ref.comp_cls)
            async with item_repo.upsert(name="uni_vio") as row:
                await asyncio.sleep(0.1)
                row.qty = 1

    async def trouble_task():
        async with backend.session("pytest", 1) as _session:
            _item_repo = _session.using(item_ref.comp_cls)
            row = item_ref.comp_cls.new_row()
            row.name = "uni_vio"
            await _item_repo.insert(row)

    task1 = asyncio.create_task(main_task())
    task2 = asyncio.create_task(trouble_task())
    await task2
    with pytest.raises(RaceCondition):
        await task1


async def test_insert_after_get_none_is_race(item_ref, mod_auto_backend):
    """
    泛化自 upsert 的特例：若本事务先 `get(unique)` 观察到该值不存在，之后 `commit`
    时却发现远程已被并发插入，应判为 `RaceCondition`（基于过期快照的乐观并发失败），
    而非不可重试的 `UniqueViolation`。

    对照 `test_insert_unique`：未先 get、直接 insert 已存在数据，仍是确定性
    `UniqueViolation`。
    """
    import asyncio

    from hetu.data.backend import RaceCondition

    backend = mod_auto_backend()

    async def observer_task():
        async with backend.session("pytest", 1) as session:
            session.only_master = True  # 强制 master 读，避免 replica 延迟
            repo = session.using(item_ref.comp_cls)
            # 观察到 name="zrc" 不存在
            assert await repo.get(name="zrc") is None
            await asyncio.sleep(0.1)  # 让 intruder 抢先插入并提交
            row = item_ref.comp_cls.new_row()
            row.name = "zrc"
            row.time = 7770002
            # 远程已被占用且本事务曾观察其不存在 → commit 时判 Race
            await repo.insert(row)

    async def intruder_task():
        async with backend.session("pytest", 1) as session:
            session.only_master = True
            repo = session.using(item_ref.comp_cls)
            row = item_ref.comp_cls.new_row()
            row.name = "zrc"
            row.time = 7770001  # time 取不同值，确保冲突只发生在 name 上
            await repo.insert(row)

    observer = asyncio.create_task(observer_task())
    intruder = asyncio.create_task(intruder_task())
    await intruder
    with pytest.raises(RaceCondition):
        await observer


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


async def test_row_cache_race_bumps_floor(item_ref, mod_auto_backend):
    """commit 因 VER 失败：本事务碰过的行全部逐出，冲突行的 floor 抬到 Lua 回显的 master 当前
    版本，重试时必走权威读并成功"""
    from contextlib import ExitStack
    from unittest.mock import patch

    from hetu.data.backend import RaceCondition

    backend: Backend = mod_auto_backend()
    cache = backend.row_cache
    if cache is None:
        return
    comp = item_ref.comp_cls
    async with backend.session("pytest", 1) as s:
        r = comp.new_row()
        r.name, r.time = "race", 1
        await s.using(comp).insert(r)
        r2 = comp.new_row()
        r2.name, r2.time = "clean", 2
        await s.using(comp).insert(r2)
    await backend.wait_for_synced()
    race_id, clean_id = int(r.id), int(r2.id)
    race_channel = backend.master.row_channel(item_ref, race_id)
    clean_channel = backend.master.row_channel(item_ref, clean_id)
    cache.activate(race_channel, "test")
    cache.activate(clean_channel, "test")
    try:
        # 预热两行
        async with backend.session("pytest", 1) as s:
            stale = await s.using(comp).get(id=race_id)
            assert stale is not None
            assert await s.using(comp).get(id=clean_id) is not None
        assert cache.get(race_channel) is not None
        assert cache.get(clean_channel) is not None
        floor_before = cache.floor(race_channel)
        assert isinstance(floor_before, int)

        with pytest.raises(RaceCondition, match="Version"):
            async with backend.session("pytest", 1) as s:
                row = await s.using(comp).get(id=race_id)  # 命中缓存
                assert row is not None
                assert await s.using(comp).get(id=clean_id) is not None  # 纯读行
                # 并发：另一事务先改了同一行
                async with backend.session("pytest", 1) as s2:
                    s2.only_master = True
                    other = await s2.using(comp).get(id=race_id)
                    assert other is not None
                    other.time = 5
                    await s2.using(comp).update(other)
                row.time = 6
                await s.using(comp).update(row)
        # 冲突行：逐出 + floor = master 当前版本；纯读行也逐出但 floor 不动
        assert cache.get(race_channel) is None
        assert cache.floor(race_channel) == floor_before + 1
        assert cache.get(clean_channel) is None

        # 重试：副本还是旧行（模拟滞后）也能识破——版本低于 floor → 权威读拿到新值并成功提交
        async def stale_get(*args, **kwargs):
            return stale.copy()

        with ExitStack() as stack:
            for client in [backend.master, *backend._servants]:
                stack.enter_context(patch.object(client, "get", side_effect=stale_get))
            auth_mock = stack.enter_context(
                patch.object(
                    backend.master,
                    "get_many_authoritative",
                    wraps=backend.master.get_many_authoritative,
                )
            )
            async for attempt in backend.session("pytest", 1).retry(3):
                async with attempt as s:
                    row = await s.using(comp).get(id=race_id)
                    assert row is not None and row.time == 5
                    row.time = 6
                    await s.using(comp).update(row)
            assert auth_mock.call_count >= 1
        cached = cache.get(race_channel)
        assert cached is not None and cached.time == 6
    finally:
        cache.deactivate(race_channel, "test")
        cache.deactivate(clean_channel, "test")
