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
    """测试服务器端提交时，牵涉unique的竞态检查。"""
    import asyncio

    from hetu.data.backend import RaceCondition

    backend: Backend = mod_auto_backend()

    # 测试insert提交时unique的RaceCondition
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

    # 相同的time会竞态
    task1 = asyncio.create_task(insert_and_sleep(222222, 0.1))
    task2 = asyncio.create_task(insert_and_sleep(222222, 0.01))
    await task2
    with pytest.raises(RaceCondition, match="UNIQUE"):
        await task1

    # 测试update提交不同的key时unique竞态
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
    with pytest.raises(RaceCondition, match="UNIQUE"):
        await task1


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
