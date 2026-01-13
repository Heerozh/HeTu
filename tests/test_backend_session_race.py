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
