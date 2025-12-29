import pytest

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend

SnowflakeID().init(1, 0)


async def test_version_race(item_ref, mod_auto_backend):
    import asyncio
    from hetu.data.backend import RaceCondition

    # 测试竞态，通过2个协程来测试
    backend: Backend = mod_auto_backend()

    # 测试query时，另一个del和update的竞态
    async with backend.session("pytest", 1) as session:
        item_select = session.select(item_ref.comp_cls)
        row = item_ref.comp_cls.new_row()
        row.owner = 65535
        row.name = "Self"
        row.time = 233874
        await item_select.insert(row)
        row.id = SnowflakeID().next_id()
        row.name = "ForUpdt"
        row.time += 1
        await item_select.insert(row)
        row.id = SnowflakeID().next_id()
        row.name = "ForDel"
        row.time += 1
        await item_select.insert(row)

    await backend.wait_for_synced()

    async def read_owner(value):
        async with backend.session("pytest", 1) as _session:
            _item_select = _session.select(item_ref.comp_cls)
            rows = await _item_select.range("owner", value)
            assert len(rows) > 0
            await asyncio.sleep(0.2)

    async def del_row(name, sleep):
        async with backend.session("pytest", 1) as _session:
            _item_select = _session.select(item_ref.comp_cls)
            _row = await _item_select.get(name=name)
            await asyncio.sleep(sleep)
            _item_select.delete(_row.id)  # type: ignore

    async def update_owner(name, sleep):
        async with backend.session("pytest", 1) as _session:
            _item_select = _session.select(item_ref.comp_cls)
            _row = await _item_select.get(name=name)
            assert _row
            _row.owner = _row.owner + 1  # type: ignore
            await asyncio.sleep(sleep)
            await _item_select.update(_row)

    # 测试update_owner和read_only不应该激发RaceCondition
    task1 = asyncio.create_task(read_owner(65535))
    task2 = asyncio.create_task(update_owner("Self", 0.2))
    await asyncio.gather(task1, task2)

    # 测试update和del竞态是否激发race condition
    task1 = asyncio.create_task(del_row("ForDel", 0.2))
    task2 = asyncio.create_task(update_owner("ForDel", 0))
    await task2
    with pytest.raises(RaceCondition):
        await task1

    # 测试update和update竞态是否激发race condition
    task1 = asyncio.create_task(update_owner("ForUpdt", 0.2))
    task2 = asyncio.create_task(update_owner("ForUpdt", 0.2))
    with pytest.raises(RaceCondition):
        await asyncio.gather(task1, task2)

    # 测试update和不同行update不应该冲突
    task1 = asyncio.create_task(update_owner("ForUpdt", 0.2))
    task2 = asyncio.create_task(update_owner("Self", 0.2))
    await asyncio.gather(task1, task2)


async def test_unique_race(item_ref, mod_auto_backend):
    import asyncio
    from hetu.data.backend import RaceCondition

    backend: Backend = mod_auto_backend()

    # 测试事务提交时unique的RaceCondition
    async def insert_and_sleep(uni_val, sleep):
        async with backend.session("pytest", 1) as _session:
            _item_select = _session.select(item_ref.comp_cls)
            _row = item_ref.comp_cls.new_row()
            _row.owner = 874233
            _row.name = str(uni_val)
            _row.time = uni_val
            await _item_select.insert(_row)
            await asyncio.sleep(sleep)

    # 测试insert不同的值应该没有竞态
    task1 = asyncio.create_task(insert_and_sleep(111111, 0.1))
    task2 = asyncio.create_task(insert_and_sleep(111112, 0.01))
    await asyncio.gather(task1, task2)

    # 相同的time会竞态
    task1 = asyncio.create_task(insert_and_sleep(222222, 0.1))
    task2 = asyncio.create_task(insert_and_sleep(222222, 0.01))
    await asyncio.gather(task2)
    with pytest.raises(RaceCondition):
        await task1

    # 测试事务提交时的watch的RaceCondition
    async def update_and_sleep(db, sleep):
        async with backend.session("pytest", 1) as _session:
            _item_select = _session.select(item_ref.comp_cls)
            _row = await _tbl.select("111111", "name")
            _row.time = 874233
            await _tbl.update(_row.id, _row)
            await asyncio.sleep(sleep)

    task1 = asyncio.create_task(update_and_sleep(item_table, 0.1))
    task2 = asyncio.create_task(update_and_sleep(item_table, 0.02))
    await asyncio.gather(task2)
    with pytest.raises(RaceCondition):
        await task1

    # 测试query后该值是否激发竞态
    async def query_then_update(sleep):
        async with backend.session("pytest", 1) as _session:
            _item_select = _session.select(item_ref.comp_cls)
            _rows = await _tbl.query("model", 2)
            await asyncio.sleep(sleep)
            if len(_rows) == 0:
                _row = await _tbl.select(0, "model")
                _row.model = 2
                await _tbl.update(_row.id, _row)

    task1 = asyncio.create_task(query_then_update(0.1))
    task2 = asyncio.create_task(query_then_update(0.02))
    await asyncio.gather(task2)
    with pytest.raises(RaceCondition):
        await task1


async def test_update_or_insert_race_bug(item_ref, mod_auto_backend):
    import asyncio
    from hetu.data.backend import RaceCondition

    backend = mod_auto_backend()

    # 测试update_or_insert UniqueViolation是否转化为了RaceCondition
    async def main_task():
        async with backend.session("pytest", 1) as session:
            item_select = session.select(item_ref.comp_cls)
            async with tbl.update_or_insert("uni_vio", "name") as row:
                await asyncio.sleep(0.1)
                row.qty = 1

    async def trouble_task():
        async with backend.session("pytest", 1) as _session:
            _item_select = _session.select(item_ref.comp_cls)
            row = mod_item_model.new_row()
            row.name = "uni_vio"
            await _tbl.insert(row)

    task1 = asyncio.create_task(main_task())
    task2 = asyncio.create_task(trouble_task())
    await asyncio.gather(task2)
    with pytest.raises(RaceCondition):
        await task1

    # close backend
    await backend.close()
