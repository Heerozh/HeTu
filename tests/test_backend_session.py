from hetu.data.backend import ComponentTable
import numpy as np
import pytest

async def test_table( mod_item_component, item_table):
    backend = item_table.backend

    # 测试插入数据
    async with backend.transaction(1) as session:
        tbl = item_table.attach(session)
        row = mod_item_component.new_row()
        row.name = 'Item1'
        row.owner = 1
        row.time = 1
        await tbl.insert(row)

        row.id = 0
        row.name = 'Item2'
        row.owner = 1
        row.time = 2
        await tbl.insert(row)

        row.id = 0
        row.name = 'Item3'
        row.owner = 2
        row.time = 3
        await tbl.insert(row)
        row_ids = await session.end_transaction(False)
    assert row_ids, [1, 2 == 3]

    async with backend.transaction(1) as session:
        tbl = item_table.attach(session)
        result = await tbl.query('id', -np.inf, +np.inf)
        np.testing.assert_array_equal(result.id, [1, 2, 3])
        assert (await tbl.select(1)).name == 'Item1'
        # 测试第一行dict select不出来的历史bug
        assert (await tbl.select('Item1', 'name')).name == 'Item1'
        assert type(await tbl.select('Item1', 'name')) is not np.recarray

    np.testing.assert_array_equal(
        (await item_table.direct_query('id', -np.inf, +np.inf)).id,
        [1, 2, 3])

    # 测试update是否正确
    async with backend.transaction(1) as session:
        tbl = item_table.attach(session)
        row = await tbl.select(1)
        row.qty = 2
        await tbl.update(1, row)
    np.testing.assert_array_equal((await item_table.direct_get(1)).qty, 2)

    # 测试插入Unique重复数据
    from hetu.data.backend import UniqueViolation
    async with backend.transaction(1) as session:
        tbl = item_table.attach(session)
        row.id = 0
        row.name = 'Item2'
        row.owner = 2
        row.time = 999
        with pytest.raises(UniqueViolation, match="name"):
            await tbl.insert(row)
        row.id = 0
        row.name = 'Item4'
        row.time = 2
        with pytest.raises(UniqueViolation, match="time"):
            await tbl.insert(row)
        # 测试能用update_or_insert
        async with tbl.update_or_insert(1, 'owner') as row:
            assert row.owner == 1


async def test_unique_table(mod_auto_backend):
    backend_component_table, get_or_create_backend = mod_auto_backend
    backend = get_or_create_backend()

    from hetu.data import define_component, Property, BaseComponent
    @define_component(namespace="ssw")
    class UniqueTest(BaseComponent):
        name: 'U8' = Property('', unique=True, index=True)
        timestamp: float = Property(0, unique=False, index=True)

    # 测试连接数据库并创建表
    unique_test_table = backend_component_table(UniqueTest, 'UniqueTest', 1, backend)
    unique_test_table.create_or_migrate()

    # 测试insert是否正确
    async with backend.transaction(1) as session:
        tbl = unique_test_table.attach(session)
        row = UniqueTest.new_row()
        assert type(row) is not np.ndarray
        await tbl.insert(row)
        row_ids = await session.end_transaction(False)
    assert row_ids == [1]

    async with backend.transaction(1) as session:
        tbl = unique_test_table.attach(session)
        result = await tbl.query('id', 0, 2)
        assert type(result) is np.recarray

    # 测试可用update_or_insert
    async with backend.transaction(1) as session:
        tbl = unique_test_table.attach(session)
        async with tbl.update_or_insert('test', 'name') as row:
            assert row.name == 'test'
        async with tbl.update_or_insert('', 'name') as row:
            assert row.id == 1
        row_ids = await session.end_transaction(False)
    assert row_ids == [2]

    async with backend.transaction(1) as session:
        tbl = unique_test_table.attach(session)
        result = await tbl.query('name', 'test')
        assert result.id[0] == 2


async def test_session_exception(mod_item_component, item_table):
    backend = item_table.backend
    try:
        async with backend.transaction(1) as session:
            tbl = item_table.attach(session)
            row = mod_item_component.new_row()
            row.owner = 123
            await tbl.insert(row)

            raise Exception("测试异常回滚")

            row = defined_item_component.new_row()
            row.owner = 321
            await tbl.insert(row)
    except Exception as e:
        pass

    # 验证数据没有被提交
    row = await item_table.direct_get(0)
    assert row is None

    row = await item_table.direct_get(1)
    assert row is None
