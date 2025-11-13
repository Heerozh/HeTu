from hetu.data.backend import ComponentTable
import numpy as np
import pytest


async def test_table(mod_item_component, item_table):
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


async def test_query_number_index(filled_item_table):
    backend = filled_item_table.backend

    # 测试各种query是否正确，表内值参考test_data.py的filled_item_table夹具
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        # time范围为110-134，共25个，query是[l, r]，但range是[l, r)
        np.testing.assert_array_equal(
            (await tbl.query('time', 110, 115)).time, range(110, 116))
        np.testing.assert_array_equal(
            (await tbl.query('time', 110, 115, desc=True)).time, range(115, 109, -1))
        # query owner单项和limit
        assert (await tbl.query('owner', 10)).shape[0] == 10
        assert (await tbl.query('owner', 10, limit=30)).shape[0] == 25
        assert (await tbl.query('owner', 10, limit=8)).shape[0] == 8
        assert (await tbl.query('owner', 11)).shape[0] == 0
        # query id
        np.testing.assert_array_equal((await tbl.query('id', 5, 7, limit=999)).id,
                                      [5, 6, 7])
        # 测试query的方向反了
        # AssertionError: right必须大于等于left，你的:
        with pytest.raises(AssertionError, match="right.*left"):
            await tbl.query('time', 115, 110)


async def test_query_string_index(filled_item_table):
    backend = filled_item_table.backend

    # 测试各种query是否正确，表内值参考test_data.py的filled_item_table夹具
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        # 测试query的值类型和定义不符：
        # AssertionError: 字符串类型索引`name`的查询(left=11, <class 'int'>)变量类型必须是str
        with pytest.raises(AssertionError, match="name.*int.*str"):
            assert (await tbl.query('name', 11)).shape[0] == 0
        # query on str typed unique
        assert (await tbl.query('name', '11')).shape[0] == 0
        assert (await tbl.query('name', "Itm11")).shape[0] == 1
        assert (await tbl.query('name', "Itm11")).time == 111
        assert (await tbl.select('Itm11', where='name')).id == 2
        assert (await tbl.select('Itm13', where='name')).id == 4
        np.testing.assert_array_equal(
            (await tbl.query('name', 'Itm11', 'Itm12')).time,
            [111, 112])
        # reverse query one row
        assert (await tbl.query('time', 111)).name == ['Itm11']
        assert len((await tbl.query('time', 111)).name) == 1


async def test_query_bool(filled_item_table):
    backend = filled_item_table.backend

    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(5)
        row.used = True
        await tbl.update(row.id, row)
        row = await tbl.select(7)
        row.used = True
        await tbl.update(row.id, row)

    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        assert set((await tbl.query('used', True)).id) == {5, 7}
        assert set((await tbl.query('used', False, limit=99)).id) == set(
            range(1, 26)) - {5, 7}
        assert set((await tbl.query('used', 0, 1, limit=99)).id) == set(range(1, 26))
        assert set((await tbl.query('used', False, True, limit=99)).id) == set(
            range(1, 26))

    # delete
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        await tbl.delete(5)
        await tbl.delete(7)

    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        assert set((await tbl.query('used', True)).id) == set()


async def test_query_after_update(filled_item_table):
    backend = filled_item_table.backend

    # update
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = (await tbl.query('owner', 10))[0]
        old_name = row.name
        assert (await tbl.select(old_name, where='name')).name == old_name
        row.owner = 11
        row.name = 'updated'
        await tbl.update(row.id, row)
        # 测试能否命中cache
        row = await tbl.select(row.id)
        assert row.name == 'updated'

    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(row.id)  # 测试用numpy type进行select是否报错
        assert row.name == 'updated'
        assert (await tbl.query('owner', row.owner, limit=30)).shape[0] == 1
        assert (await tbl.query('owner', 10, limit=30)).shape[0] == 24
        assert (await tbl.query('owner', 11)).shape[0] == 1
        assert (await tbl.query('owner', 11)).name == 'updated'
        assert (await tbl.select('updated', where='name')).name == 'updated'
        assert await tbl.select(old_name, where='name') is None
        assert len(await tbl.query('id', -np.inf, +np.inf, limit=999)) == 25


async def test_query_after_delete(filled_item_table):
    backend = filled_item_table.backend

    # delete
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        await tbl.delete(5)
        await tbl.delete(7)
        # 测试能否命中cache
        row = await tbl.select(5)
        assert row is None

    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        assert len(await tbl.query('id', -np.inf, +np.inf, limit=999)) == 23
        assert await tbl.select('Itm14', where='name') is None
        assert await tbl.select('Itm16', where='name') is None
        assert (await tbl.query('time', 114, 116)).shape[0] == 1


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
