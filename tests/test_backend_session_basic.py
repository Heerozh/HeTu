import numpy as np
import pytest

from hetu.data.backend import UniqueViolation, RedisBackend


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


async def test_upsert(mod_item_component, item_table):
    backend = item_table.backend

    # 测试能用update_or_insert
    async with backend.transaction(1) as session:
        tbl = item_table.attach(session)

        async with tbl.update_or_insert("item1", 'name') as row:
            row.time = 1

        async with tbl.update_or_insert("items4", 'name') as row:
            row.time = 4

    async with backend.transaction(1) as session:
        tbl = item_table.attach(session)

        async with tbl.update_or_insert("item1", 'name') as row:
            assert row.time == 1

        async with tbl.update_or_insert("items4", 'name') as row:
            assert row.time == 4


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


async def test_string_length_cutoff(filled_item_table, mod_item_component):
    backend = filled_item_table.backend

    # 测试插入的字符串超出长度是否截断
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = mod_item_component.new_row()
        row.name = "reinsert2"  # 超出U8长度会被截断
        await tbl.insert(row)

    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        assert (await tbl.select('reinsert', 'name') is not None,
                "超出U8长度应该要被截断，这里没索引出来说明没截断")
        assert (await tbl.select('reinsert', 'name')).id == 26
        assert len(await tbl.query('id', -np.inf, +np.inf, limit=999)) == 26


async def test_batch_delete(filled_item_table):
    backend = filled_item_table.backend

    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        for i in range(20):
            await tbl.delete(24 - i)  # 再删掉

    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        np.testing.assert_array_equal((await tbl.query('id', 0, 100, limit=999)).id,
                                      [1, 2, 3, 4, 25])
        np.testing.assert_array_equal((await tbl.query('id', 3, 25, limit=999)).id,
                                      [3, 4, 25])
        assert len(await tbl.query('id', -np.inf, +np.inf, limit=999)) == 5

    # 测试is_exist是否正常
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        x = await tbl.is_exist(999, 'id')
        assert x[0] == False
        x = await tbl.is_exist(5, 'id')
        assert x[0] == False
        x = await tbl.is_exist(4, 'id')
        assert x[0] == True


async def test_unique_table(mod_auto_backend):
    backend_component_table, get_or_create_backend = mod_auto_backend
    backend = get_or_create_backend()

    from hetu.data import define_component, Property, BaseComponent
    @define_component(namespace="pytest")
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


async def test_upsert_limit(mod_item_component, item_table):
    backend = item_table.backend

    with pytest.raises(ValueError, match="unique"):
        async with backend.transaction(1) as session:
            tbl = item_table.attach(session)
            async with tbl.update_or_insert(True, 'used') as row:
                pass


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


async def test_redis_empty_index(mod_item_component, filled_item_table):
    backend = filled_item_table.backend
    if not isinstance(backend, RedisBackend):
        pytest.skip("Not a redis backend, skip")
    # 测试更新name后再把所有key删除后index是否正常为空
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(2)
        row.name = f'TST{row.id}'
        await tbl.update(row.id, row)

    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        rows = await tbl.query('id', -np.inf, +np.inf, limit=999)
        for row in rows:
            await tbl.delete(row.id)

    # time.sleep(1)  # 等待部分key过期
    assert backend.io.keys('test:Item:{CLU*') == []


async def test_redis_insert_stack(mod_item_component, item_table):
    backend = item_table.backend
    if not isinstance(backend, RedisBackend):
        pytest.skip("Not a redis backend, skip")

    # 检测插入2行数据是否有2个stack
    async with backend.transaction(1) as session:
        tbl = item_table.attach(session)
        row = mod_item_component.new_row()
        row.time = 12345
        row.name = 'Stack1'
        await tbl.insert(row)
        row = mod_item_component.new_row()
        row.time = 22345
        row.name = 'Stack2'
        await tbl.insert(row)
        assert len(session._stack) > 0

    # 检测update没有变化时没有stacked命令
    async with backend.transaction(1) as session:
        tbl = item_table.attach(session)
        row = await tbl.select(2)
        row.time = 22345
        await tbl.update(2, row)
        assert len(session._stack) == 0


async def test_unique_batch_add_in_same_session_bug(mod_item_component, item_table):
    backend = item_table.backend

    # 同事务中插入多个重复Unique数据应该失败
    with pytest.raises(UniqueViolation, match="name"):
        async with backend.transaction(1) as session:
            tbl = item_table.attach(session)

            row = mod_item_component.new_row()
            row.name = "Item1"
            row.time = 1
            await tbl.insert(row)

            row = mod_item_component.new_row()
            row.name = "Item1"
            row.time = 2
            await tbl.insert(row)

    with pytest.raises(UniqueViolation, match="time"):
        async with backend.transaction(1) as session:
            tbl = item_table.attach(session)

            row = mod_item_component.new_row()
            row.name = "Item1"
            row.time = 1
            await tbl.insert(row)

            row = mod_item_component.new_row()
            row.name = "Item2"
            row.time = 2
            await tbl.insert(row)

            row = mod_item_component.new_row()
            row.name = "Item3"
            row.time = 2
            await tbl.insert(row)


async def test_unique_batch_upsert_in_same_session_bug(mod_item_component, item_table):
    backend = item_table.backend

    # 同事务中upsert多个重复Unique数据时，应该失败，不能跳RaceCondition死循环 todo 改成可以顺利执行
    with pytest.raises(UniqueViolation, match="name"):
        async with backend.transaction(1) as session:
            tbl = item_table.attach(session)

            async with tbl.upsert("Item1", 'name') as row:
                row.time = 1

            async with tbl.upsert("Item1", "name") as row:
                row.time = 2
