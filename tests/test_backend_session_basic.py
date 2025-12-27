#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import numpy as np
import pytest
from typing import Callable

from hetu.data.backend import UniqueViolation, Backend
from hetu.data.backend.session import Session
from hetu.common.snowflake_id import SnowflakeID

SnowflakeID().init(1, 0)


async def test_select_outside(item_ref):
    """测试SessionSelect在未进入Session上下文时抛出异常。"""
    backend = Backend.__new__(Backend)
    backend._master = None  # type: ignore
    session = Session(backend, "pytest", 1)
    async with session as session:
        item_select = session.select(item_ref.comp_cls)

    with pytest.raises(AssertionError, match="Session"):
        await item_select.range(limit=10, id=(10, 5))


async def test_basic_crud(item_ref, mod_auto_backend: Callable[..., Backend]):
    backend = mod_auto_backend()
    # 测试插入数据
    row_ids = []
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        # 插入3行数据
        item_select = session.select(item_ref.comp_cls)
        row = item_ref.comp_cls.new_row()
        row.name = "Item1"
        row.owner = 1
        row.time = 1
        row_ids.append(row.id)
        await item_select.insert(row)

        row = item_ref.comp_cls.new_row()
        row.name = "Item2"
        row.owner = 1
        row.time = 2
        row_ids.append(row.id)
        await item_select.insert(row)

        row = item_ref.comp_cls.new_row()
        row.name = "Item3"
        row.owner = 2
        row.time = 3
        row_ids.append(row.id)
        await item_select.insert(row)

        # 测试刚添加的缓存
        np.testing.assert_array_equal((await item_select.get(id=row_ids[2])).time, 3)  # type: ignore

    # 测试基本的range和get
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_select = session.select(item_ref.comp_cls)
        result = await item_select.range(limit=10, id=(-np.inf, +np.inf))
        np.testing.assert_array_equal(result.id, row_ids)
        assert (await item_select.get(id=row_ids[0])).name == "Item1"  # type: ignore
        # 测试第一行dict select不出来的历史bug
        assert (await item_select.get(name="Item1")).name == "Item1"  # type: ignore
        assert type(await item_select.get(name="Item1")) is not np.recarray

    # 测试update是否正确
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_select = session.select(item_ref.comp_cls)
        row = await item_select.get(id=row_ids[0])
        assert row
        row.qty = 2
        await item_select.update(row)
        # 测试刚update的缓存
        np.testing.assert_array_equal((await item_select.get(id=row_ids[0])).qty, 2)  # type: ignore
    # 测试写入后
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_select = session.select(item_ref.comp_cls)
        np.testing.assert_array_equal((await item_select.get(id=row_ids[0])).qty, 2)  # type: ignore

    # 测试delete是否正确
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_select = session.select(item_ref.comp_cls)
        with pytest.raises(LookupError):
            item_select.delete(row_ids[1])

        await item_select.get(id=row_ids[1])  # 确保缓存中有数据
        item_select.delete(row_ids[1])
        # 测试刚delete的缓存
        result = await item_select.range(limit=10, id=(-np.inf, +np.inf))
        np.testing.assert_array_equal(result.id, [row_ids[0], row_ids[2]])


async def test_insert_unique(item_ref, mod_auto_backend: Callable[..., Backend]):
    # 测试插入Unique重复数据
    from hetu.data.backend import UniqueViolation

    backend = mod_auto_backend()
    row_ids = []

    # 测试本地缓存中unique违反：
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_select = session.select(item_ref.comp_cls)
        row = item_ref.comp_cls.new_row()
        row.name = "Item1"
        row.owner = 1
        row.time = 1
        row_ids.append(row.id)
        await item_select.insert(row)

        row = item_ref.comp_cls.new_row()
        row.name = "Item2"
        row.owner = 2
        row.time = 2
        row_ids.append(row.id)
        await item_select.insert(row)

        # 关掉remote unique check，只测本地
        async def mock_remote_has_unique_conflicts(row, field):
            assert False, "不应该调用远程unique检查"

        item_select._remote_has_unique_conflicts = mock_remote_has_unique_conflicts  # type: ignore

        # update 重复time
        row.name = "Item2"
        row.owner = 2
        row.time = 1
        with pytest.raises(UniqueViolation, match="time"):
            await item_select.update(row)

        # insert 重复name
        row = item_ref.comp_cls.new_row()
        row.name = "Item1"
        row.owner = 3
        row.time = 3
        with pytest.raises(UniqueViolation, match="name"):
            await item_select.insert(row)
        # insert 重复time
        row.name = "Item3"
        row.owner = 3
        row.time = 1
        with pytest.raises(UniqueViolation, match="time"):
            await item_select.insert(row)

    # 测试和数据库已有数据unique违反，依然是提交前报错（提交报错属于race范围）
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_select = session.select(item_ref.comp_cls)
        row = item_ref.comp_cls.new_row()
        row.name = "Item2"
        row.owner = 2
        row.time = 999
        with pytest.raises(UniqueViolation, match="name"):
            await item_select.insert(row)

        row = item_ref.comp_cls.new_row()
        row.name = "Item4"
        row.time = 2
        with pytest.raises(UniqueViolation, match="time"):
            await item_select.insert(row)


async def test_upsert(item_ref, mod_auto_backend: Callable[..., Backend]):
    backend = mod_auto_backend()

    # 测试能用update_or_insert
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_select = session.select(item_ref.comp_cls)

        async with item_select.upsert(name="item1") as row:
            row.time = 1

        async with item_select.upsert(name="items4") as row:
            row.time = 4

    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_select = session.select(item_ref.comp_cls)

        async with item_select.upsert(name="item1") as row:
            assert row.time == 1

        async with item_select.upsert(name="items4") as row:
            assert row.time == 4


async def test_range_interval(filled_item_ref, mod_auto_backend):
    # 测试开闭区间
    backend: Backend = mod_auto_backend()

    # 测试range的区间是否正确，表内值参考test_data.py的filled_item_ref夹具
    # time范围为110-134，共25个
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        # 默认查询区间检测
        np.testing.assert_array_equal(
            (await item_select.range(time=(110, 115))).time, range(110, 116)
        )
        # 左闭右开
        np.testing.assert_array_equal(
            (await item_select.range(time=("[110", "(115"))).time, range(110, 115)
        )
        # 左开右闭
        np.testing.assert_array_equal(
            (await item_select.range(time=("(110", "[115"))).time, range(111, 116)
        )
        # 左开右开
        np.testing.assert_array_equal(
            (await item_select.range(time=("(110", "(115"))).time, range(111, 115)
        )


async def test_range_infinite(filled_item_ref, mod_auto_backend):
    # 测试np.inf作为范围值
    backend: Backend = mod_auto_backend()

    # 测试range的区间是否正确，表内值参考test_data.py的filled_item_ref夹具
    # time范围为110-134，共25个
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        # 左无限右有限
        np.testing.assert_array_equal(
            (await item_select.range(time=(-np.inf, 115))).time, range(110, 116)
        )
        # 左有限右无限
        np.testing.assert_array_equal(
            (await item_select.range(time=(120, np.inf))).time, range(120, 135)
        )
        # 左无限右无限
        np.testing.assert_array_equal(
            (await item_select.range(time=(-np.inf, np.inf))).time, range(110, 135)
        )

    # 测试字符串类型的无限不允许
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        with pytest.raises(AssertionError, match="str"):
            np.testing.assert_array_equal(
                (await item_select.range(name=(-np.inf, "Itm15"))).time, range(110, 116)
            )


async def test_range_number_index(filled_item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()

    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        ids = (await item_select.range(id=(-np.inf, np.inf), limit=999)).id

    # 测试各种query是否正确，表内值参考test_data.py的filled_item_ref夹具
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        # time范围为110-134，共25个
        np.testing.assert_array_equal(
            (await item_select.range(time=(110, 115))).time, range(110, 116)
        )
        np.testing.assert_array_equal(
            (await item_select.range(time=(110, 115), desc=True)).time,
            range(115, 109, -1),
        )
        # range owner单项和limit
        assert (await item_select.range(owner=(10, 10))).shape[0] == 10
        assert (await item_select.range(owner=(10, 10), limit=30)).shape[0] == 25
        assert (await item_select.range(owner=(10, 10), limit=8)).shape[0] == 8
        assert (await item_select.range(owner=(11, 11))).shape[0] == 0
        # range id
        np.testing.assert_array_equal(
            (await item_select.range(id=(ids[5], ids[10]), limit=999)).id, ids[5:11]
        )
        # 测试range的方向反了
        # AssertionError: right必须大于等于left，你的:
        with pytest.raises(AssertionError, match="right.*left"):
            await item_select.range(time=(115, 110))


async def test_query_string_index(filled_item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()

    # 测试各种query是否正确，表内值参考test_data.py的filled_item_ref夹具
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        # range string with int
        with pytest.raises(AssertionError, match="str"):
            assert (await item_select.range(name=(11, 11))).shape[0] == 0
        # range on str typed unique
        assert (await item_select.range(name=("11", "11"))).shape[0] == 0
        assert (row11 := await item_select.range(name=("Itm11", "Itm11"))).shape[0] == 1
        assert (await item_select.range(name=("Itm11", "Itm11"))).time == 111
        # get on name index
        row11_13 = await item_select.range(name=("Itm11", "Itm13"))
        assert (await item_select.get(name="Itm11")).id == row11.id  # type: ignore
        assert (await item_select.get(name="Itm13")).id == row11_13.id[-1]  # type: ignore
        np.testing.assert_array_equal(
            (await item_select.range(name=("Itm11", "Itm12"))).time, [111, 112]
        )
        # reverse range one row
        assert (await item_select.range(time=(111, 111))).name == ["Itm11"]
        assert len((await item_select.range(time=(111, 111))).name) == 1


async def test_query_bool(filled_item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()

    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        row = await tbl.select(5)
        row.used = True
        await tbl.update(row.id, row)
        row = await tbl.select(7)
        row.used = True
        await tbl.update(row.id, row)

    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        assert set((await tbl.query("used", True)).id) == {5, 7}
        assert set((await tbl.query("used", False, limit=99)).id) == set(
            range(1, 26)
        ) - {5, 7}
        assert set((await tbl.query("used", 0, 1, limit=99)).id) == set(range(1, 26))
        assert set((await tbl.query("used", False, True, limit=99)).id) == set(
            range(1, 26)
        )

    # delete
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        await tbl.delete(5)
        await tbl.delete(7)

    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        assert set((await tbl.query("used", True)).id) == set()


async def test_string_length_cutoff(filled_item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()

    # 测试插入的字符串超出长度是否截断
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        row = mod_item_model.new_row()
        row.name = "reinsert2"  # 超出U8长度会被截断
        await tbl.insert(row)

    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        assert (await tbl.select("reinsert", "name")) is not None, (
            "超出U8长度应该要被截断，这里没索引出来说明没截断"
        )

        assert (await tbl.select("reinsert", "name")).id == 26
        assert len(await tbl.query("id", -np.inf, +np.inf, limit=999)) == 26


async def test_batch_delete(filled_item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()

    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        for i in range(20):
            await tbl.delete(24 - i)  # 再删掉

    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        np.testing.assert_array_equal(
            (await tbl.query("id", 0, 100, limit=999)).id, [1, 2, 3, 4, 25]
        )
        np.testing.assert_array_equal(
            (await tbl.query("id", 3, 25, limit=999)).id, [3, 4, 25]
        )
        assert len(await tbl.query("id", -np.inf, +np.inf, limit=999)) == 5

    # 测试is_exist是否正常
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        x = await tbl.is_exist(999, "id")
        assert x[0] == False
        x = await tbl.is_exist(5, "id")
        assert x[0] == False
        x = await tbl.is_exist(4, "id")
        assert x[0] == True


async def test_unique_table(mod_auto_backend):
    backend: Backend = mod_auto_backend()

    from hetu.data import define_component, property_field, BaseComponent

    @define_component(namespace="pytest")
    class UniqueTest(BaseComponent):
        name: "U8" = property_field("", unique=True, index=True)
        timestamp: float = property_field(0, unique=False, index=True)

    # 测试连接数据库并创建表
    unique_test_table = backend_component_table(UniqueTest, "UniqueTest", 1, backend)
    unique_test_table.create_or_migrate()

    # 测试insert是否正确
    async with backend.session("pytest", 1) as session:
        tbl = unique_test_table.attach(session)
        row = UniqueTest.new_row()
        assert type(row) is not np.ndarray
        await tbl.insert(row)
        row_ids = await session.end_transaction(False)
    assert row_ids == [1]

    async with backend.session("pytest", 1) as session:
        tbl = unique_test_table.attach(session)
        result = await tbl.query("id", 0, 2)
        assert type(result) is np.recarray

    # 测试可用update_or_insert
    async with backend.session("pytest", 1) as session:
        tbl = unique_test_table.attach(session)
        async with tbl.update_or_insert("test", "name") as row:
            assert row.name == "test"
        async with tbl.update_or_insert("", "name") as row:
            assert row.id == 1
        row_ids = await session.end_transaction(False)
    assert row_ids == [2]

    async with backend.session("pytest", 1) as session:
        tbl = unique_test_table.attach(session)
        result = await tbl.query("name", "test")
        assert result.id[0] == 2


async def test_upsert_limit(item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()

    with pytest.raises(ValueError, match="unique"):
        async with backend.session("pytest", 1) as session:
            tbl = item_table.attach(session)
            async with tbl.update_or_insert(True, "used") as row:
                pass


async def test_session_exception(item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()

    try:
        async with backend.session("pytest", 1) as session:
            tbl = item_table.attach(session)
            row = mod_item_model.new_row()
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


async def test_redis_empty_index(filled_item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()

    if not isinstance(backend, RedisBackend):
        pytest.skip("Not a redis backend, skip")
    # 测试更新name后再把所有key删除后index是否正常为空
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        row = await tbl.select(2)
        row.name = f"TST{row.id}"
        await tbl.update(row.id, row)

    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        rows = await tbl.query("id", -np.inf, +np.inf, limit=999)
        for row in rows:
            await tbl.delete(row.id)

    # time.sleep(1)  # 等待部分key过期
    assert backend.io.keys("test:Item:{CLU*") == []


async def test_redis_insert_stack(item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()

    if not isinstance(backend, RedisBackend):
        pytest.skip("Not a redis backend, skip")

    # 检测插入2行数据是否有2个stack
    async with backend.session("pytest", 1) as session:
        tbl = item_table.attach(session)
        row = mod_item_model.new_row()
        row.time = 12345
        row.name = "Stack1"
        await tbl.insert(row)
        row = mod_item_model.new_row()
        row.time = 22345
        row.name = "Stack2"
        await tbl.insert(row)
        assert len(session._stack) > 0

    # 检测update没有变化时没有stacked命令
    async with backend.session("pytest", 1) as session:
        tbl = item_table.attach(session)
        row = await tbl.select(2)
        row.time = 22345
        await tbl.update(2, row)
        assert len(session._stack) == 0


async def test_unique_batch_add_in_same_session_bug(item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()

    # 同事务中插入多个重复Unique数据应该失败
    with pytest.raises(UniqueViolation, match="name"):
        async with backend.session("pytest", 1) as session:
            tbl = item_table.attach(session)

            row = mod_item_model.new_row()
            row.name = "Item1"
            row.time = 1
            await tbl.insert(row)

            row = mod_item_model.new_row()
            row.name = "Item1"
            row.time = 2
            await tbl.insert(row)

    with pytest.raises(UniqueViolation, match="time"):
        async with backend.session("pytest", 1) as session:
            tbl = item_table.attach(session)

            row = mod_item_model.new_row()
            row.name = "Item1"
            row.time = 1
            await tbl.insert(row)

            row = mod_item_model.new_row()
            row.name = "Item2"
            row.time = 2
            await tbl.insert(row)

            row = mod_item_model.new_row()
            row.name = "Item3"
            row.time = 2
            await tbl.insert(row)


async def test_unique_batch_upsert_in_same_session_bug(item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()

    # 同事务中upsert多个重复Unique数据时，应该失败，不能跳RaceCondition死循环 todo 改成可以顺利执行
    with pytest.raises(UniqueViolation, match="name"):
        async with backend.session("pytest", 1) as session:
            tbl = item_table.attach(session)

            async with tbl.upsert("Item1", "name") as row:
                row.time = 1

            async with tbl.upsert("Item1", "name") as row:
                row.time = 2
