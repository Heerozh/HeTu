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


async def test_select_outside(mod_item_model):
    """测试SessionSelect在未进入Session上下文时抛出异常。"""
    backend = Backend.__new__(Backend)
    backend._master = None  # type: ignore
    session = Session(backend, "pytest", 1)
    async with session as session:
        item_select = session.select(mod_item_model)

    with pytest.raises(AssertionError, match="Session"):
        await item_select.range(limit=10, id=(10, 5))


async def test_basic_crud(item_ref, mod_auto_backend: Callable[..., Backend]):
    """测试基本的CRUD操作。"""
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

        # 测试提前commit
        await session.commit()

        # 测试Session退出后的select报错
        with pytest.raises(AssertionError, match="Session"):
            await item_select.range(limit=10, id=(10, 5))

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
    """测试插入Unique重复数据"""
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

        # 测试update
        row = await item_select.get(name="Item2")
        assert row
        row.time = 1
        with pytest.raises(UniqueViolation, match="time"):
            await item_select.update(row)


async def test_upsert(item_ref, mod_auto_backend: Callable[..., Backend]):
    """测试upsert操作"""
    backend = mod_auto_backend()

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
    """测试开闭区间"""
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
    """测试np.inf作为范围值"""
    backend: Backend = mod_auto_backend()

    # 测试range的区间是否正确，表内值参考test_data.py的filled_item_ref夹具
    # 测试int索引的inf范围，time范围为110-134，共25个
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        # 左无限右有限
        np.testing.assert_array_equal(
            (await item_select.range(time=(-np.inf, 115))).time, range(110, 116)
        )
        # 左有限右无限
        np.testing.assert_array_equal(
            (await item_select.range(time=(120, np.inf))).time, range(120, 130)
        )
        # 左无限右无限
        np.testing.assert_array_equal(
            (await item_select.range(time=(-np.inf, np.inf), limit=100)).time,
            range(110, 135),
        )

    # 测试float索引的inf范围，model范围为0.0-2.4，共25个
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        np.testing.assert_array_almost_equal(
            (await item_select.range(time=(-np.inf, np.inf), limit=99)).model,
            np.arange(0, 2.5, 0.1),
        )

    # 测试字符串类型的无限不允许
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        with pytest.raises(AssertionError, match="str"):
            np.testing.assert_array_equal(
                (await item_select.range(name=(-np.inf, "Itm15"))).time, range(110, 116)
            )


async def test_range_number_index(filled_item_ref, mod_auto_backend):
    """测试number类型索引的各种range查询"""
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
        # 测试float类型索引
        np.testing.assert_array_equal(
            (await item_select.range(model=(1.1, 2.3), limit=99)).time,
            range(121, 134),
        )


async def test_query_string_index(filled_item_ref, mod_auto_backend):
    """测试string类型索引的各种query查询"""
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
    """测试bool类型索引的各种query查询"""
    backend: Backend = mod_auto_backend()

    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        row1 = await item_select.get(time=115)
        assert row1
        row1.used = True
        await item_select.update(row1)
        row2 = await item_select.get(time=117)
        assert row2
        row2.used = True
        await item_select.update(row2)

    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_select = session.select(filled_item_ref.comp_cls)
        assert set((await item_select.range(used=(True, True))).id) == {
            row1.id,
            row2.id,
        }
        np.testing.assert_array_equal(
            (await item_select.range(used=(False, False), limit=99)).time,
            sorted(set(range(110, 135)) - {115, 117}),
        )
        np.testing.assert_array_equal(
            (await item_select.range(used=(0, 1), limit=99)).time,
            sorted(set(range(110, 135)) - {115, 117}) + [115, 117],  # 等与1的排后面
        )
        np.testing.assert_array_equal(
            (await item_select.range(used=(False, True), limit=99)).time,
            sorted(set(range(110, 135)) - {115, 117}) + [115, 117],  # 等与1的排后面
        )

    # delete
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        _ = await item_select.range(used=(True, True))  # 必须get获得乐观锁
        item_select.delete(row1.id)
        item_select.delete(row2.id)

    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_select = session.select(filled_item_ref.comp_cls)
        assert set((await item_select.range(used=(True, True))).id) == set()


async def test_string_length_cutoff(filled_item_ref, mod_auto_backend):
    """测试字符串长度截断功能"""
    backend: Backend = mod_auto_backend()

    # 测试插入的字符串超出长度是否截断
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        row = filled_item_ref.comp_cls.new_row()
        row.name = "reinsert2"  # 超出U8长度会被截断
        await item_select.insert(row)

    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_select = session.select(filled_item_ref.comp_cls)
        assert (await item_select.get(name="reinsert")) is not None, (
            "超出U8长度应该要被截断，这里没索引出来说明没截断"
        )

        assert (await item_select.get(name="reinsert")).id == row.id  # type: ignore
        assert len(await item_select.range("id", -np.inf, +np.inf, limit=999)) == 26


async def test_batch_delete(filled_item_ref, mod_auto_backend):
    """测试批量删除功能"""
    backend: Backend = mod_auto_backend()

    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        rows = await item_select.range(id=(-np.inf, +np.inf), limit=999)
        for i in reversed(rows.id[4:-1]):  # 保留前4个和最后一个
            item_select.delete(i)  # 再删掉

    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        item_select = session.select(filled_item_ref.comp_cls)
        np.testing.assert_array_equal(
            (await item_select.range("id", rows.id[0], rows.id[-1], limit=999)).id,
            rows.id[:4].tolist() + rows.id[-1:].tolist(),
        )
        assert len(await item_select.range("id", -np.inf, +np.inf, limit=999)) == 5

    # 测试get=None是否正常
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        x = await item_select.get(id=999)
        assert x is None
        # 不再设计is_exist/exist方法，因为这样无法利用缓存和乐观锁
        x = await item_select.get("id", rows.id[5])
        assert x is None


async def test_unique_table(new_component_env, mod_auto_backend):
    """另一个unique table测试， 忘记测试啥了"""
    backend: Backend = mod_auto_backend()

    from hetu.data import define_component, property_field, BaseComponent
    from hetu.data.backend import TableReference, RaceCondition

    @define_component(namespace="pytest")
    class UniqueTest(BaseComponent):
        name: "U8" = property_field("", unique=True, index=True)  # noqa # type: ignore
        timestamp: float = property_field(0, unique=False, index=True)

    # 测试连接数据库并创建表
    model_ref = TableReference(UniqueTest, "pytest", 1)
    table_maint = backend.get_table_maintenance()
    try:
        table_maint.create_table(model_ref)
    except RaceCondition:
        table_maint.flush(model_ref, force=True)

    # 测试insert是否正确
    async with backend.session("pytest", 1) as session:
        ut_select = session.select(UniqueTest)
        row = UniqueTest.new_row()
        first_row_id = row.id
        assert type(row) is not np.ndarray
        await ut_select.insert(row)

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        ut_select = session.select(UniqueTest)
        result = await ut_select.range("id", -np.inf, np.inf)
        assert result.shape[0] == 1

    await backend.wait_for_synced()

    # 测试可用update_or_insert
    async with backend.session("pytest", 1) as session:
        ut_select = session.select(UniqueTest)
        async with ut_select.upsert(name="test") as row:
            assert row.name == "test"
            last_row_id = row.id
        async with ut_select.upsert(name="") as row:
            assert row.id == first_row_id

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        ut_select = session.select(UniqueTest)
        result = await ut_select.range("name", "test", "test")
        assert result.id[0] == last_row_id


async def test_upsert_limit(mod_item_model):
    """测试upsert不能用于非unique字段"""
    backend = Backend.__new__(Backend)
    backend._master = None  # type: ignore
    with pytest.raises(AssertionError, match="unique"):
        async with backend.session("pytest", 1) as session:
            item_select = session.select(mod_item_model)
            async with item_select.upsert(used=True) as _:
                pass


async def test_session_exception(item_ref, mod_auto_backend):
    """测试Session中途抛出异常时回滚"""
    backend: Backend = mod_auto_backend()

    try:
        async with backend.session("pytest", 1) as session:
            item_select = session.select(item_ref.comp_cls)
            row = item_ref.comp_cls.new_row()
            row.owner = 123
            await item_select.insert(row)

            raise Exception("测试异常回滚")

    except Exception as _:  # noqa
        pass

    await backend.wait_for_synced()

    # 验证数据没有被提交
    async with backend.session("pytest", 1) as session:
        item_select = session.select(item_ref.comp_cls)
        row = await item_select.range(id=(-np.inf, +np.inf), limit=999)
        assert len(row) == 0


async def test_redis_empty_index(filled_item_ref, mod_redis_backend, backend_name):
    """测试Redis后端删除所有key后，index key应该为空"""
    backend: Backend = mod_redis_backend()

    # 因为item_ref还是会启动所有backend，所以要跳过下
    if backend_name != "redis":
        pytest.skip("Not a redis backend, skip")

    # 测试更新name后再把所有key删除后index是否正常为空
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        row = await item_select.get(time=115)
        assert row
        row.name = "TST1"
        await item_select.update(row)

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        rows = await item_select.range("id", -np.inf, +np.inf, limit=999)
        for row in rows:
            item_select.delete(row.id)

    # time.sleep(1)  # 等待部分key过期
    assert backend.master.io.keys("pytest:Item:{CLU*") == []  # type: ignore


async def test_unique_batch_add_in_same_session_bug(item_ref, mod_auto_backend):
    """测试同事务中插入多个重复Unique数据应该报错（和test_unique重复了，但这是最早版本的bug）"""
    backend: Backend = mod_auto_backend()

    # 同事务中插入多个重复Unique数据应该失败
    with pytest.raises(UniqueViolation, match="name"):
        async with backend.session("pytest", 1) as session:
            item_select = session.select(item_ref.comp_cls)

            row = item_ref.comp_cls.new_row()
            row.name = "Item1"
            row.time = 1
            await item_select.insert(row)

            row = item_ref.comp_cls.new_row()
            row.name = "Item1"
            row.time = 2
            await item_select.insert(row)

    with pytest.raises(UniqueViolation, match="time"):
        async with backend.session("pytest", 1) as session:
            item_select = session.select(item_ref.comp_cls)

            row = item_ref.comp_cls.new_row()
            row.name = "Item1"
            row.time = 1
            await item_select.insert(row)

            row = item_ref.comp_cls.new_row()
            row.name = "Item2"
            row.time = 2
            await item_select.insert(row)

            row = item_ref.comp_cls.new_row()
            row.name = "Item3"
            row.time = 2
            await item_select.insert(row)


async def test_unique_batch_upsert_in_same_session_bug(item_ref, mod_auto_backend):
    """测试同事务中upsert多个重复Unique数据应该报错的bug"""
    backend: Backend = mod_auto_backend()

    # 同事务中upsert多个重复Unique数据时，应该成功
    async with backend.session("pytest", 1) as session:
        item_select = session.select(item_ref.comp_cls)

        async with item_select.upsert(name="Item1") as row:
            row.time = 1
            last_row_id = row.id

        async with item_select.upsert(name="Item1") as row:
            row.time = 2
            assert row.id == last_row_id
