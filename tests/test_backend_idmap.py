import numpy as np
import pytest

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend.idmap import IdentityMap, RowState
from hetu.data.backend.table import TableReference
from hetu.data.component import BaseComponent

SnowflakeID().init(1, 0)


def test_add_clean_and_get(mod_item_model):
    """测试添加干净行和获取行"""
    Item = mod_item_model
    item_ref = TableReference(Item, "TestServer", 1)

    id_map = IdentityMap()

    # 创建测试数据
    row = Item.new_row()
    row.id = 100
    row.name = "TestItem"
    row.owner = 1

    # 添加到缓存
    id_map.add_clean(item_ref, row)

    # 验证获取
    fetched_row, status = id_map.get(item_ref, 100)
    assert fetched_row is not None
    assert fetched_row["id"] == 100
    assert fetched_row["name"] == "TestItem"
    assert status == RowState.CLEAN

    # 事务第一行直接建缓存时，缓存也必须是拷贝：改调用方的行不能影响缓存
    row.name = "Changed after add"
    cached, _ = id_map.get(item_ref, 100)
    assert cached is not None and cached.name == "TestItem"

    # 验证重复添加报错
    with pytest.raises(ValueError, match="already exists"):
        id_map.add_clean(item_ref, row)


def test_get_returns_copy_not_view(mod_item_model):
    """get 返回的行必须是拷贝：numpy 结构化数组的标量下标是视图，直接返回会让调用方改字段
    时把缓存里的"旧值"一起改掉，后续 update 的变化检测就判不出差异。"""
    Item = mod_item_model
    item_ref = TableReference(Item, "TestServer", 1)
    id_map = IdentityMap()

    row = Item.new_row()
    row.id = 100
    row.qty = 1
    id_map.add_clean(item_ref, row)

    fetched, _ = id_map.get(item_ref, 100)
    assert fetched is not None
    fetched.qty = 7  # 改调用方拿到的行
    again, _ = id_map.get(item_ref, 100)
    assert again is not None
    assert again.qty == 1  # 缓存里的值不能跟着变

    # 正常 update 仍然生效（update 是显式写回）
    fetched["_version"] = again["_version"]
    id_map.update(item_ref, fetched)
    updated, _ = id_map.get(item_ref, 100)
    assert updated is not None and updated.qty == 7


def test_add_wrong_component(mod_item_model, mod_rls_test_model):
    """测试添加错误组件类型报错"""
    item_ref = TableReference(mod_item_model, "TestServer", 1)
    id_map = IdentityMap()

    item_row = mod_item_model.new_row()
    item_row.name = "AnotherItem"

    rls_row = mod_rls_test_model.new_row()
    rls_row.friend = 1

    id_map.add_clean(item_ref, item_row)

    with pytest.raises(AssertionError, match="dtype"):
        id_map.add_clean(item_ref, rls_row)
    with pytest.raises(AssertionError, match="dtype"):
        id_map.add_insert(item_ref, rls_row)
    with pytest.raises(AssertionError, match="dtype"):
        id_map.update(item_ref, rls_row)


def test_add_insert(mod_item_model):
    """测试添加新插入行"""
    Item = mod_item_model  # noqa
    item_ref = TableReference(Item, "TestServer", 1)
    id_map = IdentityMap()

    row = Item.new_row()
    row.name = "NewItem"
    row.owner = 2

    # 添加插入
    id_map.add_insert(item_ref, row)

    # 验证分配了ID
    assert row["id"] > 0
    temp_id = row["id"]

    # 验证缓存中存在
    fetched, status = id_map.get(item_ref, temp_id)
    assert fetched is not None
    assert fetched["name"] == "NewItem"
    assert status == RowState.INSERT

    # 验证状态
    dirties = id_map.get_dirty_rows()
    assert item_ref in dirties
    inserts = dirties[item_ref][0]
    assert len(inserts) == 1
    assert int(inserts[0]["id"]) == temp_id

    # 测试添加多行干净
    rows = Item.new_rows(5)
    rows.id = [1, 2, 3, 4, 5]

    id_map.add_clean(item_ref, rows)
    # 验证状态
    _, clean_cache, _ = id_map._cache(item_ref)
    assert list(clean_cache.keys()) == [1, 2, 3, 4, 5]

    # 测试version!=0
    row_v = Item.new_row()
    row_v._version = 1
    with pytest.raises(AssertionError, match="_version"):
        id_map.add_insert(item_ref, row_v)


def test_update_clean_row(mod_item_model):
    """测试更新干净行"""
    Item = mod_item_model
    item_ref = TableReference(Item, "TestServer", 1)
    id_map = IdentityMap()

    # 初始数据
    row = Item.new_row()
    row.id = 200
    row.name = "Original"
    id_map.add_clean(item_ref, row)

    # 更新数据
    row_update = row.copy()
    row_update.name = "Updated"
    id_map.update(item_ref, row_update)

    # 验证数据已更新
    fetched, status = id_map.get(item_ref, 200)
    assert fetched is not None
    assert fetched["name"] == "Updated"
    assert status == RowState.UPDATE

    # 验证状态流转为 UPDATE
    dirties = id_map.get_dirty_rows()
    assert item_ref in dirties
    updates = dirties[item_ref][1]
    assert len(updates[1]) == 1
    assert updates[1][0]["name"] == "Updated"
    # 只含有更新的字段
    assert set(updates[1][0].keys()) == {"name"}

    # 修改_version 字段报错
    row_update._version += 1
    with pytest.raises(AssertionError, match="_version"):
        id_map.update(item_ref, row_update)


def test_update_inserted_row(mod_item_model):
    """测试更新刚插入的行（状态应保持INSERT）"""
    Item = mod_item_model  # noqa
    item_ref = TableReference(Item, "TestServer", 1)
    id_map = IdentityMap()

    # 插入数据
    row = Item.new_row()
    row.name = "Original"
    id_map.add_insert(item_ref, row)
    temp_id = row["id"]

    # 更新插入的数据
    row_update = row.copy()
    row_update.name = "Updated"
    id_map.update(item_ref, row_update)

    # 验证数据更新
    fetched, status = id_map.get(item_ref, temp_id)
    assert fetched is not None
    assert fetched["name"] == "Updated"

    # 验证状态仍为 INSERT，不应出现在 UPDATE 列表中
    dirties = id_map.get_dirty_rows()
    inserts, (_, update_new) = dirties[item_ref][0], dirties[item_ref][1]
    assert len(inserts) == 1
    assert inserts[0]["name"] == "Updated"
    assert len(update_new) == 0


def test_mark_deleted(mod_item_model):
    """测试标记删除"""
    Item = mod_item_model  # noqa
    item_ref = TableReference(Item, "TestServer", 1)
    id_map = IdentityMap()

    row = Item.new_row()
    row.id = 300
    id_map.add_clean(item_ref, row)

    # 标记删除
    id_map.mark_deleted(item_ref, 300)

    # 验证 get 返回 None
    fetched, status = id_map.get(item_ref, 300)
    assert fetched is not None
    assert fetched.id == 300
    assert status is RowState.DELETE

    # 验证无法更新已删除的行
    with pytest.raises(ValueError, match="marked as DELETE"):
        id_map.update(item_ref, row)

    # 验证脏数据列表
    dirties = id_map.get_dirty_rows()
    assert item_ref in dirties
    deletes = dirties[item_ref][2]
    assert "300" in [d["id"] for d in deletes]


def test_exceptions(mod_item_model):
    """测试异常情况"""
    Item = mod_item_model  # noqa
    item_ref = TableReference(Item, "TestServer", 1)
    id_map = IdentityMap()

    # 获取不存在的 Component
    assert id_map.get(item_ref, 999) == (None, None)

    # 更新不存在的 Component
    row = Item.new_row()
    row.id = 999
    with pytest.raises(ValueError, match="not in cache"):
        id_map.update(item_ref, row)

    # 初始化 Component 缓存后，更新不存在的 ID
    dummy = Item.new_row()
    dummy.id = 1
    id_map.add_clean(item_ref, dummy)

    with pytest.raises(ValueError, match="exists"):
        id_map.add_clean(item_ref, dummy)

    with pytest.raises(ValueError, match="not found in cache"):
        id_map.update(item_ref, row)

    # 删除不存在的 Row
    with pytest.raises(ValueError, match="not found in cache"):
        id_map.mark_deleted(item_ref, row.id)

    # 删除不存在的 Component
    # 注意：mark_deleted 检查的是 _row_states，add_clean 会初始化它
    # 如果完全没加过该 Component，会报错
    class OtherComponent(BaseComponent):
        pass

    other_ref = TableReference(OtherComponent, "TestServer", 1)
    with pytest.raises(ValueError, match="not in cache"):
        id_map.mark_deleted(other_ref, 1)


def test_filter(mod_item_model):
    """测试过滤已删除行"""
    Item = mod_item_model  # noqa
    item_ref = TableReference(Item, "TestServer", 1)
    id_map = IdentityMap()

    # 添加干净行
    row1 = Item.new_row()
    row1.id = 1
    row1.name = "Item1"
    row1.level = 10
    id_map.add_clean(item_ref, row1)

    row2 = Item.new_row()
    row2.id = 2
    row2.name = "Item2"
    row2.level = 10
    id_map.add_clean(item_ref, row2)

    row3 = Item.new_row()
    row3.id = 3
    row3.name = "Item3"
    row3.level = 10
    id_map.add_clean(item_ref, row3)

    row4 = Item.new_row()
    row4.id = 4
    row4.name = "Item1"
    row4.level = 20
    id_map.add_clean(item_ref, row4)

    # 标记第二行删除
    id_map.mark_deleted(item_ref, 2)

    # 获取所有符合条件行，过滤已删除的
    rows = id_map.filter(item_ref, level=10, name="Item1")

    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "Item1"


def test_get_absent_unique_fields(mod_item_model):
    """commit 用：对待 INSERT/UPDATE 的行，返回其 unique 列中本事务曾 get 读空的列集合"""
    Item = mod_item_model
    ref = TableReference(Item, "TestServer", 1)
    idmap = IdentityMap()
    assert idmap.get_absent_unique_fields() == {}

    # 只有 absent 记录、没有缓存行 → 空（不能 KeyError）
    idmap.mark_absent(ref, "name", "nobody")
    assert idmap.get_absent_unique_fields() == {}

    # get(id=100) / get(name="a") 读空后 insert 同值：id/name 命中，time 未观察
    idmap.mark_absent(ref, "id", 100)
    idmap.mark_absent(ref, "name", "a")
    row = Item.new_row(id_=100)
    row.name, row.time = "a", 1
    idmap.add_insert(ref, row)
    # 未观察过的 insert 行不出现
    row2 = Item.new_row(id_=101)
    row2.name, row2.time = "b", 2
    idmap.add_insert(ref, row2)
    # CLEAN 行即使值命中也不出现
    row3 = Item.new_row(id_=102)
    row3.name, row3.time = "c", 3
    idmap.add_clean(ref, row3)
    idmap.mark_absent(ref, "name", "c")
    # UPDATE 行：改成曾观察不存在的 time
    row4 = Item.new_row(id_=103)
    row4.name, row4.time = "d", 4
    idmap.add_clean(ref, row4)
    idmap.mark_absent(ref, "time", 44)
    row4, _ = idmap.get(ref, 103)
    assert row4 is not None
    row4.time = 44
    idmap.update(ref, row4)
    # DELETE 行不出现
    row5 = Item.new_row(id_=104)
    row5.name, row5.time = "e", 5
    idmap.add_clean(ref, row5)
    idmap.mark_absent(ref, "name", "e")
    idmap.mark_deleted(ref, 104)

    assert idmap.get_absent_unique_fields() == {
        ref: {100: {"id", "name"}, 103: {"time"}}
    }

    # np 标量与 python 原生值归一化后能对上
    idmap2 = IdentityMap()
    idmap2.mark_absent(ref, "time", np.int64(7))
    r = Item.new_row(id_=1)
    r.time = 7
    idmap2.add_insert(ref, r)
    assert idmap2.get_absent_unique_fields() == {ref: {1: {"time"}}}


def test_dirty_rows_mixed_states_and_reverted_update(mod_item_model):
    """混合状态只输出实际写入，改回原值的 UPDATE 不生成空更新。"""
    ref = TableReference(mod_item_model, "TestServer", 1)
    idmap = IdentityMap()
    rows = mod_item_model.new_rows(5)
    rows.id = [11, 12, 13, 14, 15]
    rows.name = ["clean", "update", "delete", "revert", "insert"]
    rows.qty = 1
    idmap.add_clean(ref, rows[:4])
    idmap.add_insert(ref, rows[4])
    changed = rows[1].copy()
    changed.qty = 9
    idmap.update(ref, changed)
    reverted = rows[3].copy()
    reverted.qty = 8
    idmap.update(ref, reverted)
    reverted.qty = 1
    idmap.update(ref, reverted)
    idmap.mark_deleted(ref, 13)

    inserts, (old_rows, new_rows), deletes = idmap.get_dirty_rows()[ref]
    assert [r["id"] for r in inserts] == ["15"]
    assert [r["id"] for r in old_rows] == ["12"]
    assert old_rows[0]["qty"] == "1"
    assert new_rows == [{"qty": "9"}]
    assert [r["id"] for r in deletes] == ["13"]
    assert set(idmap.get_clean_rows()[ref]) == {11}


def test_dirty_rows_skip_read_only_tables(mod_item_model):
    """读 A 写 B 的事务：只读过的表不输出，单行缓存和多行全是 CLEAN 的表都一样"""
    Item = mod_item_model
    read_one = TableReference(Item.duplicate("pytest", "read_one"), "TestServer", 1)
    read_many = TableReference(Item.duplicate("pytest", "read_many"), "TestServer", 1)
    written = TableReference(Item, "TestServer", 1)
    idmap = IdentityMap()

    idmap.add_clean(read_one, read_one.comp_cls.new_row(id_=1))
    rows = read_many.comp_cls.new_rows(2)
    rows.id = [2, 3]
    idmap.add_clean(read_many, rows)
    target = Item.new_row(id_=4)
    idmap.add_clean(written, target)
    changed = target.copy()
    changed.qty = 9
    idmap.update(written, changed)

    dirties = idmap.get_dirty_rows()
    assert dirties[read_one] == ([], ([], []), [])
    assert dirties[read_many] == ([], ([], []), [])
    assert dirties[written][1][1] == [{"qty": "9"}]


def test_dirty_rows_unchanged_nan_is_not_a_change(mod_item_model):
    """没动过的 NaN 不算变更：改回原值不发更新，改别的字段时只写那个字段；
    0.0 改成 -0.0 仍和按值比较一样算没变"""
    Item = mod_item_model
    ref = TableReference(Item, "TestServer", 1)
    idmap = IdentityMap()
    rows = Item.new_rows(3)
    rows.id = [1, 2, 3]
    rows.model = [np.nan, np.nan, 0.0]
    idmap.add_clean(ref, rows)

    reverted = rows[0].copy()
    reverted.qty = 5
    idmap.update(ref, reverted)
    reverted.qty = rows[0].qty
    idmap.update(ref, reverted)
    changed = rows[1].copy()
    changed.qty = 7
    idmap.update(ref, changed)
    signed_zero = rows[2].copy()
    signed_zero.model = -0.0
    signed_zero.level = 3
    idmap.update(ref, signed_zero)

    _, (old_rows, new_rows), _ = idmap.get_dirty_rows()[ref]
    assert [r["id"] for r in old_rows] == ["2", "3"]
    assert new_rows == [{"qty": "7"}, {"level": "3"}]
