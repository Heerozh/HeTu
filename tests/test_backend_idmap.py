import pytest
import numpy as np

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

    # 验证重复添加报错
    with pytest.raises(ValueError, match="already exists"):
        id_map.add_clean(item_ref, row)


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
    Item = mod_item_model
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
    dirty = id_map.get_dirty_rows()
    assert item_ref in dirty["insert"]
    assert len(dirty["insert"][item_ref]) == 1
    assert int(dirty["insert"][item_ref][0]["id"]) == temp_id

    # 测试添加多行干净
    rows = Item.new_rows(5)
    rows.id = [1, 2, 3, 4, 5]

    id_map.add_clean(item_ref, rows)
    # 验证状态
    _, clean_cache, _ = id_map._cache(item_ref)
    np.testing.assert_array_equal(clean_cache.id, [1, 2, 3, 4, 5])


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
    dirty = id_map.get_dirty_rows()
    assert item_ref in dirty["update"]
    assert len(dirty["update"][item_ref]) == 1
    assert dirty["update"][item_ref][0]["name"] == "Updated"
    # 只含有更新的字段
    assert set(dirty["update"][item_ref][0].keys()) == {"id", "name", "_version"}

    # 修改_version 字段报错
    row_update._version += 1
    with pytest.raises(AssertionError, match="_version"):
        id_map.update(item_ref, row_update)


def test_update_inserted_row(mod_item_model):
    """测试更新刚插入的行（状态应保持INSERT）"""
    Item = mod_item_model
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
    dirty = id_map.get_dirty_rows()
    assert len(dirty["insert"][item_ref]) == 1
    assert dirty["insert"][item_ref][0]["name"] == "Updated"
    assert item_ref not in dirty["update"]


def test_mark_deleted(mod_item_model):
    """测试标记删除"""
    Item = mod_item_model
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
    dirty = id_map.get_dirty_rows()
    assert item_ref in dirty["delete"]
    assert "300" in [d["id"] for d in dirty["delete"][item_ref]]


def test_exceptions(mod_item_model):
    """测试异常情况"""
    Item = mod_item_model
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

    with pytest.raises(ValueError, match="not in cache"):
        id_map.mark_deleted(OtherComponent, 1)


def test_filter(mod_item_model):
    """测试过滤已删除行"""
    Item = mod_item_model
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
