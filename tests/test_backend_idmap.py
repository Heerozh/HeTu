import pytest
from hetu.data.component import BaseComponent
from hetu.data.backend_v2.idmap import IdentityMap, RowState


def test_add_clean_and_get(mod_item_model):
    """测试添加干净行和获取行"""
    Item = mod_item_model
    id_map = IdentityMap()

    # 创建测试数据
    row = Item.new_row()
    row.id = 100
    row.name = "TestItem"
    row.owner = 1

    # 添加到缓存
    id_map.add_clean(Item, row)

    # 验证获取
    fetched_row, status = id_map.get(Item, 100)
    assert fetched_row is not None
    assert fetched_row["id"] == 100
    assert fetched_row["name"] == "TestItem"
    assert status == RowState.CLEAN

    # 验证重复添加报错
    with pytest.raises(ValueError, match="already exists"):
        id_map.add_clean(Item, row)


def test_add_insert(mod_item_model):
    """测试添加新插入行"""
    Item = mod_item_model
    id_map = IdentityMap()

    row = Item.new_row()
    row.name = "NewItem"
    row.owner = 2

    # 添加插入
    id_map.add_insert(Item, row)

    # 验证分配了负ID
    assert row["id"] < 0
    temp_id = row["id"]

    # 验证缓存中存在
    fetched, status = id_map.get(Item, temp_id)
    assert fetched is not None
    assert fetched["name"] == "NewItem"
    assert status == RowState.INSERT

    # 验证状态
    dirty = id_map.get_dirty_rows()
    assert Item in dirty["insert"]
    assert len(dirty["insert"][Item]) == 1
    assert dirty["insert"][Item][0]["id"] == temp_id


def test_update_clean_row(mod_item_model):
    """测试更新干净行"""
    Item = mod_item_model
    id_map = IdentityMap()

    # 初始数据
    row = Item.new_row()
    row.id = 200
    row.name = "Original"
    id_map.add_clean(Item, row)

    # 更新数据
    row_update = row.copy()
    row_update.name = "Updated"
    id_map.update(Item, row_update)

    # 验证数据已更新
    fetched, status = id_map.get(Item, 200)
    assert fetched is not None
    assert fetched["name"] == "Updated"
    assert status == RowState.UPDATE

    # 验证状态流转为 UPDATE
    dirty = id_map.get_dirty_rows()
    assert Item in dirty["update"]
    assert len(dirty["update"][Item]) == 1
    assert dirty["update"][Item][0]["name"] == "Updated"


def test_update_inserted_row(mod_item_model):
    """测试更新刚插入的行（状态应保持INSERT）"""
    Item = mod_item_model
    id_map = IdentityMap()

    # 插入数据
    row = Item.new_row()
    row.name = "Original"
    id_map.add_insert(Item, row)
    temp_id = row["id"]

    # 更新插入的数据
    row_update = row.copy()
    row_update.name = "Updated"
    id_map.update(Item, row_update)

    # 验证数据更新
    fetched, status = id_map.get(Item, temp_id)
    assert fetched is not None
    assert fetched["name"] == "Updated"

    # 验证状态仍为 INSERT，不应出现在 UPDATE 列表中
    dirty = id_map.get_dirty_rows()
    assert len(dirty["insert"][Item]) == 1
    assert dirty["insert"][Item][0]["name"] == "Updated"
    assert Item not in dirty["update"]


def test_mark_deleted(mod_item_model):
    """测试标记删除"""
    Item = mod_item_model
    id_map = IdentityMap()

    row = Item.new_row()
    row.id = 300
    id_map.add_clean(Item, row)

    # 标记删除
    id_map.mark_deleted(Item, 300)

    # 验证 get 返回 None
    fetched, status = id_map.get(Item, 300)
    assert fetched is not None
    assert fetched.id == 300
    assert status is RowState.DELETE

    # 验证无法更新已删除的行
    with pytest.raises(ValueError, match="marked as DELETE"):
        id_map.update(Item, row)

    # 验证脏数据列表
    dirty = id_map.get_dirty_rows()
    assert Item in dirty["delete"]
    assert 300 in dirty["delete"][Item]


def test_exceptions(mod_item_model):
    """测试异常情况"""
    Item = mod_item_model
    id_map = IdentityMap()

    # 获取不存在的 Component
    assert id_map.get(Item, 999) == (None, None)

    # 更新不存在的 Component
    row = Item.new_row()
    row.id = 999
    with pytest.raises(ValueError, match="not in cache"):
        id_map.update(Item, row)

    # 初始化 Component 缓存后，更新不存在的 ID
    dummy = Item.new_row()
    dummy.id = 1
    id_map.add_clean(Item, dummy)

    with pytest.raises(ValueError, match="not found in cache"):
        id_map.update(Item, row)

    # 标记删除不存在的 Component
    # 注意：mark_deleted 检查的是 _row_states，add_clean 会初始化它
    # 如果完全没加过该 Component，会报错
    class OtherComponent(BaseComponent):
        pass

    with pytest.raises(ValueError, match="not in cache"):
        id_map.mark_deleted(OtherComponent, 1)


def test_filter(mod_item_model):
    """测试过滤已删除行"""
    Item = mod_item_model
    id_map = IdentityMap()

    # 添加干净行
    row1 = Item.new_row()
    row1.id = 1
    row1.name = "Item1"
    row1.level = 10
    id_map.add_clean(Item, row1)

    row2 = Item.new_row()
    row2.id = 2
    row2.name = "Item2"
    row2.level = 10
    id_map.add_clean(Item, row2)

    row3 = Item.new_row()
    row3.id = 3
    row3.name = "Item3"
    row3.level = 10
    id_map.add_clean(Item, row3)

    row4 = Item.new_row()
    row4.id = 4
    row4.name = "Item1"
    row4.level = 20
    id_map.add_clean(Item, row4)

    # 标记第二行删除
    id_map.mark_deleted(Item, 2)

    # 获取所有符合条件行，过滤已删除的
    rows = id_map.filter(Item, level=10, name="Item1")

    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "Item1"
