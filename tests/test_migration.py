import numpy as np
import pytest


async def test_migration_unique_violation(filled_item_table):
    # 测试自动迁移
    backend = filled_item_table.backend
    table_cls = filled_item_table.__class__

    # 重新定义新的属性
    from hetu.data import define_component, Property, BaseComponent, ComponentDefines
    ComponentDefines().clear_()

    @define_component(namespace="pytest")
    class ItemNew(BaseComponent):
        owner: np.int64 = Property(0, unique=False, index=True)
        model: np.int32 = Property(0, unique=False, index=True)
        qty_new: np.int16 = Property(111, unique=False, index=False)
        level: np.int8 = Property(1, unique=False, index=False)
        time: np.int64 = Property(0, unique=True, index=True)
        name: 'U4' = Property("", unique=True, index=False)
        used: bool = Property(False, unique=False, index=True)

    # 从ItemNew改名回Item，以便迁移同名的
    import json
    define = json.loads(ItemNew.json_)
    define['component_name'] = 'Item'
    renamed_new_item_cls = BaseComponent.load_json(json.dumps(define))

    # 测试迁移
    new_item_table = table_cls(renamed_new_item_cls, 'ItemTestTable', 2, backend)

    # item.name是U4截断，导致unique违反，不能迁移
    with pytest.raises(RuntimeError, match="unique"):
        new_item_table.create_or_migrate()


async def test_auto_migration(filled_item_table, caplog):
    # 测试自动迁移
    backend = filled_item_table.backend
    table_cls = filled_item_table.__class__

    # 重新定义新的属性
    from hetu.data import define_component, Property, BaseComponent, ComponentDefines
    ComponentDefines().clear_()

    @define_component(namespace="pytest")
    class ItemNew(BaseComponent):
        owner: np.int64 = Property(0, unique=False, index=True)
        model: np.int32 = Property(0, unique=False, index=True)
        qty_new: np.int16 = Property(111, unique=False, index=False)
        level: np.int8 = Property(1, unique=False, index=False)
        time: np.int64 = Property(0, unique=True, index=True)
        name: 'U4' = Property("", unique=False, index=True)
        used: bool = Property(False, unique=False, index=True)

    # 从ItemNew改名回Item，以便迁移同名的
    import json
    define = json.loads(ItemNew.json_)
    define['component_name'] = 'Item'
    renamed_new_item_cls = BaseComponent.load_json(json.dumps(define))

    # 测试迁移
    new_item_table = table_cls(renamed_new_item_cls, 'ItemTestTable', 2, backend)

    new_item_table.create_or_migrate()

    assert "qty 在新的组件定义中不存在" in caplog.text
    assert "多出属性 qty_new" in caplog.text
    assert "25行 * 1个属性" in caplog.text

    # 检测跨cluster报错
    with pytest.raises(AssertionError, match="cluster"):
        async with backend.transaction(1) as session:
            new_item_table.attach(session)

    async with (backend.transaction(2) as session):
        tbl = new_item_table.attach(session)
        assert (await tbl.select(111, where='time')).name == 'Itm1'
        assert (await tbl.select(111, where='time')).qty_new == 111
        assert (await tbl.select(111, where='time')).qty_new == 111

        assert (await tbl.select('Itm3', where='name')).name == 'Itm3'
        # 截断后有重复值了
        np.testing.assert_array_equal(
            (await tbl.query("name", 'Itm3')).time,
            [130, 131, 132, 133, 134]
        )
