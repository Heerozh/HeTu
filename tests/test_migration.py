#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """

import numpy as np
import pytest

from hetu.data.backend import Table
from hetu.common.snowflake_id import SnowflakeID

SnowflakeID().init(1, 0)


async def test_migration_unique_violation(filled_item_ref):
    # 测试自动迁移
    backend = filled_item_ref.backend

    # 重新定义新的属性
    from hetu.data import (
        define_component,
        property_field,
        BaseComponent,
        ComponentDefines,
    )

    ComponentDefines().clear_()

    @define_component(namespace="pytest")
    class ItemNew(BaseComponent):
        owner: np.int64 = property_field(0, unique=False, index=True)
        model: np.float64 = property_field(0, unique=False, index=True)
        qty_new: np.int16 = property_field(111, unique=False, index=False)
        level: np.int8 = property_field(1, unique=False, index=False)
        time: np.int64 = property_field(0, unique=True, index=True)
        name: "U4" = property_field("", unique=True, index=False)
        used: bool = property_field(False, unique=False, index=True)

    # 从ItemNew改名回Item，以便迁移同名的
    import json

    define = json.loads(ItemNew.json_)
    define["component_name"] = "Item"
    renamed_new_item_cls = BaseComponent.load_json(json.dumps(define))
    new_table = Table(
        renamed_new_item_cls,
        filled_item_ref.instance_name,
        filled_item_ref.cluster_id,
        backend,
    )

    # 测试迁移
    maint = backend.get_table_maintenance()
    tbl_status, old_meta = maint.check_table(new_table)
    assert tbl_status == "schema_mismatch"

    # 有qty删除，不能迁移
    with pytest.raises(ValueError, match="丢弃"):
        maint.migration_schema(new_table, old_meta)

    # item.name是U4截断，导致unique违反，不能迁移
    with pytest.raises(RuntimeError, match="unique"):
        maint.migration_schema(new_table, old_meta, force=True)
        maint.rebuild_index(new_table)


async def test_auto_migration(filled_item_ref, caplog):
    # 测试自动迁移
    backend = filled_item_ref.backend

    # 重新定义新的属性
    from hetu.data import (
        define_component,
        property_field,
        BaseComponent,
        ComponentDefines,
    )

    ComponentDefines().clear_()

    @define_component(namespace="pytest")
    class ItemNew(BaseComponent):
        owner: np.int64 = property_field(0, unique=False, index=True)
        model: np.int32 = property_field(0, unique=False, index=True)
        qty_new: np.int16 = property_field(111, unique=False, index=False)
        level: np.int8 = property_field(1, unique=False, index=False)
        time: np.int64 = property_field(0, unique=True, index=True)
        name: "U4" = property_field("", unique=False, index=True)
        used: bool = property_field(False, unique=False, index=True)

    # 从ItemNew改名回Item，以便迁移同名的
    import json

    define = json.loads(ItemNew.json_)
    define["component_name"] = "Item"
    renamed_new_item_cls = BaseComponent.load_json(json.dumps(define))
    new_table = Table(
        renamed_new_item_cls,
        filled_item_ref.instance_name,
        2,
        backend,
    )

    # 测试迁移
    maint = backend.get_table_maintenance()
    tbl_status, old_meta = maint.check_table(new_table)
    assert tbl_status == "cluster_mismatch" or tbl_status == "schema_mismatch"

    maint.migration_cluster_id(new_table, old_meta)
    maint.migration_schema(new_table, old_meta, force=True)
    maint.rebuild_index(new_table)

    assert "qty 在新的组件定义中不存在" in caplog.text
    assert "多出属性 qty_new" in caplog.text
    assert "25行 * 1个属性" in caplog.text

    # 检测跨cluster报错
    renamed_new_item_cls.hosted_ = new_table
    with pytest.raises(AssertionError, match="cluster"):
        async with backend.session("pytest", 3) as session:
            repo = session.using(renamed_new_item_cls)

    async with backend.session("pytest", 2) as session:
        repo = session.using(renamed_new_item_cls)
        assert (await repo.get(time=111)).name == "Itm1"
        assert (await repo.get(time=111)).qty_new == 111
        assert (await repo.get(time=111)).qty_new == 111

        assert (await repo.get(name="Itm3")).name == "Itm3"
        # 截断后有重复值了
        np.testing.assert_array_equal(
            (await repo.range("name", "Itm3")).time, [130, 131, 132, 133, 134]
        )
