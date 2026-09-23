#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """


import numpy as np
import pytest

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Table

SnowflakeID().init(1, 0)


async def test_migration_unique_violation(filled_item_ref, caplog, tmp_path):
    # 假app文件，迁移脚本会生成在它旁边的 maint/migration 目录
    test_app_file = tmp_path / "test.py"

    # 测试自动迁移
    backend = filled_item_ref.backend

    # 重新定义新的属性
    from hetu.data import (
        BaseComponent,
        ComponentDefines,
        define_component,
        property_field,
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
    define["name"] = "Item"
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
    caplog.clear()

    assert not maint.migration_schema(test_app_file, new_table, old_meta)
    assert "丢弃" in caplog.text

    # item.name是U4截断，导致unique违反，不能迁移
    with pytest.raises(RuntimeError, match="unique"):
        maint.migration_schema(test_app_file, new_table, old_meta, force=True)
        maint.rebuild_index(new_table)


async def test_migration_add_unique_column(filled_item_ref, caplog, tmp_path):
    """为已有数据的表新增 unique 列：所有现有行会被填入相同默认值，自动迁移无法生成
    唯一值，应抛出可操作的报错（说明成因 + 两条修复建议）。"""
    test_app_file = tmp_path / "test.py"

    backend = filled_item_ref.backend

    from hetu.data import (
        BaseComponent,
        ComponentDefines,
        define_component,
        property_field,
    )

    ComponentDefines().clear_()

    # 与原 Item 完全一致，只新增一个 unique 列 tag（无删除、无类型变更）
    @define_component(namespace="pytest")
    class ItemNew(BaseComponent):
        owner: np.int64 = property_field(0, unique=False, index=True)
        model: np.float32 = property_field(0, unique=False, index=True)
        qty: np.int16 = property_field(1, unique=False, index=False)
        level: np.int8 = property_field(1, unique=False, index=False)
        time: np.int64 = property_field(0, unique=True, index=True)
        name: "U8" = property_field("", unique=True, index=True)  # type: ignore  # noqa
        used: bool = property_field(False, unique=False, index=True)
        tag: "U8" = property_field("", unique=True, index=True)  # type: ignore  # noqa

    import json

    define = json.loads(ItemNew.json_)
    define["name"] = "Item"
    renamed_new_item_cls = BaseComponent.load_json(json.dumps(define))
    new_table = Table(
        renamed_new_item_cls,
        filled_item_ref.instance_name,
        filled_item_ref.cluster_id,
        backend,
    )

    maint = backend.get_table_maintenance()
    tbl_status, old_meta = maint.check_table(new_table)
    assert tbl_status == "schema_mismatch"

    # 新增列被判定为安全迁移，但 25 行都会被填入相同默认值 '' → unique 冲突
    with pytest.raises(RuntimeError) as exc_info:
        maint.migration_schema(test_app_file, new_table, old_meta)

    msg = str(exc_info.value)
    assert "unique" in msg


async def test_auto_migration(filled_item_ref, caplog, tmp_path):
    # 假app文件，迁移脚本会生成在它旁边的 maint/migration 目录
    test_app_file = tmp_path / "test.py"

    # 测试自动迁移
    backend = filled_item_ref.backend

    # 重新定义新的属性
    from hetu.data import (
        BaseComponent,
        ComponentDefines,
        define_component,
        property_field,
    )

    ComponentDefines().clear_()

    @define_component(namespace="pytest")
    class ItemNew(BaseComponent):
        owner: np.int64 = property_field(0, unique=False, index=True)
        model: np.int32 = property_field(0, unique=False, index=True)
        qty_new: np.int16 = property_field(111, unique=False, index=True)
        level: np.int8 = property_field(1, unique=False, index=False)
        time: np.int64 = property_field(0, unique=True, index=True)
        name: "U4" = property_field("", unique=False, index=True)
        used: bool = property_field(False, unique=False, index=True)

    # 从ItemNew改名回Item，以便迁移同名的
    import json

    define = json.loads(ItemNew.json_)
    define["name"] = "Item"
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
    maint.migration_schema(test_app_file, new_table, old_meta, force=True)

    assert "qty 在新的组件定义中不存在" in caplog.text
    assert "多出属性 qty_new" in caplog.text
    assert "25行" in caplog.text

    async with backend.session("pytest", 2) as session:
        repo = session.using(renamed_new_item_cls)
        assert (await repo.get(time=111)).name == "Itm1"
        assert (await repo.get(time=111)).qty_new == 111
        assert (await repo.get(time=111)).qty_new == 111

        assert (await repo.range(qty_new=(111, 112), limit=99)).shape[0] == 25

        assert (await repo.get(name="Itm3")).name == "Itm3"
        # 截断后有重复值了
        np.testing.assert_array_equal(
            (await repo.range("name", "Itm3")).time, [130, 131, 132, 133, 134]
        )


async def test_migration_declaration_only(filled_item_ref, tmp_path):
    """
    只改声明、dtype 不变（table_sub / point_sub，同理只改 index、权限）：迁移脚本判 skip、
    不用搬数据，但 meta 要写成新定义，否则服务器一直报 schema_mismatch 拒绝启动、upgrade
    又什么都不做。给没索引的字段加 point_sub 会打开 index，这个索引也要建出来。
    """
    test_app_file = tmp_path / "test.py"
    backend = filled_item_ref.backend

    from hetu.data import (
        BaseComponent,
        ComponentDefines,
        Permission,
        define_component,
        property_field,
    )

    ComponentDefines().clear_()

    # dtype 与原 Item 完全一致：去掉 table_sub，model 加 point_sub（本来就有索引），
    # qty 加 point_sub（本来没有索引，会打开 index）
    @define_component(namespace="pytest", permission=Permission.OWNER)
    class ItemNew(BaseComponent):
        owner: np.int64 = property_field(0, unique=False, index=True, point_sub=True)
        model: np.float32 = property_field(0, unique=False, index=True, point_sub=True)
        qty: np.int16 = property_field(1, unique=False, point_sub=True)
        level: np.int8 = property_field(1, unique=False, index=False)
        time: np.int64 = property_field(0, unique=True, index=True)
        name: "U8" = property_field("", unique=True, index=True, point_sub=True)  # type: ignore  # noqa
        used: bool = property_field(False, unique=False, index=True, point_sub=True)

    import json

    define = json.loads(ItemNew.json_)
    define["name"] = "Item"
    renamed_new_item_cls = BaseComponent.load_json(json.dumps(define))
    new_table = Table(
        renamed_new_item_cls,
        filled_item_ref.instance_name,
        filled_item_ref.cluster_id,
        backend,
    )

    maint = backend.get_table_maintenance()
    tbl_status, old_meta = maint.check_table(new_table)
    assert tbl_status == "schema_mismatch"

    assert maint.migration_schema(test_app_file, new_table, old_meta)
    assert maint.check_table(new_table)[0] == "ok"

    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        repo = session.using(renamed_new_item_cls)
        # 数据原样保留；qty 新开的索引要能查到已有的 25 行
        assert (await repo.get(time=111)).name == "Itm11"
        assert (await repo.range("qty", 999, limit=99)).shape[0] == 25


async def test_read_meta_by_name(item_ref, mod_auto_backend):
    """read_meta 接受组件类或组件名：不持有本地类定义的进程（headless）按名字读 meta。"""
    maint = mod_auto_backend().get_table_maintenance()
    by_cls = maint.read_meta(item_ref.instance_name, item_ref.comp_cls)
    by_name = maint.read_meta(item_ref.instance_name, item_ref.comp_cls.name_)
    assert by_cls is not None
    assert by_cls == by_name
    assert by_name.cluster_id == item_ref.cluster_id
    assert maint.read_meta(item_ref.instance_name, "NoSuchComponent") is None
