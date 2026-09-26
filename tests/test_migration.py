#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """


import os
import subprocess
import sys

import numpy as np
import pytest
from fixtures.backends import use_redis_family_backend_only

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


async def test_migration_without_snowflake(
    filled_item_ref, mod_auto_backend, tmp_path, monkeypatch
):
    """
    hetu upgrade 进程不初始化 SnowflakeID（它不占 worker 租约），迁移时不能发号。以前 Redis
    搬完行、旧索引随旧表删掉之后，重建索引时发号失败：表里有行却没有索引，meta 已是新版本，
    下次 upgrade 不再迁移，服务器就带着空索引起来了。
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

    # 与原 Item 只差 qty 的 dtype（int16 → int32，能安全转换），迁移要搬数据
    @define_component(namespace="pytest", permission=Permission.OWNER, table_sub=True)
    class ItemNew(BaseComponent):
        owner: np.int64 = property_field(0, unique=False, index=True, point_sub=True)
        model: np.float32 = property_field(0, unique=False, index=True)
        qty: np.int32 = property_field(1, unique=False, index=False)
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

    monkeypatch.setattr(SnowflakeID(), "worker_id", -1)  # 模拟 upgrade 进程里未初始化
    assert maint.migration_schema(test_app_file, new_table, old_meta)
    assert maint.check_table(new_table)[0] == "ok"

    # 迁移在 upgrade 进程里做，服务器之后用新连接来读
    reader = mod_auto_backend("after_upgrade")
    await reader.wait_for_synced()
    async with reader.session("pytest", 1) as session:
        repo = session.using(renamed_new_item_cls)
        # 索引按搬过来的行建好了
        assert (await repo.get(time=111)).qty == 999
        assert (await repo.range("owner", 10, limit=99)).shape[0] == 25


def test_duplicate_component_migration_script_name(tmp_path):
    """副本组件名带冒号（FutureCalls:Loot），生成的迁移脚本文件名不能带冒号：Windows 上冒号
    是 NTFS 备用数据流分隔符，脚本会写进 0 字节文件 FutureCalls 的隐藏流，目录里看不到。
    换掉冒号后仍要能按版本号找回脚本。"""
    from hetu.data import BaseComponent
    from hetu.data.migration import MigrationScript
    from hetu.system import FutureCalls

    dup = BaseComponent.load_json(FutureCalls.json_, "Loot")
    assert dup.name_ == "FutureCalls:Loot"

    script = MigrationScript._generate_default_migration_script(
        tmp_path, dup, FutureCalls.json_, "aaa", "bbb"
    )
    assert script.name == "FutureCalls-Loot_vaaa_to_vbbb.py"
    migration_dir = tmp_path / "maint" / "migration"
    assert [f.name for f in migration_dir.iterdir()] == [script.name]
    assert script.stat().st_size > 0
    assert MigrationScript._find_script(tmp_path, "aaa") == (script, "bbb")


async def test_read_meta_by_name(item_ref, mod_auto_backend):
    """read_meta 接受组件类或组件名：不持有本地类定义的进程（headless）按名字读 meta。"""
    maint = mod_auto_backend().get_table_maintenance()
    by_cls = maint.read_meta(item_ref.instance_name, item_ref.comp_cls)
    by_name = maint.read_meta(item_ref.instance_name, item_ref.comp_cls.name_)
    assert by_cls is not None
    assert by_cls == by_name
    assert by_name.cluster_id == item_ref.cluster_id
    assert maint.read_meta(item_ref.instance_name, "NoSuchComponent") is None


async def test_maintenance_range_infinite_bounds(item_ref, mod_auto_backend):
    """维护接口按 ±inf 边界查浮点索引（取全部）"""
    backend = mod_auto_backend()
    comp = item_ref.comp_cls
    ids = []
    async with backend.session("pytest", 1) as session:
        for i, model in enumerate([-2.5, 0.5]):
            row = comp.new_row()
            row.name, row.time, row.model = f"m{i}", i, model
            await session.using(comp).insert(row)
            ids.append(int(row.id))
    maint = backend.get_table_maintenance()
    found = maint.range(item_ref, "model", float("-inf"), float("inf"))
    assert found == ids


# ============ ComponentTableManager：`hetu upgrade` 的建表 / 迁移 / 清易失数据 ============


def _def_mgr_comps(alpha_v: bool, beta_v: bool = False, beta_permission=None):
    """定义 MgrAlpha / MgrBeta 两个组件。alpha_v / beta_v 决定有没有 v 属性（去掉是
    不安全迁移、加上是安全迁移），beta_permission 只改 MgrBeta 的权限（不动 dtype）"""
    from hetu.data import (
        BaseComponent,
        ComponentDefines,
        Permission,
        define_component,
        property_field,
    )

    ComponentDefines().clear_()
    if alpha_v:

        @define_component(namespace="pytest", force=True)
        class MgrAlpha(BaseComponent):
            owner: np.int64 = property_field(0, unique=True)
            v: np.int32 = property_field(7)

    else:

        @define_component(namespace="pytest", force=True)
        class MgrAlpha(BaseComponent):
            owner: np.int64 = property_field(0, unique=True)

    beta_permission = beta_permission or Permission.USER
    if beta_v:

        @define_component(namespace="pytest", force=True, permission=beta_permission)
        class MgrBeta(BaseComponent):
            owner: np.int64 = property_field(0, unique=True)
            v: np.int32 = property_field(7)

    else:

        @define_component(namespace="pytest", force=True, permission=beta_permission)
        class MgrBeta(BaseComponent):
            owner: np.int64 = property_field(0, unique=True)

    return MgrAlpha, MgrBeta


def _mgr(backend, instance, alpha, beta, merged: bool):
    """按 System 划分重建簇，返回表管理器：分开时 MgrAlpha / MgrBeta 各在一簇（id 0 / 1），
    合并时两者同簇（id 都是 0），MgrBeta 的 cluster id 因此改变"""
    from hetu.manager import ComponentTableManager
    from hetu.system import SystemClusters, define_system

    SystemClusters()._clear()
    if merged:

        @define_system(namespace="pytest", components=(alpha, beta))
        async def mgr_use_both(ctx):
            pass

    else:

        @define_system(namespace="pytest", components=(alpha,))
        async def mgr_use_alpha(ctx):
            pass

        @define_system(namespace="pytest", components=(beta,))
        async def mgr_use_beta(ctx):
            pass

    SystemClusters().build_clusters("pytest")
    return ComponentTableManager("pytest", instance, {"default": backend})


def _status(tbl_mgr, comp) -> str:
    tbl = tbl_mgr.get_table(comp)
    return tbl.backend.get_table_maintenance().check_table(tbl)[0]


async def _insert_owner(tbl_mgr, comp, owner):
    async with tbl_mgr.get_table(comp).session() as session:
        row = comp.new_row()
        row.owner = owner
        await session.using(comp).insert(row)


async def _get_owner(tbl_mgr, comp, owner):
    async with tbl_mgr.get_table(comp).session() as session:
        return await session.using(comp).get(owner=owner)


async def test_manager_create_or_migrate_all(
    mod_auto_backend, new_component_env, new_clusters_env, tmp_path
):
    """`hetu upgrade` 的核心：新表直接建；cluster id 变了迁移过去、数据还在；schema 有
    不安全变更（删属性）时不带 force 返回 False 且不动表，force 才迁移"""
    app_file = str(tmp_path / "app.py")
    backend = mod_auto_backend()
    instance = "mgr_upgrade"

    # 新表：直接建
    alpha, beta = _def_mgr_comps(alpha_v=True)
    tm = _mgr(backend, instance, alpha, beta, merged=False)
    assert _status(tm, alpha) == _status(tm, beta) == "not_exists"
    assert tm.create_or_migrate_all(app_file) is True
    assert _status(tm, alpha) == _status(tm, beta) == "ok"
    await _insert_owner(tm, alpha, 1)
    await _insert_owner(tm, beta, 2)

    # 两个组件并进同一簇：MgrBeta 的 cluster id 变了，迁移后数据跟着过去
    tm = _mgr(backend, instance, alpha, beta, merged=True)
    assert _status(tm, alpha) == "ok"
    assert _status(tm, beta) == "cluster_mismatch"
    assert tm.create_or_migrate_all(app_file) is True
    assert _status(tm, beta) == "ok"
    await backend.wait_for_synced()
    assert await _get_owner(tm, beta, 2) is not None

    # 删掉 MgrAlpha.v：有损迁移，不带 force 不做
    alpha, beta = _def_mgr_comps(alpha_v=False)
    tm = _mgr(backend, instance, alpha, beta, merged=True)
    assert _status(tm, alpha) == "schema_mismatch"
    assert tm.create_or_migrate_all(app_file) is False
    assert _status(tm, alpha) == "schema_mismatch"
    assert tm.create_or_migrate_all(app_file, force=True) is True
    assert _status(tm, alpha) == "ok"
    await backend.wait_for_synced()
    row = await _get_owner(tm, alpha, 1)
    assert row is not None and "v" not in row.dtype.names


async def test_manager_migrates_cluster_and_schema_in_one_run(
    mod_auto_backend, new_component_env, new_clusters_env, tmp_path
):
    """同一张表 cluster id 和 schema（安全变更：加属性）同时变了：一次
    create_or_migrate_all 应该两步都做完。否则 `hetu upgrade` 报成功，表却仍是
    schema_mismatch，服务器启动时 check_and_create_new_tables 不通过"""
    app_file = str(tmp_path / "app.py")
    backend = mod_auto_backend()
    instance = "mgr_both"

    alpha, beta = _def_mgr_comps(alpha_v=False)
    tm = _mgr(backend, instance, alpha, beta, merged=False)
    assert tm.create_or_migrate_all(app_file) is True
    await _insert_owner(tm, beta, 2)

    # MgrBeta 并簇换 cluster id，同时加属性 v
    alpha, beta = _def_mgr_comps(alpha_v=False, beta_v=True)
    tm = _mgr(backend, instance, alpha, beta, merged=True)
    assert _status(tm, beta) == "cluster_mismatch"
    assert tm.create_or_migrate_all(app_file) is True
    assert _status(tm, beta) == "ok"
    await backend.wait_for_synced()
    row = await _get_owner(tm, beta, 2)
    assert row is not None and row.v == 7


async def test_manager_permission_only_change(
    mod_auto_backend, new_component_env, new_clusters_env, tmp_path
):
    """只改组件权限：meta 里存的 json / 版本号变了但 dtype 没变。`hetu upgrade` 之后表
    应该是 ok，否则 check_and_create_new_tables 一直报需要迁移，服务器起不来"""
    from hetu.data import Permission

    app_file = str(tmp_path / "app.py")
    backend = mod_auto_backend()
    instance = "mgr_perm"

    alpha, beta = _def_mgr_comps(alpha_v=False)
    tm = _mgr(backend, instance, alpha, beta, merged=False)
    assert tm.create_or_migrate_all(app_file) is True

    alpha, beta = _def_mgr_comps(alpha_v=False, beta_permission=Permission.ADMIN)
    tm = _mgr(backend, instance, alpha, beta, merged=False)
    assert _status(tm, beta) == "schema_mismatch"
    assert tm.create_or_migrate_all(app_file) is True
    assert _status(tm, beta) == "ok"
    assert tm.check_and_create_new_tables() is True


async def test_manager_flush_volatile(
    mod_auto_backend, new_component_env, new_clusters_env
):
    """flush_volatile 只清易失组件的数据，持久组件不动；持久组件不带 force 不许 flush"""
    from hetu.data import BaseComponent, define_component, property_field
    from hetu.manager import ComponentTableManager
    from hetu.system import SystemClusters, define_system

    @define_component(namespace="pytest", force=True)
    class MgrKeep(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)

    @define_component(namespace="pytest", force=True, volatile=True)
    class MgrTemp(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)

    @define_system(namespace="pytest", components=(MgrKeep, MgrTemp))
    async def mgr_use_keep_temp(ctx):
        pass

    SystemClusters().build_clusters("pytest")
    backend = mod_auto_backend()
    tm = ComponentTableManager("pytest", "mgr_flush", {"default": backend})
    assert tm.check_and_create_new_tables() is True
    await _insert_owner(tm, MgrKeep, 1)
    await _insert_owner(tm, MgrTemp, 1)

    tm.flush_volatile()
    await backend.wait_for_synced()
    assert await _get_owner(tm, MgrKeep, 1) is not None
    assert await _get_owner(tm, MgrTemp, 1) is None

    keep = tm.get_table(MgrKeep)
    with pytest.raises(ValueError):
        keep.backend.get_table_maintenance().flush(keep)


# ---------------------------------------------------------------------------
# 重建索引：`hetu upgrade` 默认每次都按行数据重建持久组件的索引，修掉索引残留
# ---------------------------------------------------------------------------


async def _insert_items(backend, comp, *fields):
    """插入 Item 行，fields 每项是 (owner, time, name)，返回插入的行"""
    rows = []
    async with backend.session("pytest", 1) as session:
        for owner, time_, name in fields:
            row = comp.new_row()
            row.owner, row.time, row.name = owner, time_, name
            await session.using(comp).insert(row)
            rows.append(row)
    await backend.wait_for_synced()
    return rows


@use_redis_family_backend_only
async def test_rebuild_index_removes_orphans(item_ref, mod_auto_backend):
    """重建按行数据来：索引里残留的、行已经不存在的项被清掉；表里一行都不剩时也要清"""
    from hetu.data.backend.redis import RedisBackendClient

    backend = mod_auto_backend()
    maint = backend.get_table_maintenance()
    io = backend.master.io
    idx_key = RedisBackendClient.index_key(item_ref, "owner")
    x, y = await _insert_items(backend, item_ref.comp_cls, (6, 1, "x"), (7, 2, "y"))

    maint.delete_row(item_ref, int(x.id))  # 只删行 key，owner 索引里留下 x
    maint.rebuild_index(item_ref)
    members = io.zrange(idx_key, 0, -1)
    assert [m.rsplit(b"\x00", 1)[-1] for m in members] == [str(y.id).encode()]

    maint.delete_row(item_ref, int(y.id))  # 表空了，索引里只剩残留
    maint.rebuild_index(item_ref)
    assert io.zrange(idx_key, 0, -1) == []


@use_redis_family_backend_only
async def test_rebuild_index_failure_keeps_old_index(item_ref, mod_auto_backend):
    """重建中途失败（这里是行数据违反 unique）：旧索引原样保留，不能留下空的或半截的
    索引——每次 hetu upgrade 都重建，失败后照样得能起服"""
    from hetu.data.backend.redis import RedisBackendClient

    backend = mod_auto_backend()
    maint = backend.get_table_maintenance()
    io = backend.master.io
    _a, b = await _insert_items(backend, item_ref.comp_cls, (1, 1, "a"), (1, 2, "b"))
    io.hset(RedisBackendClient.row_key(item_ref, int(b.id)), "name", "a")

    idx_key = RedisBackendClient.index_key(item_ref, "name")
    before = io.zrange(idx_key, 0, -1)
    with pytest.raises(RuntimeError, match="unique"):
        maint.rebuild_index(item_ref)
    assert io.zrange(idx_key, 0, -1) == before


@use_redis_family_backend_only
async def test_rebuild_index_without_snowflake(item_ref, mod_auto_backend, monkeypatch):
    """hetu upgrade 进程不初始化 SnowflakeID（它不占 worker 租约）：重建索引不能发号，
    有行的表照样重建，建出来的与 commit 写的逐字节一致"""
    from hetu.data.backend.redis import RedisBackendClient

    backend = mod_auto_backend()
    maint = backend.get_table_maintenance()
    io = backend.master.io
    await _insert_items(backend, item_ref.comp_cls, (6, 1, "x"), (7, 2, "y"))
    idx_keys = [
        RedisBackendClient.index_key(item_ref, name)
        for name in item_ref.comp_cls.indexes_
    ]
    before = [io.zrange(key, 0, -1) for key in idx_keys]
    assert all(before)

    monkeypatch.setattr(SnowflakeID(), "worker_id", -1)  # 模拟 upgrade 进程里未初始化
    maint.rebuild_index(item_ref)
    assert [io.zrange(key, 0, -1) for key in idx_keys] == before


@use_redis_family_backend_only
async def test_manager_rebuild_index_all(
    mod_auto_backend, new_component_env, new_clusters_env, monkeypatch
):
    """`hetu upgrade` 默认重建所有持久组件的索引，索引残留因此清掉；易失组件随后会被清空，
    不用重建"""
    from hetu.data import BaseComponent, define_component, property_field
    from hetu.data.backend.base import TableMaintenance
    from hetu.manager import ComponentTableManager
    from hetu.system import SystemClusters, define_system

    @define_component(namespace="pytest", force=True)
    class MgrKeep(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)

    @define_component(namespace="pytest", force=True, volatile=True)
    class MgrTemp(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)

    @define_system(namespace="pytest", components=(MgrKeep, MgrTemp))
    async def mgr_use_keep_temp(ctx):
        pass

    SystemClusters().build_clusters("pytest")
    backend = mod_auto_backend()
    tm = ComponentTableManager("pytest", "mgr_rebuild", {"default": backend})
    assert tm.check_and_create_new_tables() is True
    await _insert_owner(tm, MgrKeep, 1)
    await backend.wait_for_synced()
    keep = tm.get_table(MgrKeep)
    row = await _get_owner(tm, MgrKeep, 1)
    keep.backend.get_table_maintenance().delete_row(keep, int(row.id))  # 索引残留
    await backend.wait_for_synced()

    rebuilt = []
    real_rebuild = TableMaintenance.rebuild_index

    def spy(self, table_ref):
        rebuilt.append(table_ref.comp_name)
        return real_rebuild(self, table_ref)

    monkeypatch.setattr(TableMaintenance, "rebuild_index", spy)
    tm.rebuild_index_all()
    assert "MgrKeep" in rebuilt and "MgrTemp" not in rebuilt
    await backend.wait_for_synced()

    # 残留清掉了：读空后插入同值能提交（残留还在时每次都抛 InconsistentRangeRead）
    async with keep.session() as session:
        repo = session.using(MgrKeep)
        assert await repo.get(owner=1) is None
        new_row = MgrKeep.new_row()
        new_row.owner = 1
        await repo.insert(new_row)


def test_upgrade_rebuilds_index_by_default(monkeypatch):
    """hetu upgrade 默认重建索引，--no-rebuild-index 关掉"""
    from hetu.cli import CommandIndex
    from hetu.cli.migrate import MigrateCommand

    passed = []

    def fake_run(cls, config, yes, drop_data, rebuild_index=True):
        passed.append(rebuild_index)

    monkeypatch.setattr(MigrateCommand, "run", classmethod(fake_run))
    index = CommandIndex()
    index.register()
    base = ["upgrade", "--app-file", "app.py", "--namespace", "ns", "--instance", "s1"]
    MigrateCommand.execute(index.parser.parse_args(base))
    MigrateCommand.execute(index.parser.parse_args([*base, "--no-rebuild-index"]))
    assert passed == [True, False]


@use_redis_family_backend_only
async def test_live_worker_ids_sees_unexpired_leases(mod_auto_backend):
    """upgrade 靠 worker 租约判断服务器还在不在跑：没过期的租约都算"""
    from hetu.data.backend.worker_keeper import live_worker_ids

    backend = mod_auto_backend()
    io = backend.master.io
    key = "snowflake:worker:1023"  # 最后一个 id，别的测试起的服务一般占不到
    io.delete(key)
    assert 1023 not in live_worker_ids(backend)
    io.set(key, "pytest-node", ex=60)
    try:
        assert 1023 in live_worker_ids(backend)
    finally:
        io.delete(key)


def _exited_process() -> subprocess.Popen:
    """起一个进程并等它退出。Windows 上调用方握着返回的 Popen（也就握着进程句柄）期间，
    这个 pid 不会被别的进程复用"""
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    return proc


@use_redis_family_backend_only
async def test_live_worker_ids_skips_exited_local_workers(mod_auto_backend):
    """Windows 上 sanic 停 worker 是硬杀（Ctrl+C、DEBUG 自动重载都是 TerminateProcess），
    租约来不及释放。本机上进程已经退出了的租约不算"服务器在跑"，不然 upgrade 得干等它过期。

    只在 Windows 上这么认：别的平台上同一个机器码下可能是另一个 PID 空间（容器用 host
    网络），本地查不到 pid 不代表进程不在，照旧算在跑。本机活着的、别的机器的、值不是
    `机器码:pid` 的租约，哪个平台都算在跑"""
    from hetu.common.helper import get_machine_id
    from hetu.data.backend.worker_keeper import live_worker_ids

    backend = mod_auto_backend()
    io = backend.master.io
    exited = _exited_process()
    leases = {
        1019: f"{get_machine_id()}:{exited.pid}",
        1020: f"{get_machine_id()}:{os.getpid()}",
        1021: f"other-machine:{exited.pid}",
        1022: "pytest-node",
    }
    try:
        for worker_id, owner in leases.items():
            io.set(f"snowflake:worker:{worker_id}", owner, ex=60)
        live = set(live_worker_ids(backend)) & set(leases)
        assert live == ({1020, 1021, 1022} if sys.platform == "win32" else set(leases))
        # 只是不算，不删：key 照旧等 TTL 过期
        assert io.exists("snowflake:worker:1019")
    finally:
        for worker_id in leases:
            io.delete(f"snowflake:worker:{worker_id}")


def test_upgrade_refuses_while_servers_running(monkeypatch, tmp_path, capsys):
    """还有服务器持有 worker 租约时 upgrade 以退出码 1 退出：迁移、清空易失表、重建索引
    在服务器运行时执行都会写坏数据。在加载 app 之前就退出，什么都没动"""
    from hetu.cli.migrate import MigrateCommand
    from hetu.data.backend import worker_keeper

    monkeypatch.setattr(worker_keeper, "live_worker_ids", lambda backend: [3])
    db = (tmp_path / "db.sqlite3").as_posix()
    config = {
        "APP_FILE": str(tmp_path / "no_such_app.py"),
        "NAMESPACE": "ns",
        "INSTANCES": ["s1"],
        "BACKENDS": {"SQLite": {"type": "sqlite", "master": f"sqlite:///{db}"}},
    }
    with pytest.raises(SystemExit) as exc_info:
        MigrateCommand.run(config, True, False)
    assert exc_info.value.code == 1
    assert "3" in capsys.readouterr().out
