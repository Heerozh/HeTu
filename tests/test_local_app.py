"""
hetu.local.LocalApp（hetu call / shell 与 Sandbox 共用的进程内应用运行时）的测试：
打开用户的库（不建表）、身份默认值、按需表结构校验、提交观察钩子。
"""

import asyncio

import pytest

from hetu.common.permission import Permission
from hetu.data.backend.session import commit_observer
from hetu.endpoint.executor import permission_allows
from hetu.local import (
    IdentityRequired,
    LocalApp,
    TableNotReady,
    open_local_app,
    resolve_identity,
)
from hetu.manager import ComponentTableManager
from hetu.system import SystemClusters

INSTANCE = "localapp"


@pytest.fixture(scope="module")
def mod_local_config(mod_test_app, mod_auto_backend, mod_backend_config):
    """按服务器开服的方式（check_and_create_new_tables）在独立 instance 上建好表，返回配置"""
    backend = mod_auto_backend()
    tbl_mgr = ComponentTableManager("pytest", INSTANCE, {"default": backend})
    tbl_mgr.check_and_create_new_tables()
    tbl_mgr._flush_all(force=True)
    return {
        "APP_FILE": "unused.py",  # 簇已由 mod_test_app 建好，open_local_app 不再加载
        "NAMESPACE": "pytest",
        "INSTANCES": [INSTANCE],
        "BACKENDS": {"main": mod_backend_config},
    }


async def test_open_local_app_runs_systems(mod_local_config):
    app = await open_local_app(mod_local_config, address="cli")
    try:
        assert isinstance(app, LocalApp)
        assert app.lease is not None and app.lease.worker_id >= 1000
        # USER System 显式给 caller；raw 拿原始返回值
        ret = await app.call_system("add_rls_comp_value", 9, caller=4321, raw=True)
        assert ret == 109
        assert (await app.must_get("RLSComp", owner=4321)).value == 109
        # 默认返回客户端实际收到的内容：普通返回值在线路上是 "ok"
        assert await app.call_system("add_rls_comp_value", 1, caller=4321) == "ok"
        assert await app.call_system("echo_response", (1, "a")) == [1, "a"]
        # insert / upsert / range 与 Sandbox 同一套实现
        rid = await app.insert("IndexComp1", owner=1, value=1.5)
        assert rid > 0
        async with app.upsert("IndexComp1", owner=2) as row:
            row.value = 2.5
        rows = await app.range("IndexComp1", "value", 1.0, 3.0)
        assert sorted(rows.owner.tolist()) == [1, 2]
    finally:
        await app.aclose()
    assert app.lease is None


async def test_identity_defaults(mod_local_config):
    clusters = SystemClusters()
    user = clusters.get_system("add_rls_comp_value")
    admin = clusters.get_system("push_headless_command")
    everybody = clusters.get_system("echo_response")
    internal = clusters.get_system("on_disconnect")
    assert user and admin and everybody and internal
    assert internal.permission is None

    with pytest.raises(IdentityRequired):
        resolve_identity(user, None, None)
    with pytest.raises(IdentityRequired):
        resolve_identity(user, None, "admin")  # 只给 group 也不行：caller=0 线上不可能
    assert resolve_identity(user, 1001, None) == (1001, "guest")
    assert resolve_identity(user, 1001, "gm") == (1001, "gm")
    assert resolve_identity(admin, None, None) == (0, "admin")
    assert resolve_identity(internal, None, None) == (0, "admin")
    assert resolve_identity(everybody, None, None) == (0, "guest")

    app = await open_local_app(mod_local_config, mint_ids=False)
    try:
        with pytest.raises(IdentityRequired):
            await app.call_system("add_rls_comp_value", 1)
    finally:
        await app.aclose()


def test_permission_allows_matches_endpoint_rules():
    """permission_allows 就是 execute_check 用的判定，默认拒绝"""
    cases = {
        (Permission.EVERYBODY, 0, "guest"): True,
        (Permission.USER, 0, "admin"): False,
        (Permission.USER, 5, "guest"): True,
        (Permission.GM, 0, "admin"): True,
        (Permission.GM, 0, "gm"): False,
        (Permission.GM, 5, "gm"): True,
        (Permission.GM, 5, "guest"): False,
        (Permission.ADMIN, 5, "gm"): False,
        (Permission.ADMIN, 0, "admin_tool"): True,
        (Permission.OWNER, 5, "admin"): False,
        (Permission.RLS, 5, "admin"): False,
        (None, 0, "admin"): False,
    }
    for (permission, caller, group), expected in cases.items():
        assert permission_allows(permission, caller, group) is expected, (
            permission,
            caller,
            group,
        )


async def test_tables_verified_lazily(
    mod_auto_backend, mod_backend_config, mod_test_app
):
    """只校验用到的表：缺表的组件只在用到时报错，不挡别的；绝不建表"""
    backend = mod_auto_backend()
    instance = "localapp_partial"
    tbl_mgr = ComponentTableManager("pytest", instance, {"default": backend})
    from hetu.data.backend.worker_keeper import WorkerLease
    from hetu.system.lock import SystemLock

    # 只建 add_rls_comp_value 用到的表（含它的调用锁副本）和租约表
    wanted = {"RLSComp", WorkerLease.name_}
    for comp, tbl in tbl_mgr.items():
        if comp.name_ in wanted or comp.master_ is SystemLock:
            maint = backend.get_table_maintenance()
            if maint.check_table(tbl)[0] == "not_exists":
                maint.create_table(tbl)
    config = {
        "APP_FILE": "unused.py",
        "NAMESPACE": "pytest",
        "INSTANCES": [instance],
        "BACKENDS": {"main": mod_backend_config},
    }
    app = await open_local_app(config)
    try:
        assert await app.call_system("add_rls_comp_value", 3, caller=7, raw=True) == 103
        with pytest.raises(TableNotReady) as exc_info:
            await app.get("IndexComp1", owner=1)
        assert exc_info.value.status == "not_exists"
        with pytest.raises(TableNotReady):
            await app.call_system("set_public_name", 1, "x")
        tbl = tbl_mgr.get_table("IndexComp1")
        assert tbl is not None
        assert backend.get_table_maintenance().check_table(tbl)[0] == "not_exists"
    finally:
        await app.aclose()


async def test_table_mismatch_reported(mod_local_config, monkeypatch):
    """check_table 报不一致时拒绝使用该表"""
    app = await open_local_app(mod_local_config, mint_ids=False)
    try:
        tbl = app.tbl_mgr.get_table("IndexComp2")
        assert tbl is not None
        maint = type(tbl.backend.get_table_maintenance())
        original = maint.check_table

        def fake_check(self, table_ref):
            if table_ref.comp_name == "IndexComp2":
                return "cluster_mismatch", None
            return original(self, table_ref)

        monkeypatch.setattr(maint, "check_table", fake_check)
        with pytest.raises(TableNotReady) as exc_info:
            await app.get("IndexComp2", owner=1)
        assert exc_info.value.status == "cluster_mismatch"
    finally:
        await app.aclose()


async def test_commit_observer(mod_local_config):
    """提交观察钩子：dry-run 不提交但拿得到写集；拒绝写入时 System 失败且不重试"""
    app = await open_local_app(mod_local_config)
    try:
        seen = []

        async def dry_run(session, _commit_fn):
            seen.append(session.idmap.get_dirty_rows())

        token = commit_observer.set(dry_run)
        try:
            await app.call_system("set_public_name", 77, "dry")
        finally:
            commit_observer.reset(token)
        assert await app.get("PublicNames", owner=77) is None
        assert len(seen) == 1
        inserts = next(iter(seen[0].values()))[0]
        assert inserts[0]["name"] == "dry"

        class Forbidden(Exception):
            pass

        calls = 0

        async def forbid(_session, _commit_fn):
            nonlocal calls
            calls += 1
            raise Forbidden

        token = commit_observer.set(forbid)
        try:
            with pytest.raises(Forbidden):
                await app.call_system("set_public_name", 78, "no")
            # 只读：不触发观察者
            assert await app.call_system("get_disconnect_count", 1, raw=True)
        finally:
            commit_observer.reset(token)
        assert calls == 1

        # 观察者设置之前创建的任务（如租约循环）不受它影响
        committed = []

        async def record(session, commit_fn):
            committed.append(session)
            await commit_fn(session.idmap)

        started = asyncio.Event()
        go = asyncio.Event()

        async def earlier_task():
            started.set()
            await go.wait()
            await app.call_system("set_public_name", 79, "bg")

        task = asyncio.create_task(earlier_task())
        await started.wait()
        token = commit_observer.set(record)
        try:
            go.set()
            await task
        finally:
            commit_observer.reset(token)
        assert committed == []
        assert (await app.must_get("PublicNames", owner=79)).name == "bg"
    finally:
        await app.aclose()
