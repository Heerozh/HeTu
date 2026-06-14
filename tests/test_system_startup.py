"""
App 启动钩子 @define_system(on_start=True) 的测试
@author: Heerozh (Zhang Jianhao)
"""

import numpy as np

from hetu.common.snowflake_id import SnowflakeID
from hetu.data import BaseComponent, define_component, property_field
from hetu.system import SystemClusters, define_system
from hetu.system.lock import SystemLock

# 运行期 System 创建新行需要 SnowflakeID（生产中由 start_backends 初始化）
SnowflakeID().init(1, 0)


def _has_system_lock(full_components) -> bool:
    return any(
        c == SystemLock or getattr(c, "master_", None) == SystemLock
        for c in full_components
    )


def test_on_start_auto_enables_call_lock(new_component_env, new_clusters_env):
    """on_start=True 的 System 即使没写 call_lock，也会自动挂上 SystemLock 副本，
    否则框架级"永久只跑一次"(uuid 去重)无法工作。"""

    @define_component(namespace="pytest", force=True)
    class SeedComp(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)
        v: np.int32 = property_field(0)

    @define_system(namespace="pytest", components=(SeedComp,), on_start=True)
    async def seed_sys(ctx):
        pass

    # 对照：普通 System 不挂锁
    @define_system(namespace="pytest", components=(SeedComp,))
    async def normal_sys(ctx):
        pass

    SystemClusters().build_clusters("pytest")

    seed_def = SystemClusters().get_system("seed_sys", namespace="pytest")
    normal_def = SystemClusters().get_system("normal_sys", namespace="pytest")
    assert seed_def is not None and normal_def is not None
    assert _has_system_lock(seed_def.full_components)
    assert not _has_system_lock(normal_def.full_components)


def test_get_startup_systems_lists_only_marked(new_component_env, new_clusters_env):
    """get_startup_systems 只返回 on_start=True 的 System 名字，普通 System 不在内。"""

    @define_component(namespace="pytest", force=True)
    class SeedComp(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)
        v: np.int32 = property_field(0)

    @define_system(namespace="pytest", components=(SeedComp,), on_start=True)
    async def seed_sys(ctx):
        pass

    @define_system(namespace="pytest", components=(SeedComp,))
    async def normal_sys(ctx):
        pass

    SystemClusters().build_clusters("pytest")
    assert SystemClusters().get_startup_systems("pytest") == ["seed_sys"]


async def test_run_startup_systems_same_boot_dedups(
    mod_auto_backend, new_component_env, new_clusters_env
):
    """同一 boot uuid（=同次开服的多个 worker）只成功提交一次（SystemLock uuid 去重）。"""
    from hetu.manager import ComponentTableManager
    from hetu.system.startup import run_startup_systems

    @define_component(namespace="pytest", force=True)
    class SeedCounter(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)
        n: np.int32 = property_field(0)

    @define_system(namespace="pytest", components=(SeedCounter,), on_start=True)
    async def seed_counter(ctx):
        async with ctx.repo[SeedCounter].upsert(owner=1) as row:
            row.n += 1

    SystemClusters().build_clusters("pytest")

    backend = mod_auto_backend()
    tbl_mgr = ComponentTableManager("pytest", "server1", {"default": backend})
    tbl_mgr._flush_all(force=True)

    await run_startup_systems("pytest", {"server1": tbl_mgr}, "boot-same")
    await run_startup_systems("pytest", {"server1": tbl_mgr}, "boot-same")

    tbl = tbl_mgr.get_table(SeedCounter)
    async with tbl.session() as session:
        repo = session.using(SeedCounter)
        row = await repo.get(owner=1)
        assert row is not None
        assert row.n == 1


async def test_run_startup_systems_per_instance_isolated(
    mod_auto_backend, new_component_env, new_clusters_env
):
    """多 instance 时，每个 instance 各自 seed 一次，互不影响。"""
    from hetu.manager import ComponentTableManager
    from hetu.system.startup import run_startup_systems

    @define_component(namespace="pytest", force=True)
    class SeedCounter(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)
        n: np.int32 = property_field(0)

    @define_system(namespace="pytest", components=(SeedCounter,), on_start=True)
    async def seed_counter(ctx):
        async with ctx.repo[SeedCounter].upsert(owner=1) as row:
            row.n += 1

    SystemClusters().build_clusters("pytest")

    backend = mod_auto_backend()
    tm1 = ComponentTableManager("pytest", "server1", {"default": backend})
    tm2 = ComponentTableManager("pytest", "server2", {"default": backend})
    tm1._flush_all(force=True)
    tm2._flush_all(force=True)

    await run_startup_systems("pytest", {"server1": tm1, "server2": tm2}, "boot-x")

    for tm in (tm1, tm2):
        tbl = tm.get_table(SeedCounter)
        async with tbl.session() as session:
            row = await session.using(SeedCounter).get(owner=1)
            assert row is not None
            assert row.n == 1


async def test_run_startup_systems_propagates_failure(
    mod_auto_backend, new_component_env, new_clusters_env
):
    """on_start System 抛出非竞态异常时，run_startup_systems 向上传播（供 worker_start 中止启动）。"""
    import pytest

    from hetu.manager import ComponentTableManager
    from hetu.system.startup import run_startup_systems

    @define_component(namespace="pytest", force=True)
    class SeedComp(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)
        v: np.int32 = property_field(0)

    @define_system(namespace="pytest", components=(SeedComp,), on_start=True)
    async def seed_boom(ctx):
        raise RuntimeError("boom")

    SystemClusters().build_clusters("pytest")

    backend = mod_auto_backend()
    tbl_mgr = ComponentTableManager("pytest", "server1", {"default": backend})
    tbl_mgr._flush_all(force=True)

    with pytest.raises(RuntimeError, match="boom"):
        await run_startup_systems("pytest", {"server1": tbl_mgr}, "boot-x")


def _fake_app(tbl_mgr):
    """构造一个够 worker_start 用的最小 app 替身（config 用 dict，ctx 持有 table_managers）。"""
    from types import SimpleNamespace

    stopped = []
    app = SimpleNamespace()
    app.config = {"NAMESPACE": "pytest", "APP_FILE": "x", "INSTANCES": ["server1"]}
    app.ctx = SimpleNamespace(table_managers={"server1": tbl_mgr})
    app.stop = lambda: stopped.append(True)
    app.stopped = stopped
    return app


async def test_worker_start_runs_on_start_systems(
    mod_auto_backend, new_component_env, new_clusters_env, monkeypatch
):
    """worker_start 在 backends 就绪后会执行 on_start System（接线正确）。"""
    import hetu.server.main as main_mod
    from hetu.manager import ComponentTableManager

    @define_component(namespace="pytest", force=True)
    class SeedComp(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)
        n: np.int32 = property_field(0)

    @define_system(namespace="pytest", components=(SeedComp,), on_start=True)
    async def seed_sys(ctx):
        async with ctx.repo[SeedComp].upsert(owner=1) as row:
            row.n += 1

    SystemClusters().build_clusters("pytest")

    backend = mod_auto_backend()
    tbl_mgr = ComponentTableManager("pytest", "server1", {"default": backend})
    tbl_mgr._flush_all(force=True)

    # start_backends 的产物（table_managers/雪花id）我们已手动备好，stub 掉它
    async def fake_start_backends(_app):
        pass

    monkeypatch.setattr(main_mod, "start_backends", fake_start_backends)

    app = _fake_app(tbl_mgr)
    await main_mod.worker_start(app)

    assert app.stopped == []
    tbl = tbl_mgr.get_table(SeedComp)
    async with tbl.session() as session:
        row = await session.using(SeedComp).get(owner=1)
        assert row is not None
        assert row.n == 1


async def test_worker_start_aborts_on_on_start_failure(
    mod_auto_backend, new_component_env, new_clusters_env, monkeypatch
):
    """on_start System 启动失败时，worker_start 调用 app.stop() 中止，且不向上抛。"""
    import hetu.server.main as main_mod
    from hetu.manager import ComponentTableManager

    @define_component(namespace="pytest", force=True)
    class SeedComp(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)
        v: np.int32 = property_field(0)

    @define_system(namespace="pytest", components=(SeedComp,), on_start=True)
    async def seed_boom(ctx):
        raise RuntimeError("boom")

    SystemClusters().build_clusters("pytest")

    backend = mod_auto_backend()
    tbl_mgr = ComponentTableManager("pytest", "server1", {"default": backend})
    tbl_mgr._flush_all(force=True)

    async def fake_start_backends(_app):
        pass

    monkeypatch.setattr(main_mod, "start_backends", fake_start_backends)

    app = _fake_app(tbl_mgr)
    # 不应抛出，而是 stop
    await main_mod.worker_start(app)

    assert app.stopped == [True]


def test_on_start_rejects_client_permission(new_component_env, new_clusters_env):
    """on_start System 不能用客户端可调用权限（USER/EVERYBODY），防止种子逻辑被外部触发。"""
    import pytest

    from hetu.data import Permission

    @define_component(namespace="pytest", force=True)
    class SeedComp(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)
        v: np.int32 = property_field(0)

    with pytest.raises(AssertionError, match="on_start"):

        @define_system(
            namespace="pytest",
            components=(SeedComp,),
            on_start=True,
            permission=Permission.USER,
        )
        async def seed_user(ctx):
            pass

    with pytest.raises(AssertionError, match="on_start"):

        @define_system(
            namespace="pytest",
            components=(SeedComp,),
            on_start=True,
            permission=Permission.EVERYBODY,
        )
        async def seed_everybody(ctx):
            pass


def test_on_start_allows_admin_permission(new_component_env, new_clusters_env):
    """on_start System 允许 permission=ADMIN（不被过度限制为只能 None）。"""
    from hetu.data import Permission

    @define_component(namespace="pytest", force=True)
    class SeedComp(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)
        v: np.int32 = property_field(0)

    @define_system(
        namespace="pytest",
        components=(SeedComp,),
        on_start=True,
        permission=Permission.ADMIN,
    )
    async def seed_admin(ctx):
        pass

    SystemClusters().build_clusters("pytest")
    assert SystemClusters().get_startup_systems("pytest") == ["seed_admin"]


async def test_run_startup_systems_reruns_next_boot(
    mod_auto_backend, new_component_env, new_clusters_env
):
    """不同 boot uuid（=不同次开服）会让 on_start System 再次执行（每次开服跑一次）。"""
    from hetu.manager import ComponentTableManager
    from hetu.system.startup import run_startup_systems

    @define_component(namespace="pytest", force=True)
    class SeedCounter(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)
        n: np.int32 = property_field(0)

    @define_system(namespace="pytest", components=(SeedCounter,), on_start=True)
    async def seed_counter(ctx):
        async with ctx.repo[SeedCounter].upsert(owner=1) as row:
            row.n += 1

    SystemClusters().build_clusters("pytest")

    backend = mod_auto_backend()
    tbl_mgr = ComponentTableManager("pytest", "server1", {"default": backend})
    tbl_mgr._flush_all(force=True)

    await run_startup_systems("pytest", {"server1": tbl_mgr}, "boot-A")
    await run_startup_systems("pytest", {"server1": tbl_mgr}, "boot-B")

    tbl = tbl_mgr.get_table(SeedCounter)
    async with tbl.session() as session:
        row = await session.using(SeedCounter).get(owner=1)
        assert row is not None
        assert row.n == 2


def test_make_boot_uuid_unique_and_within_lock_width():
    """make_boot_uuid 每次不同，且长度 <= 32（SystemLock.uuid 为 <U32）。"""
    from hetu.system.startup import make_boot_uuid

    a, b = make_boot_uuid(), make_boot_uuid()
    assert a != b
    assert 0 < len(a) <= 32


async def test_worker_start_dedups_with_shared_boot_uuid(
    mod_auto_backend, new_component_env, new_clusters_env, monkeypatch
):
    """同次开服的多个 worker（worker_start 用相同 config boot uuid）只 seed 一次。"""
    import hetu.server.main as main_mod
    from hetu.manager import ComponentTableManager
    from hetu.system.startup import ON_START_UUID_CONFIG_KEY

    @define_component(namespace="pytest", force=True)
    class SeedCounter(BaseComponent):
        owner: np.int64 = property_field(0, unique=True)
        n: np.int32 = property_field(0)

    @define_system(namespace="pytest", components=(SeedCounter,), on_start=True)
    async def seed_counter(ctx):
        async with ctx.repo[SeedCounter].upsert(owner=1) as row:
            row.n += 1

    SystemClusters().build_clusters("pytest")

    backend = mod_auto_backend()
    tbl_mgr = ComponentTableManager("pytest", "server1", {"default": backend})
    tbl_mgr._flush_all(force=True)

    async def fake_start_backends(_app):
        pass

    monkeypatch.setattr(main_mod, "start_backends", fake_start_backends)

    # 两个 worker 共享同一 config boot uuid（模拟同次开服的多进程）
    for _ in range(2):
        app = _fake_app(tbl_mgr)
        app.config[ON_START_UUID_CONFIG_KEY] = "boot-shared"
        await main_mod.worker_start(app)

    tbl = tbl_mgr.get_table(SeedCounter)
    async with tbl.session() as session:
        row = await session.using(SeedCounter).get(owner=1)
        assert row is not None
        assert row.n == 1
