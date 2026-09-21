import time

import pytest

import hetu.endpoint.connection as connection
from hetu.common.snowflake_id import SnowflakeID
from hetu.endpoint.executor import EndpointExecutor

SnowflakeID().init(1, 0)


async def test_connect_kick(mod_test_app, tbl_mgr, new_ctx):
    # 先登录2个连接
    executor1 = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    await executor1.initialize("")
    await executor1.execute("login", 1)

    executor2 = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    await executor2.initialize("")
    await executor2.execute("login", 2)

    ok, _ = await executor1.execute("add_rls_comp_value", 1)
    assert ok
    ok, _ = await executor2.execute("add_rls_comp_value", 10)
    assert ok

    # 测试重复登录踢出已登录用户
    executor1_replaced = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    await executor1_replaced.initialize("")
    await executor1_replaced.execute("login", 1)

    # 测试运行第一个连接的system，然后看是否失败
    ok, _ = await executor1.execute("test_rls_comp_value", 101)
    assert not ok
    # 这个的值应该是之前executor1的
    ok, _ = await executor1_replaced.execute("test_rls_comp_value", 101)
    assert ok

    # 结束连接
    await executor1.terminate()
    await executor2.terminate()
    await executor1_replaced.terminate()


async def test_connect_not_kick(mod_test_app, tbl_mgr, new_ctx):
    # 初始化第一个连接
    executor1 = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    await executor1.initialize("")
    await executor1.execute("login", 1)
    ok, _ = await executor1.execute("add_rls_comp_value", 2)
    assert ok
    ok, _ = await executor1.execute("test_rls_comp_value", 102)
    assert ok

    # 不强制踢出是否生效
    executor1_not_replace = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    await executor1_not_replace.initialize("")
    # 默认为0, 要设为高值防止下面依旧强制踢出。注意目前t是按连接方ctx的imeout值来判断的，此值
    connection.ENDPOINT_CALL_IDLE_TIMEOUT = 2
    ok, app_login_rsp = await executor1_not_replace.execute("login", 1, False)
    assert app_login_rsp and type(app_login_rsp.message) is dict
    assert app_login_rsp.message["id"] == 0  # app中定义的返回值
    ok, _ = await executor1.execute("test_rls_comp_value", 102)
    assert ok

    # 结束连接
    await executor1.terminate()
    await executor1_not_replace.terminate()


async def test_connect_kick_timeout(monkeypatch, mod_test_app, tbl_mgr, new_ctx):
    time_time = time.time

    # 初始化第一个连接
    executor1 = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    await executor1.initialize("")
    await executor1.execute("login", 1)
    ok, _ = await executor1.execute("add_rls_comp_value", 3)
    assert ok
    ok, _ = await executor1.execute("test_rls_comp_value", 103)
    assert ok

    # 测试last active超时是否踢出用户
    # 不强制踢出，但是timeout应该生效
    executor1_timeout_replaced = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    monkeypatch.setattr(
        time, "time", lambda: time_time() + connection.ENDPOINT_CALL_IDLE_TIMEOUT
    )
    await executor1_timeout_replaced.initialize("")
    ok, app_login_rsp = await executor1_timeout_replaced.execute("login", 1, False)
    assert app_login_rsp and type(app_login_rsp.message) is dict
    assert app_login_rsp.message["id"] == 1  # app中定义的返回值

    # 上一次kick的连接应该失效
    ok, _ = await executor1.execute("test_rls_comp_value", 103)
    assert not ok

    # 新的应该有效
    ok, _ = await executor1_timeout_replaced.execute("add_rls_comp_value", -2)
    assert ok
    ok, _ = await executor1_timeout_replaced.execute("test_rls_comp_value", 101)
    assert ok

    # 结束连接
    await executor1.terminate()
    await executor1_timeout_replaced.terminate()


def _patch_reads(stack, backend, conn_id: int):
    """把 backend 所有 servant / master 的 get 包上计数，只数读 Connection 表 conn_id 那行的
    次数（事务内别的读也走同一批 client，不能混进来），返回 (servant_reads, master_reads)"""
    from unittest.mock import patch

    def count(mocks):
        return sum(
            1
            for m in mocks
            for c in m.call_args_list
            if c.args[0].comp_cls is connection.Connection and int(c.args[1]) == conn_id
        )

    servant_mocks = [
        stack.enter_context(patch.object(c, "get", wraps=c.get))
        for c in backend._servants
    ]
    master_mock = stack.enter_context(
        patch.object(backend.master, "get", wraps=backend.master.get)
    )
    return (lambda: count(servant_mocks), lambda: count([master_mock]))


async def test_alive_checker_default_checks_every_call(mod_test_app, tbl_mgr, new_ctx):
    """没接通知的裸 executor：登录用户每次调用都读一次 Connection 行（原行为，回归护栏）"""
    from contextlib import ExitStack

    executor = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    await executor.initialize("")
    await executor.execute("login", 1)
    backend = executor.alive_checker.conn_tbl.backend
    with ExitStack() as stack:
        servant_reads, _ = _patch_reads(stack, backend, executor.context.connection_id)
        for i in range(3):
            ok, _ = await executor.execute("add_rls_comp_value", i)
            assert ok
        assert servant_reads() == 3
    await executor.terminate()


async def test_alive_checker_notify_mode(monkeypatch, mod_test_app, tbl_mgr, new_ctx):
    """通知模式：只在脏标记（收到本连接 Connection 行变更通知）时才读；被顶号后仍能检出"""
    from contextlib import ExitStack

    monkeypatch.setattr(connection, "CONNECTION_ALIVE_RECHECK_INTERVAL", 3600)
    executor = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    await executor.initialize("")
    on_change = executor.alive_checker.enable_notify_mode()
    await executor.execute("login", 1)
    backend = executor.alive_checker.conn_tbl.backend
    with ExitStack() as stack:
        servant_reads, _ = _patch_reads(stack, backend, executor.context.connection_id)
        for i in range(3):
            ok, _ = await executor.execute("add_rls_comp_value", i)
            assert ok
        assert servant_reads() == 1  # 只有登录后首个调用核了一次
        on_change()
        ok, _ = await executor.execute("add_rls_comp_value", 9)
        assert ok
        assert servant_reads() == 2

        # 被另一个连接顶号 → 通知置脏 → 下次调用检出
        executor2 = EndpointExecutor("pytest", tbl_mgr, new_ctx())
        await executor2.initialize("")
        await executor2.execute("login", 1)
        await backend.wait_for_synced()
        on_change()
        ok, _ = await executor.execute("add_rls_comp_value", 10)
        assert not ok
        assert servant_reads() == 3
    await executor2.terminate()
    await executor.terminate()


async def test_alive_checker_fallback_interval(
    monkeypatch, mod_test_app, tbl_mgr, new_ctx
):
    """通知模式的兜底间隔：0 = 每次都读；超过间隔未读则读一次"""
    from contextlib import ExitStack

    time_time = time.time
    monkeypatch.setattr(connection, "CONNECTION_ALIVE_RECHECK_INTERVAL", 0)
    executor = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    await executor.initialize("")
    executor.alive_checker.enable_notify_mode()
    await executor.execute("login", 1)
    backend = executor.alive_checker.conn_tbl.backend
    with ExitStack() as stack:
        servant_reads, _ = _patch_reads(stack, backend, executor.context.connection_id)
        for i in range(3):
            await executor.execute("add_rls_comp_value", i)
        assert servant_reads() == 3

        monkeypatch.setattr(connection, "CONNECTION_ALIVE_RECHECK_INTERVAL", 3600)
        await executor.execute("add_rls_comp_value", 4)
        assert servant_reads() == 3  # 间隔内不读
        monkeypatch.setattr(time, "time", lambda: time_time() + 4000)
        await executor.execute("add_rls_comp_value", 5)
        assert servant_reads() == 4  # 超过间隔读一次
    await executor.terminate()


async def test_alive_checker_kicked_by_master_read(mod_test_app, tbl_mgr, new_ctx):
    """kicked()：从 master 读一次判断是否被顶号，不开 Session、不写 last_active"""
    from contextlib import ExitStack
    from unittest.mock import patch

    executor = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    await executor.initialize("")
    ctx = executor.context
    checker = executor.alive_checker
    backend = checker.conn_tbl.backend

    # 未登录：直接 False，不读库
    with ExitStack() as stack:
        servant_reads, master_reads = _patch_reads(stack, backend, ctx.connection_id)
        assert await checker.kicked(ctx) is False
        assert servant_reads() == 0 and master_reads() == 0

    await executor.execute("login", 1)

    def guarded():
        # kicked() 本身不开 Session、不写 last_active，只读 master
        return (
            patch.object(backend, "session", wraps=backend.session),
            patch.object(checker.conn_tbl.backend.master, "direct_set"),
        )

    with ExitStack() as stack:
        m_session, m_direct = (stack.enter_context(p) for p in guarded())
        servant_reads, master_reads = _patch_reads(stack, backend, ctx.connection_id)
        assert await checker.kicked(ctx) is False
        assert master_reads() == 1 and servant_reads() == 0
        m_session.assert_not_called()
        m_direct.assert_not_called()

    executor2 = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    await executor2.initialize("")
    await executor2.execute("login", 1)  # 顶号（这里会开事务，放在守卫之外）

    with ExitStack() as stack:
        m_session, m_direct = (stack.enter_context(p) for p in guarded())
        servant_reads, master_reads = _patch_reads(stack, backend, ctx.connection_id)
        assert await checker.kicked(ctx) is True
        assert master_reads() == 1 and servant_reads() == 0
        m_session.assert_not_called()
        m_direct.assert_not_called()
    await executor2.terminate()
    await executor.terminate()


async def test_flood_detect(mod_test_app, tbl_mgr, caplog, new_ctx):
    connection.MAX_ANONYMOUS_CONNECTION_BY_IP = 3

    executors = []
    with pytest.raises(RuntimeError, match="IP匿名连接数"):
        for i in range(5):
            loc_executor = EndpointExecutor("pytest", tbl_mgr, new_ctx())
            await loc_executor.initialize(f"233.111.111.111")
            executors.append(loc_executor)

    assert "IP匿名连接数" in caplog.text

    for loc_executor in executors:
        await loc_executor.terminate()


async def test_future_call_bypass_flood_detect(mod_test_app, tbl_mgr, new_ctx):
    # 测试连接，包括flood检测等，特别是future不应该遇到flood检测
    # 不然服务器反复重启后会提示flood
    connection.MAX_ANONYMOUS_CONNECTION_BY_IP = 3

    executors = []
    # 以下代码应该成功调用没有报错
    for i in range(5):
        loc_executor = EndpointExecutor("pytest", tbl_mgr, new_ctx())
        # 使用localhost ip地址让连接flood检测不报错
        await loc_executor.initialize(f"localhost")
        executors.append(loc_executor)

    for loc_executor in executors:
        await loc_executor.terminate()


def test_endpoint_reject_owner_rls():
    """发现5回归：define_endpoint 不允许 OWNER/RLS（与 define_system 一致），
    否则执行网关会 fall-through 成『任何人可调用』。"""
    from hetu.common import Permission
    from hetu.endpoint.definer import define_endpoint

    with pytest.raises(AssertionError, match="权限"):

        @define_endpoint(namespace="pytest", force=True, permission=Permission.OWNER)
        async def ep_owner(ctx):
            pass

    with pytest.raises(AssertionError, match="权限"):

        @define_endpoint(namespace="pytest", force=True, permission=Permission.RLS)
        async def ep_rls(ctx):
            pass


async def test_endpoint_permission_fail_closed(mod_test_app, tbl_mgr, new_ctx):
    """发现5回归：执行网关对未处理的权限级别(OWNER/RLS/未知)失败关闭。
    即使绕过 define_endpoint 定义期断言(模拟 -O 剥掉断言)注册了 OWNER 端点，
    匿名客户端也必须被拒绝。"""
    from hetu.common import Permission
    from hetu.endpoint.definer import EndpointDefines

    async def ep_owner_sneaky(ctx):
        pass

    async def ep_public(ctx):
        pass

    # 直接 add，绕过 define_endpoint 的定义期检查（等价于 -O 下断言被剥掉的情形）
    EndpointDefines().add("pytest", ep_owner_sneaky, True, Permission.OWNER)
    EndpointDefines().add("pytest", ep_public, True, Permission.EVERYBODY)

    executor = EndpointExecutor("pytest", tbl_mgr, new_ctx())  # 匿名 caller=0
    # OWNER 端点：网关失败关闭，拒绝匿名（实则拒绝所有）调用
    assert executor.execute_check("ep_owner_sneaky", ()) is None
    # 对照：EVERYBODY 端点匿名可正常通过网关
    assert executor.execute_check("ep_public", ()) is not None
