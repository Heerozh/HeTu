import asyncio
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


async def test_on_elevated_hook_fires_once_per_login(mod_test_app, tbl_mgr, new_ctx):
    """execute() 里登录成功（caller 0 → user_id）时在返回前 await 一次 on_elevated；
    其他调用、重复登录（elevate 拒绝）都不触发"""
    executor = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    await executor.initialize("")
    elevated: list[int] = []

    async def on_elevated(user_id: int):
        elevated.append(user_id)

    executor.on_elevated = on_elevated
    ok, _ = await executor.execute("add_rls_comp_value", 1)  # 未登录也能调的 endpoint
    assert elevated == []
    ok, _ = await executor.execute("login", 7)
    assert ok
    assert elevated == [7]
    ok, _ = await executor.execute("add_rls_comp_value", 2)
    assert ok
    ok, _ = await executor.execute("login", 8)  # 已提权，elevate 返回 False
    assert elevated == [7]
    await executor.terminate()


async def test_owner_value_channel_ignores_own_heartbeat(
    mod_test_app, tbl_mgr, new_ctx
):
    """websocket 层订的是 Connection 表 owner==本用户 的索引值频道：本连接自己的心跳
    （direct_set last_active）会通知行频道但碰不到它；被别的连接顶号时它一定收到通知"""
    from hetu.data.backend.redis import RedisBackendClient
    from hetu.data.sub import SubscriptionBroker

    executor = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    await executor.initialize("")
    ok, _ = await executor.execute("login", 1)
    assert ok
    ctx = executor.context
    conn_tbl = executor.alive_checker.conn_tbl
    backend = conn_tbl.backend
    broker = SubscriptionBroker(backend)
    owner_hits: list[int] = []
    row_hits: list[int] = []
    await broker.watch_channel(
        backend.servant.index_value_channel(conn_tbl, "owner", 1),
        lambda: owner_hits.append(1),
    )
    await broker.watch_channel(
        backend.servant.row_channel(conn_tbl, ctx.connection_id),
        lambda: row_hits.append(1),
    )

    async def wait_hits(hits: list[int], count: int):
        async with asyncio.timeout(3):
            while len(hits) < count:
                await asyncio.sleep(0.02)

    # 每次调用都强制写一次心跳
    for i in range(3):
        executor.alive_checker.last_active_cache = 0
        ok, _ = await executor.execute("add_rls_comp_value", i)
        assert ok
    if isinstance(backend.master, RedisBackendClient):
        # Redis 的 keyspace 通知会把心跳的 HSET 打到行频道上（SQL 的 direct_set 不发通知）
        await wait_hits(row_hits, 3)
    await asyncio.sleep(0.3)
    assert owner_hits == [], "心跳不该触发 owner 索引值频道"

    # 被顶号：owner 从 1 改成 0
    executor2 = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    await executor2.initialize("")
    ok, _ = await executor2.execute("login", 1)
    assert ok
    await wait_hits(owner_hits, 1)
    assert await executor.alive_checker.kicked(ctx) is True

    await broker.close()
    await executor2.terminate()
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


async def test_flood_detect_same_ip_concurrent_no_race(
    mod_test_app, tbl_mgr, monkeypatch
):
    """同 IP 匿名连接数检查只是粗略计数，不做区间校验：本连接数完同 IP 的连接、提交之前，
    另一个同 IP 连接先建好了，也不能判竞态（new_connection 不重试，判竞态就是连接失败）"""
    from unittest.mock import patch

    from hetu.endpoint.connection import Connection, del_connection, new_connection

    monkeypatch.setattr(connection, "MAX_ANONYMOUS_CONNECTION_BY_IP", 3)
    table = tbl_mgr.get_table(Connection)
    assert table
    master = table.backend.master
    orig_commit = master.commit
    others: list[int] = []

    async def commit_after_other(idmap):
        if not others:
            others.append(0)  # 占位，内层连接自己的提交不再嵌套
            others[0] = await new_connection(tbl_mgr, "233.1.2.3")
        return await orig_commit(idmap)

    with patch.object(master, "commit", new=commit_after_other):
        conn_id = await new_connection(tbl_mgr, "233.1.2.3")
    assert conn_id and others[0]

    await del_connection(tbl_mgr, conn_id)
    await del_connection(tbl_mgr, others[0])


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


async def test_endpoint_admin_permission(mod_test_app, tbl_mgr, new_ctx, caplog):
    """ADMIN 端点只放行 admin 组：匿名、已登录的普通用户都被网关拒绝"""
    from hetu.common import Permission
    from hetu.endpoint.definer import EndpointDefines

    async def ep_admin_only(ctx):
        pass

    EndpointDefines().add("pytest", ep_admin_only, True, Permission.ADMIN)

    anonymous = EndpointExecutor("pytest", tbl_mgr, new_ctx())
    assert anonymous.execute_check("ep_admin_only", ()) is None
    assert "ep_admin_only无调用权限" in caplog.text

    user_ctx = new_ctx()
    user_ctx.caller = 10
    assert (
        EndpointExecutor("pytest", tbl_mgr, user_ctx).execute_check("ep_admin_only", ())
        is None
    )

    admin_ctx = new_ctx()
    admin_ctx.group = "admin"
    assert (
        EndpointExecutor("pytest", tbl_mgr, admin_ctx).execute_check(
            "ep_admin_only", ()
        )
        is not None
    )


# ============ 服务端发送限流（SERVER_SEND_LIMITS，防订阅攻击） ============


def _send_limited_checker(monkeypatch, server_limits):
    """造一个 t=1000 起算的 FloodChecker 和配了 server_limits 的 ctx；返回改时钟的函数"""
    from fixtures.contexts import make_ctx

    clock = [1000.0]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    ctx = make_ctx()
    ctx.configure([], server_limits, 0, 0)

    def set_time(t):
        clock[0] = t

    return connection.ConnectionFloodChecker(), ctx, set_time


def test_send_limit_not_reached(monkeypatch):
    """每层窗口内的发送数都没超过阈值：不断开"""
    checker, ctx, set_time = _send_limited_checker(monkeypatch, [[10, 1], [30, 5]])
    checker.sent(10)
    set_time(1000.5)
    assert checker.send_limit_reached(ctx, "test") is False
    checker.sent(20)  # 共 30，1 秒层已过窗口，5 秒层刚好没超
    set_time(1003)
    assert checker.send_limit_reached(ctx, "test") is False


def test_send_limit_reached_in_any_tier(monkeypatch, caplog):
    """任一层在其窗口内超过阈值就判定为订阅攻击，并记警告"""
    checker, ctx, set_time = _send_limited_checker(monkeypatch, [[10, 1], [30, 5]])
    checker.sent(11)
    set_time(1000.5)
    assert checker.send_limit_reached(ctx, "Websocket.push") is True
    assert "可能是订阅攻击" in caplog.text and "Websocket.push" in caplog.text

    # 1 秒层的窗口过了，但 5 秒层也超了
    checker, ctx, set_time = _send_limited_checker(monkeypatch, [[10, 1], [30, 5]])
    checker.sent(31)
    set_time(1003)
    assert checker.send_limit_reached(ctx, "test") is True


def test_send_limit_resets_after_last_window(monkeypatch):
    """超过最后一层的窗口后计数清零、重新起算：长期低速发送不会累积触发"""
    checker, ctx, set_time = _send_limited_checker(monkeypatch, [[10, 1], [30, 5]])
    checker.sent(25)
    set_time(1006)  # 超过最长的 5 秒窗口
    assert checker.send_limit_reached(ctx, "test") is False

    # 新窗口里只算新的发送数
    checker.sent(10)
    set_time(1006.5)
    assert checker.send_limit_reached(ctx, "test") is False
    checker.sent(1)
    assert checker.send_limit_reached(ctx, "test") is True


def test_send_limit_disabled_without_limits(monkeypatch):
    """没配 SERVER_SEND_LIMITS 时不限流"""
    checker, ctx, _set_time = _send_limited_checker(monkeypatch, [])
    checker.sent(1_000_000)
    assert checker.send_limit_reached(ctx, "test") is False
