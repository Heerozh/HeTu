import time

import pytest

import hetu
import hetu.endpoint.connection as connection
from hetu.endpoint.executor import EndpointExecutor

from hetu.common.snowflake_id import SnowflakeID

SnowflakeID().init(1, 0)


async def test_connect_kick(mod_test_app, comp_mgr, new_ctx):
    # 先登录2个连接
    executor1 = EndpointExecutor("pytest", comp_mgr, new_ctx())
    await executor1.initialize("")
    await executor1.execute("login", 1)

    executor2 = EndpointExecutor("pytest", comp_mgr, new_ctx())
    await executor2.initialize("")
    await executor2.execute("login", 2)

    ok, _ = await executor1.execute("add_rls_comp_value", 1)
    assert ok
    ok, _ = await executor2.execute("add_rls_comp_value", 10)
    assert ok

    # 测试重复登录踢出已登录用户
    executor1_replaced = EndpointExecutor("pytest", comp_mgr, new_ctx())
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


async def test_connect_not_kick(mod_test_app, comp_mgr):
    # 初始化第一个连接
    executor1 = EndpointExecutor("pytest", comp_mgr)
    await executor1.initialize("")
    await executor1.exec("login", 1)
    ok, _ = await executor1.exec("add_rls_comp_value", 2)
    assert ok
    ok, _ = await executor1.exec("test_rls_comp_value", 102)
    assert ok

    # 不强制踢出是否生效
    executor1_not_replace = EndpointExecutor("pytest", comp_mgr)
    await executor1_not_replace.initialize("")
    # 默认为0, 要设为1防止下面依旧强制踢出。注意目前t是按连接方ctx的imeout值来判断的，此值
    connection.ENDPOINT_CALL_IDLE_TIMEOUT = 1
    ok, app_login_rsp = await executor1_not_replace.exec("login", 1, False)
    assert app_login_rsp.message["id"] is None  # app中定义的返回值
    ok, _ = await executor1.exec("test_rls_comp_value", 102)
    assert ok

    # 结束连接
    await executor1.terminate()
    await executor1_not_replace.terminate()


async def test_connect_kick_timeout(monkeypatch, mod_test_app, comp_mgr):
    time_time = time.time

    # 初始化第一个连接
    executor1 = EndpointExecutor("pytest", comp_mgr)
    await executor1.initialize("")
    await executor1.exec("login", 1)
    ok, _ = await executor1.exec("add_rls_comp_value", 3)
    assert ok
    ok, _ = await executor1.exec("test_rls_comp_value", 103)
    assert ok

    # 测试last active超时是否踢出用户
    # 不强制踢出，但是timeout应该生效
    executor1_timeout_replaced = EndpointExecutor("pytest", comp_mgr)
    monkeypatch.setattr(
        time, "time", lambda: time_time() + connection.ENDPOINT_CALL_IDLE_TIMEOUT
    )
    await executor1_timeout_replaced.initialize("")
    ok, app_login_rsp = await executor1_timeout_replaced.exec("login", 1, False)
    assert app_login_rsp.message["id"] == 1  # app中定义的返回值

    # 上一次kick的连接应该失效
    ok, _ = await executor1.exec("test_rls_comp_value", 103)
    assert not ok

    # 新的应该有效
    ok, _ = await executor1_timeout_replaced.exec("add_rls_comp_value", -2)
    assert ok
    ok, _ = await executor1_timeout_replaced.exec("test_rls_comp_value", 101)
    assert ok

    # 结束连接
    await executor1.terminate()
    await executor1_timeout_replaced.terminate()


async def test_flood_detect(mod_test_app, comp_mgr, caplog):
    connection.MAX_ANONYMOUS_CONNECTION_BY_IP = 3

    executors = []
    with pytest.raises(RuntimeError, match="new_connection调用失败"):
        for i in range(5):
            loc_executor = EndpointExecutor("pytest", comp_mgr)
            await loc_executor.initialize(f"233.111.111.111")
            executors.append(loc_executor)

    assert "IP匿名连接数" in caplog.text

    for loc_executor in executors:
        await loc_executor.terminate()


async def test_future_call_bypass_flood_detect(mod_test_app, comp_mgr):
    # 测试连接，包括flood检测等，特别是future不应该遇到flood检测
    # 不然服务器反复重启后会提示flood
    connection.MAX_ANONYMOUS_CONNECTION_BY_IP = 3

    executors = []
    # 以下代码应该成功调用没有报错
    for i in range(5):
        loc_executor = EndpointExecutor("pytest", comp_mgr)
        # 使用localhost ip地址让连接flood检测不报错
        await loc_executor.initialize(f"localhost")
        executors.append(loc_executor)

    for loc_executor in executors:
        await loc_executor.terminate()
