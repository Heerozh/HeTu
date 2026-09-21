"""ctx.timestamp（请求时间戳）在各条调用路径下都要被正确打上。

Endpoint 路径由 `EndpointExecutor.execute_` 打；不走 Endpoint 的路径（未来调用、
启动System、断线System）必须自己打，否则 System 里读到的 ctx.timestamp 恒为 0。
"""

import time

from hetu.common.snowflake_id import SnowflakeID

SnowflakeID().init(1, 0)


async def test_endpoint_context_timestamp(test_app, executor):
    """Endpoint路径：ctx.timestamp为本次请求时间"""
    before = time.time()
    ok, ts = await executor.execute("report_ctx_timestamp")
    assert ok
    assert before <= ts <= time.time()


async def test_system_context_timestamp(test_app, executor):
    """System路径（客户端调用System自动生成的endpoint）：System内也能读到请求时间"""
    before = time.time()
    ok, ts = await executor.execute("record_ctx_timestamp")
    assert ok
    assert before <= ts <= time.time()
    # System函数体内看到的值和返回的一致
    assert test_app.CTX_TIMESTAMPS == [ts]


async def test_future_call_context_timestamp(monkeypatch, test_app, tbl_mgr, executor):
    """未来调用路径：不走Endpoint，需由exec_future_call打时间戳，否则System读到0"""
    from hetu.system.future import FutureCalls, exec_future_call, pop_upcoming_call

    FutureCallsTableCopy1 = FutureCalls.duplicate("pytest", "copy1")
    fc_tbl = tbl_mgr.get_table(FutureCallsTableCopy1)

    # 创建1秒后执行record_ctx_timestamp的未来调用
    ok, fid = await executor.execute("record_ctx_timestamp_future")
    assert ok

    # 上面的endpoint调用已给共享context打过时间戳，清掉，确保断言的是未来调用自己打的值
    executor.context.timestamp = 0
    test_app.CTX_TIMESTAMPS.clear()

    # 让时间前进到任务到期（real time 捕获后再 monkeypatch）
    exec_time = time.time() + 1
    monkeypatch.setattr(time, "time", lambda: exec_time)
    call = await pop_upcoming_call(fc_tbl)
    assert call is not None and call.id == fid

    # 注：测试直接复用executor的caller，生产中是future_call_task自己的context
    assert await exec_future_call(call, executor.context.systems, fc_tbl)
    # System读到的是执行时刻的时间戳，而不是0
    assert test_app.CTX_TIMESTAMPS == [exec_time]
