import pytest

import hetu
from hetu.common.snowflake_id import SnowflakeID
from hetu.endpoint.guard import ClientReject
from hetu.endpoint.response import RejectResponse

SnowflakeID().init(1, 0)


def test_client_reject_carries_code_and_reason():
    e = ClientReject("RATE_LIMITED", "太快了")
    assert e.code == "RATE_LIMITED"
    assert e.reason == "太快了"
    assert isinstance(e, Exception)


def test_client_reject_defaults():
    e = ClientReject()
    assert e.code == "REJECTED"
    assert e.reason is None


def test_reject_response_carries_code():
    r = RejectResponse("RATE_LIMITED")
    assert r.code == "RATE_LIMITED"
    assert r.reason is None


def test_guard_marker_attaches_in_source_order():
    from hetu.endpoint.guard import guard

    async def chk_a(ctx, x):
        pass

    async def chk_b(ctx, x):
        pass

    @guard(chk_a)
    @guard(chk_b)
    async def f(ctx, x):
        pass

    # 源码自上而下：chk_a 在前
    assert getattr(f, "__hetu_guards__") == [chk_a, chk_b]


async def test_rate_limit_guard_logic_window():
    from hetu.endpoint.guard import rate_limit

    @rate_limit(times=2, per=100)
    async def f(ctx, x):
        pass

    g = f.__hetu_guards__[0]

    class FakeCtx:
        guard_state: dict = {}

    ctx = FakeCtx()
    await g(ctx, 1)  # 第1次放行
    await g(ctx, 1)  # 第2次放行
    with pytest.raises(ClientReject) as ei:
        await g(ctx, 1)  # 第3次拒绝
    assert ei.value.code == "RATE_LIMITED"


async def test_rate_limit_window_resets(monkeypatch):
    import sys
    import hetu.endpoint.guard  # noqa: F401 – ensures module is loaded
    from hetu.endpoint.guard import rate_limit

    guard_mod = sys.modules["hetu.endpoint.guard"]
    base = 1000.0
    monkeypatch.setattr(guard_mod.time, "time", lambda: base)

    @rate_limit(times=1, per=10)
    async def f(ctx, x):
        pass

    g = f.__hetu_guards__[0]

    class FakeCtx:
        guard_state: dict = {}

    ctx = FakeCtx()
    await g(ctx, 1)  # 放行
    with pytest.raises(ClientReject):
        await g(ctx, 1)  # 同窗口拒绝

    monkeypatch.setattr(guard_mod.time, "time", lambda: base + 11)
    await g(ctx, 1)  # 窗口过后重置，放行


async def test_rate_limit_violation_extends_lockout(monkeypatch):
    """触发拒绝后，封锁应从违规时刻起算满 per 秒；期间重试会顺延封锁。"""
    import sys
    import hetu.endpoint.guard  # noqa: F401
    from hetu.endpoint.guard import rate_limit

    guard_mod = sys.modules["hetu.endpoint.guard"]
    now = [1000.0]
    monkeypatch.setattr(guard_mod.time, "time", lambda: now[0])

    @rate_limit(times=2, per=10)
    async def f(ctx, x):
        pass

    g = f.__hetu_guards__[0]

    class FakeCtx:
        guard_state: dict = {}

    ctx = FakeCtx()
    await g(ctx, 1)  # t=1000 放行
    await g(ctx, 1)  # t=1000 放行

    now[0] = 1005.0
    with pytest.raises(ClientReject):
        await g(ctx, 1)  # t=1005 超限拒绝，窗口顺延至 1005

    # t=1011：旧实现窗口锚定首次调用(1000)已过期会误放行；修复后违规起算未满 per 仍拒绝
    now[0] = 1011.0
    with pytest.raises(ClientReject):
        await g(ctx, 1)

    # t=1022：距最后一次违规(1011)已过 per 秒，解封放行
    now[0] = 1022.0
    await g(ctx, 1)


def test_context_has_guard_state():
    from hetu.endpoint.context import Context

    ctx = Context(
        caller=0,
        connection_id=0,
        address="x",
        group="",
        user_data={},
        timestamp=0,
        request=None,
        systems=None,
    )
    assert ctx.guard_state == {}
    ctx.guard_state["k"] = 1
    # 不同实例不共享
    ctx2 = Context(
        caller=0,
        connection_id=0,
        address="x",
        group="",
        user_data={},
        timestamp=0,
        request=None,
        systems=None,
    )
    assert ctx2.guard_state == {}


def test_define_endpoint_collects_guards():
    from hetu.endpoint.definer import EndpointDefines
    from hetu.endpoint.guard import guard

    async def chk(ctx, x):
        pass

    @hetu.define_endpoint(
        namespace="t_guard", permission=hetu.Permission.EVERYBODY, force=True
    )
    @guard(chk)
    async def my_ep(ctx, x):
        pass

    ep = EndpointDefines().get_endpoint("t_guard", "my_ep")
    assert ep is not None
    assert ep.guards == [chk]


def test_marker_above_definer_raises():
    from hetu.endpoint.guard import rate_limit

    with pytest.raises(TypeError, match="下面"):

        @rate_limit(times=1, per=1)
        @hetu.define_endpoint(
            namespace="t_guard", permission=hetu.Permission.EVERYBODY, force=True
        )
        async def bad(ctx, x):
            pass


def test_guard_on_permission_none_system_raises():
    from hetu.endpoint.guard import rate_limit

    # permission=None 的 System 不生成 endpoint，guard 永不执行；定义即报错防 footgun
    with pytest.raises(TypeError, match="permission"):

        @hetu.define_system(namespace="t_guard", force=True)
        @rate_limit(times=1, per=1)
        async def no_perm_guard(ctx, x):
            pass


def test_system_guards_copied_to_endpoint(mod_test_app):
    from hetu.endpoint.definer import EndpointDefines

    ep = EndpointDefines().get_endpoint("pytest", "rate_limited_add")
    assert ep is not None
    assert len(ep.guards) == 1

    ep2 = EndpointDefines().get_endpoint("pytest", "guarded_add")
    assert ep2 is not None
    assert len(ep2.guards) == 1


async def test_rate_limit_rejects_second_call(mod_test_app, executor):
    from hetu.endpoint.response import RejectResponse

    ok, _ = await executor.execute("login", 7001)
    assert ok

    ok, res = await executor.execute("rate_limited_add", 5)
    assert ok
    assert not isinstance(res, RejectResponse)

    # 同窗口第二次：被软拒绝，连接保持(ok=True)
    ok, res = await executor.execute("rate_limited_add", 5)
    assert ok
    assert isinstance(res, RejectResponse)
    assert res.code == "RATE_LIMITED"

    # 被拒调用没有写入：值仍为 100+5
    ok, _ = await executor.execute("test_rls_comp_value", 105)
    assert ok


async def test_custom_guard_rejects(mod_test_app, executor):
    from hetu.endpoint.response import RejectResponse

    ok, _ = await executor.execute("login", 7002)
    assert ok

    ok, res = await executor.execute("guarded_add", -1)
    assert ok
    assert isinstance(res, RejectResponse)
    assert res.code == "NEGATIVE"

    ok, res = await executor.execute("guarded_add", 3)
    assert ok
    assert not isinstance(res, RejectResponse)


async def test_rpc_emits_rej_frame(mod_test_app, executor):
    import asyncio

    from hetu.server.receiver import rpc

    push_queue: asyncio.Queue = asyncio.Queue()

    # 登录
    cont = await rpc(["rpc", "login", 7003], executor, push_queue)
    assert cont is True
    await push_queue.get()  # 丢弃 login 的 rsp

    # 第一次正常
    cont = await rpc(["rpc", "rate_limited_add", 5], executor, push_queue)
    assert cont is True
    first = await push_queue.get()
    assert first[0] == "rsp"

    # 第二次被限流：发 rej 帧，且不关连接(cont=True)
    cont = await rpc(["rpc", "rate_limited_add", 5], executor, push_queue)
    assert cont is True
    rej = await push_queue.get()
    assert rej == ["rej", "rate_limited_add", "RATE_LIMITED"]


def test_top_level_exports():
    import hetu

    assert hasattr(hetu, "rate_limit")
    assert hasattr(hetu, "guard")
    assert hasattr(hetu, "ClientReject")
    assert hetu.ClientReject("X").code == "X"
