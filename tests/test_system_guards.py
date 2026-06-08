import pytest

import hetu
from hetu.endpoint.guard import ClientReject
from hetu.endpoint.response import RejectResponse


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
    import hetu.endpoint.guard as guard_mod
    from hetu.endpoint.guard import rate_limit

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
