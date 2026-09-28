"""
sub_scenarios_ws.py 的服务器 app（APP_FILE）：背包（Item，OWNER 权限、owner 声明 point_sub）与全服
聊天（ChatMessage，同 examples/chat）。服务器、压测客户端、写进程都 import 它，zlib 预共享字典才一致。

另带几个压测诊断用的 endpoint（只在压测服务器里）：ping（RPC 往返探针）、gcstats（GC 累计耗时）、
tickstats（订阅 tick 的次数与每个 tick 的通知条数）、prof（worker 内 cProfile 开关）。

环境变量 HETU_BENCH_TICK_SPACING 覆盖 `SubscriptionHub.TICK_SPACING_INTERVALS`（合批窗口，单位
interval；设 0 可复现没有合批窗口时 tick 碎成每条一个的情形）。
"""

import asyncio
import contextlib
import ctypes
import gc
import os
import time

import numpy as np

import hetu

NAMESPACE = "scbench"
INSTANCE = "scbench"

# 让同一用户下的 py-spy 能 attach 到 worker（Yama ptrace_scope=1 时默认只能 trace 子孙进程）。
# 只影响压测服务器进程；非 Linux 上没有 prctl，跳过
with contextlib.suppress(Exception):
    _PR_SET_PTRACER = 0x59616D61
    _PR_SET_PTRACER_ANY = ctypes.c_ulong(-1)
    ctypes.CDLL(None).prctl(_PR_SET_PTRACER, _PR_SET_PTRACER_ANY, 0, 0, 0)


@hetu.define_component(
    namespace=NAMESPACE, permission=hetu.Permission.OWNER, volatile=True
)
class Item(hetu.BaseComponent):
    """背包物品：客户端订 owner == 自己（点查询，只被自己的进出叫醒）"""

    owner: np.int64 = hetu.property_field(0, index=True, point_sub=True)
    kind: np.int32 = hetu.property_field(0)
    count: np.int32 = hetu.property_field(1)
    ts: np.float64 = hetu.property_field(0)  # 写入时刻 time.time()，算延迟用


@hetu.define_component(
    namespace=NAMESPACE, permission=hetu.Permission.EVERYBODY, volatile=True
)
class ChatMessage(hetu.BaseComponent):
    """同 examples/chat，多一个 ts 算延迟"""

    owner: np.int64 = hetu.property_field(0, index=True)
    name: str = hetu.property_field("", dtype="U32")
    text: str = hetu.property_field("", dtype="U256")
    kind: str = hetu.property_field("chat", dtype="U16")
    created_at_ms: np.int64 = hetu.property_field(0, index=True)
    ts: np.float64 = hetu.property_field(0)


@hetu.define_system(
    namespace=NAMESPACE,
    components=(Item, ChatMessage),
    permission=hetu.Permission.EVERYBODY,
)
async def _pin(ctx: hetu.SystemContext):
    """只是为了让组件进簇"""


@hetu.define_endpoint(namespace=NAMESPACE, permission=hetu.Permission.EVERYBODY)
async def login(ctx: hetu.EndpointContext, user_id):
    ok, _reason = await hetu.elevate(ctx, int(user_id), kick_logged_in=True)
    return hetu.ResponseToClient({"id": ctx.caller, "ok": ok})


@hetu.define_endpoint(namespace=NAMESPACE, permission=hetu.Permission.EVERYBODY)
async def ping(ctx: hetu.EndpointContext):
    """RPC 往返探针：测订阅负载下事件循环的响应（不返回值，服务器回 ok）"""


# ---------------------------------------------------------------------------
# 诊断：GC 耗时（gc.callbacks 在每次收集前后调用）
# ---------------------------------------------------------------------------

_GC: dict = {"t": 0.0, "n": [0, 0, 0], "t_gen": [0.0, 0.0, 0.0], "start": 0.0}


def _gc_cb(phase, info):
    if phase == "start":
        _GC["start"] = time.perf_counter()
    else:
        dt = time.perf_counter() - _GC["start"]
        g = info.get("generation", 0)
        _GC["t"] += dt
        _GC["n"][g] += 1
        _GC["t_gen"][g] += dt


gc.callbacks.append(_gc_cb)


@hetu.define_endpoint(namespace=NAMESPACE, permission=hetu.Permission.EVERYBODY)
async def gcstats(ctx: hetu.EndpointContext):
    """GC 累计耗时 / 次数（压测窗口前后各取一次做差）"""
    return hetu.ResponseToClient(
        {"t": _GC["t"], "n": list(_GC["n"]), "t_gen": list(_GC["t_gen"])}
    )


# ---------------------------------------------------------------------------
# 诊断：订阅 tick 的次数与条数。worker 级订阅器是 SubscriptionHub._tick（整个 worker 一批），
# 之前的代码是每连接的 SubscriptionBroker._apply_notifications（每连接一批）；两份代码都能挂
# ---------------------------------------------------------------------------

_TICKS: dict = {"n": 0, "items": 0, "hist": [0] * 12}


def _count(batch) -> None:
    n = len(batch)
    _TICKS["n"] += 1
    _TICKS["items"] += n
    _TICKS["hist"][min(n.bit_length(), 11)] += 1


def _install_tick_counter() -> None:
    import hetu.data.sub as sub_mod

    hub_cls = getattr(sub_mod, "SubscriptionHub", None)
    if hub_cls is not None:
        spacing = os.environ.get("HETU_BENCH_TICK_SPACING")
        if spacing is not None and hasattr(hub_cls, "TICK_SPACING_INTERVALS"):
            hub_cls.TICK_SPACING_INTERVALS = float(spacing)
        orig_tick = hub_cls._tick

        async def _tick(self, batch):
            _count(batch)
            return await orig_tick(self, batch)

        hub_cls._tick = _tick
        return
    # 之前的代码才有这个方法（每连接处理一批通知）
    name = "_apply_notifications"
    orig_apply = getattr(sub_mod.SubscriptionBroker, name)

    async def _apply(self, updated_channels):
        _count(updated_channels)
        return await orig_apply(self, updated_channels)

    setattr(sub_mod.SubscriptionBroker, name, _apply)


_install_tick_counter()


@hetu.define_endpoint(namespace=NAMESPACE, permission=hetu.Permission.EVERYBODY)
async def tickstats(ctx: hetu.EndpointContext):
    """tick 次数、弹出的通知条数，以及每 tick 条数的 log2 直方图"""
    return hetu.ResponseToClient(
        {"n": _TICKS["n"], "items": _TICKS["items"], "hist": list(_TICKS["hist"])}
    )


# ---------------------------------------------------------------------------
# 诊断：worker 内 cProfile 开关（精确的调用次数；协程每次恢复都算一次调用）
# ---------------------------------------------------------------------------

_PROF: dict = {"p": None}


@hetu.define_endpoint(namespace=NAMESPACE, permission=hetu.Permission.EVERYBODY)
async def prof(ctx: hetu.EndpointContext, action, path=""):
    import cProfile

    if action == "start" and _PROF["p"] is None:
        _PROF["p"] = cProfile.Profile()
        _PROF["p"].enable()
    elif action == "stop" and _PROF["p"] is not None:
        _PROF["p"].disable()
        # dump_stats 写文件，别卡事件循环
        await asyncio.to_thread(_PROF["p"].dump_stats, path)
        _PROF["p"] = None
