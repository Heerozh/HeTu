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


@hetu.define_system(
    namespace=NAMESPACE,
    components=(Item, ChatMessage),
    permission=hetu.Permission.USER,
)
async def rpcs_write(ctx: hetu.SystemContext, nonce, item_id, chat):
    """
    等推送的调用压测（--rpcs-rate）：把自己的物品 item_id 的 kind 改成 -nonce（客户端按它认出这次调用的
    推送）；chat 时再发一条聊天（kind="rpcs"、name="r{uid}:{nonce}"），客户端等的是聊天这行的推送。
    物品被写进程删了就什么也不写，回 ok=False
    """
    repo = ctx.repo[Item]
    row = await repo.get(id=int(item_id))
    if row is None or int(row.owner) != ctx.caller:
        return hetu.ResponseToClient({"ok": False})
    row.kind = -int(nonce)
    await repo.update(row)
    if chat:
        msg = ChatMessage.new_row()
        msg.owner = ctx.caller
        msg.name = f"r{ctx.caller}:{int(nonce)}"
        msg.text = f"rpcs message number {int(nonce)} " * 3
        msg.kind = "rpcs"
        t = time.time()
        msg.created_at_ms = int(t * 1000)
        msg.ts = t
        await ctx.repo[ChatMessage].insert(msg)
    return hetu.ResponseToClient({"ok": True})


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


# ---------------------------------------------------------------------------
# 诊断：等推送的调用（rpcs）的栅栏时序（设计稿 2026-10-10-rpcs-sync §8.2）。只看 rpcs_write：
# - δ：commit 返回（rsp 入队、起栅栏那一刻）→ 这次改的物品行的通知进 hub 的 MQ 队列；
# - g 实际：commit 返回 → 栅栏键真正入队（call_later 在负载下会晚到）；
# - 漏：通知晚于栅栏键入队（排在栅栏后面，sync 会先于这次的推送）。
# 通知比 commit 的回复先被事件循环处理时 δ 为负
# ---------------------------------------------------------------------------


class _FenceWatch:
    __slots__ = ("arrive", "fenced", "key", "merged", "t0", "t_rsp")

    def __init__(self, key: str) -> None:
        self.key = key
        self.t0 = time.monotonic()
        self.t_rsp: float | None = None
        self.arrive: float | None = None
        self.fenced: float | None = None
        self.merged = False


# wait：物品 row_id（str）→ 还没凑齐三个时刻的；by_sync：(id(门面), sync_id) → 同一个，只在 rpcs 执行期间；
# by_fid：栅栏 id → 同一个，到栅栏键入队为止；done：凑齐了的 (δ, g 实际, 漏, 合并进已排着的)
_FS: dict = {"wait": {}, "by_sync": {}, "by_fid": {}, "done": [], "lost": 0}


def _fence_watch_done(w: _FenceWatch) -> None:
    if w.t_rsp is None or w.arrive is None or w.fenced is None:
        return
    if _FS["wait"].get(w.key) is w:
        del _FS["wait"][w.key]
    _FS["done"].append(
        (w.arrive - w.t_rsp, w.fenced - w.t_rsp, w.arrive > w.fenced, w.merged)
    )


def _install_fence_probe() -> None:
    import hetu.data.sub as sub_mod
    from hetu.data.backend import base
    from hetu.server import receiver

    orig_rpcs = receiver.rpcs

    async def rpcs(data, executor, broker, push_queue, debug=0):
        # ["rpcs", sync_id, "rpcs_write", nonce, item_id, chat]
        key = None
        if len(data) >= 5 and data[2] == "rpcs_write":
            w = _FenceWatch(str(int(data[4])))
            _FS["wait"][w.key] = w
            key = (id(broker), data[1])
            _FS["by_sync"][key] = w
        try:
            return await orig_rpcs(data, executor, broker, push_queue, debug)
        finally:
            if key is not None:
                _FS["by_sync"].pop(key, None)

    # client_handler 按模块全局名调 rpcs，换掉模块属性就行
    receiver.rpcs = rpcs

    orig_sync = sub_mod.SubscriptionBroker.sync_

    def sync_(self, sync_id):
        w = _FS["by_sync"].get((id(self), sync_id))
        if w is not None:
            w.t_rsp = time.monotonic()
        orig_sync(self, sync_id)
        fences = self._hub._fences
        if w is not None and fences:
            _FS["by_fid"][next(reversed(fences))] = w

    sub_mod.SubscriptionBroker.sync_ = sync_

    orig_enqueue_fence = sub_mod.SubscriptionHub._enqueue_fence

    def _enqueue_fence(self, fid):
        w = _FS["by_fid"].pop(fid, None)
        if w is not None:
            w.fenced = time.monotonic()
        orig_enqueue_fence(self, fid)
        if w is not None:
            _fence_watch_done(w)

    sub_mod.SubscriptionHub._enqueue_fence = _enqueue_fence

    orig_enqueue = base.MQClient._enqueue

    def _enqueue(self, channel_name, payload_ids):
        if _FS["wait"] and channel_name[:1] != "\0":
            i = channel_name.rfind(":id:")
            if i >= 0:
                w = _FS["wait"].get(channel_name[i + 4 :])
                if w is not None and w.arrive is None:
                    w.arrive = time.monotonic()
                    w.merged = channel_name in self.pulled_set
                    _fence_watch_done(w)
        return orig_enqueue(self, channel_name, payload_ids)

    base.MQClient._enqueue = _enqueue


# HETU_BENCH_NO_FENCE_PROBE=1 不装这些打点（量 rpcs 本身的开销时用，fencestats 全是 0）
if not os.environ.get("HETU_BENCH_NO_FENCE_PROBE"):
    _install_fence_probe()


def _ms_pcts(vals: list[float]) -> dict:
    if not vals:
        return {}
    s = sorted(vals)
    n = len(s)

    def p(q: float) -> float:
        return round(s[min(n - 1, int(n * q))] * 1000, 3)

    return {
        "min": round(s[0] * 1000, 3),
        "p50": p(0.5),
        "p90": p(0.9),
        "p99": p(0.99),
        "p999": p(0.999),
        "max": round(s[-1] * 1000, 3),
    }


@hetu.define_endpoint(namespace=NAMESPACE, permission=hetu.Permission.EVERYBODY)
async def fencestats(ctx: hetu.EndpointContext, reset=False):
    """rpcs_write 的栅栏时序汇总（毫秒），reset 时清零重新统计。5 秒还凑不齐的算丢（含物品已被删、没写）"""
    now = time.monotonic()
    for key, w in list(_FS["wait"].items()):
        if now - w.t0 > 5:
            del _FS["wait"][key]
            _FS["lost"] += 1
    done = _FS["done"]
    res = {
        "n": len(done),
        "lost": _FS["lost"],
        "miss": sum(d[2] for d in done),
        "merged": sum(d[3] for d in done),
        "delta_ms": _ms_pcts([d[0] for d in done]),
        "g_ms": _ms_pcts([d[1] for d in done]),
        # 通知晚于栅栏键的那几次，晚了多少
        "late_ms": _ms_pcts([d[0] - d[1] for d in done if d[2]]),
    }
    if reset:
        _FS["done"] = []
        _FS["lost"] = 0
    return hetu.ResponseToClient(res)
