"""通用调用前守卫(guard)机制：标记式装饰器只附加元数据，由网关执行。"""

import time

from ..i18n import _

# 函数上存放 guard 列表的属性名
_GUARD_ATTR = "__hetu_guards__"
# 定义产物(define_system/define_endpoint 的返回)标记，用于防呆装饰器顺序
_DEFINED_ATTR = "__hetu_defined__"


class ClientReject(Exception):
    """guard 抛出它 = 软拒绝当次调用：不开事务、不断连接、回 rej 帧给客户端。

    code 会随 rej 帧发送给客户端（``["rej", system, code]``），用于客户端通用回调
    ``OnCallRejected`` 与异常 ``HeTuCallRejectedException`` 区分原因（如 ``RATE_LIMITED``）。
    reason 可选，仅用于服务端日志/诊断，**不会**发送给客户端。
    """

    def __init__(self, code: str = "REJECTED", reason: str | None = None):
        self.code = code
        self.reason = reason
        super().__init__(reason or code)


def _attach_guard(func, g):
    """把 guard 可调用对象挂到 func 上。markers 必须在 define_* 下面。"""
    if getattr(func, _DEFINED_ATTR, False):
        raise TypeError(
            _("@rate_limit/@guard 必须放在 @define_system/@define_endpoint 的下面")
        )
    guards = func.__dict__.setdefault(_GUARD_ATTR, [])
    # 装饰器自底向上应用；insert(0) 让最终顺序 == 源码自上而下
    guards.insert(0, g)
    return func


def collect_guards(func) -> list:
    """供 define_system/define_endpoint 读取已附加的 guard 列表（拷贝一份）。"""
    return list(getattr(func, _GUARD_ATTR, []))


def mark_defined(obj):
    """供 define_system/define_endpoint 标记其产物，配合 _attach_guard 防呆。"""
    obj.__dict__[_DEFINED_ATTR] = True
    return obj


def guard(check):
    """通用自定义守卫装饰器（标记式）。

    check 签名 ``(ctx, *args) -> None``，可同步或 async；想拒绝就 raise ClientReject(...)。
    用法：放在 @define_system / @define_endpoint 下面。
    """

    def deco(func):
        return _attach_guard(func, check)

    return deco


def rate_limit(times: int, per: float):
    """内置 guard：每「连接 × system」固定窗口限流。

    per 秒窗口内最多允许 times 次，超出 raise ClientReject('RATE_LIMITED')。
    状态存 ctx.guard_state，以本装饰应用的唯一 key 索引 [window_start, count]。
    """

    def deco(func):
        key = f"ratelimit:{id(func)}:{times}:{per}"

        async def _rate_limit_guard(ctx, *args):
            now = time.time()
            st = ctx.guard_state.get(key)
            if st is None or now - st[0] > per:
                ctx.guard_state[key] = [now, 1]
                return
            st[1] += 1
            if st[1] > times:
                raise ClientReject("RATE_LIMITED")

        return _attach_guard(func, _rate_limit_guard)

    return deco
