"""通用调用前守卫(guard)机制：标记式装饰器只附加元数据，由网关执行。"""

import time

from ..i18n import _

# 函数上存放 guard 列表的属性名
_GUARD_ATTR = "__hetu_guards__"
# 定义产物(define_system/define_endpoint 的返回)标记，用于防呆装饰器顺序
_DEFINED_ATTR = "__hetu_defined__"


class ClientReject(Exception):
    """guard 抛出它 = 软拒绝当次调用：不开事务、不断连接、回 rej 帧给客户端。

    code 用于客户端通用回调区分原因（如 ``RATE_LIMITED``）；
    reason 可选，仅放进客户端异常对象。
    """

    def __init__(self, code: str = "REJECTED", reason: str | None = None):
        self.code = code
        self.reason = reason
        super().__init__(reason or code)
