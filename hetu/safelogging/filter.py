"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import contextvars
import logging
import os

# 必须在构造时提供 default：filter 会在未 set 过此 ContextVar 的线程里运行
# （如 aiosqlite 的 DB worker thread），ContextVar 不跨线程继承，缺省时 .get()
# 会抛 LookupError。不能改用 .set() 代替，.set() 只对当前线程的 context 生效。
log_contex_var = contextvars.ContextVar("client_ctx", default="[None|None|Startup]")


class ContextFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """

    IDENT = os.environ.get("SANIC_WORKER_IDENTIFIER", "Main ") or "Main "

    @classmethod
    def set_log_context(cls, ctx):
        log_contex_var.set(ctx)

    def filter(self, record):
        record.ident = self.IDENT
        record.ctx = log_contex_var.get()
        return True
