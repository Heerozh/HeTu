"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import contextvars
import logging

from ..common.helper import worker_ident

# 必须在构造时提供 default：filter 会在未 set 过此 ContextVar 的线程里运行
# （如 SQLite 后端的专用线程），ContextVar 不跨线程继承，缺省时 .get()
# 会抛 LookupError。不能改用 .set() 代替，.set() 只对当前线程的 context 生效。
log_contex_var = contextvars.ContextVar("client_ctx", default="[None|None|Startup]")


class ContextFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """

    # 本进程的标识（"Srv 0" / "Main "），导入时就定下：worker 的序号在进程启动前就设进了环境变量
    IDENT = worker_ident()

    @classmethod
    def set_log_context(cls, ctx):
        log_contex_var.set(ctx)

    def filter(self, record):
        record.ident = self.IDENT
        record.ctx = log_contex_var.get()
        return True
