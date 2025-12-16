"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import contextvars
import logging
import os

log_contex_var = contextvars.ContextVar("client_ctx")
log_contex_var.set("[None|None|Startup]")


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
        # todo 使用 sentence-transformers 库中的轻量级模型（如 all-MiniLM-L6-v2）
        #      通过向量对照寻找对应的翻译对照表翻译
        #      并cache结果，如果向量差距过大，显示警告对照表改变。
        return True
