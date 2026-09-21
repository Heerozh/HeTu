"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging

from sanic.logging.formatter import AutoFormatter


class ContextAutoFormatter(AutoFormatter):
    """尊重 record 上已有 ident 的彩色 formatter，多 worker + 日志队列时必须用它。

    sanic 的 AutoFormatter.format() 无条件执行 `record.ident = self.IDENT`，而 IDENT 是
    模块导入时从环境变量 SANIC_WORKER_IDENTIFIER 取的类属性——该变量只在 sanic 拉起
    worker 子进程的那一刻存在，所以只有 worker 进程导入 sanic 时才拿得到 "Srv N"，
    管理进程里永远是默认的 "Main "。

    但推荐配置里 console handler 是挂在 QueueListener 上的：worker 只把 record 塞进
    进程安全队列，真正的格式化发生在管理进程的监听线程里。于是所有 worker 的日志都会
    显示成 Main；更糟的是 QueueListener 把同一个 record 对象依次交给各 handler，被
    覆写的 record.ident 还会污染排在后面、用 %(ident)s 的文本 formatter。

    ContextFilter 已经在 worker 侧把真实 ident 写进了 record，这里优先用它。
    """

    def format(self, record: logging.LogRecord) -> str:
        ident = getattr(record, "ident", None)
        if not ident:  # 没经过 ContextFilter，退回 sanic 的原行为
            return super().format(record)
        # 用实例属性遮蔽类属性，让 super() 用 record 自带的 ident 渲染。
        # QueueListener 只有一个监听线程，formatter 实例不会被并发使用。
        self.IDENT = ident
        try:
            return super().format(record)
        finally:
            self.__dict__.pop("IDENT", None)
