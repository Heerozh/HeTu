"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
import logging.handlers
import multiprocessing
from logging.handlers import QueueListener

logger = logging.getLogger("HeTu.root")
replay_logger = logging.getLogger("HeTu.replay")

CREATED_LISTENERS = []
CREATED_QUEUES = []


class AutoListener(QueueListener):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        CREATED_LISTENERS.append(self)
        # print('created listener', id(CREATED_LISTENERS), self)

    def __del__(self):
        # del有可能在全局变量销毁后运行
        if CREATED_LISTENERS:
            CREATED_LISTENERS.remove(self)

    @classmethod
    def start_all(cls):
        for listener in CREATED_LISTENERS:
            listener.start()

    @classmethod
    def stop_all(cls):
        for listener in CREATED_LISTENERS:
            try:
                listener.stop()
            except AttributeError:
                pass  # 正常，可能thread未启动


def process_safe_queue(maxsize=-1):
    # multiprocessing.Queue() == multiprocessing.get_context().Queue()，会初始化默认context。
    # 用 spawn 的 context 建，不碰默认 context（原来 Sanic 不让初始化它），worker 也是 spawn 起的
    ctx = multiprocessing.get_context("spawn")
    q = ctx.Queue(maxsize)
    CREATED_QUEUES.append(q)
    return q


def share_queues(logging_config):
    """
    dictConfig 之后调用：把它建好的进程安全队列放回 LOGGING 配置（替换掉 `process_safe_queue`
    这样的工厂），这份配置传给 spawn 出来的 worker 之后，worker 的 QueueHandler 写进同一个队列，
    由本进程的 AutoListener 统一输出。

    After dictConfig: put the created process-safe queues back into the LOGGING config, so
    spawned workers log into the same queues, drained by this process's listeners.
    """
    for name, handler_dict in logging_config.get("handlers", {}).items():
        if isinstance(handler_dict.get("queue"), (str, dict, list)):
            handler = logging.getHandlerByName(name)
            if isinstance(handler, logging.handlers.QueueHandler):
                handler_dict["queue"] = handler.queue


def abandon_queues():
    """
    读日志队列的进程（管理进程）没了时调用：退出时不再等队列的后台线程把积压写完。没人读的话管道
    写满，那个线程就一直卡着（还可能拿着跨进程的写锁，卡住别的 worker），进程永远退不出去；积压的
    日志本来也没人收了。

    Call when the process draining the log queues is gone: don't wait at exit for the
    queue feeder threads to flush, which would otherwise block forever.
    """
    loggers = [logging.getLogger()]
    loggers += [
        lg
        for lg in logging.Logger.manager.loggerDict.values()
        if isinstance(lg, logging.Logger)
    ]
    for lg in loggers:
        for handler in lg.handlers:
            if isinstance(handler, logging.handlers.QueueHandler):
                cancel = getattr(handler.queue, "cancel_join_thread", None)
                if cancel is not None:
                    cancel()


def stop_all_logging_handlers():
    AutoListener.stop_all()

    # 移除注册防止queue is closed的错误
    c = logger
    while c:
        for handler in list(c.handlers):
            if (
                isinstance(handler, logging.handlers.QueueHandler)
                and handler.queue in CREATED_QUEUES
            ):
                c.removeHandler(handler)
        c = c.parent

    # 停止queue的进程
    for q in CREATED_QUEUES:
        q.close()
        q.join_thread()

    CREATED_QUEUES.clear()
