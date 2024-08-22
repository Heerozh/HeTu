"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import logging
import multiprocessing
from logging.handlers import QueueListener

logger = logging.getLogger('HeTu.root')
replay_logger = logging.getLogger('HeTu.replay')

CREATED_LISTENERS = []


class AutoListener(QueueListener):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        CREATED_LISTENERS.append(self)
        # print('created listener', id(CREATED_LISTENERS), self)

    def __del__(self):
        CREATED_LISTENERS.remove(self)

    @classmethod
    def start_all(cls):
        for listener in CREATED_LISTENERS:
            listener.start()

    @classmethod
    def stop_all(cls):
        for listener in CREATED_LISTENERS:
            listener.stop()


def process_safe_queue(maxsize=-1):
    # multiprocessing.Queue() == multiprocessing.get_context().Queue()，会初始化默认context
    # sanic限制默认context不能被初始化，这里为了绕过所以get_context传入spawn，不会初始化默认context
    ctx = multiprocessing.get_context('spawn')
    return ctx.Queue(maxsize)
