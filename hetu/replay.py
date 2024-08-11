"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import os
from datetime import datetime


class BaseReplayLogger:
    def info(self, text):
        pass

    def recv(self, data):
        pass

    def send(self, data):
        pass

    def flush(self):
        pass


class ConnectionAndTimedRotatingReplayLogger(BaseReplayLogger):
    """
    记录收发的所有消息，按连接分文件，可能导致文件特别多。
    和python logger不同的是，这个保持速度不会flush。
    """
    def __init__(self, directory, connection_id):
        self.path = directory
        self.date_dir = os.path.join(directory, datetime.now().strftime("%Y%m%d"))
        os.mkdir(self.date_dir) if not os.path.exists(self.date_dir) else None
        # 根据连接id和时间戳组合文件名
        self.filename = f'{datetime.now().strftime("%H%M%S")}_{connection_id}.log'
        self.file = open(os.path.join(self.date_dir, self.filename), 'a', encoding="utf-8")

        self.format = lambda dt, direction, data: \
            f'[{dt.strftime("%Y-%m-%d %H:%M:%S")}][{direction}]{data}\n'

    def __del__(self):
        self.file.close()

    def info(self, text):
        self.file.write(self.format(datetime.now(), 'INFO', text))
        self.flush()

    def recv(self, data):
        self.file.write(self.format(datetime.now(), '>>>', data))

    def send(self, data):
        self.file.write(self.format(datetime.now(), '<<<', data))

    def flush(self):
        self.file.flush()
        os.fsync(self.file.fileno())