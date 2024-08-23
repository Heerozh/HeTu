"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import logging

from tabulate import tabulate

logger = logging.getLogger('HeTu.root')
SLOW_LOG_TIME_THRESHOLD = 0.1
SLOW_LOG_RETRY_THRESHOLD = 5


class InplaceAverage:
    def __init__(self):
        self.value = 0
        self.size = 0

    def add(self, value):
        self.value = (self.value * self.size + value) / (self.size + 1)
        self.size += 1

    def subtract(self, value):
        if self.size <= 1:
            self.value = 0
            self.size = 0
        self.value = (self.value * self.size - value) / (self.size - 1)
        self.size -= 1


class SlowLog:

    def __init__(self):
        self._time_averages = {}
        self._retry_averages = {}

    def log(self, time, name, retry):
        if (time_avg := self._time_averages.get(name)) is None:
            time_avg = InplaceAverage()
            self._time_averages[name] = time_avg
        if (retry_avg := self._retry_averages.get(name)) is None:
            retry_avg = InplaceAverage()
            self._retry_averages[name] = retry_avg
        time_avg.add(time)
        retry_avg.add(retry)
        if time > SLOW_LOG_TIME_THRESHOLD or retry > SLOW_LOG_RETRY_THRESHOLD:
            logger.warning(f"⚠️ [📞慢日志] 系统 {name} 执行时间 {time:.3f}秒，事务冲突次数 {retry}，"
                           f"平均时间 {time_avg.value:.3f}秒\n{self}")

    def __str__(self):
        slow20 = sorted(self._time_averages.items(), key=lambda x: x[1].value, reverse=True)[:20]
        retry20 = sorted(self._retry_averages.items(), key=lambda x: x[1].value, reverse=True)[:20]
        tops = [name for name, avg in slow20]
        tops.extend(name for name, avg in retry20 if name not in tops)
        rows = [(name, self._time_averages[name].value, self._time_averages[name].value)
                for name in tops]
        return tabulate(rows, headers=['系统', '平均时间', '平均冲突次数'],
                        tablefmt='github', floatfmt=".2f")
