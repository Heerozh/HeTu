"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
import random
import time

from tabulate import tabulate

from ..i18n import _

logger = logging.getLogger("HeTu.root")
SLOW_LOG_TIME_THRESHOLD = 1
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
        self._logged = {}
        # 每个进程都会打印相同内容，所以随机下间隔
        self.log_interval = random.randint(60, 600)
        self._last_clean = time.time()

    def clear(self):
        self._time_averages.clear()
        self._retry_averages.clear()
        self._logged.clear()
        self._last_clean = time.time()

    def log(self, elapsed, name, retry):
        # 每小时清理一次，防止InplaceAverage的数据越来越不准
        now = time.time()
        if now - self._last_clean > 3600:
            self.clear()
        # 记录慢日志
        if (time_avg := self._time_averages.get(name)) is None:
            time_avg = InplaceAverage()
            self._time_averages[name] = time_avg
        if (retry_avg := self._retry_averages.get(name)) is None:
            retry_avg = InplaceAverage()
            self._retry_averages[name] = retry_avg
        time_avg.add(elapsed)
        retry_avg.add(retry)
        # 1分钟打印一次消息
        if elapsed > SLOW_LOG_TIME_THRESHOLD or retry > SLOW_LOG_RETRY_THRESHOLD:
            if now - self._logged.get(name, 0) > self.log_interval:
                logger.warning(
                    _(
                        "⚠️ [📞慢日志] 系统 {name} 执行时间 {elapsed}秒，"
                        "事务冲突次数 {retry}，平均时间 {avg_time}秒\n{table}"
                    ).format(
                        name=name,
                        elapsed=f"{elapsed:.3f}",
                        retry=retry,
                        avg_time=f"{time_avg.value:.3f}",
                        table=self,
                    )
                )
                self._logged[name] = now

    def __str__(self):
        slow20 = sorted(
            self._time_averages.items(), key=lambda x: x[1].value, reverse=True
        )[:20]
        retry20 = sorted(
            self._retry_averages.items(), key=lambda x: x[1].value, reverse=True
        )[:20]
        tops = [name for name, avg in slow20]
        tops.extend(name for name, avg in retry20 if name not in tops)
        rows = [
            (name, self._time_averages[name].value, self._time_averages[name].value)
            for name in tops
        ]
        return tabulate(
            rows,
            headers=[_("系统"), _("平均时间"), _("平均冲突次数")],
            tablefmt="github",
            floatfmt=".2f",
        )
