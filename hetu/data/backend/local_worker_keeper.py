"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
from pathlib import Path
from time import time
from typing import final, override

from filelock import FileLock

from ...common.snowflake_id import MAX_WORKER_ID, WorkerKeeper

logger = logging.getLogger("HeTu.root")

_COUNTER_FILE = ".hetu_worker_counter.txt"
_LOCK_FILE = ".hetu_worker_counter.lock"
_TIMESTAMP_FILE_PREFIX = ".hetu_worker_last_timestamp_"
_PID_TO_WORKER_ID: dict[int, int] = {}


@final
class LocalWorkerKeeper(WorkerKeeper):
    """
    单机模式的WorkerKeeper默认实现。

    Single-machine WorkerKeeper for local development.
    - 仅保证同一台机器上的进程分配递增 worker id
    - `keep_alive` 只落盘 `last_timestamp`
    """

    def __init__(self, pid: int):
        super().__init__()
        self.pid = pid
        self.worker_id = -1

    @staticmethod
    def _counter_path() -> Path:
        return Path.cwd() / _COUNTER_FILE

    @staticmethod
    def _timestamp_path(worker_id: int) -> Path:
        return Path.cwd() / f"{_TIMESTAMP_FILE_PREFIX}{worker_id}.txt"

    @override
    def get_worker_id(self) -> int:
        """
        获取递增worker id（单机多进程）。

        Allocate an increasing worker id on a single machine.
        """
        if self.worker_id >= 0:
            return self.worker_id

        lock_path = Path.cwd() / _LOCK_FILE
        with FileLock(str(lock_path), timeout=-1):
            if self.pid in _PID_TO_WORKER_ID:
                self.worker_id = _PID_TO_WORKER_ID[self.pid]
                return self.worker_id

            counter_path = self._counter_path()
            if counter_path.exists():
                counter = int(counter_path.read_text(encoding="ascii").strip() or "-1")
            else:
                counter = -1
            next_worker_id = counter + 1

            if next_worker_id > MAX_WORKER_ID:
                raise KeyError(
                    "无法获取可用的 Worker ID，超过雪花ID可用范围。"
                    f"当前上限：{MAX_WORKER_ID}"
                )

            counter_path.write_text(str(next_worker_id), encoding="ascii")
            _PID_TO_WORKER_ID[self.pid] = next_worker_id

        self.worker_id = next_worker_id
        logger.info(f"[❄️ID] [SingleMachine] 分配 Worker ID: {self.worker_id}")
        return self.worker_id

    @override
    def release_worker_id(self):
        """
        单机默认实现不回收ID（只递增）。

        Default single-machine keeper does not recycle IDs.
        """
        if self.worker_id >= 0:
            logger.info(f"[❄️ID] [SingleMachine] 释放 Worker ID: {self.worker_id}")

    @override
    def get_last_timestamp(self) -> int:
        """
        读取当前worker id对应的本地last_timestamp文件。

        Read last timestamp from local file for this worker id.
        """
        now_ms = int(time() * 1000)
        if self.worker_id < 0:
            return now_ms

        timestamp_path = self._timestamp_path(self.worker_id)
        if not timestamp_path.exists():
            return now_ms

        try:
            last_timestamp = int(timestamp_path.read_text(encoding="ascii").strip())
        except ValueError:
            return now_ms

        return max(last_timestamp, now_ms)

    @override
    async def keep_alive(self, last_timestamp: int):
        """
        记录last_timestamp到当前目录本地文件。

        Persist last_timestamp to a local file under current directory.
        """
        worker_id = self.worker_id
        if worker_id < 0:
            worker_id = self.get_worker_id()

        timestamp_path = self._timestamp_path(worker_id)
        timestamp_path.write_text(str(last_timestamp), encoding="ascii")
