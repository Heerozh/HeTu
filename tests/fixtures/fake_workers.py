"""
supervisor 测试用的假 worker。管理进程用 spawn 起它们，所以放在能 import 的模块里。按 config["BEHAVIOR"]
行事，在 config["DIR"]/events-<pid>.log 里记下每一步（时刻、事件、序号、pid），`events()` 合起来读。

Fake worker processes for the supervisor tests.
"""

import os
import sys
import time
from pathlib import Path
from typing import Any

from hetu.server.supervisor import (
    FAILED_ABORTED,
    FAILED_ERROR,
    MSG_FAILED,
    MSG_LEASE_LOST,
    MSG_READY,
    MSG_STOP,
)


def record(config: dict[str, Any], event: str) -> None:
    # 每个进程写自己的文件：Windows 上几个进程往同一个文件追加不是原子的，会互相覆盖、丢行
    index = os.environ.get("HETU_WORKER_INDEX")
    path = Path(config["DIR"]) / f"events-{os.getpid()}.log"
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{time.monotonic():.6f} {event} {index} {os.getpid()}\n")


def events(directory: Path) -> list[tuple[float, str, int, int]]:
    """读各进程的 events-<pid>.log，按时刻排好：[(时刻, 事件, 序号, pid)]"""
    rows = []
    for path in directory.glob("events-*.log"):
        # 最后一段没有换行的是正在写的半行，下次再读
        for line in path.read_text(encoding="utf-8").split("\n")[:-1]:
            t, event, index, pid = line.split()
            rows.append((float(t), event, int(index), int(pid)))
    return sorted(rows)


def _wait_stop(inbox: Any, timeout: float = 60) -> str:
    """等管理进程的停服通知：返回 "stop"，管理进程没了是 "eof"，超时是 "timeout" """
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return "timeout"
        try:
            if inbox.poll(remaining) and inbox.recv()[0] == MSG_STOP:
                return "stop"
        except EOFError:
            return "eof"


def _ready(config: dict[str, Any], outbox: Any, report_modules: bool) -> None:
    modules = config.get("MODULES") if report_modules else None
    outbox.send((MSG_READY, modules))
    record(config, "ready")


def _once(config: dict[str, Any], index: int) -> bool:
    """CRASH_INDEX 这个序号第一次跑到这里时返回 True（之后的重启返回 False）"""
    if index != config.get("CRASH_INDEX"):
        return False
    marker = Path(config["DIR"]) / f"once-{index}"
    if marker.exists():
        return False
    marker.touch()
    return True


def worker(
    config: dict[str, Any], sock: Any, inbox: Any, outbox: Any, report_modules: bool
) -> None:
    behavior = config["BEHAVIOR"]
    index = int(os.environ["HETU_WORKER_INDEX"])
    record(config, "start")
    if behavior == "fail":
        detail = "Traceback (most recent call last):\nRuntimeError: boom\n"
        outbox.send((MSG_FAILED, FAILED_ERROR, detail))
        sys.exit(1)
    if behavior == "abort":
        outbox.send((MSG_FAILED, FAILED_ABORTED, "启动 System boom 失败"))
        sys.exit(1)
    if behavior == "exit3":
        sys.exit(3)
    if behavior == "hang":
        # 一直不就绪，但听停服通知
        how = _wait_stop(inbox)
        record(config, f"exit-{how}")
        sys.exit(0)
    if behavior == "reload_broken" and Path(config["BROKEN"]).exists():
        outbox.send((MSG_FAILED, FAILED_ERROR, "SyntaxError: broken\n"))
        sys.exit(1)
    if behavior == "crash_then_hang" and index == config.get("CRASH_INDEX"):
        if not _once(config, index):
            # 重启出来的：一直不就绪，但听停服通知
            how = _wait_stop(inbox)
            record(config, f"exit-{how}")
            sys.exit(0)
        _ready(config, outbox, report_modules)
        time.sleep(0.05)
        os._exit(1)
    _ready(config, outbox, report_modules)
    if behavior == "crash_after_delay" and index == config.get("CRASH_INDEX"):
        time.sleep(config["CRASH_DELAY"])
        os._exit(1)
    if behavior == "error_after_ready" and _once(config, index):
        detail = "Traceback (most recent call last):\nRuntimeError: boom-after-ready\n"
        outbox.send((MSG_FAILED, FAILED_ERROR, detail))
        sys.exit(1)
    if behavior == "crash_once" and _once(config, index):
        time.sleep(0.05)
        os._exit(1)
    if behavior == "crash_always":
        time.sleep(0.05)
        os._exit(1)
    if behavior == "lease_lost_once" and _once(config, index):
        outbox.send((MSG_LEASE_LOST,))
        sys.exit(1)
    if behavior == "ignore_stop":
        time.sleep(60)
        sys.exit(0)
    how = _wait_stop(inbox)
    if behavior == "slow_stop":
        time.sleep(1)
    record(config, f"exit-{how}")
    if behavior == "error_on_stop":
        detail = "Traceback (most recent call last):\nRuntimeError: boom-on-stop\n"
        outbox.send((MSG_FAILED, FAILED_ERROR, detail))
        sys.exit(1)
    sys.exit(0)
