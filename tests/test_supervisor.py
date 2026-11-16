"""
aiohttp 栈的多进程管理（hetu/server/supervisor.py，设计稿 §5.7）：

- 管理进程的逻辑：用假 worker（tests/fixtures/fake_workers.py）真起进程，超时、退避都调小；
- 端到端：真起 `hetu start`（aiohttp 栈、多个 worker），只在 POSIX 上跑。Windows 的人工检查清单见设计稿。
"""

import asyncio
import json
import logging
import multiprocessing
import multiprocessing.connection
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import psutil
import pytest
from fixtures import fake_workers
from websockets.exceptions import ConnectionClosed
from websockets.legacy.client import connect

from hetu.server.pipeline import JSONBinaryLayer, MessagePipeline
from hetu.server.supervisor import (
    MSG_STOP,
    RestartPolicy,
    Supervisor,
    WorkerChannel,
    worker_stop_timeout,
)

POSIX_ONLY = pytest.mark.skipif(sys.platform == "win32", reason="POSIX 信号")
HETU = Path(sys.executable).with_name("hetu")


class _Records(logging.Handler):
    """直接挂在 HeTu.root 上收日志：起服类测试会把它的 propagate 关掉，caplog 收不到"""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def messages(self) -> list[str]:
        return [record.getMessage() for record in self.records]


@pytest.fixture
def logs():
    handler = _Records()
    hetu_logger = logging.getLogger("HeTu.root")
    level = hetu_logger.level
    hetu_logger.addHandler(handler)
    hetu_logger.setLevel(logging.INFO)
    try:
        yield handler
    finally:
        hetu_logger.removeHandler(handler)
        hetu_logger.setLevel(level)


def _supervisor(
    tmp_path: Path, behavior: str, count: int = 2, **kwargs: Any
) -> Supervisor:
    config = {"DIR": str(tmp_path), "BEHAVIOR": behavior, **kwargs.pop("config", {})}
    kwargs.setdefault("ack_timeout", 20.0)
    kwargs.setdefault("stop_timeout", 10.0)
    kwargs.setdefault("policy", RestartPolicy(base_delay=0.05, max_delay=0.2))
    kwargs.setdefault("reload", False)
    return Supervisor(
        config, None, count, target=fake_workers.worker, poll_interval=0.05, **kwargs
    )


def _run(
    sup: Supervisor, until: Callable[[], bool] | None = None, timeout: float = 30
) -> int:
    """
    在主线程跑 sup.run()（它要装信号处理），另起一个线程盯着：until() 成立时 request_stop。until 为
    None 时等 run 自己返回。超时也 request_stop
    """
    finished = threading.Event()

    def watch() -> None:
        deadline = time.monotonic() + timeout
        while not finished.is_set():
            if (until is not None and until()) or time.monotonic() > deadline:
                sup.request_stop()
                return
            time.sleep(0.02)

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    try:
        return sup.run()
    finally:
        finished.set()
        watcher.join(5)


def _events(tmp_path: Path, event: str | None = None) -> list[tuple]:
    return [e for e in fake_workers.events(tmp_path) if event in (None, e[1])]


# ---------------------------------------------------------------- 管理进程的逻辑


def test_restart_policy_backoff():
    policy = RestartPolicy()
    assert [policy.delay(n) for n in (1, 2, 3, 5, 6, 9)] == [0.5, 1, 2, 8, 10, 10]


def test_worker_stop_timeout_covers_every_shutdown_budget():
    """排空一份、取消收发任务 / 等连接拆完 / 停 FutureCalls 各一份 GRACEFUL，再加余量"""
    config = {"SHUTDOWN_DRAIN_TIMEOUT": 5, "GRACEFUL_SHUTDOWN_TIMEOUT": 15}
    assert worker_stop_timeout(config) == 5 + 3 * 15 + 10


def test_starts_workers_with_distinct_index_and_stops_them(tmp_path, logs):
    """各 worker 的 HETU_WORKER_INDEX 互不相同，管理进程自己的环境里不留；关服时都经控制管道收到停服"""
    sup = _supervisor(tmp_path, "serve", count=3)
    code = _run(sup, until=lambda: len(_events(tmp_path, "ready")) == 3)
    assert code == 0
    assert sorted(e[2] for e in _events(tmp_path, "start")) == [0, 1, 2]
    assert sorted(e[2] for e in _events(tmp_path, "exit-stop")) == [0, 1, 2]
    assert "HETU_WORKER_INDEX" not in os.environ
    assert any("3 个 worker 都已就绪" in m for m in logs.messages())


def test_failure_before_ready_aborts_and_is_reported_once(tmp_path, logs):
    """就绪之前出错：整体启动失败，以 1 退出，不重启；几个 worker 一起出错时说明只打一次"""
    started = time.monotonic()
    code = _run(_supervisor(tmp_path, "fail", count=3))
    assert code == 1
    assert time.monotonic() - started < 10
    messages = logs.messages()
    assert sum("RuntimeError: boom" in m for m in messages) == 1, messages
    assert sum("也没能开服" in m for m in messages) == 2, messages
    assert len(_events(tmp_path, "start")) == 3


def test_startup_aborted_uses_the_single_process_message(tmp_path, logs):
    assert _run(_supervisor(tmp_path, "abort", count=1)) == 1
    assert any(
        "服务器启动失败，已中止" in m and "启动 System boom 失败" in m
        for m in logs.messages()
    ), logs.messages()


def test_exit_before_ready_without_detail_reports_exit_code(tmp_path, logs):
    assert _run(_supervisor(tmp_path, "exit3", count=1)) == 1
    assert any("就绪之前退出了（退出码 3）" in m for m in logs.messages())


def test_ack_timeout_aborts_and_names_the_stuck_workers(tmp_path, logs):
    """
    开服卡住：ack 超时后以 1 退出，日志写明哪个 worker 卡住了，各一行，没有别的说明。卡住的 worker 直接强制
    结束（多半卡在阻塞调用里，收不到停服通知），不等满关服的上限
    """
    started = time.monotonic()
    sup = _supervisor(tmp_path, "hang", count=2, ack_timeout=0.5, stop_timeout=30)
    code = _run(sup)
    assert code == 1
    assert time.monotonic() - started < 8
    messages = logs.messages()
    stuck = [m for m in messages if "没在 0.5 秒内就绪" in m]
    assert len(stuck) == 2, messages
    assert all("强制结束" in m for m in stuck), messages
    assert not any("也没能开服" in m or "就绪之前退出了" in m for m in messages)
    assert _events(tmp_path, "exit-stop") == []


def test_stop_requested_during_startup_aborts_quietly(tmp_path, logs):
    sup = _supervisor(tmp_path, "hang", count=2)
    code = _run(sup, until=lambda: len(_events(tmp_path, "start")) == 2)
    assert code == 0
    assert any("开服途中收到关服信号" in m for m in logs.messages())


def test_crashed_worker_is_restarted_with_the_same_index(tmp_path, logs):
    """就绪之后崩溃的 worker：退避之后以同一个序号重启，其余 worker 不受影响"""
    sup = _supervisor(tmp_path, "crash_once", config={"CRASH_INDEX": 1})
    code = _run(sup, until=lambda: len(_events(tmp_path, "ready")) == 3)
    assert code == 0
    starts = _events(tmp_path, "start")
    assert sorted(e[2] for e in starts) == [0, 1, 1]
    pids = [e[3] for e in starts if e[2] == 1]
    assert pids[0] != pids[1]
    assert any(
        "worker 1 意外退出（退出码 1）" in m and "以同一序号重启" in m
        for m in logs.messages()
    ), logs.messages()


def test_gives_up_after_too_many_consecutive_failures(tmp_path, logs):
    policy = RestartPolicy(base_delay=0.01, max_delay=0.05, max_failures=2)
    code = _run(_supervisor(tmp_path, "crash_always", count=1, policy=policy))
    assert code == 1
    assert len(_events(tmp_path, "start")) == 3  # 第一次 + 重启两次
    assert any("连续 3 次意外退出" in m for m in logs.messages()), logs.messages()


def test_worker_that_lost_its_lease_is_restarted(tmp_path, logs):
    sup = _supervisor(tmp_path, "lease_lost_once", count=1, config={"CRASH_INDEX": 0})
    code = _run(sup, until=lambda: len(_events(tmp_path, "ready")) == 2)
    assert code == 0
    assert any("Worker ID 租约丢失" in m for m in logs.messages()), logs.messages()


def test_worker_that_ignores_stop_is_killed_after_the_timeout(tmp_path, logs):
    sup = _supervisor(tmp_path, "ignore_stop", count=1, stop_timeout=0.5)
    started = time.monotonic()
    code = _run(sup, until=lambda: len(_events(tmp_path, "ready")) == 1)
    assert code == 0
    assert time.monotonic() - started < 8
    assert any("没在 0.5 秒内退出，强制结束" in m for m in logs.messages())


def test_listen_socket_closes_once_every_worker_stops_listening(tmp_path, logs):
    """
    关服时 worker 各自先停止监听（关掉自己那份 socket），再排空、跑断线 System，要好几秒才退出。都停止
    监听之后新连接要马上被拒，网关好换一台；不能因为管理进程手里还有一份，socket 接着监听：新连接在内核里
    握完手、进了监听队列却没人接，一直挂到管理进程退出
    """
    sock = socket.create_server(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    config = {"DIR": str(tmp_path), "BEHAVIOR": "close_sock_on_stop", "LINGER": 3.0}
    sup = Supervisor(
        config,
        sock,
        2,
        target=fake_workers.worker,
        ack_timeout=20.0,
        stop_timeout=10.0,
        reload=False,
        poll_interval=0.05,
    )
    probe: list[str] = []

    def connect_after_workers_stop_listening() -> None:
        deadline = time.monotonic() + 30
        while len(_events(tmp_path, "sock-closed")) < 2:
            if time.monotonic() > deadline:
                probe.append("worker 没有停止监听")
                return
            time.sleep(0.02)
        try:
            # Windows 上连一个没人监听的端口，要重试两次 SYN、约 2 秒才报被拒
            with socket.create_connection(("127.0.0.1", port), timeout=5):
                probe.append("连上了")
        except ConnectionRefusedError:
            probe.append("被拒")
        except OSError as e:
            probe.append(repr(e))

    prober = threading.Thread(target=connect_after_workers_stop_listening, daemon=True)
    prober.start()
    try:
        code = _run(sup, until=lambda: len(_events(tmp_path, "ready")) == 2)
    finally:
        sock.close()
    prober.join(10)
    assert code == 0
    assert probe == ["被拒"]


def test_only_the_first_shutdown_signal_counts(tmp_path, logs):
    """
    关服信号只认第一个：关服途中再来的不打断它（不会像原来 Sanic 那样第二个信号就强杀），worker 照常关完服
    自己退出。收到过信号之后，管理进程退出前一直忽略关服信号；测完把测试进程的信号处理恢复原样
    """
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    sent: list[int] = []

    def until() -> bool:
        if len(_events(tmp_path, "ready")) == 2 and not sent:
            signal.raise_signal(signal.SIGTERM)
            sent.append(1)
            time.sleep(0.3)  # 关服已经开始，worker 正慢慢停
            signal.raise_signal(signal.SIGTERM)
            signal.raise_signal(signal.SIGINT)
        return False

    try:
        sup = _supervisor(tmp_path, "slow_stop")
        code = _run(sup, until=until)
        assert signal.getsignal(signal.SIGTERM) == signal.SIG_IGN
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    assert code == 0
    assert sorted(e[2] for e in _events(tmp_path, "exit-stop")) == [0, 1]
    assert not any("强制结束" in m for m in logs.messages())


def test_ctrl_c_interrupting_the_wait_still_shuts_down_cleanly(
    tmp_path, logs, monkeypatch
):
    """
    Windows 上主线程在 multiprocessing.connection.wait 里等的时候按 Ctrl+C：信号处理跑完之后它抛
    InterruptedError（不像 POSIX 那样自动重试）。这里在 Linux 上照样模拟：管理进程要接住，照常关服，
    不能让异常冒出 run()、把 worker 都强杀了
    """
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    real_wait = multiprocessing.connection.wait
    fired: list[int] = []
    sup = _supervisor(tmp_path, "serve")

    def interrupted_wait(objects, timeout=None):
        # 只打断管理进程主循环的那次 wait（同时等好几个对象）；Connection.poll 内部也调 wait([self])
        serving = len(sup.workers) == 2 and all(
            w.ready is not None for w in sup.workers.values()
        )
        if not fired and len(objects) > 1 and serving:
            fired.append(1)
            signal.raise_signal(signal.SIGINT)
            raise InterruptedError(4, "Interrupted system call")
        return real_wait(objects, timeout)

    monkeypatch.setattr(multiprocessing.connection, "wait", interrupted_wait)
    try:
        code = _run(sup)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    assert fired
    assert code == 0
    assert sorted(e[2] for e in _events(tmp_path, "exit-stop")) == [0, 1]


def test_errors_after_ready_are_printed(tmp_path, logs):
    """开好服之后 worker 出错退出（关服时 runtime.stop() 出错等）：说明由管理进程打出来，照常重启"""
    sup = _supervisor(tmp_path, "error_after_ready", count=1, config={"CRASH_INDEX": 0})
    code = _run(sup, until=lambda: len(_events(tmp_path, "ready")) == 2)
    assert code == 0
    messages = logs.messages()
    assert any("出错退出" in m and "boom-after-ready" in m for m in messages), messages


def test_errors_while_stopping_are_printed(tmp_path, logs):
    sup = _supervisor(tmp_path, "error_on_stop", count=1)
    code = _run(sup, until=lambda: len(_events(tmp_path, "ready")) == 1)
    assert code == 0
    messages = logs.messages()
    assert any("出错退出" in m and "boom-on-stop" in m for m in messages), messages


def test_restarted_worker_stuck_in_startup_is_killed_and_counted(tmp_path, logs):
    """重启出来的 worker 没在 ack_timeout 内就绪：强制结束，算一次失败；一直卡住的话超过上限整体退出"""
    policy = RestartPolicy(base_delay=0.05, max_delay=0.1, max_failures=2)
    sup = _supervisor(
        tmp_path,
        "crash_then_hang",
        ack_timeout=2.0,
        policy=policy,
        config={"CRASH_INDEX": 1},
    )
    code = _run(sup)
    assert code == 1
    messages = logs.messages()
    stuck = [m for m in messages if "没在 2 秒内就绪" in m and "强制结束" in m]
    assert len(stuck) == 2, messages
    assert any("连续 3 次意外退出" in m for m in messages), messages
    assert _events(tmp_path, "exit-stop")[0][2] == 0  # worker 0 照常关服


def test_failure_count_resets_after_running_stably(tmp_path, logs):
    """就绪之后稳定跑满 stable_after 秒才崩的，连续失败从头计数：不会因为偶尔崩一次累计到上限"""
    policy = RestartPolicy(
        base_delay=0.05, max_delay=0.1, max_failures=1, stable_after=0.3
    )
    sup = _supervisor(
        tmp_path,
        "crash_after_delay",
        count=1,
        policy=policy,
        config={"CRASH_INDEX": 0, "CRASH_DELAY": 0.6},
    )
    code = _run(sup, until=lambda: len(_events(tmp_path, "start")) >= 4)
    assert code == 0
    assert not any("不再重启" in m for m in logs.messages())


def test_worker_channel_stops_on_message_or_when_the_manager_is_gone():
    """worker 一侧：收到停服通知，或者管理进程没了（管道读到 EOF），都调用 on_stop"""
    ctx = multiprocessing.get_context("spawn")
    for how in ("stop", "eof"):
        inbox, sender = ctx.Pipe(duplex=False)
        _unused, outbox = ctx.Pipe(duplex=False)
        stopped = threading.Event()
        WorkerChannel(inbox, outbox).listen(stopped.set)
        assert not stopped.wait(0.1)
        if how == "stop":
            sender.send(("ping",))
            assert not stopped.wait(0.1)
            sender.send((MSG_STOP,))
        else:
            sender.close()
        assert stopped.wait(2), how


def _touch(path: Path) -> None:
    """改修改时间（往后拨，同一秒内也看得出变化）"""
    mtime = path.stat().st_mtime + 5
    os.utime(path, (mtime, mtime))


def test_reload_restarts_workers_one_at_a_time(tmp_path, logs):
    """
    DEBUG 自动重载：worker 报上来的文件改了，逐个重启，每个序号都是先停旧的再起新的。假 worker 收到停服后
    过 1 秒才退出：先起新的话，新进程的 start 会早于旧进程的退出
    """
    watched = tmp_path / "app.py"
    watched.write_text("# app", encoding="utf-8")
    touched = []

    def until() -> bool:
        if not touched and len(_events(tmp_path, "ready")) == 2:
            _touch(watched)
            touched.append(True)
        return len(_events(tmp_path, "ready")) == 4

    sup = _supervisor(
        tmp_path, "slow_stop", reload=True, config={"MODULES": [str(watched)]}
    )
    assert _run(sup, until=until) == 0
    events = fake_workers.events(tmp_path)
    for index in (0, 1):
        mine = [(t, e) for t, e, i, _pid in events if i == index]
        starts = [t for t, e in mine if e == "start"]
        exits = [t for t, e in mine if e == "exit-stop"]
        assert len(starts) == 2 and len(exits) == 2, mine
        assert exits[0] < starts[1]  # 旧的停了才起新的
    # 逐个：worker 0 的新进程就绪之后才动 worker 1
    ready_0 = [t for t, e, i, _p in events if i == 0 and e == "ready"][1]
    exit_1 = next(t for t, e, i, _p in events if i == 1 and e == "exit-stop")
    assert ready_0 < exit_1
    assert any("文件有改动" in m for m in logs.messages())


def test_stop_during_reload_does_not_start_new_workers(tmp_path, logs):
    """重载途中（正在等旧 worker 停下）要求关服：停完旧的就收手，不再起新的"""
    watched = tmp_path / "app.py"
    watched.write_text("# app", encoding="utf-8")
    touched = []

    def until() -> bool:
        if not touched and len(_events(tmp_path, "ready")) == 2:
            _touch(watched)
            touched.append(True)
        return any("文件有改动" in m for m in logs.messages())

    sup = _supervisor(
        tmp_path, "slow_stop", reload=True, config={"MODULES": [str(watched)]}
    )
    assert _run(sup, until=until) == 0
    assert sorted(e[2] for e in _events(tmp_path, "start")) == [0, 1]


def test_reload_with_broken_code_keeps_the_other_workers(tmp_path, logs):
    """改坏了：新的 worker 0 没开起服来，停在这里，worker 1 照旧；改好之后的下一次改动再重启"""
    watched = tmp_path / "app.py"
    watched.write_text("# app", encoding="utf-8")
    broken = tmp_path / "broken"
    stage: list[str] = []

    def until() -> bool:
        ready = len(_events(tmp_path, "ready"))
        if not stage and ready == 2:
            broken.touch()
            _touch(watched)
            stage.append("broken")
        elif stage == ["broken"] and any("没能开服" in m for m in logs.messages()):
            # 过一会儿再改好：要是重载没停在 worker 0、接着去动 worker 1，这段时间里它会撞上改坏的代码
            time.sleep(1)
            broken.unlink()
            _touch(watched)
            stage.append("fixed")
        return stage == ["broken", "fixed"] and ready == 4

    config = {"MODULES": [str(watched)], "BROKEN": str(broken)}
    sup = _supervisor(tmp_path, "reload_broken", reload=True, config=config)
    assert _run(sup, until=until) == 0
    events = fake_workers.events(tmp_path)
    starts_1 = [t for t, e, i, _p in events if i == 1 and e == "start"]
    assert len(starts_1) == 2  # 改坏那次 worker 1 没被动
    messages = logs.messages()
    assert any("SyntaxError: broken" in m for m in messages), messages
    assert any("改动后 worker 0 没能开服" in m for m in messages), messages


# ---------------------------------------------------------------- 端到端：真起 hetu start

SUP_APP = """
import logging
import os

import numpy as np

import hetu

if os.environ.get("SUP_IMPORT_ERROR"):
    raise RuntimeError("boom-import")


@hetu.define_component(namespace="sup", force=True, permission=hetu.Permission.EVERYBODY)
class Seen(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)
    count: np.int32 = hetu.property_field(0)


def _me():
    return [int(os.environ["HETU_WORKER_INDEX"]), os.getpid()]


@hetu.define_endpoint(namespace="sup", permission=hetu.Permission.EVERYBODY)
async def whoami(ctx):
    return hetu.ResponseToClient(_me())


@hetu.define_endpoint(namespace="sup", permission=hetu.Permission.EVERYBODY)
async def login(ctx, user_id):
    await hetu.elevate(ctx, user_id)
    return hetu.ResponseToClient(_me())


@hetu.define_system(namespace="sup", components=(Seen,), permission=None)
async def on_disconnect(ctx):
    if os.environ.get("SUP_BIG_LOGS"):
        logging.getLogger("HeTu.root").info("disconnect " + "x" * 4000)
    if ctx.caller:
        async with ctx.repo[Seen].upsert(owner=ctx.caller) as row:
            row.count += 1
"""


# 同配置模板：日志经进程安全队列交给管理进程输出
QUEUE_LOGGING = """
LOGGING:
  version: 1
  disable_existing_loggers: false
  loggers:
    root:
      level: INFO
      handlers: [log_queue]
  handlers:
    console:
      class: logging.StreamHandler
      stream: ext://sys.stdout
    log_queue:
      class: logging.handlers.QueueHandler
      queue: hetu.safelogging.handlers.process_safe_queue
      listener: hetu.safelogging.handlers.AutoListener
      handlers: [console]
"""


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _alive(pid: int) -> bool:
    """进程还在跑：退出了但还没被收尸的（僵尸）算退出了"""
    try:
        return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


class _Cluster:
    """一个多 worker 的 `hetu start` 子进程（aiohttp 栈），输出写到 tmp_path/out.log"""

    def __init__(
        self,
        tmp_path: Path,
        workers: int = 2,
        backend: str | None = None,
        extra: str = "",
        **env: str,
    ):
        self.tmp_path = tmp_path
        self.port = _free_port()
        (tmp_path / "sup_app.py").write_text(SUP_APP, encoding="utf-8")
        if backend is None:
            backend = f"type: sqlite\n    master: sqlite:///{tmp_path / 'sup.db'}"
        self.config = tmp_path / "config.yml"
        self.config.write_text(
            f"""
APP_FILE: sup_app.py
NAMESPACE: sup
INSTANCES:
  - sup_1
LISTEN: 127.0.0.1:{self.port}
WORKER_NUM: {workers}
DEBUG: false
GRACEFUL_SHUTDOWN_TIMEOUT: 5
PACKET_LAYERS:
  - type: jsonb
BACKENDS:
  main:
    {backend}
{extra}
""",
            encoding="utf-8",
        )
        self.log_path = tmp_path / "out.log"
        self._log = open(self.log_path, "wb")  # noqa: SIM115 子进程活着期间一直开着，close() 里关
        self.proc = subprocess.Popen(
            [str(HETU), "start", "--config", str(self.config)],
            cwd=tmp_path,
            env=dict(os.environ, PYTHONUNBUFFERED="1", **env),
            stdout=self._log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def output(self) -> str:
        return self.log_path.read_text(encoding="utf-8", errors="replace")

    def worker_pids(self) -> list[int]:
        """管理进程的 worker 子进程（不算 multiprocessing 的 resource tracker）"""
        return [
            child.pid
            for child in psutil.Process(self.proc.pid).children()
            if "spawn_main" in " ".join(child.cmdline())
        ]

    def wait_for(self, marker: str, timeout: float = 60) -> None:
        deadline = time.monotonic() + timeout
        while marker not in self.output():
            assert self.proc.poll() is None, self.output()
            assert time.monotonic() < deadline, self.output()
            time.sleep(0.1)

    def signal_group(self, sig: int) -> None:
        """往整个进程组发信号，同终端里按 Ctrl+C"""
        try:
            os.killpg(self.proc.pid, sig)
        except ProcessLookupError:
            pass

    def close(self) -> None:
        self.signal_group(signal.SIGKILL)
        if self.proc.poll() is None:
            self.proc.wait()
        self._log.close()

    def rows(self, component: str, index: str) -> list[dict[str, Any]]:
        """服务器退出之后，用 hetu range 读一张表的全部行（读 master）"""
        proc = subprocess.run(
            [str(HETU), "range", "--config", str(self.config), "--master"]
            + ["--limit", "-1", component, index, "0", str(2**63 - 1)],
            check=False,
            cwd=self.tmp_path,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=60,
        )
        line = next(
            line for line in reversed(proc.stdout.splitlines()) if line.startswith("{")
        )
        result = json.loads(line)
        assert result["ok"], result
        return result["rows"]


READY = "个 worker 都已就绪"


async def _call(port: int, *rpc: Any, keep: bool = False) -> tuple[Any, Any]:
    """连上、握手（只有 jsonb 层）、调一次 rpc，返回 (回复, ws)。keep 为 False 时关掉连接"""
    pipe = MessagePipeline()
    pipe.add_layer(JSONBinaryLayer())
    ws = await connect(f"ws://127.0.0.1:{port}/hetu/sup_1")
    await ws.send(pipe.encode(None, []))
    hello = cast(list[bytes], pipe.decode(None, cast(bytes, await ws.recv())))
    pipe_ctx, _reply = pipe.handshake(hello)
    await ws.send(pipe.encode(pipe_ctx, ["rpc", *rpc]))
    reply = cast(list[Any], pipe.decode(pipe_ctx, cast(bytes, await ws.recv())))
    assert reply[0] == "rsp", reply
    if not keep:
        await ws.close()
    return reply[1], ws


async def _whoami_until(
    port: int, done: Callable[[dict[int, int]], bool], timeout: float = 30
) -> dict[int, int]:
    """反复新连接调 whoami，收集 序号 → pid，直到 done 成立"""
    seen: dict[int, int] = {}
    async with asyncio.timeout(timeout):
        while not done(seen):
            (index, pid), _ws = await _call(port, "whoami")
            seen[index] = pid
    return seen


async def _close_code(ws: Any, timeout: float = 20) -> int | None:
    try:
        async with asyncio.timeout(timeout):
            while True:
                await ws.recv()
    except ConnectionClosed as e:
        return None if e.rcvd is None else e.rcvd.code


@POSIX_ONLY
@pytest.mark.timeout(120)
def test_workers_serve_and_a_killed_worker_comes_back(tmp_path):
    """
    两个 worker 都在接连接；kill -9 其中一个，它以原序号被重启、重新接连接；终端里 Ctrl+C（SIGINT 发给
    整个进程组）后以 0 退出
    """
    cluster = _Cluster(tmp_path)
    try:
        cluster.wait_for(READY)
        seen = asyncio.run(_whoami_until(cluster.port, lambda s: {0, 1} <= set(s)))
        os.kill(seen[1], signal.SIGKILL)
        cluster.wait_for("worker 1 意外退出（被信号 SIGKILL 结束）")
        again = asyncio.run(
            _whoami_until(cluster.port, lambda s: s.get(1) not in (None, seen[1]))
        )
        assert again[1] != seen[1]
        cluster.signal_group(signal.SIGINT)
        code = cluster.proc.wait(60)
    finally:
        cluster.close()
    output = cluster.output()
    assert code == 0, output
    assert "强制结束" not in output, output


@POSIX_ONLY
@pytest.mark.timeout(120)
def test_sigterm_to_the_manager_shuts_every_worker_down_cleanly(tmp_path):
    """
    只给管理进程发 SIGTERM（docker stop 就是这样）：它经控制管道通知各 worker 停服。两个 worker 上
    已登录的连接都收到 1001，断线 System 各跑一次、Connection 行删光，以 0 退出
    """
    cluster = _Cluster(tmp_path)
    try:
        cluster.wait_for(READY)

        async def scenario() -> list[int | None]:
            clients = []
            workers: set[int] = set()
            for user_id in range(101, 141):
                (index, _pid), ws = await _call(
                    cluster.port, "login", user_id, keep=True
                )
                clients.append(ws)
                workers.add(index)
                if workers == {0, 1} and len(clients) >= 4:
                    break
            assert workers == {0, 1}
            os.kill(cluster.proc.pid, signal.SIGTERM)
            return [await _close_code(ws) for ws in clients]

        codes = asyncio.run(scenario())
        code = cluster.proc.wait(60)
    finally:
        cluster.close()
    output = cluster.output()
    assert code == 0, output
    assert set(codes) == {1001}, (codes, output)
    assert "没拆完" not in output and "强制结束" not in output, output
    assert cluster.rows("Connection", "id") == []
    seen = cluster.rows("Seen", "owner")
    assert sorted(r["count"] for r in seen) == [1] * len(codes)


@POSIX_ONLY
@pytest.mark.timeout(120)
def test_shutdown_releases_every_worker_lease(tmp_path, ses_redis_service):
    """Redis 后端：开着服时两个 worker 各持有一个 Worker ID 租约，关服之后都释放了"""
    import redis

    from hetu.data.backend.redis.worker_keeper import live_worker_ids

    master_url = ses_redis_service[0].rsplit("/", 1)[0] + "/11"
    io = redis.Redis.from_url(master_url)
    io.flushdb()
    cluster = _Cluster(tmp_path, backend=f"type: redis\n    master: {master_url}")
    try:
        cluster.wait_for(READY)
        assert len(live_worker_ids(io)) == 2
        cluster.signal_group(signal.SIGINT)
        code = cluster.proc.wait(60)
    finally:
        cluster.close()
    try:
        assert code == 0, cluster.output()
        assert live_worker_ids(io) == []
    finally:
        io.flushdb()
        io.close()


@POSIX_ONLY
@pytest.mark.timeout(120)
def test_import_error_aborts_startup_and_is_printed_once(tmp_path):
    """app 文件导入出错：很快以非 0 退出，不重启；三个 worker 一起出错，traceback 只打一次"""
    cluster = _Cluster(tmp_path, workers=3, SUP_IMPORT_ERROR="1")
    try:
        code = cluster.proc.wait(60)
    finally:
        cluster.close()
    output = cluster.output()
    assert code != 0, output
    assert output.count("RuntimeError: boom-import") == 1, output
    assert output.count("启动 worker") == 3, output
    assert "重启" not in output, output


@POSIX_ONLY
@pytest.mark.timeout(120)
def test_workers_exit_by_themselves_when_the_manager_is_killed(tmp_path):
    """管理进程被 kill -9：worker 读到控制管道的 EOF，自己关服退出，不会变成孤儿接着监听"""
    cluster = _Cluster(tmp_path)
    try:
        cluster.wait_for(READY)
        seen = asyncio.run(_whoami_until(cluster.port, lambda s: {0, 1} <= set(s)))
        os.kill(cluster.proc.pid, signal.SIGKILL)
        cluster.proc.wait(10)
        deadline = time.monotonic() + 30
        alive = set(seen.values())
        while alive and time.monotonic() < deadline:
            alive = {pid for pid in alive if _alive(pid)}
            time.sleep(0.1)
        assert not alive, cluster.output()
    finally:
        cluster.close()


@POSIX_ONLY
@pytest.mark.timeout(120)
def test_workers_with_queue_logging_exit_when_the_manager_is_killed(tmp_path):
    """
    同上，配了进程安全队列的日志（配置模板就是这样）：管理进程没了以后队列没人读，worker 关服时打的日志
    （每个连接的断线 System 打 4KB，共 160KB）写满管道，退出时不能卡在等队列的后台线程上
    """
    cluster = _Cluster(tmp_path, extra=QUEUE_LOGGING, SUP_BIG_LOGS="1")
    try:
        cluster.wait_for(READY)
        pids = cluster.worker_pids()
        assert len(pids) == 2

        async def scenario() -> set[int]:
            clients = []
            for user_id in range(201, 241):
                _who, ws = await _call(cluster.port, "login", user_id, keep=True)
                clients.append(ws)
            os.kill(cluster.proc.pid, signal.SIGKILL)
            # 客户端照常读，收得到 close 帧，连接拆得快
            closes = [asyncio.ensure_future(_close_code(ws, 40)) for ws in clients]
            alive = set(pids)
            deadline = time.monotonic() + 40
            while alive and time.monotonic() < deadline:
                alive = {pid for pid in alive if _alive(pid)}
                await asyncio.sleep(0.2)
            for close in closes:
                close.cancel()
            await asyncio.gather(*closes, return_exceptions=True)
            return alive

        alive = asyncio.run(scenario())
        assert not alive, cluster.output()
    finally:
        cluster.close()
