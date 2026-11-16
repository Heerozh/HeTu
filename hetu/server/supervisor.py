"""
aiohttp 栈的多进程管理（设计稿 docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §5.7），
原来用的是 Sanic 的 WorkerManager。

管理进程（`Supervisor`）不加载 app 文件：hetu start 在管理进程里配好日志（建好进程安全队列）、bind 好
监听 socket 之后交给它。它用 spawn 起 N 个 worker，每个 worker 一对单向的控制管道：

- worker → 管理进程：开好服了（ready）、开服失败的说明、Worker ID 租约丢了；
- 管理进程 → worker：停服。管理进程没了（被 kill -9、OOM）时 worker 读到 EOF，同样优雅退出。

worker 进程的入口是 `worker_entry`，里面跑 `aiohttp_app.run_worker`。

- ready 之前的任何退出都算整体启动失败：停掉所有 worker，以 1 退出，首个出错说明只打一次。
- ready 之后意外退出的（崩溃、租约丢失）：旧进程已经退出了，再以同一个序号重启，带退避和次数上限
  （`RestartPolicy`），超过上限就整体退出。
- 关服信号只认第一个：通知所有 worker 停服，超时还没退出的强制结束。
- DEBUG 开着时自动重载：worker 报上来的模块文件改了，就逐个重启 worker，先停旧的再起新的。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

Multi-process supervisor for the aiohttp stack: spawns the workers, restarts the ones
that crash, and stops them all on shutdown.
"""

import contextlib
import io
import logging
import multiprocessing
import multiprocessing.connection
import os
import signal
import socket
import sys
import threading
import time
import traceback
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from ..common.helper import WORKER_INDEX_ENV
from ..discovery.registry import DEREGISTER_WAIT, WRITE_TIMEOUT
from ..i18n import _
from .runtime import resolve_ack_timeout

logger = logging.getLogger("HeTu.root")

IS_WINDOWS = sys.platform == "win32"

# 控制管道上的消息，元组的第一项
MSG_READY = "ready"  # worker → 管理进程：(MSG_READY, 已加载的模块文件，不重载时为 None)
MSG_FAILED = (
    "failed"  # worker → 管理进程：(MSG_FAILED, FAILED_ABORTED 或 FAILED_ERROR, 说明)
)
MSG_LEASE_LOST = "lease_lost"  # worker → 管理进程
MSG_STOP = "stop"  # 管理进程 → worker
# 开服失败的两种：StartupAborted 的说明是给人看的一句话，其他异常的说明是 traceback
FAILED_ABORTED = "aborted"
FAILED_ERROR = "error"

# 主循环每轮最多等多久（秒）。Windows 上等管道、等进程时 Python 收不到信号，Ctrl+C 要等它醒来才生效
POLL_INTERVAL = 0.25
# 等一个 worker 关完服的上限 = 关服各步的上限之和再加这个余量（秒），见 worker_stop_timeout
STOP_TIMEOUT_MARGIN = 10.0
# 强制结束之后，等进程真的退出的时间（秒）
KILL_JOIN_TIMEOUT = 5.0
# DEBUG 自动重载：多久查一次文件的修改时间（秒）
RELOAD_INTERVAL = 1.0


@dataclass(frozen=True)
class RestartPolicy:
    """
    ready 之后意外退出的 worker 怎么重启：同一个序号第 n 次连续失败，等
    min(base_delay * 2 ** (n - 1), max_delay) 秒再重启；超过 max_failures 次就放弃，整体退出。
    就绪之后稳定跑满 stable_after 秒才退出的，从头计数。

    How workers that exit unexpectedly after becoming ready are restarted: exponential
    backoff per worker index, giving up after max_failures consecutive failures.
    """

    base_delay: float = 0.5
    max_delay: float = 10.0
    max_failures: int = 5
    stable_after: float = 60.0

    def delay(self, failures: int) -> float:
        """第 failures 次连续失败之后，等多久再重启（秒）"""
        return min(self.base_delay * 2 ** (failures - 1), self.max_delay)


def worker_stop_timeout(config: Mapping[str, Any]) -> float:
    """
    管理进程等一个 worker 关完服的上限（秒）。worker 关服时排空最多 SHUTDOWN_DRAIN_TIMEOUT；取消收发
    任务、等连接拆完、停 FutureCalls 各有一份 GRACEFUL_SHUTDOWN_TIMEOUT（见 AiohttpServer.stop），再加
    余量。打开了节点发现（DISCOVERY）时，关服第一步还要注销：写 draining 最多 WRITE_TIMEOUT 秒，写成功了
    再照常服务 DEREGISTER_WAIT 秒。超时还没退出的强制结束，所以编排器的宽限期（compose 的
    stop_grace_period）要比它长。

    The supervisor's limit (seconds) for a worker to finish shutting down.
    """
    drain = float(config.get("SHUTDOWN_DRAIN_TIMEOUT", 5.0))
    graceful = float(config.get("GRACEFUL_SHUTDOWN_TIMEOUT", 15.0))
    timeout = drain + 3 * graceful + STOP_TIMEOUT_MARGIN
    if config.get("DISCOVERY") is True:
        timeout += WRITE_TIMEOUT + DEREGISTER_WAIT
    return timeout


def loaded_module_files() -> list[str]:
    """本进程已加载的模块的源文件。DEBUG 自动重载监视它们，同原来 Sanic 的 reloader"""
    files = set()
    for module in list(sys.modules.values()):
        path = getattr(module, "__file__", None)
        if isinstance(path, str) and os.path.isfile(path):
            files.add(os.path.abspath(path))
    return sorted(files)


# ---------------------------------------------------------------- worker 这一端


class WorkerChannel:
    """
    worker 这一端的控制管道（`aiohttp_app.WorkerControl` 的实现）：inbox 收管理进程的消息，outbox 发给
    它。等消息用一个线程：Windows 的 selector 事件循环只能等 socket，等不了管道；Unix 上也这样，简单。

    The worker's end of the control channel to the supervisor.
    """

    def __init__(
        self, inbox: Any, outbox: Any, *, report_modules: bool = False
    ) -> None:
        # multiprocessing 的 Connection，Windows 上是 PipeConnection（typeshed 里两者没有继承关系）
        self._inbox = inbox
        self._outbox = outbox
        self._report_modules = report_modules
        # 读到了 EOF：管理进程没了（之后发给它的、写进日志队列的都没人收了）
        self.manager_gone = False
        self.ready_sent = False

    def _send(self, *message: Any) -> None:
        try:
            self._outbox.send(message)
        except OSError:  # 管理进程没了
            pass

    def listen(self, on_stop: Callable[[], None]) -> None:
        """在线程里等管理进程的停服通知；收到了、或者管理进程没了（读到 EOF），调用 on_stop"""

        def wait_stop() -> None:
            try:
                while self._inbox.recv()[0] != MSG_STOP:
                    pass
            except EOFError, OSError:
                # 管理进程没了（被 kill -9、OOM）：子进程不会跟着退出，不能变成孤儿接着监听
                self.manager_gone = True
            on_stop()

        threading.Thread(target=wait_stop, name="hetu-control", daemon=True).start()

    def send_ready(self) -> None:
        modules = loaded_module_files() if self._report_modules else None
        self._send(MSG_READY, modules)
        self.ready_sent = True

    def send_failed(self, kind: str, detail: str) -> None:
        self._send(MSG_FAILED, kind, detail)

    def send_lease_lost(self) -> None:
        self._send(MSG_LEASE_LOST)


def worker_entry(
    config: Mapping[str, Any],
    sock: socket.socket,
    inbox: Any,
    outbox: Any,
    report_modules: bool,
) -> None:
    """
    worker 进程的入口（管理进程用 spawn 起）。本 worker 的序号在环境变量 HETU_WORKER_INDEX 里，管理进程
    启动它之前设好。开服之前出的错不自己打，经控制管道交给管理进程，多个 worker 一起失败时只打一次。

    Entry point of a worker process spawned by the supervisor.
    """
    if IS_WINDOWS:
        # Ctrl+C、Ctrl+Break 会发给控制台里的所有进程：worker 不理，由管理进程经控制管道通知停服
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGBREAK, signal.SIG_IGN)
    channel = WorkerChannel(inbox, outbox, report_modules=report_modules)
    code = 1
    try:
        code = _run_worker(config, sock, channel)
    finally:
        if channel.manager_gone:
            from ..safelogging.handlers import abandon_queues

            abandon_queues()
    sys.exit(code)


def _report_error(
    channel: WorkerChannel, error: BaseException, output: str = ""
) -> int:
    """worker 出错退出：说明交给管理进程打（开服时多个 worker 一起失败只打一次）；管理进程没了就自己打"""
    from .runtime import StartupAborted

    if isinstance(error, StartupAborted):
        channel.send_failed(FAILED_ABORTED, str(error))
        return 1
    detail = output + "".join(traceback.format_exception(error))
    if channel.manager_gone:
        sys.stderr.write(detail)
    else:
        channel.send_failed(FAILED_ERROR, detail)
    return 1


def _run_worker(
    config: Mapping[str, Any], sock: socket.socket, channel: WorkerChannel
) -> int:
    from .adapters import aiohttp_app
    from .config import HeTuConfig
    from .runtime import prepare_worker, setup_logging

    output = io.StringIO()
    try:
        config = config if isinstance(config, HeTuConfig) else HeTuConfig(config)
        # 先配日志：加载 app 时打的日志照常经进程安全队列交给管理进程输出
        setup_logging(config)
        # 加载 app 时 print 的东西（比如加载失败的提示）先截下来：成功了照常输出；失败了随出错说明交给
        # 管理进程，多个 worker 一起失败时只打一次
        with contextlib.redirect_stdout(output):
            prepare_worker(config)
    except BaseException as e:  # noqa: BLE001 什么错都要报给管理进程，包括 app 里的 sys.exit
        return _report_error(channel, e, output.getvalue())
    sys.stdout.write(output.getvalue())
    try:
        aiohttp_app.finish_prepare(config)
        return aiohttp_app.run_worker(config, sock, channel)
    except BaseException as e:  # noqa: BLE001 同上，开好服之后（关服时）出的错也要报
        return _report_error(channel, e)


# ---------------------------------------------------------------- 管理进程这一端


@dataclass(eq=False)
class _Worker:
    index: int
    process: Any  # multiprocessing 的 SpawnProcess
    # 收 worker 发来的、发给 worker 的管道。multiprocessing 的 Connection，Windows 上是 PipeConnection
    inbox: Any
    outbox: Any
    started: float
    ready: float | None = None
    # 管理进程让它停的（关服、重载）：它的退出是预料之中的
    stopping: bool = False
    lease_lost: bool = False
    ack_timed_out: bool = False
    # 读到 EOF 了（worker 正在退出）：不再等它的管道，只等进程退出
    eof: bool = False
    failure: tuple[str, str] | None = None


class _ModuleWatcher:
    """DEBUG 自动重载：轮询 worker 报上来的模块文件的修改时间，同原来 Sanic 的 reloader"""

    def __init__(self, interval: float = RELOAD_INTERVAL) -> None:
        self.interval = interval
        self._mtimes: dict[str, float] = {}
        self._next_check = 0.0

    def add(self, files: Iterable[str]) -> None:
        for path in files:
            if path not in self._mtimes:
                with contextlib.suppress(OSError):
                    self._mtimes[path] = os.stat(path).st_mtime

    def changed(self, now: float) -> list[str]:
        """到了检查的时间就查一遍，返回改过的文件"""
        if now < self._next_check:
            return []
        self._next_check = now + self.interval
        changed = []
        for path, mtime in list(self._mtimes.items()):
            try:
                current = os.stat(path).st_mtime
            except OSError:
                continue
            if current != mtime:
                self._mtimes[path] = current
                changed.append(path)
        return changed


# 开服（startup）、开好服之后（running）、自动重载中（reload）
_STARTUP, _RUNNING, _RELOAD = "startup", "running", "reload"


@dataclass
class Supervisor:
    """
    管理 N 个 worker 进程（见模块说明）。`run()` 阻塞到关服，返回退出码：0 是正常关服（包括开服途中收到
    关服信号）；1 是开服失败、某个 worker 连续失败超过上限。

    target 是 worker 进程的入口，测试里换成假的；ack_timeout、stop_timeout 是 None 时按配置推算
    （`resolve_ack_timeout`、`worker_stop_timeout`）；reload 是 None 时按 DEBUG。

    Supervises N worker processes; run() blocks until shutdown and returns the exit code.
    """

    config: Mapping[str, Any]
    sock: socket.socket | None
    count: int
    target: Callable[..., None] = worker_entry
    ack_timeout: float | None = None
    stop_timeout: float | None = None
    policy: RestartPolicy = field(default_factory=RestartPolicy)
    reload: bool | None = None
    poll_interval: float = POLL_INTERVAL

    def __post_init__(self) -> None:
        if self.ack_timeout is None:
            self.ack_timeout = resolve_ack_timeout(self.config)
        if self.stop_timeout is None:
            self.stop_timeout = worker_stop_timeout(self.config)
        if self.reload is None:
            self.reload = bool(int(self.config.get("DEBUG", 0)))
        self._ctx = multiprocessing.get_context("spawn")
        self.workers: dict[int, _Worker] = {}
        self._phase = _STARTUP
        # 各序号连续失败的次数、等着重启的序号 → 什么时候重启
        self._failures: dict[int, int] = {}
        self._restart_at: dict[int, float] = {}
        # 这一轮在等哪些序号就绪（_wait_ready），其中在就绪之前就退出了的
        self._waiting: set[int] = set()
        self._failed_before_ready: set[int] = set()
        self._startup_failure_reported = False
        self._gave_up = False
        self._stop_requested = False
        self._signalled = False
        self._watcher = _ModuleWatcher() if self.reload else None

    def request_stop(self) -> None:
        """要求关服。可以重复调用，可以从别的线程调用；主循环最多 poll_interval 秒之后开始关服"""
        self._stop_requested = True

    # ------------------------------------------------------------ 主流程

    def run(self) -> int:
        restore_signals = self._install_signals()
        try:
            return self._run()
        finally:
            # 正常情况下这时已经没有 worker 了；中途出了异常也不留下孤儿进程
            for w in list(self.workers.values()):
                w.process.kill()
                w.process.join(KILL_JOIN_TIMEOUT)
                self._forget(w)
            restore_signals()

    def _run(self) -> int:
        for index in range(self.count):
            self._spawn(index)
        status = self._wait_ready(range(self.count))
        if status == "stopped":
            logger.info(_("⌚ [🗂️Supervisor] 开服途中收到关服信号，中止开服"))
            return self._shutdown(0)
        if status == "failed":
            return self._shutdown(1)
        logger.info(
            _("✅ [🗂️Supervisor] {count} 个 worker 都已就绪").format(count=self.count)
        )
        self._phase = _RUNNING
        return self._shutdown(self._serve())

    def _install_signals(self) -> Callable[[], None]:
        """
        关服信号（SIGINT、SIGTERM，Windows 上还有 Ctrl+Break）只认第一个，之后的都不理，不会像原来 Sanic
        那样第二个信号就强杀所有进程。Docker 里本进程是 PID 1，必须显式装 SIGTERM 的处理
        """
        signals = [signal.SIGINT, signal.SIGTERM]
        if IS_WINDOWS:
            signals.append(signal.SIGBREAK)

        def on_signal(_signum: int, _frame: Any) -> None:
            self._signalled = True
            self.request_stop()

        previous = {sig: signal.signal(sig, on_signal) for sig in signals}

        def restore() -> None:
            # 收到过关服信号的继续忽略：退出前的收尾（停日志、关 socket）不该被迟到的信号打断
            for sig, handler in previous.items():
                signal.signal(sig, signal.SIG_IGN if self._signalled else handler)

        return restore

    def _serve(self) -> int:
        """开好服之后：按时重启意外退出的 worker，DEBUG 时自动重载，直到要求关服"""
        while True:
            if self._stop_requested:
                logger.info(_("⌚ [🗂️Supervisor] 收到关服信号，开始关服..."))
                return 0
            if self._gave_up:
                return 1
            now = time.monotonic()
            for index, at in sorted(self._restart_at.items()):
                if now >= at:
                    del self._restart_at[index]
                    self._spawn(index)
            self._check_ready_timeout(now)
            if self._watcher is not None and (changed := self._watcher.changed(now)):
                self._reload(changed)
                continue
            timeout = self.poll_interval
            if self._restart_at:
                timeout = min(timeout, max(0.0, min(self._restart_at.values()) - now))
            self._poll(timeout)

    def _shutdown(self, code: int) -> int:
        """
        通知所有 worker 停服，等它们退出，超时还没退出的强制结束。返回 code。

        先关掉管理进程手里的那份监听 socket：之后不会再起 worker，用不着它了。不关的话，worker 关服时各自
        停止监听（关掉自己那份）之后 socket 还在监听：新连接在内核里握完手、进了监听队列却没人接，一直挂到
        管理进程退出才被重置，网关没法马上换一台。worker 手里的照常开着，最后一个 worker 停止监听时 socket
        才真正关掉
        """
        self._restart_at.clear()
        if self.sock is not None:
            self.sock.close()
        workers = list(self.workers.values())
        if not workers:
            return code
        logger.info(
            _("⌚ [🗂️Supervisor] 通知 {count} 个 worker 关服...").format(
                count=len(workers)
            )
        )
        for w in workers:
            self._request_stop(w)
        assert self.stop_timeout is not None
        deadline = time.monotonic() + self.stop_timeout
        while self.workers:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                for w in list(self.workers.values()):
                    self._kill(w, self.stop_timeout)
                break
            self._poll(min(self.poll_interval, remaining))
        logger.info(_("👋 [🗂️Supervisor] 所有 worker 都已退出"))
        return code

    # ------------------------------------------------------------ worker 进程

    def _spawn(self, index: int) -> None:
        to_worker_r, to_worker_w = self._ctx.Pipe(duplex=False)
        from_worker_r, from_worker_w = self._ctx.Pipe(duplex=False)
        process = self._ctx.Process(
            target=self.target,
            args=(self.config, self.sock, to_worker_r, from_worker_w, self.reload),
            name=f"HeTu-Worker-{index}",
        )
        # 序号经环境变量传给 worker：spawn 启动时复制环境变量，日志的进程标识在 import 时就读取，到
        # worker 里再设就晚了。启动完马上删掉，不留在管理进程的环境里（同原来的 Sanic）
        os.environ[WORKER_INDEX_ENV] = str(index)
        try:
            process.start()
        finally:
            os.environ.pop(WORKER_INDEX_ENV, None)
            # 子进程已经拿到了自己那两端；这边关掉，worker 退出（或者管理进程没了）时对面才读得到 EOF
            to_worker_r.close()
            from_worker_w.close()
        self.workers[index] = _Worker(
            index,
            process,
            inbox=from_worker_r,
            outbox=to_worker_w,
            started=time.monotonic(),
        )
        logger.info(
            _("🚀 [🗂️Supervisor] 启动 worker {index}（pid {pid}）").format(
                index=index, pid=process.pid
            )
        )

    def _request_stop(self, w: _Worker) -> None:
        if w.stopping:
            return
        w.stopping = True
        try:
            w.outbox.send((MSG_STOP,))
        except OSError:  # 已经退出了
            pass

    def _stop_worker(self, w: _Worker) -> None:
        """让一个 worker 停服并等它退出（同时照常处理别的 worker 的消息），超时还没退出的强制结束"""
        self._request_stop(w)
        assert self.stop_timeout is not None
        deadline = time.monotonic() + self.stop_timeout
        while self.workers.get(w.index) is w:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._kill(w, self.stop_timeout)
                return
            self._poll(min(self.poll_interval, remaining))

    def _kill(self, w: _Worker, waited: float) -> None:
        logger.warning(
            _(
                "⚠️ [🗂️Supervisor] worker {index}（pid {pid}）没在 {timeout:g} 秒内退出，强制结束"
            ).format(index=w.index, pid=w.process.pid, timeout=waited)
        )
        w.process.kill()
        w.process.join(KILL_JOIN_TIMEOUT)
        self._forget(w)

    def _forget(self, w: _Worker) -> None:
        if self.workers.get(w.index) is w:
            del self.workers[w.index]
        for conn in (w.inbox, w.outbox):
            with contextlib.suppress(OSError):
                conn.close()

    # ------------------------------------------------------------ 事件

    def _poll(self, timeout: float) -> None:
        """等任一 worker 发来消息或者退出，最多 timeout 秒，处理掉"""
        waitables: dict[Any, _Worker] = {}
        for w in self.workers.values():
            if not w.eof:
                waitables[w.inbox] = w
            waitables[w.process.sentinel] = w
        if not waitables:
            time.sleep(timeout)
            return
        try:
            ready_list = multiprocessing.connection.wait(list(waitables), timeout)
        except InterruptedError:
            # Windows：主线程在等的时候按了 Ctrl+C，信号处理跑完之后 WaitForMultipleObjects 抛 EINTR，
            # 不像 POSIX 那样自动重试。当作这一轮没有事件，主循环接着看要不要关服
            ready_list = []
        for ready in ready_list:
            w = waitables[ready]
            if ready is w.inbox:
                self._receive(w)
        for w in list(self.workers.values()):
            if not w.process.is_alive():
                self._on_exit(w)

    def _receive(self, w: _Worker) -> None:
        try:
            while w.inbox.poll():
                message = w.inbox.recv()
                kind = message[0]
                if kind == MSG_READY:
                    w.ready = time.monotonic()
                    if self._watcher is not None and message[1]:
                        self._watcher.add(message[1])
                elif kind == MSG_FAILED:
                    w.failure = (message[1], message[2])
                elif kind == MSG_LEASE_LOST:
                    w.lease_lost = True
        except InterruptedError:
            return  # 同 _poll（它是 OSError 的子类，不能当成 EOF），下一轮再收
        except EOFError, OSError:
            w.eof = True

    def _on_exit(self, w: _Worker) -> None:
        # 退出之前发来的消息（开服失败的说明、租约丢失）先收完
        if not w.eof:
            self._receive(w)
        w.process.join()
        self._forget(w)
        if w.ready is not None and w.failure is not None:
            # 开好服之后出的错（关服时 runtime.stop() 出错等）：worker 自己不打，这里打
            logger.error(
                _("❌ [🗂️Supervisor] worker {index} 出错退出：\n{detail}").format(
                    index=w.index, detail=w.failure[1].rstrip()
                )
            )
        if self._stop_requested:
            # 要求关服了：关服信号也会发给 worker（终端里的 Ctrl+C 会发给整个前台进程组）
            return
        if w.ready is None and (self._phase == _STARTUP or w.index in self._waiting):
            if w.ack_timed_out:
                self._failed_before_ready.add(w.index)  # 已经报过没在规定时间内就绪了
                return
            if w.stopping and w.failure is None:
                return  # 开服途中让它停的（别的 worker 开服失败了、ack 超时了）
            # 开服失败：已经让它停了也照样记下（多半和第一个失败的 worker 同一个原因，只打一行）
            self._failed_before_ready.add(w.index)
            self._report_failure_before_ready(w)
            return
        if w.stopping:
            return  # 管理进程让它停的（自动重载）
        self._schedule_restart(w)

    def _exit_reason(self, w: _Worker) -> str:
        if w.lease_lost:
            return _("Worker ID 租约丢失")
        if w.ack_timed_out:
            return _("没在 {timeout:g} 秒内就绪").format(timeout=self.ack_timeout)
        code = w.process.exitcode
        if code is not None and code < 0:
            try:
                name = signal.Signals(-code).name
            except ValueError:
                name = str(-code)
            return _("被信号 {name} 结束").format(name=name)
        return _("退出码 {code}").format(code=code)

    def _log_failure(self, w: _Worker, level: int) -> None:
        """worker 在就绪之前退出了：打出它发来的说明，没有说明的打退出原因"""
        if w.failure is None:
            logger.log(
                level,
                _(
                    "❌ [🗂️Supervisor] worker {index} 在就绪之前退出了（{reason}）"
                ).format(index=w.index, reason=self._exit_reason(w)),
            )
            return
        kind, detail = w.failure
        if kind == FAILED_ABORTED and self._phase == _STARTUP:
            # 同单进程的说法
            logger.log(
                level,
                _("❌ 服务器启动失败，已中止，未对外提供服务：{err}").format(
                    err=detail
                ),
            )
        elif kind == FAILED_ABORTED:
            logger.log(
                level,
                _("❌ [🗂️Supervisor] worker {index} 开服失败：{err}").format(
                    index=w.index, err=detail
                ),
            )
        else:
            logger.log(
                level,
                _("❌ [🗂️Supervisor] worker {index} 开服出错：\n{detail}").format(
                    index=w.index, detail=detail.rstrip()
                ),
            )

    def _report_failure_before_ready(self, w: _Worker) -> None:
        if self._phase == _STARTUP:
            if self._startup_failure_reported:
                # 多个 worker 多半是同一个原因一起失败：说明只打第一个的
                logger.error(
                    _("❌ [🗂️Supervisor] worker {index} 也没能开服（{reason}）").format(
                        index=w.index, reason=self._exit_reason(w)
                    )
                )
                return
            self._startup_failure_reported = True
        self._log_failure(w, logging.ERROR)

    def _schedule_restart(self, w: _Worker) -> None:
        now = time.monotonic()
        if w.ready is not None and now - w.ready >= self.policy.stable_after:
            self._failures.pop(w.index, None)
        failures = self._failures.get(w.index, 0) + 1
        self._failures[w.index] = failures
        if w.ready is None and not w.ack_timed_out:
            # 重启出来的 worker 没开起服来（就绪超时的已经报过了）
            self._log_failure(w, logging.WARNING)
        reason = self._exit_reason(w)
        if failures > self.policy.max_failures:
            logger.error(
                _(
                    "❌ [🗂️Supervisor] worker {index} 连续 {count} 次意外退出（最近一次：{reason}），"
                    "不再重启，关服"
                ).format(index=w.index, count=failures, reason=reason)
            )
            self._gave_up = True
            return
        delay = self.policy.delay(failures)
        logger.warning(
            _(
                "⚠️ [🗂️Supervisor] worker {index} 意外退出（{reason}），{delay:g} 秒后以同一序号重启"
            ).format(index=w.index, reason=reason, delay=delay)
        )
        self._restart_at[w.index] = now + delay

    def _check_ready_timeout(self, now: float) -> None:
        """重启出来的 worker 没在 ack_timeout 内就绪：强制结束，算一次失败"""
        if self.ack_timeout is None:
            return
        for w in self.workers.values():
            if (
                w.ready is None
                and not w.stopping
                and not w.ack_timed_out
                and now - w.started >= self.ack_timeout
            ):
                self._kill_stuck(w)

    def _kill_stuck(self, w: _Worker) -> None:
        """没在 ack_timeout 内就绪的 worker：写明是哪个卡住了，强制结束（退出由 _poll 照常处理）"""
        logger.error(
            _(
                "❌ [🗂️Supervisor] worker {index}（pid {pid}）没在 {timeout:g} 秒内就绪，"
                "卡在开服里了（比如启动 System 里有阻塞调用），强制结束"
            ).format(index=w.index, pid=w.process.pid, timeout=self.ack_timeout)
        )
        w.ack_timed_out = True
        w.process.kill()

    def _wait_ready(self, indexes: Iterable[int]) -> str:
        """
        等这些序号的 worker 都就绪，返回 "ready"；有的在就绪之前退出了、或者超时了，返回 "failed"；
        要求关服了，返回 "stopped"
        """
        wanted = set(indexes)
        self._waiting = wanted
        self._failed_before_ready.clear()
        try:
            return self._wait_ready_loop(wanted)
        finally:
            self._waiting = set()

    def _wait_ready_loop(self, wanted: set[int]) -> str:
        deadline = (
            None if self.ack_timeout is None else time.monotonic() + self.ack_timeout
        )
        while True:
            if self._failed_before_ready & wanted:
                return "failed"
            pending = [
                w
                for w in self.workers.values()
                if w.index in wanted and w.ready is None
            ]
            if not pending:
                return "ready"
            if self._stop_requested:
                return "stopped"
            timeout = self.poll_interval
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # 卡住的 worker 多半卡在阻塞调用里（异步的卡住早就被它自己的 STARTUP_TIMEOUT 拦下
                    # 了），收不到停服通知，等也是白等：直接强制结束
                    for w in pending:
                        self._kill_stuck(w)
                    if self._phase == _STARTUP:
                        self._startup_failure_reported = True
                    return "failed"
                timeout = min(timeout, remaining)
            self._poll(timeout)

    def _reload(self, changed: list[str]) -> None:
        """
        DEBUG 自动重载：逐个重启 worker，先停旧的再起新的（SQLite 上同一个序号的两个进程并存会发出重复
        的 ID）。改坏了、新的没开起服来：停在这个序号，其余 worker 照旧，等下一次改动再试
        """
        shown = ", ".join(os.path.basename(path) for path in changed[:3])
        if len(changed) > 3:
            shown += " ..."
        logger.info(
            _("🔄 [🗂️Supervisor] 文件有改动（{files}），逐个重启 worker").format(
                files=shown
            )
        )
        self._phase = _RELOAD
        try:
            for index in range(self.count):
                if self._stop_requested:
                    return
                self._restart_at.pop(index, None)
                self._failures.pop(index, None)
                if (old := self.workers.get(index)) is not None:
                    self._stop_worker(old)
                    if self._stop_requested:
                        return
                self._spawn(index)
                status = self._wait_ready([index])
                if status == "stopped":
                    return
                if status == "failed":
                    if (new := self.workers.get(index)) is not None:
                        self._stop_worker(new)  # 就绪超时的
                    logger.error(
                        _(
                            "❌ [🗂️Supervisor] 改动后 worker {index} 没能开服，先停着，其余 worker "
                            "照旧，等下一次改动再试"
                        ).format(index=index)
                    )
                    return
        finally:
            self._phase = _RUNNING


def run(config: Mapping[str, Any], sock: socket.socket, workers: int) -> int:
    """
    在已经 bind 好的 sock 上起 workers 个 worker，阻塞到关服，返回退出码（见 `Supervisor`）。

    Run the aiohttp stack with `workers` worker processes; returns the exit code.
    """
    return Supervisor(config, sock, workers).run()
