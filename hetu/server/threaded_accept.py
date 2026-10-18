"""
把监听 socket 的 accept 挪出事件循环线程（根治 Windows 多 worker 的 accept 死锁）
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

## 病因

Sanic 在 Windows 多 worker 时把同一个监听 socket 复制进每个 worker（socket.share /
fromshare），各 worker 用 selector 事件循环 select() + accept()。Windows 共享 socket 上的
操作协调由涉及的进程自己负责（MSDN 对 WSADuplicateSocket 的说明），抢输的 worker 会真的
堵死在 accept() 系统调用里：卡死现场 Python 层 gettimeout() 是 0.0，内核层却是阻塞模式
（往监听端口连喂 backlog 次才能把 asyncio 的 `for _ in range(backlog + 1)` 循环喂出来，
说明每次 accept 都在阻塞）。accept 跑在事件循环线程上，于是整个 worker 停摆——心跳、
WorkerKeeper 续约、FutureCalls、该 worker 上所有已建立的连接全部冻结，直到下一个连接恰好
被它抢到才解开。症状极具迷惑性：该 worker 只是"不再打日志了"，进程还活着，其他 worker
一切正常。

排查时已经排除的原因（别再走一遍）：

* Python 层 socket 不是阻塞模式：create_server 确实调了 setblocking(False)，卡死现场
  gettimeout() 仍是 0.0
* socket.fromshare() 把共享 socket 重置回阻塞：实测无影响，FIONBIO 在 Windows 上是每描述符的
* 主进程 spawn 新 worker 时 multiprocessing 的 sock.dup() → settimeout(None) 污染了别的
  描述符：实测不影响
* 客户端在 accept 前 RST：8 线程 × 150 次风暴不触发

为什么不干脆换成 IOCP(Proactor)：Proactor 用 AcceptEx 重叠 I/O，根本没有阻塞 accept，
Sanic 也跑得起来，但实测 200 个并发连接会全部落到同一个 worker，多进程等于废掉。Windows
又没有 SO_REUSEPORT，没法一个 worker 一个监听 socket。

## 修法

不去猜内核为什么会变回阻塞，而是让阻塞无害：accept 改到每个监听 socket 一个的专用线程里
做（select + accept），接到的连接用 call_soon_threadsafe 交回事件循环，走 asyncio 原版的
_accept_connection2 建 transport。accept 就算真堵住，堵的也只是这个线程，事件循环照转；
线程堵在 accept 里时，下一个连接进来就会被它接走，等于退化成阻塞 accept 模型，同样正确。

注入点是 asyncio 而不是 Sanic：只在 worker 自己的事件循环实例上替换 _start_serving /
_stop_serving 两个方法（在 before_server_start 里装，Sanic 的约定是此时还没开始监听）。
Sanic 的代码一行不动，升级 Sanic 不受影响。依赖的是 CPython selector 事件循环的三个私有
方法，签名由 tests/test_threaded_accept.py 守门，CPython 改了会在 CI 上直接红。

Move accept() of listening sockets off the event loop thread. On Windows, Sanic shares one
listening socket between workers, and a worker that loses the select()/accept() race can
block inside accept() for real, freezing its whole event loop. Accepting in a dedicated
thread makes that block harmless. Only the worker's own loop instance is patched; Sanic's
code is untouched.
"""

import asyncio
import errno
import logging
import os
import select
import socket
import sys
import threading
from asyncio import constants, selector_events

from ..i18n import _

logger = logging.getLogger("HeTu.root")

IS_WINDOWS = sys.platform == "win32"
# accept 线程 select 的超时。有连接来 select 立刻返回，所以它不影响接连接的延迟，
# 只决定停服后线程最迟多久发现该退出了
POLL_INTERVAL = 0.5
# 文件描述符用光之类的资源错误时，暂停多久再接，和 asyncio 原版一致
ACCEPT_RETRY_DELAY = constants.ACCEPT_RETRY_DELAY
_RESOURCE_ERRNOS = (errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM)
# 装过补丁的事件循环打这个标记，防止重复安装
_INSTALLED_FLAG = "_hetu_threaded_accept"


class _AcceptThread:
    """一个监听 socket 的 accept 线程。

    线程里只做 select + accept + setblocking(False)，其余（建 protocol、transport、
    SSL 握手、挂到 Server 上计数）全部交回事件循环线程，由 asyncio 原版
    _accept_connection2 完成，行为和 selector 事件循环自己 accept 时一致。
    """

    def __init__(
        self,
        loop: selector_events.BaseSelectorEventLoop,
        protocol_factory,
        sock,
        sslcontext,
        server,
        ssl_handshake_timeout,
        ssl_shutdown_timeout,
    ):
        self._loop = loop
        self._protocol_factory = protocol_factory
        self._sock = sock
        self._sslcontext = sslcontext
        self._server = server
        self._ssl_handshake_timeout = ssl_handshake_timeout
        self._ssl_shutdown_timeout = ssl_shutdown_timeout
        self._stopped = threading.Event()
        self.thread = threading.Thread(
            target=self._run, name="HeTuAcceptThread", daemon=True
        )

    def start(self):
        self.thread.start()

    def stop(self):
        """只打标记。监听 socket 由 _stop_serving 随后关掉，线程最迟 POLL_INTERVAL 后退出"""
        self._stopped.set()

    def _closed(self) -> bool:
        return self._stopped.is_set() or self._sock.fileno() == -1

    def _run(self):
        sock = self._sock
        while not self._stopped.is_set():
            try:
                readable, _w, _x = select.select([sock], [], [], POLL_INTERVAL)
            except (OSError, ValueError) as exc:
                if self._closed():  # 监听 socket 被关了，正常退出
                    return
                self._report(_("accept 线程 select() 出错"), exc)
                self._stopped.wait(ACCEPT_RETRY_DELAY)
                continue
            if not readable or self._stopped.is_set():
                continue

            try:
                conn, addr = sock.accept()
            except BlockingIOError, InterruptedError, ConnectionError:
                # 被别的 worker 抢走了，或对端在 accept 前就断了
                continue
            except OSError as exc:
                if self._closed():
                    return
                self._report(_("socket.accept() 出错"), exc)
                if exc.errno in _RESOURCE_ERRNOS:
                    self._stopped.wait(ACCEPT_RETRY_DELAY)
                continue

            try:
                conn.setblocking(False)
                self._loop.call_soon_threadsafe(self._deliver, conn, addr)
            except RuntimeError:  # 事件循环已关闭
                conn.close()
                return
            except OSError:
                conn.close()

    def _deliver(self, conn, addr):
        """事件循环线程上执行：把接到的连接交给 asyncio 原版流程建 transport"""
        if self._stopped.is_set():
            conn.close()
            return
        self._loop.create_task(
            self._loop._accept_connection2(  # type: ignore
                self._protocol_factory,
                conn,
                {"peername": addr},
                self._sslcontext,
                self._server,
                self._ssl_handshake_timeout,
                self._ssl_shutdown_timeout,
            )
        )

    def _report(self, message: str, exc: BaseException):
        # 走事件循环的异常处理器，和 asyncio 原版 accept 出错时的去向一样
        context = {"message": message, "exception": exc, "socket": self._sock}
        try:
            self._loop.call_soon_threadsafe(self._loop.call_exception_handler, context)
        except RuntimeError:
            pass


def install_threaded_accept(loop: asyncio.AbstractEventLoop) -> bool:
    """给 selector 事件循环实例装上"accept 在专用线程里做"的补丁，返回是否装上。

    必须在该事件循环开始监听之前调用（Sanic 里就是 before_server_start）。不是 selector
    事件循环（Proactor、uvloop 等）就不装，返回 False。只改这一个 loop 实例，不影响别的
    loop，也不改 Sanic 的代码。

    Patch a selector event loop instance so that listening sockets are accepted in a
    dedicated thread instead of on the loop thread. Must be called before the loop starts
    serving. Returns False (and does nothing) for non-selector loops.
    """
    if not isinstance(loop, selector_events.BaseSelectorEventLoop):
        return False
    if getattr(loop, _INSTALLED_FLAG, False):
        return True

    # 以 socket 对象本身为键（按身份哈希），它在 serving 期间本来就活着
    threads: dict[socket.socket, _AcceptThread] = {}
    original_stop_serving = loop._stop_serving  # type: ignore

    def _start_serving(
        protocol_factory,
        sock,
        sslcontext=None,
        server=None,
        backlog=100,
        ssl_handshake_timeout=constants.SSL_HANDSHAKE_TIMEOUT,
        ssl_shutdown_timeout=constants.SSL_SHUTDOWN_TIMEOUT,
    ):
        # backlog 原版用来限制单次 accept 循环的次数，线程里每次 select 只接一个，用不上
        del backlog
        old = threads.pop(sock, None)
        if old is not None:
            old.stop()
        worker = _AcceptThread(
            loop,
            protocol_factory,
            sock,
            sslcontext,
            server,
            ssl_handshake_timeout,
            ssl_shutdown_timeout,
        )
        threads[sock] = worker
        worker.start()

    def _stop_serving(sock):
        worker = threads.pop(sock, None)
        if worker is not None:
            worker.stop()
        # 原版：移除 reader（这里没注册过，空操作）+ 关 socket
        original_stop_serving(sock)

    loop._start_serving = _start_serving  # type: ignore
    loop._stop_serving = _stop_serving  # type: ignore
    setattr(loop, _INSTALLED_FLAG, True)
    return True


async def threaded_accept_listener(_app):
    """Sanic before_server_start 监听器：给本 worker 的事件循环装 accept 线程补丁。

    worker_main 只在 Windows 上注册它。装不上（事件循环不是 selector 的）就保持原样，
    只打一条警告。

    Sanic before_server_start listener that installs the threaded accept patch on this
    worker's event loop (registered on Windows only). Leaves the loop untouched and
    logs a warning if it is not a selector loop.
    """
    loop = asyncio.get_running_loop()
    if install_threaded_accept(loop):
        logger.info(
            _("🧵 进程[{pid}] 监听socket改由accept线程接连接").format(pid=os.getpid())
        )
    else:
        logger.warning(
            _(
                "⚠️ 事件循环 {loop} 不是 selector 事件循环，没装上 accept 线程补丁，"
                "Windows 多 worker 下如遇 worker 卡住不响应，请改用 WORKER_NUM: 1"
            ).format(loop=type(loop).__name__)
        )
