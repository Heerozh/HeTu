"""
事件循环卡死看门狗
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import faulthandler
import logging
import os
import socket
import sys
import threading
import time
import traceback

from ..i18n import _

logger = logging.getLogger("HeTu.root")

# 判定卡死的秒数配置键，0为关闭。多worker时某个worker静默卡住只表现为"它不再打日志了"，
# 光看日志无法区分是事件循环被同步代码堵死、协程等一个永不返回的await、还是日志本身没出来。
HANG_WATCHDOG_CONFIG_KEY = "HANG_WATCHDOG_TIMEOUT"
# 默认关闭：卡死本身是异常情况，没必要让每个worker常年多一个线程+每秒一次心跳，
# 需要排查时再打开。唯一的例外是 Windows，见下。
DEFAULT_HANG_TIMEOUT = 0.0
# Windows 上强制开启：那里有 Sanic 共享监听socket 引起的 accept 死锁（见
# _heal_accept_deadlock），撞上就是worker静默失联，用户看不出任何异常、日志也没有一个字，
# 而看门狗既是唯一的诊断手段也是唯一的自愈手段。所以配置关不掉它，只能调大阈值。
IS_WINDOWS = sys.platform == "win32"
WINDOWS_HANG_TIMEOUT = 10.0
# 心跳/检查间隔上限。小于它的timeout会自动按timeout/4取更细的间隔，否则检查粒度会盖过阈值
MAX_HEARTBEAT_INTERVAL = 1.0
# 单个协程最多打印的栈帧数、最多打印的协程数，防止高并发时dump出几百MB
MAX_TASK_STACK_DEPTH = 12
MAX_TASKS = 200
DUMP_DIR = "logs"

# 自愈时最多发多少次连接。asyncio 的 _accept_connection 循环体是
# `for _ in range(backlog + 1)`，每返回一次就立刻发起下一次 accept 并再次阻塞，所以最坏
# 要把整个循环喂满（backlog=100 实测正好要 100 次、1.5 秒）；多worker时还有一部分连接会被
# 健康的 worker 抢走，所以在 backlog 基础上留倍数余量，同时设硬上限防止失控狂连。
KICK_ROUNDS_PER_BACKLOG = 3
MAX_KICKS = 512
KICK_INTERVAL = 0.005
KICK_TIMEOUT = 2.0


class LoopWatchdog:
    """独立线程盯着事件循环的心跳，超时未更新就把所有线程栈+协程栈dump到文件。

    卡死现场只有两个信息源能区分病因：所有**线程**的栈（同步代码把loop堵死时，主线程
    正停在那一行）和所有**协程**的栈（loop还活着但任务都在等某个await时，能看到等在哪）。
    两个都dump，就不用再猜了。

    为什么dump全程不碰 logging：卡死时 logging 自己可能就是凶手（handler锁被卡住的主线程
    持有、日志队列满、Windows控制台QuickEdit把写stdout堵住），从看门狗线程调logger会把
    看门狗一起锁死。所以先直接写文件落盘，再往stderr写一行指路。唯一的例外见
    _heal_accept_deadlock：那个场景已经确定主线程卡在accept系统调用里，和logging无关。

    A watchdog thread that dumps every thread stack (via faulthandler) and every pending
    asyncio task stack once the event loop stops heartbeating, so an intermittent worker
    hang diagnoses itself. It deliberately avoids the logging module, which may itself be
    part of the deadlock. It also auto-heals the Windows shared-listening-socket accept()
    deadlock described in _heal_accept_deadlock.
    """

    def __init__(
        self,
        timeout: float,
        dump_dir: str = DUMP_DIR,
        autoheal: bool | None = None,
    ):
        self.timeout = timeout
        # 心跳和检查用同一间隔：太粗会漏报（阈值内根本没醒过），太细纯属浪费
        self.interval = min(MAX_HEARTBEAT_INTERVAL, max(timeout / 4, 0.01))
        self.dump_dir = dump_dir
        # 自愈针对的是 Windows 独有的 accept 死锁，别的平台开了也永远不会触发，
        # 徒增"看门狗会主动往自己端口建连"的意外，所以按平台自动决定，不给配置项。
        # 显式传参只为测试能在任何平台上验证自愈逻辑。
        self.autoheal = IS_WINDOWS if autoheal is None else autoheal
        self.dump_file = os.path.join(dump_dir, f"hang_{os.getpid()}.log")
        self._last_beat = time.monotonic()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread_id: int | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        # 下面3个只由看门狗线程读写，beat()只写_last_beat，避免跨线程状态纠缠
        self._reported = False
        self._next_report = timeout
        self._peak_stall = 0.0

    def beat(self):
        """由事件循环上的心跳task调用，证明loop还在转"""
        self._last_beat = time.monotonic()

    def start(self):
        """必须在事件循环所在的线程里调用（要记录loop和它的线程id）"""
        self._loop = asyncio.get_running_loop()
        self._loop_thread_id = threading.get_ident()
        self.beat()
        self._thread = threading.Thread(
            target=self._watch, name="HeTuHangWatchdog", daemon=True
        )
        self._thread.start()

    def stop(self):
        self._stop_event.set()

    def _watch(self):
        while not self._stop_event.wait(self.interval):
            stalled = time.monotonic() - self._last_beat
            if stalled < self.timeout:
                if self._reported:
                    self._report_recovered()
                continue

            self._peak_stall = stalled
            # 真死锁时不要每秒刷屏，报一次后阈值翻倍
            if stalled < self._next_report:
                continue
            self._next_report = stalled * 2
            self._reported = True
            try:
                self._dump(stalled)
            except Exception:  # 看门狗自己绝不能把worker搞崩
                traceback.print_exc(file=sys.stderr)
            if self.autoheal:
                try:
                    self._try_heal()
                except Exception:
                    traceback.print_exc(file=sys.stderr)

    def _report_recovered(self):
        self._reported = False
        self._next_report = self.timeout
        msg = _(
            "⏱️ [看门狗] 进程[{pid}] 事件循环已恢复，本次共卡住约 {stalled:.1f} 秒\n"
        ).format(pid=os.getpid(), stalled=self._peak_stall)
        self._peak_stall = 0.0
        self._append_dump_file(msg)
        _write_stderr(msg)

    # ------------------------------------------------------------ accept 自愈

    def _accept_frame(self):
        """事件循环线程是不是正卡在 asyncio 的 _accept_connection 里？

        是的话返回那一帧——帧上的局部变量 sock/backlog 正好是自愈需要的两个参数。
        用 sys._current_frames() 而不是解析 faulthandler 的文本，因为要拿到活的对象。
        """
        tid = self._loop_thread_id
        if tid is None:
            return None
        frame = sys._current_frames().get(tid)
        while frame is not None:
            code = frame.f_code
            if code.co_name == "_accept_connection" and code.co_filename.endswith(
                "selector_events.py"
            ):
                return frame
            frame = frame.f_back
        return None

    def _try_heal(self):
        frame = self._accept_frame()
        if frame is None:
            return
        healed = self._heal_accept_deadlock(frame)
        msg = ACCEPT_DEADLOCK_HEALED if healed else ACCEPT_DEADLOCK_FAILED
        self._append_dump_file(msg + "\n")
        _write_stderr(msg + "\n")
        # 这里可以安全地走logging：本函数只在"主线程卡在accept系统调用"时才会执行，那个
        # 位置和logging毫无关系（不可能持有handler锁），不存在把看门狗一起锁死的风险。
        # 而用户平时只看日志、不会去翻 logs/hang_<pid>.log，所以这条必须走正常日志管道。
        try:
            logger.error(msg)
        except Exception:
            traceback.print_exc(file=sys.stderr)

    def _heal_accept_deadlock(self, frame) -> bool:
        """给卡死的 accept() 喂连接，把事件循环捅醒。返回是否救活。

        ## 来龙去脉（Windows + 多worker 专属，2026-09 定位）

        Linux 上多进程服务器的标准做法是：主进程 bind+listen 一个 socket，fork 出
        worker，大家共享同一个 fd，各自 epoll + 非阻塞 accept。抢输的 worker 拿到
        EAGAIN —— 这是 POSIX 保证的，所以 asyncio 的 _accept_connection 里直接
        `except BlockingIOError: return` 就够了。

        Windows 没有 fork，Sanic 改用 WSADuplicateSocket 把同一个监听 socket 复制进每个
        worker（sanic/server/runners.py 里的 sock.share() / socket.fromshare()），其余
        照搬 Linux 那套，并强制 WindowsSelectorEventLoopPolicy。但 MSDN 对
        WSADuplicateSocket 写得很清楚：**Windows Sockets 接口不实现任何形式的访问控制，
        共享 socket 上的操作协调由涉及的进程自己负责**。多个进程同时在一个共享监听 socket
        上 select() + accept()，正是"不做协调"的用法：select 说可读并不保证 accept 不会
        阻塞。于是抢输的 worker 会真的堵死在 accept() 系统调用里，整个事件循环停摆——心跳、
        WorkerKeeper续约、FutureCalls、该worker上所有已建立的客户端连接全部冻结，直到下一个
        连接恰好被它抢到才解开。

        症状极具迷惑性：该 worker 只是"不再打日志了"，进程还活着，其他 worker 一切正常。

        ## 为什么是"发连接"而不是别的

        主线程已经陷在内核的 accept 里，从外部改 socket 属性（setblocking 之类）没法让一个
        已经发出去的阻塞调用返回。唯一能让它返回的就是真的来一个连接。所以这里连回自己的
        监听端口，用一次性的废连接把它喂出来。

        必须喂满整个循环：_accept_connection 的循环体是 `for _ in range(backlog + 1)`，
        每返回一次就立刻发起下一次 accept 并再次阻塞，所以最坏要喂 backlog+1 次它才会
        return（backlog=100 实测正好 100 次、1.5 秒）。见 KICK_ROUNDS_PER_BACKLOG。

        ## 排查时已经排除的原因（别再走一遍）

        * Python 层 socket 不是阻塞模式：create_server 确实调了 setblocking(False)，
          卡死现场 gettimeout() 仍是 0.0
        * socket.fromshare() 把共享 socket 重置回阻塞：实测无影响，FIONBIO 在 Windows 上
          是每描述符的
        * 主进程 spawn 新 worker 时 multiprocessing 的 sock.dup() → settimeout(None)
          污染了别的描述符：实测不影响
        * 客户端在 accept 前 RST：8线程 × 150 次风暴不触发

        ## 为什么不干脆换成 IOCP(Proactor)

        Proactor 用 AcceptEx 重叠 I/O，根本没有阻塞 accept，Sanic 也确实跑得起来（Sanic 和
        HeTu 都没用 loop.add_reader）。但实测 200 个并发连接会全部落到同一个 worker，多进程
        等于废掉。Windows 又没有 SO_REUSEPORT，没法一个 worker 一个监听 socket。所以没有便宜
        的正确解，只能在这兜底：生产环境请跑 Linux。

        Heal the Windows-only shared-listening-socket accept() deadlock by feeding the
        blocked accept() throwaway local connections until the event loop escapes asyncio's
        `for _ in range(backlog + 1)` accept loop.
        """
        try:
            sock = frame.f_locals.get("sock")
            backlog = int(frame.f_locals.get("backlog") or 100)
        except Exception:  # f_locals 取不到就放弃自愈，绝不能抛出去
            return False
        addr = _kick_address(sock) if sock is not None else None
        if addr is None:
            return False

        max_kicks = min((backlog + 1) * KICK_ROUNDS_PER_BACKLOG, MAX_KICKS)
        kicks = 0
        while kicks < max_kicks and self._accept_frame() is not None:
            try:
                with socket.create_connection(addr, timeout=KICK_TIMEOUT):
                    pass  # 连上立刻关，只为把阻塞的 accept() 喂出来
            except OSError:
                pass  # 连不上就下一轮，自愈本身不能抛异常
            kicks += 1
            time.sleep(KICK_INTERVAL)
        return self._accept_frame() is None

    # ---------------------------------------------------------------- dump

    def _append_dump_file(self, text: str):
        try:
            os.makedirs(self.dump_dir, exist_ok=True)
            with open(self.dump_file, "a", encoding="utf-8") as f:
                f.write(text)
        except OSError:
            pass

    def _dump(self, stalled: float):
        os.makedirs(self.dump_dir, exist_ok=True)
        # 追加模式：faulthandler直接写fd，绕过Python的缓冲，追加模式下才能保证顺序不乱
        with open(self.dump_file, "a", encoding="utf-8") as f:
            f.write("\n" + "=" * 78 + "\n")
            f.write(
                _(
                    "⛔ [看门狗] 进程[{pid}] 事件循环已卡住 {stalled:.1f} 秒（{when}）\n"
                ).format(
                    pid=os.getpid(),
                    stalled=stalled,
                    when=time.strftime("%Y-%m-%d %H:%M:%S"),
                )
            )
            f.write("=" * 78 + "\n")
            f.write(_("--- 所有线程的栈（同步代码堵死loop的话，主线程就停在这）---\n"))
            f.flush()
            faulthandler.dump_traceback(file=f, all_threads=True)
            f.write(_("\n--- 未完成的协程任务（loop还活着的话，看它们等在哪）---\n"))
            self._dump_tasks(f)

        _write_stderr(
            _(
                "⛔ [看门狗] 进程[{pid}] 事件循环已卡住 {stalled:.1f} 秒，"
                "栈已dump到 {path}\n"
            ).format(pid=os.getpid(), stalled=stalled, path=self.dump_file)
        )

    def _dump_tasks(self, f):
        loop = self._loop
        if loop is None:
            return
        try:
            tasks = list(asyncio.all_tasks(loop))
        except RuntimeError:  # 极小概率撞上task集合被并发修改
            tasks = []
        f.write(_("未完成任务数：{count}\n").format(count=len(tasks)))
        for task in tasks[:MAX_TASKS]:
            f.write(f"\n{task!r}\n")
            try:
                task.print_stack(limit=MAX_TASK_STACK_DEPTH, file=f)
            except Exception as e:  # 单个task取栈失败不能中断整个dump
                f.write(f"  <{type(e).__name__}:{e}>\n")
        if len(tasks) > MAX_TASKS:
            f.write(
                _("...省略剩余 {count} 个任务\n").format(count=len(tasks) - MAX_TASKS)
            )


# 自愈成功/失败时告知用户的话。写成模块级常量，方便测试直接引用。
ACCEPT_DEADLOCK_HEALED = _(
    "❌ [看门狗] Sanic在Windows上的BUG, 它用Linux的Socket模型直接套用Windows，"
    "而Windows 共享 socket 上的操作协调由涉及的进程自己负责。"
    "已自动帮你自愈，生产环境请跑在Linux上"
)
ACCEPT_DEADLOCK_FAILED = _(
    "❌ [看门狗] Sanic在Windows上的BUG, 它用Linux的Socket模型直接套用Windows，"
    "而Windows 共享 socket 上的操作协调由涉及的进程自己负责。"
    "自愈失败，此worker会一直无响应到有新连接被它抢到为止，"
    "请改用 WORKER_NUM: 1，生产环境请跑在Linux上"
)


def _kick_address(sock) -> tuple | None:
    """把监听地址翻译成一个能从本机连回去的地址"""
    try:
        name = sock.getsockname()
    except OSError:
        return None
    if not isinstance(name, tuple) or len(name) < 2:
        return None  # unix socket 之类，没法用TCP捅
    host, port = name[0], name[1]
    if host in ("0.0.0.0", ""):  # 通配地址换成回环地址才能连回自己
        host = "127.0.0.1"
    elif host in ("::", "::0"):
        host = "::1"
    return host, port


def _write_stderr(msg: str):
    # spawn出的子进程可能压根没有stderr（pythonw/服务方式启动），所以要能吞掉异常
    try:
        if sys.stderr is not None:
            sys.stderr.write(msg)
            sys.stderr.flush()
    except Exception:
        pass


def resolve_hang_timeout(config) -> float:
    """按 HANG_WATCHDOG_TIMEOUT 算出卡死判定秒数，0为关闭。

    Windows 上不允许关闭：那里的 accept 死锁（见 LoopWatchdog._heal_accept_deadlock）
    会让 worker 静默失联，没有看门狗就既查不出也救不回来，所以配置成0或没配置都按
    WINDOWS_HANG_TIMEOUT 处理；想调松紧配一个更大的秒数即可。

    Resolve the hang threshold in seconds (0 disables). Windows can't disable it: the
    Sanic shared-socket accept() deadlock there is silent, and the watchdog is both the
    only diagnosis and the only cure.
    """
    timeout = float(config.get(HANG_WATCHDOG_CONFIG_KEY, DEFAULT_HANG_TIMEOUT) or 0)
    if IS_WINDOWS and timeout <= 0:
        return WINDOWS_HANG_TIMEOUT
    return timeout


async def hang_watchdog_task(app):
    """worker的事件循环卡死检测task，每个Worker启动时会开一个。

    卡死判定秒数见 resolve_hang_timeout；accept死锁自愈在 Windows 上自动开启，
    不占配置项。详见 LoopWatchdog。
    """
    timeout = resolve_hang_timeout(app.config)
    if timeout <= 0:
        return

    watchdog = LoopWatchdog(timeout)
    watchdog.start()
    try:
        while True:
            watchdog.beat()
            await asyncio.sleep(watchdog.interval)
    except asyncio.CancelledError:
        pass
    finally:
        watchdog.stop()
