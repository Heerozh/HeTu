import contextlib
import itertools
import logging
import os
import secrets
import sys
import uuid

from ..i18n import _

logger = logging.getLogger("HeTu.root")

# 本进程在本机内的 worker 序号（从 0 开始），由多进程管理进程在启动 worker 时设置（设计稿
# docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §5.7）。单进程模式、管理进程、
# 工具进程都没有
WORKER_INDEX_ENV = "HETU_WORKER_INDEX"

# 本节点 token 的文件（见 node_token）。容器里它在容器自己的可写层里：同一个容器重启后还在，别的容器
# （哪怕同一个镜像、同一台宿主机、同一个主机名）没有它
NODE_TOKEN_PATH = "/var/tmp/hetu-node-token"
# 本进程读到（或生成）的 token，第一次用到时才读
_node_token: str | None = None
# 租约值里持有者标识和本进程随机部分之间的分隔符（标识里只有冒号，见 lease_owner_id）
LEASE_NONCE_SEP = "#"

# OpenProcess 查询进程状态所需的最小权限
_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
# OpenProcess 查无此 pid 时的错误码
_ERROR_INVALID_PARAMETER = 87


def batched(iterable, n):
    """Batch data into tuples of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


def is_container_env():
    """
    判断当前是否在容器环境 (Docker, Kubernetes, etc.)
    """
    # 1. 检查 /.dockerenv 文件 (Docker 标准标志)
    if os.path.exists("/.dockerenv"):
        return True

    # 2. 检查 /run/.containerenv (Podman 等其他容器运行时)
    if os.path.exists("/run/.containerenv"):
        return True

    # 3. 检查 cgroup 信息 (更通用的检测方式)
    try:
        if os.path.exists("/proc/1/cgroup"):
            with open("/proc/1/cgroup", "rt") as f:
                content = f.read()
                # 检查关键词
                if (
                    "docker" in content
                    or "kubepods" in content
                    or "containerd" in content
                ):
                    return True
    except Exception:  # noqa: BLE001, S110 读不了 cgroup 就当不在容器里
        pass

    return False


def enable_windows_console_ansi() -> None:
    """
    在 Windows 控制台打开 ANSI 转义支持，彩色日志和启动 LOGO 才不会显示成裸转义码。移植自 Sanic 的
    `enable_windows_color_support`（MIT License），原来 `import sanic.app` 时会顺带调用，现在由
    hetu start 开服前调。其他平台什么也不做。

    Enable ANSI escape sequences on the Windows console; a no-op elsewhere.
    """
    if sys.platform != "win32":
        return
    import ctypes

    kernel = ctypes.windll.kernel32
    kernel.SetConsoleMode(kernel.GetStdHandle(-11), 7)


def worker_index() -> int | None:
    """
    本进程在本机内的 worker 序号，读多进程管理进程给每个 worker 设的 HETU_WORKER_INDEX；单进程模式、
    管理进程、工具进程没有，返回 None。

    This process's worker index on this machine, or None outside a multi-worker server.
    """
    # 空串当作没设
    if value := os.environ.get(WORKER_INDEX_ENV):
        if not value.isdecimal():
            raise ValueError(
                _("环境变量 {name} 必须是非负整数，现在是 {value!r}").format(
                    name=WORKER_INDEX_ENV, value=value
                )
            )
        return int(value)
    return None


def worker_ident() -> str:
    """日志里的进程标识：worker 是 "Srv 0"、"Srv12" 这样（两位宽，和原来 Sanic 的一样），其他进程是 "Main "。"""
    index = worker_index()
    return "Main " if index is None else f"Srv{index:2}"


def get_machine_id():
    """
    获取机器ID：
    - 容器环境：使用 /etc/hostname
    - 非容器环境：使用 uuid.getnode()
    """
    if is_container_env():
        try:
            # 尝试读取 /etc/hostname
            with open("/etc/hostname", "r") as f:
                # 读取内容并去除换行符
                machine_id = f.read().strip()
                return machine_id
        except Exception:  # noqa: BLE001
            # 如果文件不存在或无法读取（极少见），回退到 socket 获取
            import socket

            return socket.gethostname()
    else:
        # 非容器环境，使用 MAC 地址生成的 UUID
        node_id = uuid.getnode()
        # uuid.getnode() 返回的是十进制整数，通常转换为16进制字符串更像 ID
        return hex(node_id)[2:]


def _read_token(path: str) -> str | None:
    try:
        with open(path, encoding="ascii") as f:
            return f.read().strip() or None
    except OSError, UnicodeDecodeError:
        return None


def read_or_create_node_token(path: str) -> str | None:
    """
    读 path 里的 token；没有就随机生成一个写进去。读写都不行（只读文件系统等）返回 None。

    同一个容器里的几个 worker 会同时第一次启动：各自先写自己的临时文件，再用硬链接放到 path 上。
    硬链接是原子的，path 已经存在就失败，于是只有一个写成，其余的读它的；path 一出现就是写完了的，
    不会读到写了一半的内容。
    """
    if (token := _read_token(path)) is not None:
        return token
    token = secrets.token_hex(8)
    tmp = f"{path}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w", encoding="ascii") as f:
            f.write(token)
        try:
            os.link(tmp, path)
        except FileExistsError:
            return _read_token(path)
        return token
    except OSError:
        return None
    finally:
        with contextlib.suppress(OSError):
            os.unlink(tmp)


def node_token() -> str | None:
    """
    本节点的 token，租约持有者标识的一部分（见 `lease_owner_id`）。第一次用到时读 NODE_TOKEN_PATH，
    没有就随机生成、写进去：在容器里就是"每个容器首次启动时持久化一个自己的 token"，同一个容器重启后
    不变。Windows 上不用（没有容器共用主机名的问题），返回 None。

    不按 `is_container_env` 判断要不要用：cgroup v2 下 /proc/1/cgroup 只剩 "0::/"，没有 /.dockerenv
    的容器（比如 k8s 的 containerd）认不出来，而 k8s 的 hostNetwork 正好会让多个 pod 共用主机名。
    不在容器里时，同一台机器上的进程读到的是同一个 token，靠 pid 区分，不受影响。

    写不进去（只读文件系统等）时用一个只在本进程有效的随机 token：照样和别的容器区分得开，只是重启后
    不能复用原来的 Worker ID。从跑过 HeTu 的容器 commit 镜像之前要删掉这个文件，不然从这个镜像起的
    容器都带着同一个 token。

    This node's token, persisted on first use (per container when containerised).
    """
    global _node_token
    if sys.platform == "win32":
        return None
    if _node_token is None:
        token = read_or_create_node_token(NODE_TOKEN_PATH)
        if token is None:
            token = secrets.token_hex(8)
            logger.info(
                _(
                    "[❄️ID] 无法把节点 token 存进 {path}（只读文件系统？），改用只在本进程有效的，"
                    "重启后不会复用原来的 Worker ID"
                ).format(path=NODE_TOKEN_PATH)
            )
        _node_token = token
    return _node_token


def lease_owner_id(pid: int, *, tool: bool = False) -> str:
    """
    Worker ID 租约的持有者标识，由冒号连接：`机器码[:节点 token]:进程部分`，工具进程再带 `cli:` 前缀。
    租约里存的是它加上本进程随机生成的部分（`lease_value`），续约、释放按整个值比对；开服时按标识认出
    "自己重启前留下的"租约，确认没人在续约了才接手（见 RedisWorkerKeeper 的"复用"）。所以要满足：

    1. **同时活着的进程之间尽量不同**。相同的话，后启动的每次开服都要多等几秒、确认对方还在续约后另拿
       一个；对方要是正好整个卡住了，还会被当成死的接手。host 网络下同一台宿主机上的容器主机名都和宿主机
       一样、pid 都是 1（实测），多个副本设了同一个 hostname、k8s 的 hostNetwork 也一样，原来的
       `主机名:pid` 就会撞。节点 token 每个容器各一个（`node_token`），把它们分开；
    2. **同一个位置重启前后尽量不变**，才能接回原来的 Worker ID（接不回也不影响正确性，只是旧的要等
       过期）。

    进程部分：
    - HeTu 的多进程管理器起的 worker（有 HETU_WORKER_INDEX）：`w<序号>:<管理进程 pid>`。worker 的 pid
      每次重启都变，序号和管理进程不变（容器里管理进程是 pid 1），管理器保证同一序号的旧进程退出之后
      才起新的。裸机上重启了 hetu start，管理进程的 pid 变了，不会和还在退出中的旧 worker 撞；
    - 其他（单进程、工具进程）：`<pid>`。

    最后一段总是一个 pid（管理进程的或本进程的），Windows 上 `lease_owner_exited` 据此判断持有者还在
    不在。

    The lease owner id: unique among live processes, stable across restarts where possible.
    """
    from .snowflake_id import TOOL_NODE_PREFIX

    parts = [get_machine_id()]
    if (token := node_token()) is not None:
        parts.append(token)
    index = None if tool else worker_index()
    if index is not None:
        parts += [f"w{index}", str(os.getppid())]  # 这个是管理进程pid
    else:
        parts.append(str(pid))
    owner = ":".join(parts)
    return TOOL_NODE_PREFIX + owner if tool else owner


def lease_value(identity: str) -> str:
    """
    租约里存的值：持有者标识 + "#" + 本进程启动时随机生成、不落盘的部分。

    续约、释放都按整个值比对：标识就算撞了，换一个进程随机部分也不同，比对不上。只存标识的话，本进程
    卡住超过租约的 60 秒、key 过期后被标识相同的另一个进程拿走，醒来续约照样比对得上，两边用同一个
    Worker ID。认"自己重启前留下的"只看标识（`lease_identity`），见 RedisWorkerKeeper.get_worker_id。

    The stored lease value: owner id plus a random per-process part.
    """
    return f"{identity}{LEASE_NONCE_SEP}{secrets.token_hex(4)}"


def lease_identity(value: str) -> str:
    """租约值里的持有者标识（去掉本进程的随机部分）"""
    return value.split(LEASE_NONCE_SEP, 1)[0]


def windows_pid_exited(pid: int) -> bool:
    """
    本机上确定已经没有进程在用这个 pid 了。只在 Windows 上判断，其它平台恒为 False。

    拿不准的情况（没权限查、别的错误）一律返回 False：调用方拿它判断"可以当作已释放"，
    宁可多等，也不能把活着的进程当成已经退出。

    不能用 `os.kill(pid, 0)` 探活：Windows 上 0 就是 CTRL_C_EVENT，os.kill 会转去调
    GenerateConsoleCtrlEvent，向共享控制台的所有进程广播 Ctrl+C。
    """
    if sys.platform != "win32":
        return False
    # CPython 自带的 Win32 薄封装，标准库 subprocess 也用它查子进程的退出码
    import _winapi

    try:
        handle = _winapi.OpenProcess(_PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    except OSError as e:
        # 只有查无此 pid 才算退出；拒绝访问说明进程在，只是没权限
        return e.winerror == _ERROR_INVALID_PARAMETER
    try:
        # 进程退出后只要还有人握着它的句柄，进程对象就还在，OpenProcess 照样打得开，
        # 得看退出码
        return _winapi.GetExitCodeProcess(handle) != _winapi.STILL_ACTIVE
    except OSError:
        return False
    finally:
        _winapi.CloseHandle(handle)


def lease_owner_exited(owner: bytes | str) -> bool:
    """
    worker id 租约的主人（node_id，格式见 `lease_owner_id`：第一段是机器码，最后一段是 pid）是本机上
    已经退出的进程。只在 Windows 上这么认。HeTu 管理进程起的 worker，最后一段是管理进程的 pid：worker
    被强制结束、管理进程还在时，它会以同一个序号重启、接着用这把租约，不能算退出。

    为什么要认：Windows 上进程被强制结束（TerminateProcess）时关服钩子里的 release_worker_id
    没机会跑——任务管理器"结束任务"、`taskkill /F`、单进程模式第二次 Ctrl+C 强退，管理进程等
    worker 停服超时后也会强制结束它（原来 Sanic 连 Ctrl+C、DEBUG 自动重载都是这样硬杀）。这些
    租约照算的话，upgrade 就得干等它们过期。

    为什么只在 Windows：判断的前提是"机器码相同就是同一个 PID 空间，本地查得到那个 pid"。
    容器里机器码是 hostname（识别不出容器时是 MAC），host 网络或写死 hostname 时多个容器
    共用一个机器码、PID 空间却各自独立，本地查不到 pid 不代表进程不在，会把活着的服务器当成
    已退出、放 upgrade 在它运行时执行。Windows 只用于开发，没有这个问题；Linux 上 worker
    都是用信号或管理进程的通知优雅停的，租约会正常释放，只有 kill -9 / OOM 才留下，交给 TTL。

    只认不删：key 照旧等 TTL 过期，开服分配有的是空位。按值删会撞上"pid 被回收、新 worker
    接手同一把 key"的竞态，删掉的就成了活租约。
    """
    if sys.platform != "win32":
        return False
    if isinstance(owner, bytes):
        owner = owner.decode("ascii", errors="replace")
    from .snowflake_id import TOOL_NODE_PREFIX

    parts = lease_identity(owner).removeprefix(TOOL_NODE_PREFIX).split(":")
    machine_id, pid = parts[0], parts[-1]
    return (
        len(parts) >= 2
        and machine_id == get_machine_id()
        and pid.isdecimal()
        and windows_pid_exited(int(pid))
    )
