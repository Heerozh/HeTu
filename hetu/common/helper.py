import itertools
import os
import sys
import uuid

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
    worker id 租约的主人（node_id，即 `机器码:pid`，工具进程再带 `cli:` 前缀）是本机上已经
    退出的进程。只在 Windows 上这么认。

    为什么要认：Windows 上 sanic 停 worker 是 TerminateProcess 硬杀——Ctrl+C 走
    `WorkerProcess.terminate()` 的 `os.kill(pid, SIGINT)`，DEBUG 自动重载走
    `multiprocessing.Process.terminate()`，从外面 `taskkill /F` 也一样——关服钩子里的
    release_worker_id 没机会跑。这些租约照算的话，upgrade 就得干等它们过期。

    为什么只在 Windows：判断的前提是"机器码相同就是同一个 PID 空间，本地查得到那个 pid"。
    容器里机器码是 hostname（识别不出容器时是 MAC），host 网络或写死 hostname 时多个容器
    共用一个机器码、PID 空间却各自独立，本地查不到 pid 不代表进程不在，会把活着的服务器当成
    已退出、放 upgrade 在它运行时执行。Windows 只用于开发，没有这个问题；Linux 上 sanic
    用信号优雅停 worker，租约会正常释放，只有 kill -9 / OOM 才留下，交给 TTL。

    只认不删：key 照旧等 TTL 过期，开服分配有的是空位。按值删会撞上"pid 被回收、新 worker
    接手同一把 key"的竞态，删掉的就成了活租约。
    """
    if sys.platform != "win32":
        return False
    if isinstance(owner, bytes):
        owner = owner.decode("ascii", errors="replace")
    from .snowflake_id import TOOL_NODE_PREFIX

    machine_id, _, pid = owner.removeprefix(TOOL_NODE_PREFIX).rpartition(":")
    return (
        machine_id == get_machine_id()
        and pid.isdecimal()
        and windows_pid_exited(int(pid))
    )
