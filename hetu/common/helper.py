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
    except Exception:
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
        except Exception:
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
