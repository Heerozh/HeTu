import itertools
import os
import sys
import uuid


def batched(iterable, n):
    """Batch data into tuples of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


def resolve_import(s):
    """
    Resolve strings to objects using standard import and attribute
    syntax.
    """
    name = s.split(".")
    used = name.pop(0)
    try:
        found = __import__(used)
        for frag in name:
            used += "." + frag
            try:
                found = getattr(found, frag)
            except AttributeError:
                __import__(used)
                found = getattr(found, frag)
        return found
    except ImportError as e:
        v = ValueError("Cannot resolve %r: %s" % (s, e))
        raise v from e


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
