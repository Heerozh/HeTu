"""
测试用 docker 基础设施 / Docker infrastructure for tests.

每个 pytest 进程（一次 pytest 运行，或 pytest-xdist 的一个 worker）独享自己的容器，
多个 worktree、多个终端、xdist 的多个 worker 可以同时跑测试，互不干扰：

- 容器名带本进程的会话 ID，不会重名；容器都挂在 docker 默认 bridge 网络上，互相用
  IP 访问，不额外建网络（docker 默认地址池只够建三十来个网络，多 worker 会耗尽）；
- 宿主机端口交给 docker 随机分配，不会撞端口（redis cluster 节点要对外宣告宿主机
  端口，改由本进程挑空闲端口，见 ``free_ports``）；
- 容器都打上 ``hetu.test*`` label，启动时只回收"属主进程已退出"或"创建超过
  ``STALE_SECONDS``"的残留，不会删掉别的进程正在用的容器。

为什么不共用一套容器、各进程分 db：HeTu 的表级/索引值通知是普通 PUBLISH 频道，
频道名不含 db 编号（pub/sub 本身也不分 db），共用 redis 会收到别的进程的通知；
redis cluster 也不支持多 db。

手动清理全部测试容器::

    docker rm -f $(docker ps -aq --filter label=hetu.test)

Each pytest process (a run, or one pytest-xdist worker) owns its own containers,
so worktrees / terminals / xdist workers can run tests concurrently: names carry
a per-process session id, host ports are allocated by docker, containers talk to
each other by IP on the default bridge (no per-run networks, which would exhaust
docker's address pools), and every container is labelled so start-up only reaps
leftovers whose owner process has exited (or that are older than
``STALE_SECONDS``). Processes cannot share one redis via
``SELECT``: HeTu's table/index-value notifications are plain PUBLISH channels,
and pub/sub is not scoped by db.
"""

import functools
import os
import socket
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

import docker
import docker.errors
import pytest
from docker.models.containers import Container
from fixtures.docker_image import ensure_image

LABEL = "hetu.test"
# 属主不在本机（或不在同一 PID namespace）时查不了进程存活，只能按创建时长回收
STALE_SECONDS = 24 * 3600
# 只绑本机回环，宿主机端口由 docker 随机分配
LOCAL_RANDOM_PORT = ("127.0.0.1", None)

_worker = os.environ.get("PYTEST_XDIST_WORKER")
SESSION_ID = f"{_worker}-{uuid.uuid4().hex[:6]}" if _worker else uuid.uuid4().hex[:8]


def _owner() -> str:
    """主机名 + PID namespace。label 里的 owner 和本进程相同时，才能用 pid 查存活"""
    try:
        pid_ns = os.readlink("/proc/self/ns/pid")
    except OSError:  # 非 Linux
        pid_ns = ""
    return f"{socket.gethostname()}/{pid_ns}"


OWNER = _owner()


def _labels() -> dict[str, str]:
    return {
        LABEL: SESSION_ID,
        f"{LABEL}.owner": OWNER,
        f"{LABEL}.pid": str(os.getpid()),
        f"{LABEL}.created": str(int(time.time())),
        # 方便 docker ps 时看出是哪个 worktree 起的
        f"{LABEL}.worktree": str(Path(__file__).resolve().parents[2]),
    }


def _pid_alive(pid: int) -> bool:
    if sys.platform == "win32":
        # Windows 上 os.kill(pid, 0) 会直接结束目标进程，不能用
        import ctypes
        from ctypes import wintypes

        try:
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.OpenProcess.restype = wintypes.HANDLE
            kernel32.OpenProcess.argtypes = (
                wintypes.DWORD,
                wintypes.BOOL,
                wintypes.DWORD,
            )
            kernel32.GetExitCodeProcess.argtypes = (
                wintypes.HANDLE,
                ctypes.POINTER(wintypes.DWORD),
            )
            kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
            # PROCESS_QUERY_LIMITED_INFORMATION
            handle = kernel32.OpenProcess(0x1000, False, pid)
            if not handle:
                # ERROR_ACCESS_DENIED：进程存在，只是无权访问
                return ctypes.get_last_error() == 5
            try:
                code = wintypes.DWORD()
                if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                    return True
                return code.value == 259  # STILL_ACTIVE
            finally:
                kernel32.CloseHandle(handle)
        except Exception:  # noqa: BLE001 判断不了就当活着，交给 STALE_SECONDS 兜底
            return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:  # PermissionError：进程存在但属于别的用户
        return True
    return True


def _is_stale(labels: dict[str, str], now: float) -> bool:
    """残留判定：属主在本机且进程已退出，或创建超过 STALE_SECONDS"""
    if labels.get(LABEL) == SESSION_ID:
        return False
    try:
        pid = int(labels[f"{LABEL}.pid"])
        created = float(labels[f"{LABEL}.created"])
    except KeyError, ValueError:
        return True
    if labels.get(f"{LABEL}.owner") == OWNER and not _pid_alive(pid):
        return True
    return now - created > STALE_SECONDS


def _remove(container: Container) -> None:
    try:
        container.remove(force=True, v=True)
    except docker.errors.NotFound:
        pass
    except docker.errors.APIError as e:
        # 409 是别的进程正在删同一个残留，忽略；其它情况留给下次回收
        if e.status_code != 409:
            print(f"⚠️ 删除容器 {container.name} 失败: {e}", file=sys.stderr)


def _reap_leftovers(client: docker.DockerClient) -> None:
    """回收之前被强杀的测试进程留下的容器"""
    now = time.time()
    containers = client.containers.list(
        all=True, filters={"label": LABEL}, ignore_removed=True
    )
    for c in containers:
        if _is_stale(c.labels, now):
            print(f"ℹ️ 回收残留测试容器 {c.name}")
            _remove(c)


@functools.cache
def _client() -> docker.DockerClient:
    """本进程共用的 docker client，第一次创建时顺带回收残留"""
    client = docker.from_env()
    try:
        _reap_leftovers(client)
    except docker.errors.DockerException as e:  # 回收失败不影响本次测试
        print(f"⚠️ 回收残留测试容器失败: {e}", file=sys.stderr)
    return client


def free_ports(n: int) -> list[int]:
    """向系统要 n 个当前空闲的 TCP 端口（同时占住再一起释放，保证互不相同）"""
    socks: list[socket.socket] = []
    try:
        for _ in range(n):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socks.append(s)
            s.bind(("", 0))
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


def is_port_conflict(e: docker.errors.APIError) -> bool:
    msg = str(e)
    return "already allocated" in msg or "already in use" in msg


def _tail(container: Container) -> str:
    try:
        return container.logs(tail=30).decode(errors="replace")
    except docker.errors.APIError as e:
        return repr(e)


def container_ip(container: Container) -> str:
    """容器在默认 bridge 网络上的 IP，同一 docker 里的其他容器用它访问"""
    container.reload()
    networks = container.attrs["NetworkSettings"]["Networks"]
    return next(iter(networks.values()))["IPAddress"]


def host_port(container: Container, container_port: str) -> int:
    """docker 给容器端口（如 ``"6379/tcp"``）分配的宿主机端口"""
    container.reload()
    bindings = (container.attrs["NetworkSettings"]["Ports"] or {}).get(container_port)
    if not bindings:
        raise RuntimeError(
            f"容器 {container.name}({container.status}) 没有映射 {container_port}"
        )
    return int(bindings[0]["HostPort"])


class DockerStack:
    """一组同生共死的测试容器，``close()`` 时全部删除"""

    def __init__(self, client: docker.DockerClient, name: str):
        self.client = client
        self.name = name
        self.containers: list[Container] = []

    def _res_name(self, role: str) -> str:
        return f"hetu_test_{SESSION_ID}_{self.name}_{role}"

    def run(self, role: str, image: str, **kwargs) -> Container:
        """后台启动一个容器，其他容器可以用 ``container_ip(container)`` 访问它"""
        ensure_image(self.client, image)
        name = self._res_name(role)
        try:
            container = self.client.containers.run(
                image, detach=True, name=name, labels=_labels(), **kwargs
            )
        except docker.errors.APIError:
            # run = create + start，start 失败（如端口被占）时容器已经建出来了
            try:
                _remove(self.client.containers.get(name))
            except docker.errors.NotFound:
                pass
            raise
        self.containers.append(container)
        return container

    def wait_until(
        self, check: Callable[[], object], what: str, timeout: float = 60
    ) -> None:
        """反复调用 check 直到返回真值；超时则带上容器日志报错"""
        deadline = time.monotonic() + timeout
        error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                if check():
                    return
            except Exception as e:  # noqa: BLE001 没就绪时各种连接错误都可能
                error = e
            time.sleep(0.2)
        logs = "\n".join(f"--- {c.name} ---\n{_tail(c)}" for c in self.containers)
        raise RuntimeError(f"{what} 启动超时，最后一次错误：{error!r}\n{logs}")

    def close(self) -> None:
        for container in self.containers:
            _remove(container)
        self.containers.clear()


@contextmanager
def docker_stack(name: str) -> Iterator[DockerStack]:
    """``with docker_stack("redis") as stack:``，块内起的容器退出时全部删除"""
    try:
        client = _client()
    except docker.errors.DockerException:
        pytest.skip("请启动DockerDesktop或者Docker服务后再运行测试")
    stack = DockerStack(client, name)
    try:
        yield stack
    finally:
        print(f"ℹ️ 清理 {name} docker...")
        stack.close()
