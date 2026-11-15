"""
真起 `hetu start` 的测试，Sanic、aiohttp 两个栈各跑一遍：

- 按终端 Ctrl+C 的方式（SIGINT 发给整个进程组）停服：带着已登录的连接关服、关服时取消在途的 RPC、
  关服途中再来的信号不打断关服；
- aiohttp 栈：开服途中收到关服信号就不再开服；端口被占时在开服之前就失败。

测试 harness 先关客户端再停服（harness 的语义 4），带着活连接关服的路径只有这里测得到。

Windows 上 Sanic 不用 loop.add_signal_handler（走 ctrlc_workaround），aiohttp 栈也另有一套，不在这里测。
"""

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
from fixtures.http_servers import HTTP_SERVERS
from websockets.exceptions import ConnectionClosed
from websockets.legacy.client import connect

from hetu.server.pipeline import JSONBinaryLayer, MessagePipeline

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX 信号")

HETU = Path(sys.executable).with_name("hetu")
APP_FILE = Path(__file__).with_name("app.py")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_port(port: int, proc: subprocess.Popen, timeout: float = 40) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        assert proc.poll() is None, "服务器没起来就退出了"
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    pytest.fail("服务器没在规定时间内开始监听")


def _busy_http_client(port: int) -> socket.socket:
    """请求头只发一半就停住：Sanic 认为这条连接还在收请求、不是空闲的，关服时不会关它，要一直等到
    GRACEFUL_SHUTDOWN_TIMEOUT 才强关。用它把服务器稳定地拖在"等连接拆完"那段里"""
    sock = socket.create_connection(("127.0.0.1", port))
    sock.sendall(f"GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n".encode())
    return sock


def _write_config(tmp_path: Path, port: int) -> Path:
    config = tmp_path / "config.yml"
    config.write_text(
        f"""
APP_FILE: {APP_FILE}
NAMESPACE: pytest
INSTANCES:
  - signals_1
LISTEN: 127.0.0.1:{port}
WORKER_NUM: 1
DEBUG: false
GRACEFUL_SHUTDOWN_TIMEOUT: 3
PACKET_LAYERS:
  - type: jsonb
BACKENDS:
  main:
    type: sqlite
    master: sqlite:///{tmp_path / "signals.db"}
""",
        encoding="utf-8",
    )
    return config


@pytest.mark.parametrize(
    "http_server", [s for s in HTTP_SERVERS if s == "sanic"], indirect=True
)
@pytest.mark.timeout(90)
def test_second_shutdown_signal_does_not_skip_cleanup(tmp_path, http_server):
    """
    Sanic 栈：关服途中再来一个 SIGINT（终端里 Ctrl+C 会把信号发给整个进程组，多 worker 时管理进程还会
    再转发一次），不能打断关服。aiohttp 栈关服不等这种半截的 HTTP 请求、拖不住，见
    test_signals_during_slow_disconnect_do_not_interrupt_shutdown：Linux 上的 uvloop 在 loop 停下后 remove_signal_handler 不起作用，Sanic 的
    处理照样 loop.stop()。落在 _cleanup 等连接拆完的那段里时，after_server_stop 会被整个跳过：
    断线 System 不跑、Connection 行不删、租约不释放、后端不关
    """
    port = _free_port()
    config = _write_config(tmp_path, port)
    env = dict(os.environ, PYTHONUNBUFFERED="1", HETU_HTTP_SERVER=http_server)
    with open(tmp_path / "out.log", "wb") as out:
        proc = subprocess.Popen(
            [str(HETU), "start", "--config", str(config)],
            cwd=tmp_path,
            env=env,
            stdout=out,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            _wait_port(port, proc)
            client = _busy_http_client(port)
            time.sleep(0.5)
            os.killpg(proc.pid, signal.SIGINT)
            # 第二个信号落在等连接拆完的那段里（被那条请求没发完的连接拖住）
            time.sleep(0.5)
            os.killpg(proc.pid, signal.SIGINT)
            proc.wait(40)
            client.close()
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
    output = (tmp_path / "out.log").read_text(encoding="utf-8", errors="replace")
    assert "Event loop stopped before Future completed" not in output, output
    assert "Closing backend main" in output, output


@pytest.mark.timeout(60)
def test_aiohttp_port_in_use_fails_before_startup(tmp_path):
    """aiohttp 栈先 bind 监听端口再开服：端口被占时直接以 1 退出，不会先连后端、跑一遍开服"""
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        config = _write_config(tmp_path, taken.getsockname()[1])
        env = dict(os.environ, PYTHONUNBUFFERED="1", HETU_HTTP_SERVER="aiohttp")
        proc = subprocess.run(
            [str(HETU), "start", "--config", str(config)],
            check=False,
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=50,
        )
    output = proc.stdout + proc.stderr
    assert proc.returncode == 1, output
    assert "监听 127.0.0.1" in output, output
    # runtime.start() 拿到租约、开完服才会打这一句
    assert "已启动 pytest 应用" not in output, output


# ==== 带着活连接关服 ====

# 生命周期测试用的 app：登录、一个慢 RPC、慢的断线 System 和启动 System（时长由环境变量控制）
LIFECYCLE_APP = """
import asyncio
import os

import numpy as np

import hetu


@hetu.define_component(namespace="lifecycle", force=True, permission=hetu.Permission.EVERYBODY)
class Mark(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)
    count: np.int32 = hetu.property_field(0)


@hetu.define_component(namespace="lifecycle", force=True, permission=hetu.Permission.EVERYBODY)
class Disconnected(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)
    count: np.int32 = hetu.property_field(0)


@hetu.define_system(namespace="lifecycle", components=(Mark,), on_start=True)
async def slow_start(ctx):
    await asyncio.sleep(float(os.environ.get("LIFECYCLE_START_DELAY", "0")))


@hetu.define_endpoint(namespace="lifecycle", permission=hetu.Permission.EVERYBODY)
async def login(ctx, user_id):
    await hetu.elevate(ctx, user_id)
    return hetu.ResponseToClient({"id": ctx.caller})


@hetu.define_system(
    namespace="lifecycle", components=(Mark,), permission=hetu.Permission.USER
)
async def slow_mark(ctx, seconds):
    await asyncio.sleep(seconds)
    async with ctx.repo[Mark].upsert(owner=ctx.caller) as row:
        row.count += 1
    return hetu.ResponseToClient("marked")


@hetu.define_system(namespace="lifecycle", components=(Disconnected,), permission=None)
async def on_disconnect(ctx):
    await asyncio.sleep(float(os.environ.get("LIFECYCLE_DISCONNECT_DELAY", "0")))
    if ctx.caller:
        async with ctx.repo[Disconnected].upsert(owner=ctx.caller) as row:
            row.count += 1
"""


class _Server:
    """一个 `hetu start` 子进程（生命周期 app、SQLite），输出写到 tmp_path/out.log"""

    def __init__(self, tmp_path: Path, http_server: str, **env: str):
        self.tmp_path = tmp_path
        self.port = _free_port()
        (tmp_path / "lifecycle_app.py").write_text(LIFECYCLE_APP, encoding="utf-8")
        self.config = tmp_path / "config.yml"
        self.config.write_text(
            f"""
APP_FILE: lifecycle_app.py
NAMESPACE: lifecycle
INSTANCES:
  - lifecycle_1
LISTEN: 127.0.0.1:{self.port}
WORKER_NUM: 1
DEBUG: false
GRACEFUL_SHUTDOWN_TIMEOUT: 5
PACKET_LAYERS:
  - type: jsonb
BACKENDS:
  main:
    type: sqlite
    master: sqlite:///{tmp_path / "lifecycle.db"}
""",
            encoding="utf-8",
        )
        self.log_path = tmp_path / "out.log"
        self._log = open(self.log_path, "wb")  # noqa: SIM115 子进程活着期间一直开着，close() 里关
        self.proc = subprocess.Popen(
            [str(HETU), "start", "--config", str(self.config)],
            cwd=tmp_path,
            env=dict(
                os.environ, PYTHONUNBUFFERED="1", HETU_HTTP_SERVER=http_server, **env
            ),
            stdout=self._log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def signal(self, sig: int) -> None:
        """往整个进程组发信号，同终端里按 Ctrl+C。进程已经退出了就什么也不做"""
        try:
            os.killpg(self.proc.pid, sig)
        except ProcessLookupError:
            pass

    def output(self) -> str:
        return self.log_path.read_text(encoding="utf-8", errors="replace")

    def close(self) -> None:
        if self.proc.poll() is None:
            self.signal(signal.SIGKILL)
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


async def _login(port: int, user_id: int) -> tuple[Any, MessagePipeline, Any]:
    """连上、握手（只有 jsonb 层）、以 user_id 登录，返回 (ws, 流水线, 流水线上下文)"""
    pipe = MessagePipeline()
    pipe.add_layer(JSONBinaryLayer())
    ws = await connect(f"ws://127.0.0.1:{port}/hetu/lifecycle_1")
    await ws.send(pipe.encode(None, []))
    pipe_ctx, _reply = pipe.handshake(pipe.decode(None, await ws.recv()))
    await ws.send(pipe.encode(pipe_ctx, ["rpc", "login", user_id]))
    assert pipe.decode(pipe_ctx, await ws.recv()) == ["rsp", {"id": user_id}]
    return ws, pipe, pipe_ctx


async def _close_code(ws: Any, timeout: float = 10) -> int | None:
    """等服务器断开，返回 close 码；没有 close 帧就是 None"""
    try:
        async with asyncio.timeout(timeout):
            while True:
                await ws.recv()
    except ConnectionClosed as e:
        return None if e.rcvd is None else e.rcvd.code


@pytest.mark.timeout(90)
def test_shutdown_with_live_connections(tmp_path, http_server):
    """
    带着已登录的连接关服：客户端都收到 1001，连接都拆完（断线 System 以各自的用户身份跑了一次、
    Connection 行删掉），没有"没拆完"的警告，以 0 退出
    """
    server = _Server(tmp_path, http_server)
    try:
        _wait_port(server.port, server.proc)

        async def scenario() -> list[int | None]:
            clients = [await _login(server.port, uid) for uid in (11, 12, 13)]
            server.signal(signal.SIGINT)
            return [await _close_code(ws) for ws, _pipe, _ctx in clients]

        codes = asyncio.run(scenario())
        server.proc.wait(30)
    finally:
        server.close()
    output = server.output()
    assert codes == [1001, 1001, 1001], output
    assert server.proc.returncode == 0, output
    assert output.count("连接断开") == 3, output
    assert "没拆完" not in output, output
    assert server.rows("Connection", "id") == []
    disconnected = server.rows("Disconnected", "owner")
    assert sorted((r["owner"], r["count"]) for r in disconnected) == [
        (11, 1),
        (12, 1),
        (13, 1),
    ]


@pytest.mark.timeout(90)
def test_shutdown_cancels_inflight_rpc(tmp_path, http_server):
    """
    关服时在途的 RPC 被取消，没有提交：不能提交了却发不出回复，客户端重连重试时就重复执行了。关服也
    不等它跑完（关服排空是第 4b 步的事）
    """
    server = _Server(tmp_path, http_server)
    try:
        _wait_port(server.port, server.proc)

        async def scenario() -> tuple[int | None, float]:
            ws, pipe, pipe_ctx = await _login(server.port, 21)
            await ws.send(pipe.encode(pipe_ctx, ["rpc", "slow_mark", 3]))
            await asyncio.sleep(0.5)  # RPC 已经在跑了
            start = time.monotonic()
            server.signal(signal.SIGINT)
            code = await _close_code(ws)
            await asyncio.to_thread(server.proc.wait, 30)
            return code, time.monotonic() - start

        code, took = asyncio.run(scenario())
    finally:
        server.close()
    output = server.output()
    assert code == 1001, output
    assert server.proc.returncode == 0, output
    assert took < 2.5, output  # RPC 要跑 3 秒
    assert server.rows("Mark", "owner") == [], output


@pytest.mark.timeout(90)
def test_signals_during_slow_disconnect_do_not_interrupt_shutdown(
    tmp_path, http_server
):
    """
    断线 System 要跑 2 秒，关服就被它拖住。这期间再来的 SIGINT（终端里再按一次 Ctrl+C）、SIGTERM
    （编排器）都不能打断关服：断线 System 跑完、后端照常关掉，以 0 退出
    """
    server = _Server(tmp_path, http_server, LIFECYCLE_DISCONNECT_DELAY="2")
    try:
        _wait_port(server.port, server.proc)

        async def scenario() -> int | None:
            ws, _pipe, _ctx = await _login(server.port, 31)
            server.signal(signal.SIGINT)
            for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGINT, signal.SIGTERM):
                await asyncio.sleep(0.3)
                server.signal(sig)
            return await _close_code(ws)

        code = asyncio.run(scenario())
        server.proc.wait(30)
    finally:
        server.close()
    output = server.output()
    assert code == 1001, output
    assert server.proc.returncode == 0, output
    assert "Event loop stopped before Future completed" not in output, output
    assert "Closing backend main" in output, output
    disconnected = server.rows("Disconnected", "owner")
    assert [(r["owner"], r["count"]) for r in disconnected] == [(31, 1)]


@pytest.mark.parametrize(
    "http_server", [s for s in HTTP_SERVERS if s == "aiohttp"], indirect=True
)
@pytest.mark.timeout(90)
def test_aiohttp_signal_during_startup_aborts_startup(tmp_path, http_server):
    """
    aiohttp 栈开服途中（启动 System 要跑 15 秒）收到 SIGTERM：不等开服跑完，马上关掉，以 0 退出，不会
    先开始接受连接。aiohttp 栈先 bind 端口再开服，所以端口能连上的时候开服还在进行
    """
    server = _Server(tmp_path, http_server, LIFECYCLE_START_DELAY="15")
    try:
        _wait_port(server.port, server.proc)
        time.sleep(1)  # 启动 System 已经在跑了
        start = time.monotonic()
        server.signal(signal.SIGTERM)
        server.proc.wait(30)
        took = time.monotonic() - start
    finally:
        server.close()
    output = server.output()
    assert server.proc.returncode == 0, output
    assert took < 5, output
    assert "中止开服" in output, output
    assert "服务器已就绪" not in output, output
