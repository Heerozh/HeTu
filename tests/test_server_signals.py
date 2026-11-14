"""
真起 `hetu start` 的测试：按终端 Ctrl+C 的方式（SIGINT 发给整个进程组）停服，检查关服流程完整跑完，
Sanic、aiohttp 两个栈各跑一遍；aiohttp 栈端口被占时在开服之前就失败。

Windows 上 Sanic 不用 loop.add_signal_handler（走 ctrlc_workaround），aiohttp 栈也另有一套，不在这里测。
"""

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

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


@pytest.mark.timeout(90)
def test_second_shutdown_signal_does_not_skip_cleanup(tmp_path, http_server):
    """
    关服途中再来一个 SIGINT（终端里 Ctrl+C 会把信号发给整个进程组，多 worker 时管理进程还会再转发
    一次），不能打断关服：Linux 上的 uvloop 在 loop 停下后 remove_signal_handler 不起作用，Sanic 的
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
