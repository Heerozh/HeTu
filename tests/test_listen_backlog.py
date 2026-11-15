"""
监听队列长度 LISTEN_BACKLOG 真的设到了内核里。以前没有这一项：bind_socket 写死 100，worker 开服时 aiohttp
又按它自己的默认值 128 再 listen 一次，实际一直是 128；大批客户端同时重连时排不下的握手被内核丢掉，尾延迟
拖到几十秒（设计稿 docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §12.4）。

读内核里的队列上限用的是 Linux 的 TCP_INFO，这几个用例在别的系统上跳过。后端用 SQLite，不需要 Docker。
"""

import socket
import struct
import sys

import pytest
from fixtures.server_harness import ServerHarness

from hetu.__main__ import main
from hetu.endpoint.definer import EndpointDefines
from hetu.safelogging.default import DEFAULT_LOGGING_CONFIG
from hetu.server.adapters import aiohttp_app
from hetu.server.config import DEFAULTS, listen_backlog
from hetu.system import SystemClusters

linux_only = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="用 Linux 的 TCP_INFO 读监听队列上限"
)

NAMESPACE = "backlog_test"
INSTANCE = "backlog_1"

# SystemClusters 要求 namespace 下至少有一个 System 才能建簇，放个空的。app 文件每个用例 exec 一遍，
# 定义都带 force=True
APP = """
import hetu
import numpy as np


@hetu.define_component(namespace="{ns}", force=True)
class Dummy(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)


@hetu.define_system(namespace="{ns}", components=(Dummy,), force=True)
async def noop(ctx):
    pass
"""


@pytest.fixture(autouse=True)
def clean_registry():
    """app 文件里的簇、Endpoint 定义是进程级的，用例前后都清掉，免得漏到别的测试文件"""
    SystemClusters()._clear()
    EndpointDefines()._clear()
    yield
    SystemClusters()._clear()
    EndpointDefines()._clear()


def kernel_backlog(sock: socket.socket) -> int:
    """
    监听 socket 在内核里的队列上限，即 listen 的 backlog 截到 somaxconn 之后的值。Linux 的 tcp_info 对
    监听 socket 把它放在 tcpi_sacked：前 8 个字节是 u8 字段，之后依次是 rto、ato、snd_mss、rcv_mss、
    unacked、sacked 六个 u32，sacked 在偏移 28
    """
    info = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 104)
    return struct.unpack_from("I", info, 28)[0]


def somaxconn() -> int:
    with open(aiohttp_app.SOMAXCONN_PATH, encoding="ascii") as f:
        return int(f.read())


def make_server(tmp_path, **config) -> ServerHarness:
    app_file = tmp_path / "app.py"
    app_file.write_text(APP.format(ns=NAMESPACE), "utf-8")
    return ServerHarness(
        {
            "APP_FILE": str(app_file),
            "NAMESPACE": NAMESPACE,
            "INSTANCES": [INSTANCE],
            "LISTEN": "127.0.0.1:0",
            "PACKET_LAYERS": [{"type": "jsonb"}],
            "BACKENDS": {
                "main": {
                    "type": "sqlite",
                    "master": f"sqlite:///{tmp_path / 'backlog.db'}",
                }
            },
            "LOGGING": DEFAULT_LOGGING_CONFIG,
            "DEBUG": False,
            "WORKER_NUM": 1,
            **config,
        }
    )


def serving_backlog(server: ServerHarness) -> int:
    """起服，服务开始监听之后读监听 socket 在内核里的队列上限，再停服"""
    seen: list[int] = []

    async def routine(connect):
        assert server.listen_socket is not None
        seen.append(kernel_backlog(server.listen_socket))

    server.websocket(f"/hetu/{INSTANCE}", routine)
    return seen[0]


def write_start_config(tmp_path, workers: int, backlog) -> str:
    """hetu start 用的配置文件。app.py 不存在：只测开服之前的步骤"""
    config = tmp_path / "config.yml"
    config.write_text(
        f"""
APP_FILE: app.py
NAMESPACE: pytest
INSTANCES: [cli_start]
LISTEN: 127.0.0.1:0
WORKER_NUM: {workers}
LISTEN_BACKLOG: {backlog}
PACKET_LAYERS:
  - type: jsonb
BACKENDS:
  main:
    type: sqlite
    master: sqlite:///{tmp_path / "cli_start.db"}
""",
        encoding="utf-8",
    )
    return str(config)


@linux_only
def test_configured_backlog_reaches_the_kernel(tmp_path):
    """
    开服之后内核里的就是 LISTEN_BACKLOG：aiohttp 开始监听时再 listen 的那一次也按它，不会被换回 aiohttp
    的默认值 128
    """
    if somaxconn() < 300:
        pytest.skip("net.core.somaxconn 小于 300，配置的值会被内核截短")
    assert serving_backlog(make_server(tmp_path, LISTEN_BACKLOG=300)) == 300


@linux_only
def test_default_backlog(tmp_path):
    """没配就是 2048，内核上限更小时截到 somaxconn"""
    assert listen_backlog(DEFAULTS) == 2048
    assert serving_backlog(make_server(tmp_path)) == min(2048, somaxconn())


@linux_only
def test_manager_binds_with_configured_backlog(tmp_path, monkeypatch):
    """多进程时监听 socket 由管理进程 bind，也按 LISTEN_BACKLOG"""
    from hetu.server import runtime, supervisor

    if somaxconn() < 300:
        pytest.skip("net.core.somaxconn 小于 300，配置的值会被内核截短")
    backlogs: list[int] = []

    def fake_run(config, sock, workers):
        backlogs.append(kernel_backlog(sock))
        return 0

    monkeypatch.setattr(supervisor, "run", fake_run)
    # 不真的 dictConfig：别改了测试进程的全局日志配置
    monkeypatch.setattr(runtime, "setup_logging", lambda config: None)
    monkeypatch.chdir(tmp_path)
    config = write_start_config(tmp_path, workers=2, backlog=300)
    monkeypatch.setattr(sys, "argv", ["hetu", "start", "--config", config])
    try:
        main()
    except SystemExit as e:
        assert e.code in (0, None)
    assert backlogs == [300]


@pytest.mark.parametrize("value", [0, -1, "2048", 1.5, True, None])
def test_backlog_must_be_positive_int(value):
    with pytest.raises(ValueError, match="LISTEN_BACKLOG"):
        listen_backlog({"LISTEN_BACKLOG": value})


def test_start_rejects_invalid_backlog_before_startup(tmp_path, monkeypatch, capsys):
    """配置文件里的 LISTEN_BACKLOG 不是正整数：hetu start 读完配置就打出说明、以 2 退出，不开服"""
    monkeypatch.chdir(tmp_path)
    config = write_start_config(tmp_path, workers=1, backlog=0)
    monkeypatch.setattr(sys, "argv", ["hetu", "start", "--config", config])
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 2
    assert "LISTEN_BACKLOG" in capsys.readouterr().out


@linux_only
def test_warns_when_kernel_caps_backlog(tmp_path, monkeypatch):
    """内核上限比 LISTEN_BACKLOG 小（5.4 之前的内核默认 128）：bind 时提示一句；够大就不提示"""
    warnings: list[str] = []
    monkeypatch.setattr(
        aiohttp_app.logger, "warning", lambda msg, *args, **kw: warnings.append(msg)
    )
    limit = tmp_path / "somaxconn"
    monkeypatch.setattr(aiohttp_app, "SOMAXCONN_PATH", str(limit))

    limit.write_text("64\n")
    aiohttp_app.bind_socket("127.0.0.1:0", 300).close()
    assert len(warnings) == 1
    assert "LISTEN_BACKLOG=300" in warnings[0]
    assert "somaxconn=64" in warnings[0]

    limit.write_text("4096\n")
    aiohttp_app.bind_socket("127.0.0.1:0", 300).close()
    assert len(warnings) == 1
