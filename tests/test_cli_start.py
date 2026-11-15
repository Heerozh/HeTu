import sys
from pathlib import Path

import pytest

from hetu.__main__ import main
from hetu.cli.base import resolve_app_file
from hetu.cli.start import resolve_worker_num


def test_required_parameters():
    sys.argv[1:] = []
    with pytest.raises(SystemExit):
        main()

    sys.argv[1:] = ["start"]
    with pytest.raises(SystemExit):
        main()

    sys.argv[1:] = ["start", "--namespace=ssw"]
    with pytest.raises(SystemExit):
        main()

    # 单进程：本进程加载 app 文件，找不到就抛出来。多进程时管理进程不加载 app 文件，由 worker 报错
    sys.argv[1:] = [
        "start",
        "--namespace=ssw",
        "--instance=unittest1",
        "--debug=2",
        "--workers=1",
    ]
    with pytest.raises(FileNotFoundError):
        main()


def test_resolve_app_file_anchors_relative_path_to_config_dir(tmp_path):
    """相对 APP_FILE 应按 config 文件所在目录解析，而非进程 CWD。"""
    config_file = tmp_path / "proj" / "config.yml"
    resolved = resolve_app_file("src/app.py", str(config_file))
    assert Path(resolved) == tmp_path / "proj" / "src" / "app.py"


def test_resolve_app_file_keeps_absolute_path(tmp_path):
    """绝对 APP_FILE 路径应原样保留。"""
    config_file = tmp_path / "config.yml"
    abs_app = str(tmp_path / "elsewhere" / "app.py")
    resolved = resolve_app_file(abs_app, str(config_file))
    assert Path(resolved) == Path(abs_app)


def test_resolve_worker_num_negative_means_auto():
    """WORKER_NUM为-1(负数)表示按CPU核心数自动，正数原样返回。

    以前这里是转交给sanic的fast=True，但fast和single_process互斥，
    单worker分支要用single_process，导致-1直接启动报错。
    """
    import os

    assert resolve_worker_num(4) == 4
    assert resolve_worker_num(1) == 1
    assert resolve_worker_num(-1) == (os.process_cpu_count() or 1)
    assert resolve_worker_num(-1) >= 1


def _write_config(tmp_path, workers: int) -> str:
    config = tmp_path / "config.yml"
    config.write_text(
        f"""
APP_FILE: app.py
NAMESPACE: pytest
INSTANCES: [cli_start]
LISTEN: 127.0.0.1:0
WORKER_NUM: {workers}
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


def test_multiple_workers_go_through_the_supervisor(tmp_path, monkeypatch):
    """
    WORKER_NUM > 1：本进程是管理进程，只配日志、不加载 app 文件（这里的 app.py 根本不存在），bind 好
    监听 socket 交给 supervisor，用完关掉
    """
    from hetu.server import runtime, supervisor

    calls = []
    sockets = []

    def fake_run(config, sock, workers):
        sockets.append(sock)
        calls.append((config["NAMESPACE"], sock.getsockname()[1] > 0, workers))
        return 0

    logged = []
    monkeypatch.setattr(supervisor, "run", fake_run)
    # 不真的 dictConfig：别改了测试进程的全局日志配置
    monkeypatch.setattr(runtime, "setup_logging", lambda config: logged.append(1))
    monkeypatch.chdir(tmp_path)
    sys.argv[1:] = ["start", "--config", _write_config(tmp_path, workers=2)]
    try:
        main()
    except SystemExit as e:
        assert e.code in (0, None)
    assert calls == [("pytest", True, 2)]
    assert logged == [1]
    assert sockets[0].fileno() == -1  # 已经关了
