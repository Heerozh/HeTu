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

    sys.argv[1:] = ["start", "--namespace=ssw", "--instance=unittest1", "--debug=2"]
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
