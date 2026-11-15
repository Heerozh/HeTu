"""
打包：Docker 镜像（Dockerfile 里 pip install .）构建时没有 .git，setuptools-scm 列不出受版本控制的文件，包里
只有 pyproject.toml 的 package-data 显式列出的数据文件。漏列的文件镜像里没有：commit_v2.lua 漏过，官方镜像
跑 Redis 后端时开服就报 FileNotFoundError。
"""

import fnmatch
import subprocess
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
# 不进包的：翻译用的提示词只给 autolang 用；.po 由 setup.py 在打包时编成 .mo
NOT_SHIPPED = {"i18n/PROMPT.md"}
CODE_SUFFIXES = {".py", ".po"}


def tracked_package_files() -> list[str]:
    try:
        out = subprocess.run(
            ["git", "ls-files", "hetu"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        ).stdout
    except OSError, subprocess.SubprocessError:
        pytest.skip("没有 git，列不出受版本控制的文件")
    return [line.removeprefix("hetu/") for line in out.splitlines() if line]


def test_every_data_file_is_declared_as_package_data():
    config = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    patterns = config["tool"]["setuptools"]["package-data"]["hetu"]
    missing = [
        path
        for path in tracked_package_files()
        if Path(path).suffix not in CODE_SUFFIXES
        and path not in NOT_SHIPPED
        and path != "py.typed"  # setuptools 自己会带上
        and not any(fnmatch.fnmatch(path, pattern) for pattern in patterns)
    ]
    assert missing == [], f"这些文件要加进 pyproject.toml 的 package-data：{missing}"
