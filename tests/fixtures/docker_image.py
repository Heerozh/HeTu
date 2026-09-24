"""
测试用 docker 镜像拉取工具 / Docker image pull helper for tests.
"""

import os
import re
import sys
import time
from pathlib import Path

import docker
import docker.errors

# 每个 pytest 进程内每个镜像只检查一次
_CHECKED: set[str] = set()
# 检查过的镜像记一个时间戳文件，PULL_INTERVAL 内不再重复检查。多个 worktree、
# xdist worker 共用这些时间戳，免得每个进程都去 registry 查一遍（每次 1~2 秒，
# Docker Hub 对匿名拉取还有频率限制）。删掉 _STAMP_DIR 即可强制重新检查。
PULL_INTERVAL = 6 * 3600
_STAMP_DIR = Path.home() / ".cache" / "hetu-test" / "image-pull"


def _pull_disabled() -> bool:
    return os.environ.get("HETU_TEST_NO_PULL", "0").lower() not in ("0", "", "false")


def _stamp(image: str) -> Path:
    return _STAMP_DIR / re.sub(r"[^\w.-]", "_", image)


def _checked_recently(image: str) -> bool:
    try:
        return time.time() - _stamp(image).stat().st_mtime < PULL_INTERVAL
    except OSError:
        return False


def _mark_checked(image: str) -> None:
    try:
        _STAMP_DIR.mkdir(parents=True, exist_ok=True)
        _stamp(image).touch()
    except OSError:
        pass


def ensure_image(client: docker.DockerClient, image: str) -> None:
    """
    启动容器前确保本地镜像是 registry 上的最新版本。

    测试固件统一使用 ``:latest`` tag，而 docker 只在本地不存在该 tag 时才会去
    拉取，因此本地镜像会永远停留在第一次拉取时的版本。这里显式 pull 一次，
    保证测试跑在最新的 Redis/Valkey/Postgres/MariaDB 上。

    同一镜像在 ``PULL_INTERVAL`` 内只检查一次（所有测试进程共享）；离线或
    registry 不可达时回退到本地已有镜像。设置环境变量 ``HETU_TEST_NO_PULL=1``
    可完全跳过检查。

    Ensures the local image matches the latest one in the registry before
    starting a container. Fixtures all use the ``:latest`` tag, which docker
    only pulls when absent locally, so the image would otherwise stay pinned to
    whatever version was first pulled. Checked at most once per image per
    ``PULL_INTERVAL`` across all test processes; falls back to the local image
    when offline. Set ``HETU_TEST_NO_PULL=1`` to skip entirely.
    """
    if image in _CHECKED:
        return
    _CHECKED.add(image)

    if _pull_disabled():
        return

    try:
        old_id = client.images.get(image).id
    except docker.errors.ImageNotFound:
        old_id = None
    except docker.errors.DockerException:
        # 本地查询都失败，交给 containers.run 自己处理
        return

    if old_id is not None and _checked_recently(image):
        return

    try:
        new_id = client.images.pull(image).id
    except Exception as e:  # 网络/registry 故障不应导致测试失败
        if old_id is None:
            raise
        print(f"⚠️ 拉取 {image} 失败({e})，使用本地已有镜像。", file=sys.stderr)
        return
    _mark_checked(image)

    if old_id is None:
        print(f"⬇️ 已拉取镜像 {image}。")
    elif old_id != new_id:
        print(f"⬆️ 镜像 {image} 已更新到新版本({(new_id or '')[7:19]})。")
