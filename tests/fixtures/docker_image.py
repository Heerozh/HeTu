"""
测试用 docker 镜像拉取工具 / Docker image pull helper for tests.
"""

import os
import sys

import docker
import docker.errors

# 每个 pytest session 内每个镜像只检查一次
_CHECKED: set[str] = set()


def _pull_disabled() -> bool:
    return os.environ.get("HETU_TEST_NO_PULL", "0").lower() not in ("0", "", "false")


def ensure_image(client: docker.DockerClient, image: str) -> None:
    """
    启动容器前确保本地镜像是 registry 上的最新版本。

    测试固件统一使用 ``:latest`` tag，而 docker 只在本地不存在该 tag 时才会去
    拉取，因此本地镜像会永远停留在第一次拉取时的版本。这里显式 pull 一次，
    保证测试跑在最新的 Redis/Valkey/Postgres/MariaDB 上。

    每个镜像每个 session 只检查一次；离线或 registry 不可达时回退到本地已有
    镜像。设置环境变量 ``HETU_TEST_NO_PULL=1`` 可完全跳过检查。

    Ensures the local image matches the latest one in the registry before
    starting a container. Fixtures all use the ``:latest`` tag, which docker
    only pulls when absent locally, so the image would otherwise stay pinned to
    whatever version was first pulled. Checked once per image per session;
    falls back to the local image when offline. Set ``HETU_TEST_NO_PULL=1``
    to skip entirely.
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

    try:
        new_id = client.images.pull(image).id
    except Exception as e:  # 网络/registry 故障不应导致测试失败
        if old_id is None:
            raise
        print(f"⚠️ 拉取 {image} 失败({e})，使用本地已有镜像。", file=sys.stderr)
        return

    if old_id is None:
        print(f"⬇️ 已拉取镜像 {image}。")
    elif old_id != new_id:
        print(f"⬆️ 镜像 {image} 已更新到新版本({(new_id or '')[7:19]})。")
