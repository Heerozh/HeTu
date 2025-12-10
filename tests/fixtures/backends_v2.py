import time

import docker
import pytest
from docker.errors import NotFound


@pytest.fixture(scope="module")
async def mod_redis_backend_v2(mod_redis_service):
    from hetu.data.backend_v2 import Backend

    backends = {}

    # 支持创建多个backend连接
    def _create_redis_backend(key="main", port=23318):
        if key in backends:
            return backends[key]

        redis_url, replica_url = mod_redis_service(port)
        config = {
            "master": redis_url,
            "servants": [
                replica_url,
            ],
        }

        _backend = Backend(config)
        backends[key] = _backend
        _backend.configure()
        return _backend

    yield _create_redis_backend

    for backend in backends.values():
        await backend.close()


# 要测试新的backend，请添加backend到params中
@pytest.fixture(
    params=[
        "redis",
    ],
    scope="module",
)
def mod_auto_backend_v2(request):
    if request.param == "redis":
        return request.getfixturevalue("mod_redis_backend_v2")
    else:
        raise ValueError("Unknown db type: %s" % request.param)


# 要测试新的backend，请添加backend到params中
@pytest.fixture(
    params=[
        "redis",
    ],
    scope="module",
)
def auto_backend_v2(request):
    if request.param == "redis":
        return request.getfixturevalue("mod_redis_backend_v2")
    else:
        raise ValueError("Unknown db type: %s" % request.param)
