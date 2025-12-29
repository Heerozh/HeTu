import pytest
from typing import Callable, cast
from hetu.data.backend import Backend, RedisBackendClient


@pytest.fixture(scope="module")
async def mod_redis_backend(mod_redis_service):
    """Redis后端工厂，返回创建Redis后端连接的工厂函数"""
    from hetu.data.component import ComponentDefines

    backends = {}

    # 支持创建多个backend连接
    def _create_redis_backend(key="main", port=23318):
        if key in backends:
            _backend = backends[key]
        else:
            redis_url, replica_url = mod_redis_service(port)
            config = {
                "type": "redis",
                "master": redis_url,
                "servants": [
                    replica_url,
                ],
            }

            _backend = Backend(config)
            backends[key] = _backend

        # mock redis client
        def _mock_redis_client_lua():
            return ComponentDefines().get_all()

        _master = cast(RedisBackendClient, _backend.master)
        _master._get_referred_components = _mock_redis_client_lua
        _backend.post_configure()
        return _backend

    yield _create_redis_backend

    for backend in backends.values():
        await backend.close()


@pytest.fixture(scope="module")
async def mod_valkey_backend(mod_valkey_service):
    """valkey后端工厂fixture，返回创建valkey后端的工厂函数"""
    from hetu.data.component import ComponentDefines

    backends = {}

    # 支持创建多个backend连接
    def _create_valkey_backend(key="main", port=23418):
        if key in backends:
            _backend = backends[key]
        else:
            redis_url, replica_url = mod_valkey_service(port)
            config = {
                "type": "redis",
                "master": redis_url,
                "servants": [
                    replica_url,
                ],
            }

            _backend = Backend(config)
            backends[key] = _backend

        # mock redis client
        def _mock_redis_client_lua():
            return ComponentDefines().get_all()

        _master = cast(RedisBackendClient, _backend.master)
        _master._get_referred_components = _mock_redis_client_lua
        _backend.post_configure()
        return _backend

    yield _create_valkey_backend

    for backend in backends.values():
        await backend.close()


@pytest.fixture(params=["redis", "valkey"], scope="module")
def backend_name(request):
    """后端名称参数化fixture，返回当前的后端名称"""
    return request.param


# 要测试新的backend，请添加backend到params中
@pytest.fixture(scope="module")
def mod_auto_backend(request, backend_name) -> Callable[..., Backend]:
    """后端工厂，根据参数返回不同后端的工厂函数"""
    if backend_name == "redis":
        return request.getfixturevalue("mod_redis_backend")
    elif backend_name == "valkey":
        return request.getfixturevalue("mod_valkey_backend")
    # todo 增加一个redis cluster的docker compose，和backend
    else:
        raise ValueError("Unknown db type: %s" % backend_name)


# 要测试新的backend，请添加backend到params中
@pytest.fixture(scope="function")
def auto_backend(request, backend_name) -> Callable[..., Backend]:
    """后端工厂，根据参数返回不同后端的工厂函数"""
    if backend_name == "redis":
        return request.getfixturevalue("mod_redis_backend")
    elif backend_name == "valkey":
        return request.getfixturevalue("mod_valkey_backend")
    else:
        raise ValueError("Unknown db type: %s" % backend_name)
