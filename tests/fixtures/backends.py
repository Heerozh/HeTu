import os
from typing import Callable, cast

import pytest

from hetu.data.backend import Backend
from hetu.data.backend.redis import RedisBackendClient
from hetu.data.backend.sql import SQLBackendClient


@pytest.fixture(scope="module")
async def mod_redis_backend(ses_redis_service):
    """Redis后端工厂，返回创建Redis后端连接的工厂函数"""
    from hetu.data.component import ComponentDefines

    backends = {}

    # 支持创建多个backend连接
    def _create_redis_backend(key="main", port=23318):
        if key in backends:
            _backend = backends[key]
        else:
            redis_url, replica_url = ses_redis_service
            config = {
                "type": "redis",
                "master": redis_url,
                "servants": [
                    replica_url,
                ],
            }

            _backend = Backend(config)
            # io = cast(RedisBackendClient, _backend.master).io
            # io.flushall()
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
async def mod_valkey_backend(ses_valkey_service):
    """valkey后端工厂fixture，返回创建valkey后端的工厂函数"""
    from hetu.data.component import ComponentDefines

    backends = {}

    # 支持创建多个backend连接
    def _create_valkey_backend(key="main", port=23418):
        if key in backends:
            _backend = backends[key]
        else:
            redis_url, replica_url = ses_valkey_service
            config = {
                "type": "redis",
                "master": redis_url,
                "servants": [
                    replica_url,
                ],
            }

            _backend = Backend(config)
            # io = cast(RedisBackendClient, _backend.master).io
            # io.flushall()
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


@pytest.fixture(scope="module")
async def mod_redis_cluster_backend(ses_redis_cluster_service):
    """Redis后端工厂，返回创建Redis后端连接的工厂函数"""
    from hetu.data.component import ComponentDefines

    backends = {}

    # 支持创建多个backend连接
    def _create_redis_backend(key="main", port=23318):
        if key in backends:
            _backend = backends[key]
        else:
            redis_url = ses_redis_cluster_service
            config = {
                "type": "redis",
                "master": redis_url,
                "raw_clustering": True,
                "servants": [],
            }

            _backend = Backend(config)
            # io = cast(RedisBackendClient, _backend.master).io
            # io.flushall()
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
async def mod_postgres_backend(ses_postgres_service):
    """PostgreSQL后端工厂fixture，返回创建postgres后端的工厂函数"""
    from hetu.data.component import ComponentDefines

    backends = {}

    def _create_postgres_backend(key="main"):
        if key in backends:
            _backend = backends[key]
        else:
            dsn = ses_postgres_service
            config = {
                "type": "sql",
                "master": dsn,
                "servants": [],
            }

            _backend = Backend(config)
            backends[key] = _backend

        # mock _get_referred_components
        def _mock_get_referred():
            return ComponentDefines().get_all()

        _master = cast(SQLBackendClient, _backend.master)
        _master._get_referred_components = _mock_get_referred
        _backend.post_configure()
        return _backend

    yield _create_postgres_backend

    for backend in backends.values():
        await backend.close()


@pytest.fixture(scope="module")
async def mod_sqlite_backend(ses_sqlite_service):
    """SQLite后端工厂fixture，返回创建sqlite后端的工厂函数"""
    from hetu.data.component import ComponentDefines

    backends = {}

    def _create_sqlite_backend(key="main"):
        if key in backends:
            _backend = backends[key]
        else:
            dsn = ses_sqlite_service
            config = {
                "type": "sql",
                "master": dsn,
                "servants": [],
            }

            _backend = Backend(config)
            backends[key] = _backend

        def _mock_get_referred():
            return ComponentDefines().get_all()

        _master = cast(SQLBackendClient, _backend.master)
        _master._get_referred_components = _mock_get_referred
        _backend.post_configure()
        return _backend

    yield _create_sqlite_backend

    for backend in backends.values():
        await backend.close()


@pytest.fixture(scope="module")
async def mod_mariadb_backend(ses_mariadb_service):
    """MariaDB后端工厂fixture，返回创建mariadb后端的工厂函数"""
    from hetu.data.component import ComponentDefines

    backends = {}

    def _create_mariadb_backend(key="main"):
        if key in backends:
            _backend = backends[key]
        else:
            dsn = ses_mariadb_service
            config = {
                "type": "sql",
                "master": dsn,
                "servants": [],
            }

            _backend = Backend(config)
            backends[key] = _backend

        def _mock_get_referred():
            return ComponentDefines().get_all()

        _master = cast(SQLBackendClient, _backend.master)
        _master._get_referred_components = _mock_get_referred
        _backend.post_configure()
        return _backend

    yield _create_mariadb_backend

    for backend in backends.values():
        await backend.close()

REDIS_BACKENDS = ["redis", "redis_cluster"]
REDIS_FORK_BACKENDS = ["valkey"]
SQL_BACKENDS = ["postgres", "sqlite", "mariadb"]

# 允许通过环境变量过滤后端，用于CI/CD优化
# Allow filtering backends via environment variable for CI/CD optimization
_env_backends = os.environ.get("HETU_TEST_BACKENDS")
if _env_backends:
    _allowed = [b.strip() for b in _env_backends.split(",")]
    REDIS_BACKENDS = [b for b in REDIS_BACKENDS if b in _allowed]
    REDIS_FORK_BACKENDS = [b for b in REDIS_FORK_BACKENDS if b in _allowed]
    SQL_BACKENDS = [b for b in SQL_BACKENDS if b in _allowed]

ALL_BACKENDS = REDIS_BACKENDS + REDIS_FORK_BACKENDS + SQL_BACKENDS


@pytest.fixture(params=ALL_BACKENDS, scope="module")
def backend_name(request):
    """后端名称参数化fixture，返回当前的后端名称"""
    return request.param


def backend_fixture_by_name(name: str, request):
    """根据后端名称返回对应的fixture名称"""
    if name == "redis":
        return request.getfixturevalue("mod_redis_backend")
    elif name == "valkey":
        return request.getfixturevalue("mod_valkey_backend")
    elif name == "redis_cluster":
        return request.getfixturevalue("mod_redis_cluster_backend")
    elif name == "postgres":
        return request.getfixturevalue("mod_postgres_backend")
    elif name == "sqlite":
        return request.getfixturevalue("mod_sqlite_backend")
    elif name == "mariadb":
        return request.getfixturevalue("mod_mariadb_backend")
    else:
        raise ValueError("Unknown db type: %s" % backend_name)


# 要测试新的backend，请添加backend到params中
@pytest.fixture(scope="module")
def mod_auto_backend(request, backend_name) -> Callable[..., Backend]:
    """后端工厂，根据参数返回不同后端的工厂函数"""
    return backend_fixture_by_name(backend_name, request)


# 要测试新的backend，请添加backend到params中
@pytest.fixture(scope="function")
def auto_backend(request, backend_name) -> Callable[..., Backend]:
    """后端工厂，根据参数返回不同后端的工厂函数"""
    return backend_fixture_by_name(backend_name, request)


def use_redis_backend_only(func):
    """自定义装饰器：只使用 Redis社区版 数据库运行测试"""
    return pytest.mark.parametrize("backend_name", REDIS_BACKENDS, indirect=True)(func)


def use_redis_family_backend_only(func):
    """自定义装饰器：使用 所有Redis兼容 数据库运行测试"""
    return pytest.mark.parametrize(
        "backend_name", REDIS_BACKENDS + REDIS_FORK_BACKENDS, indirect=True
    )(func)
