import os
from typing import Callable, cast

import pytest
from redis.cluster import RedisCluster

from hetu.data.backend import Backend
from hetu.data.backend.redis import RedisBackendClient
from hetu.data.backend.redis_model import RedisModelClient
from hetu.data.backend.sqlite import SQLiteBackendClient
from hetu.data.backend.sqlite.store import SQLiteStore


@pytest.fixture(scope="module")
async def mod_redis_backend(ses_redis_service):
    """Redis后端工厂，返回创建Redis后端连接的工厂函数"""
    from hetu.data.component import ComponentDefines

    backends = {}

    # 支持创建多个backend连接
    def _create_redis_backend(key="main"):
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
    def _create_valkey_backend(key="main"):
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
    def _create_redis_backend(key="main"):
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
                "type": "sqlite",
                "master": dsn,
                "servants": [],
            }

            _backend = Backend(config)
            backends[key] = _backend

        def _mock_get_referred():
            return ComponentDefines().get_all()

        _master = cast(SQLiteBackendClient, _backend.master)
        _master._get_referred_components = _mock_get_referred
        _backend.post_configure()
        return _backend

    yield _create_sqlite_backend

    for backend in backends.values():
        await backend.close()


REDIS_BACKENDS = ["redis", "redis_cluster"]
REDIS_FORK_BACKENDS = ["valkey"]
SQLITE_BACKENDS = ["sqlite"]

# 允许通过环境变量过滤后端，用于CI/CD优化
# Allow filtering backends via environment variable for CI/CD optimization
_env_backends = os.environ.get("HETU_TEST_BACKENDS")
if _env_backends:
    _allowed = [b.strip() for b in _env_backends.split(",")]
    REDIS_BACKENDS = [b for b in REDIS_BACKENDS if b in _allowed]
    REDIS_FORK_BACKENDS = [b for b in REDIS_FORK_BACKENDS if b in _allowed]
    SQLITE_BACKENDS = [b for b in SQLITE_BACKENDS if b in _allowed]

ALL_BACKENDS = REDIS_BACKENDS + REDIS_FORK_BACKENDS + SQLITE_BACKENDS


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
    elif name == "sqlite":
        return request.getfixturevalue("mod_sqlite_backend")
    else:
        raise ValueError(f"Unknown db type: {name}")


def backend_config_by_name(name: str, request) -> dict:
    """返回 config.yml 里 BACKENDS[x] 形状的 dict，供需要从配置自建连接的测试
    （如 hetu.headless.connect、跨线程 event loop）使用。"""
    if name == "redis":
        redis_url, replica_url = request.getfixturevalue("ses_redis_service")
        return {"type": "redis", "master": redis_url, "servants": [replica_url]}
    elif name == "valkey":
        redis_url, replica_url = request.getfixturevalue("ses_valkey_service")
        return {"type": "redis", "master": redis_url, "servants": [replica_url]}
    elif name == "redis_cluster":
        redis_url = request.getfixturevalue("ses_redis_cluster_service")
        return {
            "type": "redis",
            "master": redis_url,
            "raw_clustering": True,
            "servants": [],
        }
    elif name == "sqlite":
        dsn = request.getfixturevalue("ses_sqlite_service")
        return {"type": "sqlite", "master": dsn, "servants": []}
    else:
        raise ValueError(f"Unknown db type: {name}")


@pytest.fixture(scope="module")
def mod_backend_config(request, backend_name) -> dict:
    """与 mod_auto_backend 指向同一个数据库的 BACKENDS[x] 配置 dict"""
    return backend_config_by_name(backend_name, request)


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


# ============ 直接读写底层数据（造数据、核对索引用） ============


def raw_index_zset_keys(backend: Backend, ref, index_name: str) -> list[str]:
    """
    索引名下现有的所有 zset key，排好序：单列索引的那一个，复合索引的各组 key（每组一个 zset），
    以及残留的另一种格式的 key
    """
    master = cast(RedisModelClient, backend.master)
    idx_key = master.index_key(ref, index_name)
    if isinstance(master, RedisBackendClient):
        io = master.io
        found = io.keys(idx_key + ":*", target_nodes=RedisCluster.PRIMARIES)  # type: ignore
        keys = [key.decode() for key in found]  # type: ignore
        if io.exists(idx_key):
            keys.append(idx_key)
        return sorted(keys)
    sqlite = cast(SQLiteBackendClient, master)
    keys = sqlite.run_sync_(SQLiteStore.zkeys_with_prefix, idx_key + ":")
    if sqlite.run_sync_(SQLiteStore.zmembers, idx_key):
        keys.append(idx_key)
    return sorted(keys)


def raw_index_members(backend: Backend, ref, index_name: str) -> list[bytes]:
    """
    索引的原始 member（值的可排序编码 + \\x00 + id），按顺序。Redis 读 zset，SQLite 读模拟 zset 的表。
    复合索引每组一个 zset，合并各组的（member 带着组前缀，合并后的顺序就是一个大 zset 的顺序）
    """
    master = cast(RedisModelClient, backend.master)
    members: list[bytes] = []
    for key in raw_index_zset_keys(backend, ref, index_name):
        if isinstance(master, RedisBackendClient):
            members.extend(master.io.zrange(key, 0, -1))  # type: ignore
        else:
            members.extend(
                cast(SQLiteBackendClient, master).run_sync_(SQLiteStore.zmembers, key)
            )
    return sorted(members)


def raw_zadd(backend: Backend, key: str, member: bytes) -> None:
    """绕过事务直接往 zset 里加一个 member（造残留的索引 key 用：不动行、不发通知）"""
    master = cast(RedisModelClient, backend.master)
    if isinstance(master, RedisBackendClient):
        master.io.zadd(key, {member: 0})  # type: ignore
        return

    def _add(store: SQLiteStore) -> None:
        with store.write_txn():
            store.zadd(key, member)

    cast(SQLiteBackendClient, master).run_sync_(_add)


def raw_hset(backend: Backend, ref, row_id: int, **fields: str) -> None:
    """绕过事务直接改行字段（造数据用：不改 _version、不动索引、不发通知）"""
    master = cast(RedisModelClient, backend.master)
    key = master.row_key(ref, row_id)
    if isinstance(master, RedisBackendClient):
        master.io.hset(key, mapping=fields)  # type: ignore
    else:
        cast(SQLiteBackendClient, master).run_sync_(SQLiteStore.hset_txn, key, fields)


def raw_key_count(backend: Backend, ref) -> int:
    """这张表（这个簇）名下还有几个 key：行 + 索引（Redis 的 zset 删空了自动消失）"""
    master = cast(RedisModelClient, backend.master)
    prefix = master.cluster_prefix(ref)
    if isinstance(master, RedisBackendClient):
        keys = master.io.keys(prefix + ":*", target_nodes=RedisCluster.PRIMARIES)  # type: ignore
        return len(keys)  # type: ignore

    def count(store: SQLiteStore) -> int:
        return len(store.row_ids(prefix)) + len(store.zkeys_with_prefix(prefix + ":"))

    return cast(SQLiteBackendClient, master).run_sync_(count)


def raw_seq_counter_key(backend: Backend, ref, **group) -> str:
    """提交序号列（commit_seq）一个组的计数器 key，group 是 {分组列: 值}，不分组时不给"""
    from hetu.data.backend.base import group_prefix_, resolve_group_

    master = cast(RedisModelClient, backend.master)
    comp = ref.comp_cls
    values = resolve_group_(comp, comp.commit_seq_, group or None)
    prefix = group_prefix_(comp, comp.commit_seq_, values)
    return master.seq_counter_key_(master.cluster_prefix(ref), comp, prefix)


def raw_kv_get(backend: Backend, key: str) -> bytes | None:
    """读一个字符串 key（计数器）：Redis GET，SQLite 读 KV 表"""
    master = cast(RedisModelClient, backend.master)
    if isinstance(master, RedisBackendClient):
        return master.io.get(key)  # type: ignore
    return cast(SQLiteBackendClient, master).run_sync_(SQLiteStore.kv_get, key)


def raw_kv_set(backend: Backend, key: str, value: bytes) -> None:
    """绕过事务直接写一个字符串 key（造计数器用）"""
    master = cast(RedisModelClient, backend.master)
    if isinstance(master, RedisBackendClient):
        master.io.set(key, value)  # type: ignore
        return

    def _set(store: SQLiteStore) -> None:
        with store.write_txn():
            store.kv_set(key, value)

    cast(SQLiteBackendClient, master).run_sync_(_set)


def raw_kv_keys(backend: Backend, prefix: str) -> list[str]:
    """以 prefix 开头的字符串 key（计数器），排好序"""
    master = cast(RedisModelClient, backend.master)
    if isinstance(master, RedisBackendClient):
        found = master.io.keys(prefix + "*", target_nodes=RedisCluster.PRIMARIES)  # type: ignore
        return sorted(
            key.decode()
            for key in found  # type: ignore
            if master.io.type(key) in (b"string", "string")  # type: ignore
        )
    items = cast(SQLiteBackendClient, master).run_sync_(
        SQLiteStore.kv_items_with_prefix, prefix
    )
    return [key for key, _value in items]
