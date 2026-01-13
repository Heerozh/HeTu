import time

import numpy as np
import pytest
from fixtures.backends import use_redis_family_backend_only
from redis.asyncio.cluster import RedisCluster


def test_multimap():
    from hetu.common.multimap import MultiMap

    primary = [2, 4, 1, 3, 3, 4, 2, 3]
    second_ = [5, 3, 2, 1, 9, 8, 7, 6]
    d = MultiMap()
    for i, _ in enumerate(primary):
        d.add(primary[i], second_[i])

    # 测试查询
    np.testing.assert_array_equal(d.iloc(d.index(2)), np.array([5, 7]))
    np.testing.assert_array_equal(d.iloc(d.index(1)), np.array([2]))
    sel = d.index(2)
    np.testing.assert_array_equal(
        d.iloc(slice(sel.stop - 1, sel.start - 1, -1)), np.array([7, 5])
    )
    assert d.count() == 8
    assert d.count(0, 3) == 6
    assert d.count(5) == 0
    assert d.count(0) == 0
    np.testing.assert_array_equal(d.query(0), [])
    np.testing.assert_array_equal(d.query(1), [2])
    np.testing.assert_array_equal(d.query(0, 3), [2, 5, 7, 1, 6, 9])
    np.testing.assert_array_equal(d.query(3, 99), [1, 6, 9, 3, 8])
    np.testing.assert_array_equal(d.query(99), [])

    # 测试添加
    d.add(2, 3)
    d.add(2, 9)
    np.testing.assert_array_equal(d.iloc(d.index(2)), np.array([3, 5, 7, 9]))

    # 测试删除
    d.remove(2, 5)
    np.testing.assert_array_equal(d.iloc(d.index(2)), np.array([3, 7, 9]))

    # 测试再次添加
    d.add(2, 5)
    np.testing.assert_array_equal(d.iloc(d.index(2)), np.array([3, 5, 7, 9]))

    # 测试pop
    np.testing.assert_array_equal(d.pop(2, 3), [3, 5, 7, 9, 1, 6, 9])
    np.testing.assert_array_equal(d.query(2, 3), [])


async def test_snowflake_id(monkeypatch):
    from hetu.common.snowflake_id import SnowflakeID

    generator = SnowflakeID()
    generator.init(worker_id=1)

    # Mock time to be after TW_EPOCH (2025-12-18) to ensure positive IDs
    # TW_EPOCH = 1766000000000 ms
    # Set start time to TW_EPOCH/1000 + 1000s
    start_ts = 1766000000.0 + 1000.0

    monkeypatch.setattr("hetu.common.snowflake_id.time", lambda: start_ts)
    # 1. Test structure
    id_val = await generator.next_id_async()
    assert id_val > 0

    # worker_id is 1 (10 bits)
    # Structure: ... | worker(10) | seq(12)
    worker_id_extracted = (id_val >> 12) & 0x3FF
    assert worker_id_extracted == 1

    # 2. Test uniqueness and monotonic increase in same millisecond
    ids = []
    count = 100
    for _ in range(count):
        new_id = await generator.next_id_async()
        ids.append(new_id)

    assert len(set(ids)) == count
    assert ids == sorted(ids)

    # Check sequence increment
    # Since time is frozen by mock, sequence should increment
    first_seq = ids[0] & 0xFFF
    last_seq = ids[-1] & 0xFFF
    assert last_seq == first_seq + count - 1

    # 测试时间回拨
    monkeypatch.setattr("hetu.common.snowflake_id.time", lambda: start_ts - 2)

    id_val_rollback = await generator.next_id_async()
    assert id_val_rollback > 0
    assert id_val_rollback >= ids[-1]

    # 3. Test invalid init
    with pytest.raises(ValueError):
        generator.init(worker_id=1024)

    with pytest.raises(ValueError):
        generator.init(worker_id=-1)

    # Restore valid state
    generator.init(worker_id=1)


@pytest.mark.timeout(2)
async def test_snowflake_id_sleep(monkeypatch):
    """测试sleep"""
    from hetu.common.snowflake_id import TIME_ROLLBACK_TOLERANCE_MS, SnowflakeID

    # Mock time
    start_ts = 1766000000.0
    monkeypatch.setattr("hetu.common.snowflake_id.time", lambda: start_ts)

    generator = SnowflakeID()
    generator.init(worker_id=1)

    # 加上启动需要的 TIME_ROLLBACK_TOLERANCE_MS
    start_ts = 1766000000.0 + TIME_ROLLBACK_TOLERANCE_MS

    sleep_called = 0

    async def mock_sleep(_):
        nonlocal sleep_called
        sleep_called += 1
        monkeypatch.setattr("hetu.common.snowflake_id.time", lambda: start_ts + 1)
        return

    monkeypatch.setattr("asyncio.sleep", mock_sleep)
    last_id = 0
    for _ in range(4099):
        last_id = await generator.next_id_async()

    # 4096 IDs + 3 for the sleep
    assert sleep_called == 1
    assert last_id & 0xFFF == 2


@use_redis_family_backend_only
async def test_redis_worker_keeper(mod_auto_backend):
    redis = mod_auto_backend()
    redis_client = redis.master.aio

    # 清空数据
    keys_to_delete = await redis_client.keys(
        "snowflake:*", target_nodes=RedisCluster.PRIMARIES
    )
    if keys_to_delete:
        await redis_client.delete(*keys_to_delete)

    from hetu.data.backend.redis.worker_keeper import RedisWorkerKeeper

    worker_keeper = RedisWorkerKeeper(0, redis.master.io, redis_client)

    # 测试获得id
    worker_id = worker_keeper.get_worker_id()
    assert worker_id == 0

    # 再次获得应该id一样
    worker_id_again = worker_keeper.get_worker_id()
    assert worker_id_again == worker_id

    # 模拟另一个机器
    worker_keeper2 = RedisWorkerKeeper(1, redis.master.io, redis_client)
    worker_id_2 = worker_keeper2.get_worker_id()
    assert worker_id_2 == 1

    # 删除第一个机器的key，模拟key值过期释放worker id
    worker_keeper.release_worker_id()

    # 再次获得应该id一样
    worker_id_again = worker_keeper2.get_worker_id()
    assert worker_id_again == worker_id_2

    # 测试续约
    # 手动快过期
    expire = await redis_client.expire(
        f"{worker_keeper2.worker_id_key}:{worker_id_2}", 20
    )
    assert expire <= 20
    # 续约
    ts = int(time.time() * 1000) + 1230
    await worker_keeper2.keep_alive(ts)
    last_ts = worker_keeper2.get_last_timestamp()
    assert last_ts == ts
    expire = await redis_client.ttl(f"{worker_keeper2.worker_id_key}:{worker_id_2}")
    assert expire > 60 - 1
