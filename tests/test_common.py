import logging
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

    worker_keeper = RedisWorkerKeeper(0, redis_client)

    # 测试获得id
    worker_id = await worker_keeper.get_worker_id()
    assert worker_id == 0

    # 再次获得应该id一样
    worker_id_again = await worker_keeper.get_worker_id()
    assert worker_id_again == worker_id

    # 模拟另一个机器
    worker_keeper2 = RedisWorkerKeeper(1, redis_client)
    worker_id_2 = await worker_keeper2.get_worker_id()
    assert worker_id_2 == 1

    # 删除第一个机器的key，模拟key值过期释放worker id
    await worker_keeper.release_worker_id()

    # 再次获得应该id一样
    worker_id_again = await worker_keeper2.get_worker_id()
    assert worker_id_again == worker_id_2

    # 测试续约
    # 手动快过期
    expire = await redis_client.expire(
        f"{worker_keeper2.worker_id_key}:{worker_id_2}", 20
    )
    assert expire <= 20
    # 续约（只管租约，时间戳高水位已拆给 SnowflakeTimestampKeeper）
    await worker_keeper2.keep_alive()
    expire = await redis_client.ttl(f"{worker_keeper2.worker_id_key}:{worker_id_2}")
    assert expire > 60 - 1


@use_redis_family_backend_only
async def test_redis_keeper_detects_stolen_lease(mod_auto_backend):
    """续约必须能发现租约被别人抢走——这是会产生重复雪花ID的那种情况。

    修复前用的是 `EXPIRE`，它只看 key 在不在、不看 value，被 SET NX 抢走后照样返回1，
    于是两个 worker 拿着同一个 Worker ID 继续发号。
    """
    redis = mod_auto_backend()
    redis_client = redis.master.aio
    keys = await redis_client.keys("snowflake:*", target_nodes=RedisCluster.PRIMARIES)
    if keys:
        await redis_client.delete(*keys)

    from hetu.data.backend.redis.worker_keeper import RedisWorkerKeeper

    victim = RedisWorkerKeeper(500, redis_client)
    worker_id = await victim.get_worker_id()

    # 正常情况下续约成功
    await victim.keep_alive()

    # 模拟：victim 卡住太久导致租约过期，thief 用 SET NX 抢走了同一个 id
    await redis_client.delete(victim._key(worker_id))
    thief = RedisWorkerKeeper(501, redis_client)
    assert await thief.get_worker_id() == worker_id

    # victim 醒来续约，必须发现自己已经不是持有者
    with pytest.raises(SystemExit):
        await victim.keep_alive()

    # 而且不能把 thief 的租约刷掉或删掉
    assert await redis_client.get(victim._key(worker_id)) is not None
    await victim.release_worker_id()  # compare-and-delete：不是自己的就不该删
    assert await redis_client.get(thief._key(worker_id)) is not None
    await thief.keep_alive()  # thief 完全不受影响


async def test_fixed_worker_keeper(monkeypatch):
    """开发模式分配器：用本机进程序号，零协调"""
    from hetu.data.backend.worker_keeper import FixedWorkerKeeper

    monkeypatch.delenv("SANIC_WORKER_IDENTIFIER", raising=False)
    assert await FixedWorkerKeeper().get_worker_id() == 0  # 单进程模式

    monkeypatch.setenv("SANIC_WORKER_IDENTIFIER", "Srv 3")
    keeper = FixedWorkerKeeper()
    assert await keeper.get_worker_id() == 3
    assert await keeper.get_worker_id() == 3  # 幂等

    monkeypatch.setenv("SANIC_WORKER_IDENTIFIER", "Srv12")  # sanic对两位数不留空格
    assert await FixedWorkerKeeper().get_worker_id() == 12

    # 续约/释放都是空操作，不该抛异常
    await keeper.keep_alive()
    await keeper.release_worker_id()


async def test_worker_keeper_factory_picks_by_backend(
    mod_sqlite_backend, mod_auto_backend, monkeypatch, tmp_path
):
    """按后端类型自动选分配器，不给用户留选错模式的机会"""
    monkeypatch.chdir(tmp_path)
    from hetu.data.backend.redis.worker_keeper import RedisWorkerKeeper
    from hetu.data.backend.worker_keeper import FixedWorkerKeeper, create_worker_keeper

    assert isinstance(create_worker_keeper(mod_sqlite_backend(), 1), FixedWorkerKeeper)

    backend = mod_auto_backend()
    expected = (
        RedisWorkerKeeper
        if type(backend.master).__name__ == "RedisBackendClient"
        else FixedWorkerKeeper
    )
    assert isinstance(create_worker_keeper(backend, 1), expected)


async def _raise_backend_error(*_args, **_kwargs):
    raise ConnectionError("模拟后端读失败")


def _make_lease_table(backend):
    """建好 WorkerLease 的表。真实服务器里由 check_and_create_new_tables 在开服时建，
    测试里直接构造 Table 不会碰数据库，而 direct_set 是裸 UPDATE，表不存在会直接报错。"""
    from hetu.data.backend.table import Table
    from hetu.data.backend.worker_keeper import WorkerLease

    table = Table(WorkerLease, "pytest", 1, backend)
    maint = table.backend.get_table_maintenance()
    if maint.check_table(table)[0] == "not_exists":
        maint.create_table(table)
    return table


async def test_snowflake_timestamp_keeper(
    mod_sqlite_backend, monkeypatch, tmp_path, caplog
):
    """时间戳高水位：独立于租约的读写语义，只需要单调max"""
    monkeypatch.chdir(tmp_path)
    backend = mod_sqlite_backend()

    from hetu.data.backend.snowflake_timestamp import (
        TIMESTAMP_SAVE_INTERVAL,
        SnowflakeTimestampKeeper,
    )

    table = _make_lease_table(backend)
    pad_ms = TIMESTAMP_SAVE_INTERVAL * 1000
    now_ms = int(time.time() * 1000)
    ts_keeper = SnowflakeTimestampKeeper(table, 7)

    # "读到了空"（行不存在/水位为0）= 确认没发过号 → 用当前时间，绝不能钳制。
    # 只有"读不出来"（后端异常）才该退化成 init 的兜底值，见 load 的文档
    assert abs(await ts_keeper.load() - now_ms) < 1000

    # SQL后端的 direct_set 是 UPDATE，行不存在就静默无效；GeneralWorkerKeeper 删掉后
    # 没人替本类建行了，所以它必须自己补建，否则水位永远写不进去
    await ts_keeper.save(now_ms - 60_000)
    assert await ts_keeper.load() >= now_ms

    # 后端读异常时才返回-1，把回拨保护交还给 SnowflakeID.init
    broken = SnowflakeTimestampKeeper(table, 8)
    with caplog.at_level(logging.WARNING, logger="HeTu.root"):
        monkeypatch.setattr(
            type(table.backend.master),
            "get",
            _raise_backend_error,
        )
        assert await broken.load() == -1
    monkeypatch.undo()

    # 关键用例：水位高于当前时间（模拟重启期间时钟回拨），必须返回水位而不是当前时间，
    # 否则会拿回拨后的时间重新发号，撞上关服前已经用过的时间戳
    future_ms = now_ms + 30_000
    await ts_keeper.save(future_ms)
    assert await ts_keeper.load() == future_ms + pad_ms

    # 崩溃时最后一个写入间隔内发出的ID其时间戳已超过记录值，读回时必须补上这一段
    edge_ms = now_ms + 1000  # 水位仅略高于当前时间，不补就会重发这段时间的ID
    await ts_keeper.save(edge_ms)
    assert await ts_keeper.load() == edge_ms + pad_ms

    # 补建是幂等的：同一个 worker_id 的第二个 keeper 实例不该因为撞主键而抛异常
    second = SnowflakeTimestampKeeper(table, 7)
    with caplog.at_level(logging.WARNING, logger="HeTu.root"):
        await second.save(edge_ms)
    assert await second.load() == edge_ms + pad_ms


async def test_boot_has_no_snowflake_clamp(mod_sqlite_backend, monkeypatch, tmp_path):
    """回归：开服（首次和重启）都不该让雪花ID起始时间戳超前于当前时间。

    超前会把时间戳钳在同一毫秒，总容量塌缩成4096个ID，发完就1ms一睡地空转直到墙钟追上，
    期间还每发一个ID刷一条"时钟回拨"警告。曾因为抢租约时顺手把 last_timestamp 写成 now、
    读回时又加了写入间隔补偿，导致每次开服都白背一个5秒降级窗口。
    """
    monkeypatch.chdir(tmp_path)
    backend = mod_sqlite_backend()

    from hetu.common.snowflake_id import SnowflakeID
    from hetu.data.backend.snowflake_timestamp import SnowflakeTimestampKeeper
    from hetu.data.backend.worker_keeper import create_worker_keeper

    table = _make_lease_table(backend)

    for label in ("首次开服", "重启"):
        # 完整走一遍 worker_start 的流程：分配id → 读水位 → 初始化发号器
        worker_id = await create_worker_keeper(backend, 301).get_worker_id()
        loaded = await SnowflakeTimestampKeeper(table, worker_id).load()
        now_ms = int(time.time() * 1000)
        assert loaded - now_ms < 1000, f"{label}时发号器起始时间戳超前了"

        # 判据不是"能连发多少个"——每毫秒4096本来就是硬上限，发多少取决于机器速度；
        # 而是"耗尽后睡一下容量能不能恢复"：被钳在未来时，墙钟追上之前睡多久都恢复不了
        generator = SnowflakeID()
        generator.init(worker_id, loaded)
        for _ in range(20000):
            if generator._next_id() is None:
                break
        time.sleep(0.01)
        assert generator._next_id() is not None, (
            f"{label}时发号容量耗尽后睡10ms仍未恢复，说明起始时间戳被钳在了未来"
        )
