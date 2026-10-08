import asyncio
import logging
import os
import subprocess
import sys
import time
from typing import Any

import pytest
from fixtures.backends import raw_hset, use_redis_family_backend_only
from redis.asyncio.cluster import RedisCluster

# 导入即注册 core 组件 WorkerLease，必须赶在 mod_test_app 建簇之前，不然簇里没有它
from hetu.server.main import close_backends, start_backends


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
    测试里直接构造 Table 不会碰数据库，要自己建。"""
    from hetu.data.backend.table import Table
    from hetu.data.backend.worker_keeper import WorkerLease

    table = Table(WorkerLease, "pytest", 1, backend)
    maint = table.backend.get_table_maintenance()
    if maint.check_table(table)[0] == "not_exists":
        maint.create_table(table)
    return table


async def test_snowflake_timestamp_keeper(
    mod_auto_backend, monkeypatch, tmp_path, caplog
):
    """时间戳高水位：独立于租约的读写语义，只需要单调max"""
    monkeypatch.chdir(tmp_path)
    backend = mod_auto_backend()

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

    # 首次写入前必须先把行建好（GeneralWorkerKeeper 删掉后没人替本类建行了）：
    # direct_set 只改已存在的行，缺行时什么都不写
    await ts_keeper.save(now_ms - 60_000)
    row = await backend.master.get(table, 7)
    assert row is not None and row.id == 7 and row.last_timestamp == now_ms - 60_000
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
    # 否则会拿回拨后的时间重新发号，撞上关服前已经用过的时间戳。save 写的是正常关服的
    # 精确值，原样读回，不再补写入间隔
    future_ms = now_ms + 30_000
    await ts_keeper.save(future_ms)
    assert await ts_keeper.load() == future_ms

    # 周期写入的预留值往前多留一个间隔：崩溃前最后那段发出的ID来不及记录，靠它兜住
    await ts_keeper.reserve(future_ms)
    assert await ts_keeper.load() == future_ms + pad_ms
    # 空闲时 last_timestamp 早就落后于当前时间，要按当前时间预留，否则盖不住接下来发的ID
    before_ms = int(time.time() * 1000)
    await ts_keeper.reserve(before_ms - 60_000)
    reserved = await ts_keeper.load()
    assert before_ms + pad_ms <= reserved <= int(time.time() * 1000) + pad_ms

    # 补建是幂等的：同一个 worker_id 的第二个 keeper 实例不该因为撞主键而抛异常
    second = SnowflakeTimestampKeeper(table, 7)
    with caplog.at_level(logging.WARNING, logger="HeTu.root"):
        await second.save(future_ms)
    assert await second.load() == future_ms


async def test_snowflake_timestamp_keeper_legacy_partial_row(mod_auto_backend):
    """旧版本的 save 先 direct_set 再确认行在不在，Redis 上给缺行建出了只有
    last_timestamp、缺 id 的残缺 hash：之后按 STRUCT 读就 KeyError，每次重启 load 都
    退化成固定容忍度。已部署的库里还留着这种行，load 要照样读出它的水位，save 照常写"""
    from hetu.data.backend import RowFormat
    from hetu.data.backend.snowflake_timestamp import SnowflakeTimestampKeeper

    backend = mod_auto_backend()
    table = _make_lease_table(backend)
    worker_id = 9
    # 高于当前时间，才看得出读回的是不是这个水位
    stored = int(time.time() * 1000) + 30_000
    # 旧版本的 direct_set 是裸 HSET，行还不存在时就建出这样的残缺行（现在的 direct_set
    # 缺行不写，这里直接造）
    raw_hset(backend, table, worker_id, last_timestamp=str(stored))
    raw = await backend.master.get(table, worker_id, RowFormat.RAW)
    assert raw is not None and "id" not in raw

    keeper = SnowflakeTimestampKeeper(table, worker_id)
    assert await keeper.load() == stored
    await keeper.save(stored + 1)
    restarted = SnowflakeTimestampKeeper(table, worker_id)
    assert await restarted.load() == stored + 1


async def test_snowflake_timestamp_keeper_recreates_deleted_row(mod_auto_backend):
    """运行中行被删掉了（比如开着服跑了 `hetu upgrade`，它会清空易失的 WorkerLease 表），
    下一次 save 要把完整的行重新建出来。direct_set 只改已存在的行、缺行时什么都不写，
    不看它的返回值的话，水位从此静默地再也写不进去"""
    from hetu.data.backend.snowflake_timestamp import SnowflakeTimestampKeeper
    from hetu.data.backend.worker_keeper import WorkerLease

    backend = mod_auto_backend()
    table = _make_lease_table(backend)
    worker_id = 10
    stored = int(time.time() * 1000) + 30_000
    keeper = SnowflakeTimestampKeeper(table, worker_id)
    await keeper.save(stored)  # 首次写入：建行
    await keeper.save(stored + 1)  # 之后走 direct_set

    async with table.session() as session:
        repo = session.using(WorkerLease)
        assert await repo.get(id=worker_id) is not None
        repo.delete(worker_id)
    assert await backend.master.get(table, worker_id) is None

    await keeper.save(stored + 2)
    row = await backend.master.get(table, worker_id)
    assert row is not None and row.id == worker_id
    assert row.last_timestamp == stored + 2
    assert await SnowflakeTimestampKeeper(table, worker_id).load() == stored + 2


async def test_snowflake_timestamp_keeper_legacy_direct_set_contract():
    """第三方后端的 direct_set 还按老契约什么都不返回（None）：不能当成行没了，每个周期
    都去补建、撞主键、刷警告。只有明确返回 False 才是行没了"""
    from types import SimpleNamespace

    from hetu.data.backend.snowflake_timestamp import SnowflakeTimestampKeeper

    writes: list[dict] = []
    created: list[int] = []

    async def direct_set(worker_id, **kwargs):
        writes.append(kwargs)

    async def create_row(last_timestamp):
        created.append(last_timestamp)

    async def get(*_args, **_kwargs):
        return {"id": "3", "last_timestamp": "1"}  # 行在

    table = SimpleNamespace(
        direct_set=direct_set, backend=SimpleNamespace(master=SimpleNamespace(get=get))
    )
    keeper = SnowflakeTimestampKeeper(table, 3)  # type: ignore[arg-type]
    keeper._row_ready = True  # 行已确认存在
    keeper._create_row = create_row  # type: ignore[method-assign]
    await keeper.save(123)
    await keeper.save(456)
    assert created == []
    assert writes == [{"last_timestamp": "123"}, {"last_timestamp": "456"}]


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


def _server_app(backend_config: dict) -> Any:
    """够 start_backends/close_backends 用的最小 app 替身（config 要能按属性读）"""
    from types import SimpleNamespace

    class Config(dict):
        __getattr__ = dict.__getitem__

    config = Config(
        NAMESPACE="pytest",
        # 独立 instance：别的模块在 server1 等 instance 上按各自的簇建过表，同名会撞
        # cluster_mismatch
        INSTANCES=["snowflake_restart"],
        BACKENDS={"main": backend_config},
    )
    return SimpleNamespace(config=config, ctx=SimpleNamespace(), stop=lambda: None)


async def test_restart_resumes_from_snowflake_watermark(
    mod_test_app, mod_backend_config
):
    """开关服的水位接线：正常关服后马上重启不该被钳在未来，崩溃后重启要从开服时预留的
    水位接着发。

    以前补偿在读端，一律补一个写入间隔：正常关服后5秒内重启也被钳在未来，关服又把钳住
    的值原样写回，连续快速重启越推越远——test_websocket 每个用例起停一次服务器，跑完
    超前一分钟，漏给同进程后面的用例，每发一个号刷一条"时钟回拨"告警。
    """
    from hetu.common.snowflake_id import SnowflakeID
    from hetu.data.backend.snowflake_timestamp import TIMESTAMP_SAVE_INTERVAL

    generator = SnowflakeID()
    for label in ("首次开服", "正常关服后重启", "再次重启"):
        app = _server_app(mod_backend_config)
        await start_backends(app)
        ahead = generator.last_timestamp - int(time.time() * 1000)
        assert ahead < 1000, f"{label}时发号器起始时间戳超前了 {ahead} 毫秒"
        generator.next_id()
        await close_backends(app)

    # 崩溃：不走 close_backends 直接断开。开服时预留的水位盖住了崩溃前发出的所有ID，
    # 重启必须从它接着发，停机期间时钟被拨回也不会重复
    boot_ms = int(time.time() * 1000)
    app = _server_app(mod_backend_config)
    await start_backends(app)
    generator.next_id()
    await app.ctx.default_backend.close()

    app = _server_app(mod_backend_config)
    await start_backends(app)
    assert generator.last_timestamp >= boot_ms + TIMESTAMP_SAVE_INTERVAL * 1000
    await close_backends(app)


async def test_snowflake_lease_fence():
    """发号围栏：租约超出安全期就拒绝发号，而不是继续发可能重复的ID"""
    from hetu.common.snowflake_id import (
        SnowflakeID,
        WorkerKeeper,
        WorkerLeaseExpired,
    )

    class FakeKeeper(WorkerKeeper):
        pass

    keeper = FakeKeeper()
    generator = SnowflakeID()

    # 安全期内正常发号
    keeper.lease_deadline = time.monotonic() + 60
    generator.init(worker_id=1, lease=keeper)
    assert generator.next_id() > 0

    # 超出安全期就拒绝。注意不能是 RaceCondition —— SystemCaller 会重试，而围栏跳闸后
    # 重试多少次都是跳闸，只会空转到 max_retry
    from hetu.data.backend.base import RaceCondition

    keeper.lease_deadline = time.monotonic() - 0.001
    with pytest.raises(WorkerLeaseExpired):
        generator.next_id()
    assert not issubclass(WorkerLeaseExpired, RaceCondition)

    # 续约成功推进安全期后恢复发号
    keeper.lease_deadline = time.monotonic() + 60
    assert generator.next_id() > 0

    # 不提供租约的分配器（开发模式）不启用围栏
    keeper.lease_deadline = None
    assert generator.next_id() > 0
    generator.init(worker_id=1)  # 完全不传lease
    assert generator.next_id() > 0


@use_redis_family_backend_only
async def test_redis_keeper_arms_fence(mod_auto_backend):
    """Redis租约必须武装围栏，且安全期要留出余量、抢到的那一刻就生效"""
    from hetu.data.backend.redis.worker_keeper import (
        FENCE_MARGIN_SEC,
        WORKER_ID_EXPIRE_SEC,
        RedisWorkerKeeper,
    )

    redis = mod_auto_backend()
    redis_client = redis.master.aio
    keys = await redis_client.keys("snowflake:*", target_nodes=RedisCluster.PRIMARIES)
    if keys:
        await redis_client.delete(*keys)

    keeper = RedisWorkerKeeper(600, redis_client)
    assert keeper.lease_deadline is None  # 还没拿到id时围栏不该生效

    await keeper.get_worker_id()
    # 抢到就武装，不用等第一次续约（那要5秒后）
    assert keeper.lease_deadline is not None
    # 安全期必须早于Redis侧的过期时刻，留出余量给时钟漂移。用发起请求前的时刻算，所以
    # 它一定不晚于 "现在 + TTL - 余量"
    now = time.monotonic()
    assert keeper.lease_deadline <= now + WORKER_ID_EXPIRE_SEC - FENCE_MARGIN_SEC
    assert keeper.lease_deadline > now

    # 续约成功要推进安全期
    old = keeper.lease_deadline
    time.sleep(0.01)
    await keeper.keep_alive()
    assert keeper.lease_deadline > old


def test_batched():
    from hetu.common.helper import batched

    assert list(batched("ABCDEFG", 3)) == [("A", "B", "C"), ("D", "E", "F"), ("G",)]
    with pytest.raises(ValueError):
        list(batched([1, 2], 0))


def _fake_fs(monkeypatch, files: dict[str, str | Exception]):
    """让 helper 看到的文件系统只有 files：值为内容，或读取时抛出的异常"""
    import io
    from types import SimpleNamespace

    from hetu.common import helper

    def fake_open(path, *args, **kwargs):
        content = files[path]
        if isinstance(content, Exception):
            raise content
        return io.StringIO(content)

    # 只换 helper 模块里的 os 引用，不动全进程的 os.path.exists
    fake_path = SimpleNamespace(exists=lambda p: p in files)
    monkeypatch.setattr(helper, "os", SimpleNamespace(path=fake_path))
    monkeypatch.setattr(helper, "open", fake_open, raising=False)


@pytest.mark.parametrize(
    "files, expected",
    [
        ({}, False),
        ({"/.dockerenv": ""}, True),
        ({"/run/.containerenv": ""}, True),
        ({"/proc/1/cgroup": "0::/kubepods/besteffort/pod1234\n"}, True),
        ({"/proc/1/cgroup": "12:cpu:/docker/abcdef\n"}, True),
        ({"/proc/1/cgroup": "0::/system.slice/containerd.service\n"}, True),
        ({"/proc/1/cgroup": "0::/init.scope\n"}, False),
        # 读不了 cgroup 不能让启动崩掉，当作非容器
        ({"/proc/1/cgroup": PermissionError("denied")}, False),
    ],
)
def test_is_container_env(monkeypatch, files, expected):
    from hetu.common.helper import is_container_env

    _fake_fs(monkeypatch, files)
    assert is_container_env() is expected


def test_get_machine_id(monkeypatch):
    """机器ID是 Redis worker 租约 node_id 的一部分：容器环境用 /etc/hostname，
    /etc/hostname 读不到时回退 socket.gethostname()；非容器用 MAC 的十六进制"""
    import socket
    import uuid

    from hetu.common.helper import get_machine_id

    _fake_fs(monkeypatch, {"/.dockerenv": "", "/etc/hostname": "pod-7f9c\n"})
    assert get_machine_id() == "pod-7f9c"

    _fake_fs(monkeypatch, {"/.dockerenv": "", "/etc/hostname": OSError("gone")})
    monkeypatch.setattr(socket, "gethostname", lambda: "fallback-host")
    assert get_machine_id() == "fallback-host"

    _fake_fs(monkeypatch, {})
    monkeypatch.setattr(uuid, "getnode", lambda: 0x1A2B3C4D5E6F)
    assert get_machine_id() == "1a2b3c4d5e6f"


@pytest.mark.skipif(sys.platform != "win32", reason="只在 Windows 上判断")
def test_windows_pid_exited():
    """认得出本机已经退出的进程：进程对象还在（有人握着句柄）、查无此 pid 两种都算；
    活着的、没权限查的（System 进程）都不算"""
    from hetu.common.helper import windows_pid_exited

    assert not windows_pid_exited(os.getpid())
    # System 进程：OpenProcess 拒绝访问，拿不准就当活着
    assert not windows_pid_exited(4)

    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    # Popen 还握着句柄，进程对象没释放，OpenProcess 照样打得开，得看退出码
    assert windows_pid_exited(proc.pid)

    assert windows_pid_exited(0xFFFFFFFC)  # Windows 的 pid 到不了这么大，查无此进程


@use_redis_family_backend_only
async def test_redis_tool_keeper(mod_auto_backend):
    """hetu call / shell 的 tool 模式：不做复用扫描、从上往下 SET NX、node_id 带 cli: 前缀。

    复用扫描用的 GETEX 会把所有现存租约（包括已死 worker 的）TTL 刷回满值，工具进程调用得
    勤，死租约就永不过期，hetu upgrade 会一直以为有服务器在跑
    """
    from hetu.data.backend.redis.worker_keeper import (
        RedisWorkerKeeper,
        live_worker_leases,
    )

    backend = mod_auto_backend()
    aio = backend.master.aio
    keys = await aio.keys("snowflake:*", target_nodes=RedisCluster.PRIMARIES)
    if keys:
        await aio.delete(*keys)

    # 一个已死 worker 留下的租约，还剩 30 秒
    await aio.set("snowflake:worker:5", "dead-machine:1", ex=30)

    tool = RedisWorkerKeeper(900, aio, tool=True)
    assert tool.node_id.startswith("cli:")
    assert await tool.get_worker_id() == 1023
    assert await RedisWorkerKeeper(901, aio, tool=True).get_worker_id() == 1022
    # 没有碰别人的租约
    assert await aio.ttl("snowflake:worker:5") <= 30
    assert tool.lease_deadline is not None

    owners = live_worker_leases(backend.master.io)
    assert owners[1023] == tool.node_id and owners[5] == "dead-machine:1"

    # 续约、被抢后发现、释放只删自己的，与服务器模式相同
    await tool.keep_alive()
    await aio.set("snowflake:worker:1023", "thief:1", ex=60)
    with pytest.raises(SystemExit):
        await tool.keep_alive()
    await tool.release_worker_id()
    assert await aio.get("snowflake:worker:1023") == b"thief:1"
    await aio.delete(
        *await aio.keys("snowflake:*", target_nodes=RedisCluster.PRIMARIES)
    )


async def test_sqlite_tool_keeper(mod_sqlite_backend, monkeypatch, tmp_path):
    """SQLite 上的工具进程在预留段 [1000, 1023] 里用 KV 租约互斥，语义同 Redis"""
    monkeypatch.chdir(tmp_path)
    from hetu.common.snowflake_id import TOOL_WORKER_ID_FLOOR, WorkerKeeper
    from hetu.data.backend.sqlite.store import SQLiteStore
    from hetu.data.backend.worker_keeper import (
        SQLiteToolWorkerKeeper,
        create_worker_keeper,
        live_worker_ids,
        live_worker_leases,
    )

    backend = mod_sqlite_backend()
    master = backend.master
    first = create_worker_keeper(backend, 900, tool=True)
    assert isinstance(first, SQLiteToolWorkerKeeper)
    keepers: list[WorkerKeeper] = [first]
    try:
        assert await first.get_worker_id() == 1023
        second = create_worker_keeper(backend, 901, tool=True)
        assert isinstance(second, SQLiteToolWorkerKeeper)
        keepers.append(second)
        assert await second.get_worker_id() == 1022
        assert first.lease_deadline is not None

        leases = live_worker_leases(backend)
        assert leases[1023] == first.node_id and leases[1022] == second.node_id
        assert first.node_id.startswith("cli:")
        assert {1022, 1023} <= set(live_worker_ids(backend))

        # 续约推进围栏；被抢后续约抛 SystemExit，释放不删别人的
        deadline = first.lease_deadline
        await first.keep_alive()
        assert first.lease_deadline >= deadline
        key = "snowflake:worker:1023"
        master.run_sync_(SQLiteStore.kv_delete_if, key, first.node_id.encode())
        assert master.run_sync_(SQLiteStore.kv_set_nx, key, b"thief", 60)
        with pytest.raises(SystemExit):
            await first.keep_alive()
        await first.release_worker_id()
        assert master.run_sync_(SQLiteStore.kv_get, key) == b"thief"
        master.run_sync_(SQLiteStore.kv_delete_if, key, b"thief")

        # 预留段占满就报错，不会越界去撞服务器 worker 的序号（second 还占着 1022）
        for pid in range(1000, 1000 + 1024 - TOOL_WORKER_ID_FLOOR - 1):
            keeper = create_worker_keeper(backend, pid, tool=True)
            keepers.append(keeper)
            await keeper.get_worker_id()
        with pytest.raises(KeyError):
            await create_worker_keeper(backend, 5000, tool=True).get_worker_id()
    finally:
        for keeper in keepers:
            await keeper.release_worker_id()
    assert live_worker_leases(backend) == {}


def test_sqlite_kv_expire_if(tmp_path):
    """kv_expire_if：值相符且未过期才续期（租约续期用的 CAS）"""
    from hetu.data.backend.sqlite.store import SQLiteStore

    store = SQLiteStore(str(tmp_path / "kv.sqlite3"))
    try:
        assert not store.kv_expire_if("k", b"me", 60)  # 不存在
        assert store.kv_set_nx("k", b"me", 0.05)
        assert not store.kv_expire_if("k", b"other", 60)  # 不是自己的
        assert store.kv_expire_if("k", b"me", 60)
        time.sleep(0.1)
        assert store.kv_get("k") == b"me"  # 已续到 60 秒后，没过期
        assert store.kv_set_nx("k2", b"me", 0.01)
        time.sleep(0.05)
        assert not store.kv_expire_if("k2", b"me", 60)  # 已过期就不能续
    finally:
        store.close()


async def test_fixed_worker_keeper_leaves_tool_range(monkeypatch):
    """SQLite 服务器 worker 的序号不能进工具进程的预留段"""
    from hetu.data.backend.worker_keeper import FixedWorkerKeeper

    monkeypatch.setenv("SANIC_WORKER_IDENTIFIER", "Srv 1000")
    with pytest.raises(KeyError):
        await FixedWorkerKeeper().get_worker_id()


async def test_snowflake_lease_lifecycle(mod_auto_backend, monkeypatch, tmp_path):
    """SnowflakeLease：拿 id → 接水位 → 发号 → 退出时写精确水位、释放租约；丢租约时回调"""
    monkeypatch.chdir(tmp_path)
    from hetu.common.snowflake_id import SnowflakeID
    from hetu.data.backend import snowflake_lease
    from hetu.data.backend.base import RowFormat
    from hetu.data.backend.snowflake_lease import SnowflakeLease
    from hetu.data.backend.worker_keeper import create_worker_keeper, live_worker_ids

    backend = mod_auto_backend()
    table = _make_lease_table(backend)
    generator = SnowflakeID()

    lease = SnowflakeLease(create_worker_keeper(backend, 902, tool=True), table)
    async with lease:
        worker_id = lease.worker_id
        assert worker_id >= 1000 and generator.worker_id == worker_id
        assert worker_id in live_worker_ids(backend)
        new_id = generator.next_id()
        assert (new_id >> 12) & 1023 == worker_id
        last = generator.last_timestamp
    # 精确水位不低于最后发出的 id 的时间戳；租约已释放
    row = await backend.master.get(table, worker_id, row_format=RowFormat.RAW)
    assert row is not None and int(row["last_timestamp"]) >= last
    assert worker_id not in live_worker_ids(backend)

    # 续约发现租约被抢走 → on_lost
    monkeypatch.setattr(snowflake_lease, "RENEW_INTERVAL", 0.05)
    lost = asyncio.Event()
    lease = SnowflakeLease(create_worker_keeper(backend, 903, tool=True), table)
    lease.on_lost = lost.set
    async with lease:
        keeper = lease.keeper
        await keeper.release_worker_id()  # 模拟租约过期后被别人拿走：自己那把没了
        await asyncio.wait_for(lost.wait(), 5)
