"""
hetu.headless：无服务器进程的表直读写客户端。

服务器侧的表由 mod_test_app + mod_tbl_mgr 建好（含 meta / cluster_id），headless 只连
同一个后端按 meta 认表；System 侧的写入经 mod_new_ctx 的 SystemCaller 完成。
"""

import asyncio
import json
import logging
import threading
import time

import pytest

from hetu import headless
from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import RaceCondition, Table
from hetu.data.component import BaseComponent, ComponentDefines

SnowflakeID().init(1, 0)


@pytest.fixture(scope="module")
async def hl(mod_test_app, mod_tbl_mgr, mod_auto_backend):
    """复用 module 级 backend 的 headless client：两个传类，一个传名字"""
    app = mod_test_app
    client = await headless.HeadlessClient.from_backend(
        mod_auto_backend(),
        "server1",
        [app.HeadlessCommand, app.HeadlessSim, "PublicNames"],
    )
    yield client
    await client.close()


def _variant(comp_cls, **changes):
    """基于组件 json 造一个同名但定义有差异的类（不注册进 ComponentDefines）"""
    data = json.loads(comp_cls.json_)
    for key, value in changes.items():
        if key == "properties":
            for col, prop in value.items():
                if prop is None:
                    data["properties"].pop(col)
                else:
                    data["properties"].setdefault(col, {}).update(prop)
        else:
            data[key] = value
    return BaseComponent.load_json(json.dumps(data))


# ---------------------------------------------------------------- connect / 认表


async def test_from_backend_matches_server_tables(hl, mod_test_app, mod_tbl_mgr):
    app = mod_test_app
    for comp in (app.HeadlessCommand, app.HeadlessSim):
        server_tbl = mod_tbl_mgr.get_table(comp)
        tbl = hl.table(comp)
        assert isinstance(tbl, Table)
        assert tbl.instance_name == "server1"
        assert tbl.cluster_id == server_tbl.cluster_id
        assert tbl.comp_cls is comp  # 传类：认本地类
        assert hl.table(comp.name_) is tbl  # 按名字也能取
    assert set(hl.tables) == {"HeadlessCommand", "HeadlessSim", "PublicNames"}

    # 传名字：从服务器 meta 生成的类，布局与本地类一致，但不是同一个类、不注册进 ComponentDefines
    tbl = hl.table("PublicNames")
    assert tbl.cluster_id == mod_tbl_mgr.get_table(app.PublicNames).cluster_id
    assert tbl.comp_cls is not app.PublicNames
    assert tbl.comp_cls.dtypes == app.PublicNames.dtypes
    assert tbl.comp_cls.uniques_ == app.PublicNames.uniques_
    assert tbl.comp_cls.indexes_ == app.PublicNames.indexes_
    assert ComponentDefines().get_component("pytest", "PublicNames") is app.PublicNames
    # 名字模式下拿本地类去查表会被拒绝（不是同一个类）
    with pytest.raises(KeyError):
        hl.table(app.PublicNames)
    with pytest.raises(KeyError):
        hl.table("PublicConfig")  # 没声明


async def test_from_backend_rejects_bad_components(mod_test_app, mod_auto_backend):
    backend = mod_auto_backend()
    with pytest.raises(ValueError):
        await headless.HeadlessClient.from_backend(backend, "server1", [])
    with pytest.raises(ValueError):
        await headless.HeadlessClient.from_backend(
            backend, "server1", [mod_test_app.HeadlessSim, "HeadlessSim"]
        )


async def test_table_not_found_does_not_create(mod_auto_backend):
    """表不存在 → 明确报错；建表 / 迁移权归服务器，headless 不建表"""
    backend = mod_auto_backend()
    with pytest.raises(headless.TableNotFound):
        await headless.HeadlessClient.from_backend(backend, "server1", ["NoSuchComp"])
    assert backend.get_table_maintenance().read_meta("server1", "NoSuchComp") is None
    # 换个 instance 也一样：表按 instance 隔离
    with pytest.raises(headless.TableNotFound):
        await headless.HeadlessClient.from_backend(
            backend, "no_such_instance", ["HeadlessSim"]
        )


async def test_schema_guard(mod_test_app, mod_auto_backend, caplog):
    """本地类与服务器 meta 的数据布局不一致 → SchemaMismatch 且信息含差异；
    权限类字段差异忽略，default 差异只告警。"""
    app = mod_test_app
    backend = mod_auto_backend()
    Sim = app.HeadlessSim

    async def expect_mismatch(cls, *needles):
        with pytest.raises(headless.SchemaMismatch) as ei:
            await headless.HeadlessClient.from_backend(backend, "server1", [cls])
        assert ei.value.comp_name == "HeadlessSim"
        for needle in needles:
            assert needle in str(ei.value), (needle, str(ei.value))

    extra = {"default": 0, "unique": False, "index": False, "dtype": "<i4"}
    await expect_mismatch(_variant(Sim, properties={"extra": extra}), "extra")
    await expect_mismatch(_variant(Sim, properties={"epoch": None}), "epoch")
    await expect_mismatch(
        _variant(Sim, properties={"epoch": {"dtype": "<i4"}}), "epoch", "dtype"
    )
    await expect_mismatch(
        _variant(Sim, properties={"system_id": {"unique": False}}),
        "system_id",
        "unique",
    )
    await expect_mismatch(_variant(Sim, namespace="other"), "namespace")

    # 只改权限：与 headless 无关，正常连接
    client = await headless.HeadlessClient.from_backend(
        backend, "server1", [_variant(Sim, permission="EVERYBODY", volatile=True)]
    )
    await client.close()

    # 只改 default：连接成功，但有告警
    with caplog.at_level(logging.WARNING, logger="HeTu.root"):
        client = await headless.HeadlessClient.from_backend(
            backend, "server1", [_variant(Sim, properties={"epoch": {"default": 5}})]
        )
        await client.close()
    assert any("epoch" in rec.getMessage() for rec in caplog.records)


# ---------------------------------------------------------------- 读


async def test_reads_rows_written_by_system(hl, mod_test_app, mod_new_ctx):
    """反向读取：System 插入 → headless servant_range / servant_get_many 读到"""
    app = mod_test_app
    ctx = mod_new_ctx()
    ids = [
        await ctx.systems.call("push_headless_command", 1, i, 1000.0 + i, f"p{i}")
        for i in range(3)
    ]
    await hl.backend.wait_for_synced()

    tbl = hl.table(app.HeadlessCommand)
    rows = await tbl.servant_range("created_at", 1000.0, 1002.0, limit=10)
    assert [int(r.seq) for r in rows] == [0, 1, 2]
    assert [str(r.payload) for r in rows] == ["p0", "p1", "p2"]

    many = await tbl.servant_get_many([ids[1], 999999999, ids[0]])
    assert many[1] is None
    assert int(many[0].id) == ids[1] and int(many[2].id) == ids[0]
    assert (await tbl.servant_get(ids[2])).payload == "p2"


# ---------------------------------------------------------------- 写


async def test_session_guards(hl, mod_test_app):
    app = mod_test_app
    # 跨簇 → 报错而不是静默拆成两个事务
    with pytest.raises(ValueError):
        hl.session(app.HeadlessCommand, "PublicNames")
    with pytest.raises(ValueError):
        hl.session()
    async with hl.session(app.HeadlessSim) as s:
        assert s.only_master is True
        assert s.explicit_ids_only is True
        with pytest.raises(KeyError):
            s[app.HeadlessCommand]  # 未声明
        with pytest.raises(KeyError):
            s["PublicNames"]
        assert s[app.HeadlessSim] is s["HeadlessSim"]  # 同一个 repo
    s2 = hl.session(app.HeadlessSim, only_master=False)
    assert s2.only_master is False


async def test_write_and_server_reads_back(hl, mod_test_app, mod_tbl_mgr):
    """headless 写的行，服务器侧按自己的 Table 能原样读到（含名字模式生成的类）"""
    app = mod_test_app
    Names = hl.table("PublicNames").comp_cls
    async with hl.session("PublicNames") as s:
        row = Names.new_row(id_=-5)
        row.owner = 5
        row.name = "hl-5"
        await s["PublicNames"].insert(row)
        async with s[Names].upsert(id=-6) as row:
            row.owner = 6
            row.name = "hl-6"
    server_tbl: Table = mod_tbl_mgr.get_table(app.PublicNames)
    got = await server_tbl.backend.master.get_many(server_tbl, [-5, -6])
    assert [str(r.name) for r in got] == ["hl-5", "hl-6"]
    assert [int(r.owner) for r in got] == [5, 6]


async def test_explicit_ids_only(hl, mod_test_app, monkeypatch):
    """不发号：insert 必须带非零 id；upsert 只在锚定 id 时允许新建；SnowflakeID 不被动到"""
    app = mod_test_app
    sf = SnowflakeID()
    monkeypatch.setattr(sf, "worker_id", -1)  # 模拟 headless 进程里未初始化
    Cmd, Sim = app.HeadlessCommand, app.HeadlessSim

    async with hl.session(Cmd) as s:
        with pytest.raises(ValueError):
            await s[Cmd].insert(Cmd.new_row(id_=0))
        row = Cmd.new_row(id_=-1001)
        row.system_id, row.seq, row.created_at = 9, 1, 5.0
        await s[Cmd].insert(row)

    with pytest.raises(LookupError):
        async with hl.session(Sim) as s:
            async with s[Sim].upsert(system_id=12345) as r:
                r.epoch = 1

    async with hl.session(Sim) as s, s[Sim].upsert(id=-2002) as r:
        r.system_id, r.epoch = 77, 1
    # 重发同一批：命中 → 走 update，字段没变连写都不写
    async with hl.session(Sim) as s, s[Sim].upsert(id=-2002) as r:
        r.system_id, r.epoch = 77, 1
    got = await hl.backend.master.get_many(hl.table(Sim), [-2002])
    assert got[0] is not None and int(got[0].epoch) == 1 and int(got[0]._version) == 1
    assert sf.worker_id == -1


async def test_race_with_system_and_retry(hl, mod_test_app, mod_new_ctx):
    """headless 与 System 并发 update 同一行 → 一方 RaceCondition；retry 后终态正确"""
    app = mod_test_app
    Sim = app.HeadlessSim
    ctx = mod_new_ctx()
    row_id = await ctx.systems.call(
        "bump_headless_sim", 501, "srv"
    )  # 服务器预建，epoch=1

    with pytest.raises(RaceCondition):
        async with hl.session(Sim) as s:
            row = await s[Sim].get(system_id=501)
            assert row is not None and int(row.id) == row_id
            await ctx.systems.call("bump_headless_sim", 501)  # 并发修改 → epoch=2
            row.owner_host = "stale"
            await s[Sim].update(row)

    attempts = 0
    async for attempt in hl.session(Sim).retry(3):
        async with attempt as s:
            attempts += 1
            row = await s[Sim].get(system_id=501)
            assert row is not None
            if attempts == 1:
                await ctx.systems.call("bump_headless_sim", 501)  # epoch=3
            row.owner_host = "headless"
            await s[Sim].update(row)
    assert attempts == 2
    final = await hl.backend.master.get(hl.table(Sim), row_id)
    assert final is not None
    assert str(final.owner_host) == "headless" and int(final.epoch) == 3


# ---------------------------------------------------------------- check_schema


async def test_check_schema(mod_test_app, mod_auto_backend):
    app = mod_test_app
    backend = mod_auto_backend()
    Cfg = app.PublicConfig  # 用其它 headless 测试不碰的表做破坏性检查
    client = await headless.HeadlessClient.from_backend(backend, "server1", [Cfg])
    await client.check_schema()

    maint = backend.get_table_maintenance()
    tbl = client.table(Cfg)
    moved = Table(Cfg, "server1", tbl.cluster_id + 50, backend)
    maint.do_rename_table_(tbl, moved)  # 模拟服务器 hetu upgrade 迁簇
    try:
        with pytest.raises(headless.ClusterChanged) as ei:
            await client.check_schema()
        assert ei.value.old_id == tbl.cluster_id
        assert ei.value.new_id == moved.cluster_id
    finally:
        maint.do_rename_table_(moved, tbl)
    await client.check_schema()

    maint.do_drop_table_(tbl)  # 模拟表被删
    try:
        with pytest.raises(headless.TableNotFound):
            await client.check_schema()
    finally:
        maint.do_create_table_(tbl)
    await client.check_schema()
    await client.close()


# ---------------------------------------------------------------- 轮询读（验收 8）


async def test_polling_read_no_gap_no_dup(hl, mod_test_app, mod_new_ctx):
    """服务器持续插入命令，headless 周期性索引 range + 按 seq 去重 → 无漏无重"""
    app = mod_test_app
    ctx = mod_new_ctx()
    n = 40
    base = 5000.0
    tbl = hl.table(app.HeadlessCommand)

    async def producer():
        for i in range(n):
            await ctx.systems.call("push_headless_command", 7, i, base + i * 0.02)
            await asyncio.sleep(0.01)

    seen: list[int] = []

    async def consumer():
        cursor = -1
        watermark = base
        deadline = time.monotonic() + 20
        while len(seen) < n and time.monotonic() < deadline:
            rows = await tbl.servant_range(
                "created_at", watermark - 0.5, float("inf"), limit=4096
            )
            for row in rows:
                if int(row.system_id) != 7 or int(row.seq) <= cursor:
                    continue
                assert int(row.seq) == cursor + 1, "漏读"
                cursor = int(row.seq)
                seen.append(cursor)
                watermark = max(watermark, float(row.created_at))
            await asyncio.sleep(0.1)

    await asyncio.gather(producer(), consumer())
    assert seen == list(range(n))


# ---------------------------------------------------------------- 线程（验收 7）


def test_connect_in_thread_loop(mod_test_app, mod_tbl_mgr, mod_backend_config):
    """在 threading.Thread 里新建 event loop：connect / 写 / retry / check_schema / close"""
    app = mod_test_app
    Sim = app.HeadlessSim
    result: dict = {}

    def worker():
        async def main():
            client = await headless.connect(mod_backend_config, "server1", [Sim])
            try:
                async for attempt in client.session(Sim).retry(3):
                    async with attempt as s, s[Sim].upsert(id=-3003) as row:
                        row.system_id = 303
                        row.owner_host = "thread"
                row = await client.backend.master.get(client.table(Sim), -3003)
                result["host"] = str(row.owner_host)
                await client.check_schema()
            finally:
                await client.close()

        try:
            asyncio.run(main())
        except BaseException as e:  # noqa: BLE001
            result["error"] = repr(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join(60)
    assert not t.is_alive()
    assert "error" not in result, result["error"]
    assert result["host"] == "thread"
