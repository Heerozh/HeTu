"""
SQLite 开发后端（hetu/data/backend/sqlite）自己的用例：配置、库文件、行表的可读形态、lex zset 语义、
通知的产生与送达、执行模型。和 Redis 一致的行为由各后端参数化的用例覆盖，这里只测 SQLite 独有的部分。
"""

import asyncio
import logging
import sqlite3
import threading
import time
from pathlib import Path

import numpy as np
import pytest
from fixtures.testdata import create_ref, def_item, def_rls_test

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend
from hetu.data.backend.base import MQClient
from hetu.data.backend.redis_model import msg_packer
from hetu.data.backend.sqlite import SQLiteBackendClient
from hetu.data.backend.sqlite.commit import run_commit
from hetu.data.backend.sqlite.mq import (
    MAX_CHANNELS_IN_FILTER,
    SQLiteMQClient,
    SQLiteNotifyHub,
)
from hetu.data.backend.sqlite.store import (
    APPLICATION_ID,
    FORMAT_VERSION,
    NOTIFY_TABLE,
    SQLiteStore,
    open_store,
    quote,
)

SnowflakeID().init(1, 0)


def _dsn(path: Path) -> str:
    return f"sqlite:///{path.as_posix()}"


def _notify_rows(store: SQLiteStore) -> list[tuple[str, bytes | None]]:
    rows = store.conn.execute(
        f"SELECT channel, payload FROM {quote(NOTIFY_TABLE)} ORDER BY id"
    ).fetchall()
    return [(str(ch), None if p is None else bytes(p)) for ch, p in rows]


def _clear_notify(store: SQLiteStore) -> None:
    with store.write_txn():
        store.conn.execute(f"DELETE FROM {quote(NOTIFY_TABLE)}")


# ============ 配置 ============


@pytest.mark.parametrize(
    ("dsn", "path"),
    [
        ("sqlite:///./hetu.db", "./hetu.db"),
        ("sqlite:////tmp/hetu.db", "/tmp/hetu.db"),
        ("sqlite:///C:/data/hetu.db", "C:/data/hetu.db"),
        ("SQLite:///hetu.db", "hetu.db"),
    ],
)
def test_parse_dsn(dsn, path):
    assert SQLiteBackendClient.parse_dsn(dsn) == path


@pytest.mark.parametrize(
    "dsn",
    ["sqlite://", "sqlite:///", "sqlite:///:memory:", "./hetu.db", "redis://h", None],
)
def test_parse_dsn_rejects(dsn):
    with pytest.raises(ValueError):
        SQLiteBackendClient.parse_dsn(dsn)


def test_removed_sql_backend_is_reported(tmp_path):
    """type: SQL 与 postgres / mysql 地址都明确报错，告诉用户怎么改"""
    from hetu.cli.start import infer_backend_type_from_db_url

    with pytest.raises(ValueError, match="type 改成 SQLite"):
        Backend({"type": "SQL", "master": _dsn(tmp_path / "a.db")})
    for url in (
        "postgresql://u@h/db",
        "postgres://u@h/db",
        "mysql://h/db",
        "mariadb://h",
    ):
        with pytest.raises(ValueError, match="已移除"):
            infer_backend_type_from_db_url(url)
    assert infer_backend_type_from_db_url("sqlite:///./hetu.db") == "sqlite"


async def test_config_checks(tmp_path):
    """servants 非空报错；hetu init 生成的配置里带着的 Redis 专用项忽略；别的未知项报错"""
    dsn = _dsn(tmp_path / "a.db")
    with pytest.raises(ValueError, match="servants"):
        Backend(
            {"type": "sqlite", "master": dsn, "servants": [_dsn(tmp_path / "b.db")]}
        )

    config = {
        "type": "SQLite",
        "master": dsn,
        "servants": [],
        "master_weight": 1.0,
        "raw_clustering": False,
        "max_connections": 64,
        "pool_timeout": 5,
    }
    backend = Backend(config)
    backend_again = Backend(
        config
    )  # 同一份配置再建一次：Backend 没往 servants 里塞 master
    assert config["servants"] == []
    await backend.close()
    await backend_again.close()

    with pytest.raises(TypeError, match="no_such_option"):
        Backend({"type": "sqlite", "master": dsn, "no_such_option": 1})


# ============ 库文件 ============


async def test_new_file_is_marked(tmp_path):
    """新库写入 HeTu 的标识，开 WAL；所在目录不存在就建"""
    path = tmp_path / "sub" / "dir" / "hetu.db"
    backend = Backend({"type": "sqlite", "master": _dsn(path)})
    assert await backend.master.is_synced() == (True, 0)
    await backend.close()

    conn = sqlite3.connect(path)
    try:
        assert conn.execute("PRAGMA application_id").fetchone()[0] == APPLICATION_ID
        assert conn.execute("PRAGMA user_version").fetchone()[0] == FORMAT_VERSION
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    finally:
        conn.close()


def _make_db(path: Path, *sqls: str) -> None:
    conn = sqlite3.connect(path)
    try:
        for sql in sqls:
            conn.execute(sql)
        conn.commit()
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("sqls", "match"),
    [
        (['CREATE TABLE "_HeTu_Component_Meta" (instance_name TEXT)'], "旧 SQL 后端"),
        (["CREATE TABLE users (id INTEGER)"], "不是 HeTu 的库文件"),
        (
            [
                f"PRAGMA application_id={APPLICATION_ID}",
                f"PRAGMA user_version={FORMAT_VERSION + 1}",
            ],
            "格式版本",
        ),
    ],
    ids=["legacy", "foreign", "version"],
)
def test_refuses_other_files(tmp_path, sqls, match):
    """认不出的库文件（旧 SQL 后端的、别的程序的、格式版本不对的）报错，一个字节都不改"""
    path = tmp_path / "other.db"
    _make_db(path, *sqls)
    before = path.read_bytes()
    with pytest.raises(ValueError, match=match):
        SQLiteBackendClient(_dsn(path), False)
    assert path.read_bytes() == before


# ============ 行表：每组件一张、每字段一列，DB 工具能直接看 ============


async def test_rows_are_readable(tmp_path, new_component_env):
    """提交后物理表每字段一列，值是和 Redis 一样的文本（id 在前、_version 在后）"""
    path = tmp_path / "hetu.db"
    backend = Backend({"type": "sqlite", "master": _dsn(path)})
    try:
        ref = create_ref(def_item(), backend)
        comp = ref.comp_cls
        row = comp.new_row()
        row.owner, row.name, row.time, row.model = 7, "河图", 1, 0.5
        async with backend.session("pytest", 1) as session:
            await session.using(comp).insert(row)
    finally:
        await backend.close()

    conn = sqlite3.connect(path)
    try:
        cur = conn.execute(f"SELECT * FROM {quote('pytest:Item:{CLU1}')}")
        names = [d[0] for d in cur.description]
        values = dict(zip(names, cur.fetchone()))
    finally:
        conn.close()
    assert names[:2] == ["_hetu_key", "id"] and names[-1] == "_version"
    assert set(names[1:]) == set(comp.dtype_map_)
    assert values["name"] == "河图"
    assert (values["owner"], values["model"], values["_version"]) == ("7", "0.5", "1")


async def test_bytes_field_roundtrip(tmp_path, new_component_env):
    """bytes 字段：合法 UTF-8 存成 TEXT 方便看，不合法的存 BLOB，读回都逐字节一致"""
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", force=True)
    class RawBytes(BaseComponent):
        blob: "S8" = property_field(b"")  # type: ignore  # noqa
        tag: "S8" = property_field(b"", index=True)  # type: ignore  # noqa

    path = tmp_path / "hetu.db"
    backend = Backend({"type": "sqlite", "master": _dsn(path)})
    try:
        ref = create_ref(RawBytes, backend)
        row = RawBytes.new_row()
        row.blob, row.tag = b"\xff\x00a", b"ok"
        async with backend.session("pytest", 1) as session:
            await session.using(RawBytes).insert(row)
        got = await backend.master.get(ref, int(row.id))
        assert got is not None and (bytes(got.blob), bytes(got.tag)) == (
            b"\xff\x00a",
            b"ok",
        )
        found = await backend.master.range(ref, "tag", b"ok")
        assert [int(r.id) for r in found] == [int(row.id)]
    finally:
        await backend.close()

    conn = sqlite3.connect(path)
    try:
        blob, tag = conn.execute(
            f"SELECT blob, tag FROM {quote('pytest:RawBytes:{CLU1}')}"
        ).fetchone()
    finally:
        conn.close()
    assert (blob, tag) == (b"\xff\x00a", "ok")


def test_case_insensitive_names_rejected(tmp_path):
    """SQLite 表名、列名不分大小写（Redis 的 key 分）：只差大小写的撞上时报错，不静默共用"""
    store = open_store(str(tmp_path / "s.db"), 1000)
    try:
        store.hset_txn("t:Item:{CLU1}:id:1", {"name": "a"})
        with pytest.raises(ValueError, match="大小写"):
            store.hset_txn("t:item:{CLU1}:id:1", {"name": "b"})
        with pytest.raises(ValueError, match="大小写"):
            store.hset_txn("t:Item:{CLU1}:id:2", {"Name": "c"})
        assert store.hgetall("t:Item:{CLU1}:id:1") == {b"name": b"a"}
        assert store.hgetall("t:Item:{CLU1}:id:2") == {}
    finally:
        store.close()


def test_hset_needs_fields(tmp_path):
    """同 Redis 的 HSET：一个字段都不给是错误（direct_set 不带字段也一样），给出明确的报错"""
    store = open_store(str(tmp_path / "s.db"), 1000)
    try:
        with pytest.raises(ValueError, match="至少"):
            store.hset_txn("t:Item:{CLU1}:id:1", {})
    finally:
        store.close()


# ============ lex zset ============


def test_zset_lex_semantics(tmp_path):
    """ZRANGE BYLEX / ZLEXCOUNT 照 Redis 的规则：member 按 memcmp 再比长度排序，`[` 闭 `(` 开，
    `-` / `+` 无界，REV 时 start 是上界"""
    store = open_store(str(tmp_path / "z.db"), 1000)
    members = [b"a", b"a\x00", b"a\x00\xff", b"b", b"B", b"\xc3\xa9", b"_"]
    try:
        with store.write_txn():
            assert all(store.zadd("k", m) for m in members)
            assert not store.zadd("k", b"a")  # 已存在：zset 没变
        assert store.zmembers("k") == sorted(members)
        assert store.zrange_bylex("k", b"[a", b"[b") == [
            b"a",
            b"a\x00",
            b"a\x00\xff",
            b"b",
        ]
        assert store.zrange_bylex("k", b"(a", b"(b") == [b"a\x00", b"a\x00\xff"]
        assert store.zrange_bylex("k", b"-", b"(a") == [b"B", b"_"]
        assert store.zrange_bylex("k", b"[b", b"+") == [b"b", b"\xc3\xa9"]
        rev = store.zrange_bylex("k", b"[b", b"[a", desc=True)
        assert rev == [b"b", b"a\x00\xff", b"a\x00", b"a"]
        assert store.zrange_bylex("k", b"+", b"-", desc=True, num=2) == [
            b"\xc3\xa9",
            b"b",
        ]
        assert store.zrange_bylex("k", b"-", b"+", num=0) == []
        assert store.zrange_bylex("k", b"+", b"-") == []  # 下界是 +inf
        assert store.zrange_bylex("k", b"[b", b"[a") == []  # 两端交叉
        assert store.zlexcount("k", b"[a", b"[a\x00\xff") == 3
        assert store.zlexcount("k", b"-", b"+") == len(members)
        with pytest.raises(ValueError):
            store.zrange_bylex("k", b"a", b"b")  # 缺 [ / (
        with store.write_txn():
            assert store.zrem("k", b"a")
            assert not store.zrem("k", b"a")
    finally:
        store.close()


# ============ 提交与通知 ============


def _commit(store, *, checks=(), pushes=(), deleted=None, table_pubs=(), chans=()):
    payload = [list(checks), list(pushes), deleted or {}, list(table_pubs), list(chans)]
    return run_commit(store, msg_packer.pack(payload))


def test_commit_notifies_like_keyspace(tmp_path):
    """
    通知按 Redis 产生通知的规则写：HSET 过的行、ZADD / ZREM 真改了的索引（keyspace 频道），
    表频道与值频道照 PUBLISH；zset 没变、要删的行不存在时不发。校验不过什么都不写
    """
    store = open_store(str(tmp_path / "c.db"), 1000)
    row, idx = "t:C:{CLU1}:id:1", "t:C:{CLU1}:index:v"
    table_ch, value_ch = "t:C:{CLU1}:table", "t:C:{CLU1}:index:v:tok"
    try:
        hset = ["HSET", row, "_version", "1", "id", "1", "v", "5"]
        resp = _commit(
            store,
            pushes=[hset, ["ZADD", idx, "0", b"m1"]],
            table_pubs=[[table_ch, msg_packer.pack(["1"])]],
            chans=[value_ch],
        )
        assert resp == b"committed"
        assert _notify_rows(store) == [
            ("__keyspace@0__:" + row, None),
            ("__keyspace@0__:" + idx, None),
            (table_ch, msg_packer.pack(["1"])),
            (value_ch, None),
        ]

        _clear_notify(store)
        resp = _commit(
            store,
            pushes=[
                ["ZADD", idx, "0", b"m1"],
                ["ZREM", idx, b"nope"],
                ["DEL", "t:C:{CLU1}:id:9"],
            ],
        )
        assert resp == b"committed" and _notify_rows(store) == []

        resp = _commit(
            store, checks=[["VER", row, "7"]], pushes=[["HSET", row, "v", "6"]]
        )
        assert resp == b"RACE: Version mismatch " + row.encode() + b" exp:7 got:1"
        resp = _commit(store, checks=[["VER", "t:C:{CLU1}:id:9", "1"]])
        assert resp.endswith(
            b" exp:1 got:false"
        )  # HGET 读不到，同 Lua 的 tostring(false)
        assert store.hget(row, "v") == b"5" and _notify_rows(store) == []
    finally:
        store.close()


def test_commits_from_two_connections_serialize(tmp_path):
    """
    两条连接（相当于两个进程）同时提交同一个 unique 值：写事务一开始就拿写锁，后到的等先到的
    提交完、再校验，看得到对方写入的值，判 UNIQUE（不会两边都校验通过）
    """
    path = str(tmp_path / "x.db")
    second = open_store(path, 5000)
    idx = "t:C:{CLU1}:index:name"
    holding = threading.Event()

    def hold_write_lock():
        first = open_store(path, 5000)  # sqlite3 连接只能在打开它的线程里用
        try:
            with first.write_txn():
                row = {"id": "1", "name": "dup", "_version": "1"}
                first.hset("t:C:{CLU1}:id:1", row)
                first.zadd(idx, b"dup\x001")
                holding.set()
                time.sleep(0.3)
        finally:
            first.close()

    thread = threading.Thread(target=hold_write_lock)
    thread.start()
    try:
        assert holding.wait(3)
        uniq = [
            "UNIQ",
            idx,
            b"[dup\x00",
            b"[dup\x00\xff",
            "UNIQUE",
            "C.name id=2 insert",
        ]
        resp = _commit(
            second,
            checks=[uniq],
            pushes=[["HSET", "t:C:{CLU1}:id:2", "_version", "1", "name", "dup"]],
        )
        assert resp == b"UNIQUE: Unique violation C.name id=2 insert"
    finally:
        thread.join()
        second.close()


async def test_direct_set_and_maintenance_do_not_notify(tmp_path, new_component_env):
    """direct_set 与维护接口的写入不发订阅通知（direct_set 的契约是不保证通知，SQLite 取不发）"""
    backend = Backend({"type": "sqlite", "master": _dsn(tmp_path / "hetu.db")})
    client = backend.master
    assert isinstance(client, SQLiteBackendClient)

    def count() -> int:
        return client.run_sync_(lambda store: len(_notify_rows(store)))

    try:
        ref = create_ref(def_rls_test(), backend)
        comp = ref.comp_cls
        row = comp.new_row()
        row.owner = 5
        async with backend.session("pytest", 1) as session:
            await session.using(comp).insert(row)
        committed = count()
        assert committed > 0

        await client.direct_set(ref, int(row.id), friend="9")
        maint = backend.get_table_maintenance()
        stored = maint.get(ref, int(row.id))
        assert stored is not None and stored.friend == 9
        maint.upsert_row(ref, stored)
        maint.delete_row(ref, int(row.id))
        assert count() == committed
    finally:
        await backend.close()


# ============ 通知的送达（SQLiteNotifyHub） ============


def _publish(client: SQLiteBackendClient, *channels: str, payload=None) -> None:
    """直接往通知表写通知，当作某个提交发出的"""

    def insert(store: SQLiteStore):
        with store.write_txn():
            store.notify_insert([(ch, payload) for ch in channels], time.time())

    client.run_sync_(insert)


@pytest.fixture
async def hub_client(tmp_path):
    client = SQLiteBackendClient(_dsn(tmp_path / "hub.db"), True)
    yield client
    await client.close()


def _quiet_hub(client: SQLiteBackendClient) -> SQLiteNotifyHub:
    """不起后台轮询的 hub：用例自己调 poll_once"""
    hub = SQLiteNotifyHub(client)
    hub._run = lambda: asyncio.sleep(3600)  # type: ignore[method-assign]
    return hub


def test_hub_channel_filter_threshold():
    assert SQLiteNotifyHub._should_use_channel_in_filter(1)
    assert SQLiteNotifyHub._should_use_channel_in_filter(MAX_CHANNELS_IN_FILTER)
    assert not SQLiteNotifyHub._should_use_channel_in_filter(MAX_CHANNELS_IN_FILTER + 1)


async def test_hub_delivers_only_after_subscribe(hub_client):
    """订阅生效之前提交的通知不送，之后的都送；同一轮里没订的频道不送"""
    hub = _quiet_hub(hub_client)
    mq = SQLiteMQClient(hub)
    _publish(hub_client, "A")
    await mq.subscribe("A")
    assert await hub.poll_once() == (0, 0)

    _publish(hub_client, "A", "B")
    assert await hub.poll_once() == (1, 1)
    assert mq.pulled_set == {"A"}
    await hub.close()


async def test_hub_table_channel_payload(hub_client):
    """表频道的 payload 是 msgpack 的 row_id 列表，按频道合并；值频道 / 行频道没有 payload"""
    hub = _quiet_hub(hub_client)
    mq = SQLiteMQClient(hub)
    await mq.subscribe("t:C:{CLU1}:table", "t:C:{CLU1}:index:v:tok")
    _publish(hub_client, "t:C:{CLU1}:table", payload=msg_packer.pack(["1", "2"]))
    _publish(hub_client, "t:C:{CLU1}:table", payload=msg_packer.pack(["3"]))
    _publish(hub_client, "t:C:{CLU1}:index:v:tok")
    assert await hub.poll_once() == (3, 3)
    assert mq.pulled_payload["t:C:{CLU1}:table"] == {"1", "2", "3"}
    assert "t:C:{CLU1}:index:v:tok" not in mq.pulled_payload
    await hub.close()


async def test_hub_concurrent_first_subscribers_keep_earliest_watermark(hub_client):
    """两个连接并发订阅同一个新频道：先登记再取水位，后到的不再各自查一次表尾把先到者的
    水位抬高——否则先到者订阅之后、后到者查表尾之前提交的通知会被当成旧通知丢掉"""
    hub = _quiet_hub(hub_client)
    gate = asyncio.Event()
    tails = iter([10, 20])
    queries = 0

    async def fake_tail():
        nonlocal queries
        queries += 1
        await gate.wait()
        return next(tails)

    hub._get_current_notify_id = fake_tail  # type: ignore[method-assign]
    mq_a, mq_b = SQLiteMQClient(hub), SQLiteMQClient(hub)
    t_a = asyncio.create_task(mq_a.subscribe("C"))
    await asyncio.sleep(0)
    t_b = asyncio.create_task(mq_b.subscribe("C"))
    await asyncio.sleep(0)
    assert hub.subscriber_count("C") == 2, "登记应在等表尾之前完成"

    gate.set()
    async with asyncio.timeout(1):
        await asyncio.gather(t_a, t_b)
    assert queries == 1, "第二个订阅者应复用先到者的水位，不再查表尾"
    assert hub._since["C"] == 10
    assert hub._last_notify_id == 10
    await hub.close()


async def test_hub_new_channel_while_polling_keeps_cursor(hub_client):
    """轮询已经在跑时新频道照样取表尾做水位，但不能动游标"""
    hub = _quiet_hub(hub_client)
    tails = iter([100, 300])

    async def fake_tail():
        return next(tails)

    hub._get_current_notify_id = fake_tail  # type: ignore[method-assign]
    mq = SQLiteMQClient(hub)
    await mq.subscribe("A")
    assert hub._since["A"] == 100 and hub._last_notify_id == 100
    hub._last_notify_id = 105  # 轮询前进了一点，表尾已经到 300
    await mq.subscribe("B")
    assert hub._since["B"] == 300, "新频道的水位是表尾，不是游标"
    assert hub._last_notify_id == 105, "轮询在跑，不能动游标"
    await hub.close()


async def test_hub_watermark_survives_callers_cancellation(hub_client):
    """先到者等表尾时被取消（连接断了）：搭车登记的连接还等着这个水位，取水位不能跟着中断；
    先到者只撤自己的登记"""
    hub = _quiet_hub(hub_client)
    gate = asyncio.Event()

    async def fake_tail():
        await gate.wait()
        return 10

    hub._get_current_notify_id = fake_tail  # type: ignore[method-assign]
    mq_a, mq_b = SQLiteMQClient(hub), SQLiteMQClient(hub)
    t_a = asyncio.create_task(mq_a.subscribe("C"))
    await asyncio.sleep(0)
    t_b = asyncio.create_task(mq_b.subscribe("C"))
    await asyncio.sleep(0)
    t_a.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t_a
    assert hub._subs["C"] == {mq_b}
    assert "C" not in hub._since

    gate.set()
    async with asyncio.timeout(1):
        await t_b
    assert hub._since["C"] == 10
    assert mq_b.subscribed_channels == {"C"}
    await hub.close()


async def test_hub_poll_skips_channel_without_watermark(hub_client):
    """已登记但水位还没取到的频道，轮询遇到它的通知先跳过（取水位与轮询互斥，取回的表尾
    只会 >= 这些行的 id，本来就不属于它）"""
    hub = _quiet_hub(hub_client)
    mq = SQLiteMQClient(hub)
    hub._subs["C"] = {mq}  # 登记了、水位还没到
    _publish(hub_client, "C")
    assert await hub.poll_once() == (1, 0)
    assert not mq.pulled_set

    hub._since["C"] = hub._last_notify_id
    _publish(hub_client, "C")
    assert await hub.poll_once() == (1, 1)
    assert "C" in mq.pulled_set
    await hub.close()


async def test_hub_cursor_follows_tail(hub_client):
    """订阅的频道一直没有通知时，游标也跟着表尾走：清理掉的都是它不关心的，不能误判成丢了"""
    hub = _quiet_hub(hub_client)
    mq = SQLiteMQClient(hub)
    await mq.subscribe("A")
    _publish(hub_client, *[f"other-{i}" for i in range(5)])
    assert await hub.poll_once() == (0, 0)
    assert hub._last_notify_id == hub_client.run_sync_(SQLiteStore.notify_tail)

    def cleanup(store: SQLiteStore):
        with store.write_txn():
            store.notify_cleanup(time.time() + 1)

    hub_client.run_sync_(cleanup)
    _publish(hub_client, "A")
    assert await hub.poll_once() == (1, 1)
    assert mq.pulled_payload == {}  # 没有补发 RESYNC
    await hub.close()


async def test_hub_resyncs_after_notifications_cleaned(hub_client, caplog):
    """游标之后的通知在被读到之前就清理掉了：同 Redis pubsub 断线重订，给订阅的频道各补一条，
    表频道带 RESYNC（整表重同步），行 / 索引频道不带"""
    hub = _quiet_hub(hub_client)
    mq = SQLiteMQClient(hub)
    table_ch, row_ch = "t:C:{CLU1}:table", "__keyspace@0__:t:C:{CLU1}:id:1"
    await mq.subscribe(table_ch, row_ch)
    _publish(hub_client, row_ch, "x")

    def cleanup(store: SQLiteStore):
        with store.write_txn():
            store.notify_cleanup(time.time() + 1)

    hub_client.run_sync_(cleanup)
    with caplog.at_level(logging.WARNING, logger="HeTu.root"):
        assert await hub.poll_once() == (0, 0)
    assert mq.pulled_set == {table_ch, row_ch}
    assert mq.pulled_payload == {table_ch: {MQClient.RESYNC}}
    assert any("已被清理" in r.getMessage() for r in caplog.records)
    await hub.close()


async def test_hub_poll_failure_backs_off_and_throttles_logs(
    hub_client, monkeypatch, caplog
):
    """轮询失败：指数退避（0.5s 起、封顶 5s），栈只记第一次，之后一行一条，恢复记一条"""
    hub = SQLiteNotifyHub(hub_client)
    hub._subs["C"] = {SQLiteMQClient(hub)}
    hub._since["C"] = 0
    outcomes = iter([Exception("db down")] * 6 + [(0, 0)] * 2)

    async def fake_poll_once():
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    sleeps: list[float] = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) >= 8:
            hub._closed = True  # 够了，让 _run 退出

    hub.poll_once = fake_poll_once  # type: ignore[method-assign]
    monkeypatch.setattr("hetu.data.backend.sqlite.mq.asyncio.sleep", fake_sleep)
    caplog.set_level(logging.INFO, logger="HeTu.root")
    async with asyncio.timeout(3):
        await hub._run()

    assert sleeps[:6] == [0.5, 1.0, 2.0, 4.0, 5.0, 5.0], "指数退避并封顶"
    records = [r for r in caplog.records if "轮询通知表" in r.getMessage()]
    with_stack = [r for r in records if r.exc_info]
    assert len(with_stack) == 1, "栈只记第一次"
    assert sum(1 for r in records if r.levelno == logging.ERROR) == 6
    assert sum(1 for r in records if "恢复" in r.getMessage()) == 1


async def test_subscription_end_to_end(tmp_path, new_component_env):
    """真的起轮询：订阅行频道后提交修改，通知经通知表送到"""
    backend = Backend({"type": "sqlite", "master": _dsn(tmp_path / "hetu.db")})
    try:
        ref = create_ref(def_item(), backend)
        comp = ref.comp_cls
        row = comp.new_row()
        row.name, row.time = "a", 1
        async with backend.session("pytest", 1) as session:
            await session.using(comp).insert(row)

        mq = backend.get_mq_client()
        channel = backend.servant.row_channel(ref, int(row.id))
        assert channel == "__keyspace@0__:" + backend.master.row_key(ref, int(row.id))  # type: ignore[attr-defined]
        await mq.subscribe(channel)
        async with backend.session("pytest", 1) as session:
            repo = session.using(comp)
            got = await repo.get(id=int(row.id))
            assert got is not None
            got.qty = np.int16(5)
            await repo.update(got)
        async with asyncio.timeout(5):
            updates = await mq.get_message()
        assert channel in updates
        await mq.close()
    finally:
        await backend.close()


# ============ 执行模型 ============


async def test_client_is_bound_to_one_event_loop(tmp_path):
    """同 Redis：客户端只能在一个事件循环里用，开发期就暴露跨 loop 使用"""
    client = SQLiteBackendClient(_dsn(tmp_path / "a.db"), False)
    try:
        assert await client.run_(SQLiteStore.notify_tail) == 0
        errors: list[BaseException] = []

        def other_loop():
            try:
                asyncio.run(client.run_(SQLiteStore.notify_tail))
            except AssertionError as exc:
                errors.append(exc)

        thread = threading.Thread(target=other_loop)
        thread.start()
        thread.join()
        assert errors, "换了事件循环应当断言失败"
    finally:
        await client.close()


async def test_close_is_idempotent(tmp_path):
    client = SQLiteBackendClient(_dsn(tmp_path / "a.db"), False)
    await client.close()
    await client.close()
    with pytest.raises(ConnectionError):
        await client.run_(SQLiteStore.notify_tail)
    with pytest.raises(ConnectionError):
        client.get_table_maintenance()


def test_maintenance_lock_blocks_until_release(tmp_path):
    """维护锁：持有期间另一个 get_lock 阻塞，释放后才拿到"""
    client = SQLiteBackendClient(_dsn(tmp_path / "lock.db"), False)
    try:
        maint1 = client.get_table_maintenance()
        maint2 = client.get_table_maintenance()
        holder_ready, holder_release, waiter_done = (
            threading.Event(),
            threading.Event(),
            threading.Event(),
        )
        errors: list[BaseException] = []
        elapsed: dict[str, float] = {}

        def holder():
            try:
                with maint1.get_lock():
                    holder_ready.set()
                    holder_release.wait(timeout=3.0)
            except BaseException as exc:  # noqa: BLE001  线程里的任何错误都带回主线程报告
                errors.append(exc)
                holder_ready.set()

        def waiter():
            started = time.perf_counter()
            try:
                with maint2.get_lock():
                    elapsed["seconds"] = time.perf_counter() - started
            except BaseException as exc:  # noqa: BLE001  线程里的任何错误都带回主线程报告
                errors.append(exc)
            finally:
                waiter_done.set()

        t_holder = threading.Thread(target=holder, daemon=True)
        t_waiter = threading.Thread(target=waiter, daemon=True)
        t_holder.start()
        assert holder_ready.wait(timeout=3.0)
        t_waiter.start()
        time.sleep(0.2)
        assert not waiter_done.is_set(), "锁未释放前，第二个get_lock不应拿到锁"

        holder_release.set()
        t_holder.join(timeout=3.0)
        t_waiter.join(timeout=3.0)
        assert not errors, f"线程执行出现异常: {errors!r}"
        assert waiter_done.is_set()
        assert elapsed["seconds"] >= 0.15
    finally:
        asyncio.run(client.close())


def test_maintenance_lock_expires(tmp_path):
    """维护锁带过期时间（同 redis-py Lock）：持有者卡住过期后别人能拿到，原持有者释放时报错"""
    client = SQLiteBackendClient(_dsn(tmp_path / "lock.db"), False)
    try:
        stuck = client.get_table_maintenance().get_lock()
        stuck.timeout = 0.2  # type: ignore[attr-defined]
        stuck.__enter__()
        time.sleep(0.3)
        with (
            client.get_table_maintenance().get_lock(),
            pytest.raises(RuntimeError, match="过期"),
        ):
            stuck.__exit__(None, None, None)
    finally:
        asyncio.run(client.close())
