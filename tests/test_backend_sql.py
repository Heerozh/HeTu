import asyncio
import threading
import time
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pytest
import sqlalchemy as sa
from fixtures.backends import SQL_BACKENDS
from sqlalchemy.dialects import mysql, postgresql

from hetu.data.backend.sql import SQLBackendClient
from hetu.data.backend.sql.mq import MAX_CHANNELS_IN_FILTER, SQLMQClient, SQLNotifyHub


def test_sql_parse_engine_urls_auto_driver():
    sync_url, async_url = SQLBackendClient.parse_engine_urls(
        "postgresql://user:pwd@127.0.0.1:5432/db"
    )
    assert sync_url.startswith("postgresql+psycopg://")
    assert async_url.startswith("postgresql+asyncpg://")

    sync_url, async_url = SQLBackendClient.parse_engine_urls(
        "mysql://user:pwd@127.0.0.1:3306/db"
    )
    assert sync_url.startswith("mysql+pymysql://")
    assert async_url.startswith("mysql+aiomysql://")

    sync_url, async_url = SQLBackendClient.parse_engine_urls(
        "mariadb://user:pwd@127.0.0.1:3306/db"
    )
    assert sync_url.startswith("mysql+pymysql://")
    assert async_url.startswith("mysql+aiomysql://")

    sync_url, async_url = SQLBackendClient.parse_engine_urls("sqlite:////tmp/hetu.db")
    assert sync_url.startswith("sqlite:///")
    assert async_url.startswith("sqlite+aiosqlite:///")


def test_sql_parse_engine_urls_reject_explicit_driver():
    with pytest.raises(ValueError, match="driver"):
        SQLBackendClient.parse_engine_urls(
            "postgresql+asyncpg://user:pwd@127.0.0.1:5432/db"
        )


def test_sql_range_normalize_keeps_inf_handling():
    left, right, li, ri = SQLBackendClient.range_normalize_(
        np.dtype(np.int64), -np.inf, np.inf, False
    )
    assert left == np.iinfo(np.int64).min
    assert right == np.iinfo(np.int64).max
    assert li is True
    assert ri is True


def test_sql_notify_table_uses_channel_column():
    table = SQLBackendClient.notify_table()
    assert "channel" in table.c
    assert "created_at" in table.c


def test_sql_notify_table_channel_is_cross_dialect_text():
    table = SQLBackendClient.notify_table(sa.MetaData())
    pg_ddl = str(sa.schema.CreateTable(table).compile(dialect=postgresql.dialect()))
    mysql_ddl = str(sa.schema.CreateTable(table).compile(dialect=mysql.dialect()))
    assert "VARCHAR(256)" in pg_ddl
    assert "VARCHAR(256)" in mysql_ddl


def test_sql_mq_channel_filter_threshold():
    assert SQLNotifyHub._should_use_channel_in_filter(1)
    assert SQLNotifyHub._should_use_channel_in_filter(MAX_CHANNELS_IN_FILTER)
    assert not SQLNotifyHub._should_use_channel_in_filter(MAX_CHANNELS_IN_FILTER + 1)


def test_sql_post_configure_runs_support_table_ddl_on_master():
    client = object.__new__(SQLBackendClient)
    client.is_servant = False
    client._ensure_open = Mock()
    client.ensure_support_tables_sync = Mock()
    client._schema_checking_for_sql = Mock()

    client.post_configure()

    client._ensure_open.assert_called_once_with()
    client.ensure_support_tables_sync.assert_called_once_with()
    client._schema_checking_for_sql.assert_called_once_with(None)


def test_sql_post_configure_skips_support_table_ddl_on_servant():
    client = object.__new__(SQLBackendClient)
    client.is_servant = True
    client._ensure_open = Mock()
    client.ensure_support_tables_sync = Mock()
    client._schema_checking_for_sql = Mock()

    client.post_configure()

    client._ensure_open.assert_called_once_with()
    client.ensure_support_tables_sync.assert_not_called()
    client._schema_checking_for_sql.assert_called_once_with(None)


@pytest.mark.asyncio
async def test_sql_mq_pull_waits_for_subscribed_channel_in_fallback_mode():
    """回退到按id扫描模式时，查到一批全是无关频道不算命中（返回False），命中订阅才True"""
    notify_table = SQLBackendClient.notify_table(sa.MetaData())
    target_channel = "target-channel"
    responses = [
        [{"id": 1, "channel": "unrelated-channel"}],
        [{"id": 2, "channel": target_channel}],
    ]
    calls = {"count": 0}

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def mappings(self):
            return self

        def all(self):
            return self._rows

    class _FakeConn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def execute(self, stmt):
            del stmt
            idx = calls["count"]
            calls["count"] += 1
            if idx >= len(responses):
                return _FakeResult([])
            return _FakeResult(responses[idx])

    class _FakeAio:
        def connect(self):
            return _FakeConn()

    hub = SQLNotifyHub(
        SimpleNamespace(notify_table=lambda: notify_table, aio=_FakeAio())  # type: ignore[arg-type]
    )
    mq = SQLMQClient(hub)
    # 直接登记订阅（不走 hub.add，它会先去查游标），订阅数超过阈值触发回退模式
    hub._subs[target_channel] = {mq}
    hub._since[target_channel] = 0  # 登记完成的频道都有水位（add() 取的），这里直接给
    for i in range(MAX_CHANNELS_IN_FILTER):
        hub._subs[f"extra-{i}"] = {mq}
        hub._since[f"extra-{i}"] = 0
    hub._large_sub_warned = True

    assert await hub.poll_once() == (1, 0)  # 第一批只有无关频道，游标照样前进
    assert hub._last_notify_id == 1
    assert not mq.pulled_set
    assert await hub.poll_once() == (1, 1)
    assert calls["count"] == 2
    assert target_channel in mq.pulled_set
    assert hub._last_notify_id == 2


def test_sql_maintenance_get_lock_blocks_until_release(tmp_path):
    db_path = tmp_path / "maintenance_lock.sqlite3"
    client = SQLBackendClient(f"sqlite:///{db_path.as_posix()}", is_servant=False)
    try:
        maint1 = client.get_table_maintenance()
        maint2 = client.get_table_maintenance()

        holder_ready = threading.Event()
        holder_release = threading.Event()
        waiter_done = threading.Event()
        errors: list[BaseException] = []
        elapsed: dict[str, float] = {}

        def holder():
            try:
                with maint1.get_lock():
                    holder_ready.set()
                    holder_release.wait(timeout=3.0)
            except BaseException as exc:  # pragma: no cover - debug only
                errors.append(exc)
                holder_ready.set()

        def waiter():
            started = time.perf_counter()
            try:
                with maint2.get_lock():
                    elapsed["seconds"] = time.perf_counter() - started
            except BaseException as exc:  # pragma: no cover - debug only
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


@pytest.mark.asyncio
async def test_sql_mq_pull_updates_subscribed_channels_during_loop():
    """每次轮询的 IN 过滤都要用最新的频道集合，轮询中途新增的订阅下一轮就能查到"""
    notify_table = SQLBackendClient.notify_table(sa.MetaData())
    new_channel = "new-channel"

    # We'll use a side effect to simulate adding a subscription mid-loop
    calls = {"count": 0}

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def mappings(self):
            return self

        def all(self):
            return self._rows

    class _FakeConn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def execute(self, stmt):
            calls["count"] += 1
            # On first call, return nothing and add a new subscription
            if calls["count"] == 1:
                hub._subs[new_channel] = {mq}
                hub._since[new_channel] = 0
                return _FakeResult([])
            # On second call, simulate the DB having a message for the new channel
            # In the BUGGY version, the query's WHERE IN (...) clause will NOT include new_channel
            # because 'channels' was captured before the loop.
            # However, since we are MOCKING the execution, we need to check the STMT.

            # Check if new_channel is in the statement's IN clause if use_channel_filter is true
            # For simplicity, we can just return the row and see if SQLMQClient filters it out locally.
            # SQLMQClient also has a local check: if channel_name not in self.subscribed: continue
            # But the MAIN issue is the SQL query itself being too restrictive.

            # To strictly prove the SQL bug, we check 'stmt'
            compiled_stmt = str(stmt.compile(compile_kwargs={"literal_binds": True}))
            if new_channel not in compiled_stmt:
                # This simulates the DB NOT returning the row because it's not in the IN clause
                return _FakeResult([])

            return _FakeResult([{"id": 1, "channel": new_channel}])

    class _FakeAio:
        def connect(self):
            return _FakeConn()

    hub = SQLNotifyHub(
        SimpleNamespace(notify_table=lambda: notify_table, aio=_FakeAio())  # type: ignore[arg-type]
    )
    mq = SQLMQClient(hub)
    # Start with one existing channel to ensure use_channel_filter is True
    hub._subs["existing-channel"] = {mq}
    hub._since["existing-channel"] = 0

    # 第一轮：fake 在执行查询时新增了 new_channel 的订阅；第二轮的 IN 过滤必须包含它
    assert await hub.poll_once() == (0, 0)
    assert await hub.poll_once() == (1, 1)
    assert calls["count"] == 2
    assert new_channel in mq.pulled_set
    assert hub._last_notify_id == 1


def _check_notify_payload_column_upgrade(engine):
    """旧版本的通知表没有payload列，ensure_notify_payload_column_sync应补上且幂等"""
    name = SQLBackendClient.NOTIFY_TABLE_NAME
    # 先清掉可能残留的表，造一张旧结构的表
    old_meta = sa.MetaData()
    old_table = sa.Table(
        name,
        old_meta,
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("channel", sa.String(length=256), nullable=False, index=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False, index=True),
    )
    old_table.drop(engine, checkfirst=True)
    old_meta.create_all(engine)
    cols = {c["name"] for c in sa.inspect(engine).get_columns(name)}
    assert "payload" not in cols

    SQLBackendClient.ensure_notify_payload_column_sync(engine)
    cols = {c["name"] for c in sa.inspect(engine).get_columns(name)}
    assert "payload" in cols
    # 幂等
    SQLBackendClient.ensure_notify_payload_column_sync(engine)

    # 新结构能正常读写payload
    payload = bytes([0x91, 0xA1, 0x31])  # msgpack(["1"])
    table = SQLBackendClient.notify_table(sa.MetaData())
    with engine.begin() as conn:
        conn.execute(
            sa.insert(table),
            [{"channel": "c", "created_at": datetime.now(UTC), "payload": payload}],
        )
        got = conn.execute(sa.select(table.c.payload)).scalar()
    assert bytes(got) == payload
    table.drop(engine, checkfirst=True)


def test_sql_notify_table_payload_column_upgrade_sqlite(tmp_path):
    db_path = tmp_path / "notify_upgrade.sqlite3"
    engine = sa.create_engine(f"sqlite:///{db_path.as_posix()}")
    try:
        _check_notify_payload_column_upgrade(engine)
    finally:
        engine.dispose()


def test_sql_notify_table_payload_column_upgrade_postgres(ses_postgres_service):
    # PostgreSQL 会把未引用的标识符折叠成小写，专门验证 ALTER TABLE 的表名引用
    sync_url, _ = SQLBackendClient.parse_engine_urls(ses_postgres_service)
    engine = sa.create_engine(sync_url)
    try:
        _check_notify_payload_column_upgrade(engine)
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_sql_hub_concurrent_first_subscribers_keep_earliest_watermark():
    """两个连接并发订阅同一个新频道：先登记再放游标，后到的不再各自查一次表尾把先到者的
    水位抬高——否则先到者订阅之后、后到者查表尾之前提交的通知会被当成旧通知丢掉"""
    notify_table = SQLBackendClient.notify_table(sa.MetaData())
    hub = SQLNotifyHub(
        SimpleNamespace(notify_table=lambda: notify_table, aio=None)  # type: ignore[arg-type]
    )
    gate = asyncio.Event()
    max_ids = iter([10, 20])
    queries = 0

    async def fake_max_id():
        nonlocal queries
        queries += 1
        await gate.wait()
        return next(max_ids)

    hub._get_current_notify_id = fake_max_id  # type: ignore[method-assign]
    hub._run = lambda: asyncio.sleep(3600)  # type: ignore[method-assign]  轮询本身不测
    mq_a, mq_b = SQLMQClient(hub), SQLMQClient(hub)
    t_a = asyncio.create_task(mq_a.subscribe("C"))
    await asyncio.sleep(0)
    t_b = asyncio.create_task(mq_b.subscribe("C"))
    await asyncio.sleep(0)
    assert hub.subscriber_count("C") == 2, "登记应在等表尾之前完成"

    gate.set()
    async with asyncio.timeout(1):
        await asyncio.gather(t_a, t_b)
    assert queries == 1, "第二个订阅者应复用先到者的游标，不再查表尾"
    assert hub._since["C"] == 10
    assert hub._last_notify_id == 10
    await hub.close()


@pytest.mark.asyncio
async def test_sql_hub_new_channel_while_polling_keeps_cursor():
    """轮询已经在跑时新频道照样取表尾做水位（游标只随命中订阅频道的行前进，可能远落后于
    表尾，不能拿它当水位重放旧通知），但不能动游标"""
    notify_table = SQLBackendClient.notify_table(sa.MetaData())
    hub = SQLNotifyHub(
        SimpleNamespace(notify_table=lambda: notify_table, aio=None)  # type: ignore[arg-type]
    )
    max_ids = iter([100, 300])

    async def fake_max_id():
        return next(max_ids)

    hub._get_current_notify_id = fake_max_id  # type: ignore[method-assign]
    hub._run = lambda: asyncio.sleep(3600)  # type: ignore[method-assign]  轮询本身不测
    mq = SQLMQClient(hub)
    await mq.subscribe("A")
    assert hub._since["A"] == 100 and hub._last_notify_id == 100
    hub._last_notify_id = 105  # 轮询前进了一点，表尾已经到 300
    await mq.subscribe("B")
    assert hub._since["B"] == 300, "新频道的水位是表尾，不是游标"
    assert hub._last_notify_id == 105, "轮询在跑，不能动游标"
    await hub.close()


@pytest.mark.asyncio
async def test_sql_hub_watermark_survives_callers_cancellation():
    """先到者等表尾时被取消（连接断了）：搭车登记的连接还等着这个水位，取水位不能跟着中断；
    先到者只撤自己的登记"""
    notify_table = SQLBackendClient.notify_table(sa.MetaData())
    hub = SQLNotifyHub(
        SimpleNamespace(notify_table=lambda: notify_table, aio=None)  # type: ignore[arg-type]
    )
    gate = asyncio.Event()

    async def fake_max_id():
        await gate.wait()
        return 10

    hub._get_current_notify_id = fake_max_id  # type: ignore[method-assign]
    hub._run = lambda: asyncio.sleep(3600)  # type: ignore[method-assign]
    mq_a, mq_b = SQLMQClient(hub), SQLMQClient(hub)
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


@pytest.mark.asyncio
async def test_sql_hub_poll_skips_channel_without_watermark():
    """已登记但水位还没取到的频道，轮询遇到它的通知先跳过（取水位与轮询互斥，取回的表尾
    只会 >= 这些行的 id，本来就不属于它）"""
    notify_table = SQLBackendClient.notify_table(sa.MetaData())

    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def mappings(self):
            return self

        def all(self):
            return self._rows

    class _FakeConn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def execute(self, stmt):
            return _FakeResult([{"id": 7, "channel": "C", "payload": None}])

    class _FakeAio:
        def connect(self):
            return _FakeConn()

    hub = SQLNotifyHub(
        SimpleNamespace(notify_table=lambda: notify_table, aio=_FakeAio())  # type: ignore[arg-type]
    )
    mq = SQLMQClient(hub)
    hub._subs["C"] = {mq}  # 登记了、水位还没到
    fetched, hits = await hub.poll_once()
    assert (fetched, hits) == (1, 0)
    assert not mq.pulled_set
    hub._since["C"] = 5
    fetched, hits = await hub.poll_once()
    assert (fetched, hits) == (1, 1)
    assert "C" in mq.pulled_set
    await hub.close()


@pytest.mark.asyncio
async def test_sql_hub_poll_failure_backs_off_and_throttles_logs(monkeypatch, caplog):
    """轮询通知表失败：指数退避（0.5s 起、封顶 5s），栈只记第一次，之后一行一条，恢复记一条"""
    import logging

    notify_table = SQLBackendClient.notify_table(sa.MetaData())
    hub = SQLNotifyHub(
        SimpleNamespace(notify_table=lambda: notify_table, aio=None)  # type: ignore[arg-type]
    )
    hub._subs["C"] = {SQLMQClient(hub)}
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
    monkeypatch.setattr("hetu.data.backend.sql.mq.asyncio.sleep", fake_sleep)
    caplog.set_level(logging.INFO, logger="HeTu.root")
    async with asyncio.timeout(3):
        await hub._run()

    assert sleeps[:6] == [0.5, 1.0, 2.0, 4.0, 5.0, 5.0], "指数退避并封顶"
    records = [r for r in caplog.records if "轮询通知表" in r.getMessage()]
    with_stack = [r for r in records if r.exc_info]
    assert len(with_stack) == 1, "栈只记第一次"
    assert sum(1 for r in records if r.levelno == logging.ERROR) == 6
    assert sum(1 for r in records if "恢复" in r.getMessage()) == 1


@pytest.mark.parametrize(
    "backend_name", [b for b in SQL_BACKENDS if b == "mariadb"], indirect=True
)
async def test_sql_mariadb_bytes_param_roundtrip(mod_backend_config):
    """bytes 参数经 aiomysql 转义后原样读回。aiomysql 0.3.2 转义 bytes 用的 PyMySQL 内部
    函数从 PyMySQL 1.2.1 起没了（1.2.3 只剩字符串占位），一有 bytes 参数就
    TypeError: 'str' object is not callable"""
    client = SQLBackendClient(mod_backend_config["master"], False)
    payload = b"foo'bar\x00\\\xff"
    try:
        async with client.aio.connect() as conn:
            result = await conn.execute(sa.text("SELECT :p"), {"p": payload})
            assert result.scalar_one() == payload
    finally:
        await client.close()


def test_aiomysql_escape_bytes_patch_only_replaces_placeholder(monkeypatch):
    """垫片只替换 PyMySQL>=1.2.3 留下的字符串占位；老 PyMySQL（它还是函数）或 aiomysql
    已经不再用这个名字（>=0.3.3）时什么都不动"""
    import aiomysql.connection as aiomysql_conn

    from hetu.data.backend.sql.client import _patch_aiomysql_escape_bytes

    monkeypatch.setattr(
        aiomysql_conn, "escape_bytes_prefixed", "DO NOT IMPORT THIS!!!", raising=False
    )
    _patch_aiomysql_escape_bytes()
    assert aiomysql_conn.escape_bytes_prefixed(b"a'b") == "_binary X'612762'"

    def original(value: bytes) -> str:
        return "untouched"

    monkeypatch.setattr(aiomysql_conn, "escape_bytes_prefixed", original)
    _patch_aiomysql_escape_bytes()
    assert aiomysql_conn.escape_bytes_prefixed is original

    monkeypatch.delattr(aiomysql_conn, "escape_bytes_prefixed")
    _patch_aiomysql_escape_bytes()
    assert not hasattr(aiomysql_conn, "escape_bytes_prefixed")
