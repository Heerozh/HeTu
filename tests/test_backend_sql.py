import asyncio
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mysql, postgresql

from hetu.common.multimap import MultiMap
from hetu.data.backend.sql import SQLBackendClient
from hetu.data.backend.sql.mq import MAX_CHANNELS_IN_FILTER, SQLMQClient


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
    assert SQLMQClient._should_use_channel_in_filter(1)
    assert SQLMQClient._should_use_channel_in_filter(MAX_CHANNELS_IN_FILTER)
    assert not SQLMQClient._should_use_channel_in_filter(MAX_CHANNELS_IN_FILTER + 1)


def test_sql_post_configure_runs_support_table_ddl_on_master():
    client = object.__new__(SQLBackendClient)
    client.is_servant = False
    client._ensure_open = Mock()
    client.ensure_support_tables_sync = Mock()
    client._schema_checking_for_sql = Mock()

    client.post_configure()

    client._ensure_open.assert_called_once_with()
    client.ensure_support_tables_sync.assert_called_once_with()
    client._schema_checking_for_sql.assert_called_once_with()


def test_sql_post_configure_skips_support_table_ddl_on_servant():
    client = object.__new__(SQLBackendClient)
    client.is_servant = True
    client._ensure_open = Mock()
    client.ensure_support_tables_sync = Mock()
    client._schema_checking_for_sql = Mock()

    client.post_configure()

    client._ensure_open.assert_called_once_with()
    client.ensure_support_tables_sync.assert_not_called()
    client._schema_checking_for_sql.assert_called_once_with()


@pytest.mark.asyncio
async def test_sql_mq_pull_waits_for_subscribed_channel_in_fallback_mode(monkeypatch):
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

    async def _fast_sleep(_seconds: float):
        return None

    monkeypatch.setattr("hetu.data.backend.sql.mq.asyncio.sleep", _fast_sleep)

    mq = object.__new__(SQLMQClient)
    mq._client = SimpleNamespace(
        notify_table=lambda: notify_table,
        aio=_FakeAio(),
    )
    mq.subscribed = {target_channel}
    for i in range(MAX_CHANNELS_IN_FILTER):
        mq.subscribed.add(f"extra-{i}")
    mq.pulled_deque = MultiMap()
    mq.pulled_set = set()
    mq._last_notify_id = 0
    mq._large_sub_warned = True

    await mq.pull()

    assert calls["count"] >= 2
    assert target_channel in mq.pulled_set
    assert mq._last_notify_id == 2


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
async def test_sql_mq_pull_updates_subscribed_channels_during_loop(monkeypatch):
    notify_table = SQLBackendClient.notify_table(sa.MetaData())
    new_channel = "new-channel"
    
    # We'll use a side effect to simulate adding a subscription mid-loop
    calls = {"count": 0}
    
    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows
        def mappings(self): return self
        def all(self): return self._rows

    class _FakeConn:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): return False
        async def execute(self, stmt):
            calls["count"] += 1
            # On first call, return nothing and add a new subscription
            if calls["count"] == 1:
                mq.subscribed.add(new_channel)
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
        def connect(self): return _FakeConn()

    orig_sleep = asyncio.sleep
    async def _fast_sleep(_seconds: float):
        # Allow the loop to continue
        await orig_sleep(0)
        if calls["count"] > 10: # Safety break
            raise Exception("Looping too much - reproduction failed or stale channels used")

    monkeypatch.setattr("hetu.data.backend.sql.mq.asyncio.sleep", _fast_sleep)

    mq = object.__new__(SQLMQClient)
    mq._client = SimpleNamespace(
        notify_table=lambda: notify_table,
        aio=_FakeAio(),
    )
    # Start with one existing channel to ensure use_channel_filter is True
    mq.subscribed = {"existing-channel"}
    mq.pulled_deque = MultiMap()
    mq.pulled_set = set()
    mq._last_notify_id = 0
    mq._large_sub_warned = False
    
    # Run pull. It should finish when it receives the message for new_channel.
    # In the buggy version, it will loop indefinitely (or until our safety break)
    # because the SQL query will never include 'new-channel' in its IN filter.
    try:
        await asyncio.wait_for(mq.pull(), timeout=2.0)
    except asyncio.TimeoutError:
        pytest.fail("Timed out! SQLMQClient.pull() did not pick up the new channel (BUG REPRODUCED)")
    except Exception as e:
        if "Looping too much" in str(e):
            pytest.fail("SQLMQClient.pull() is stuck in a loop because it's using stale channels (BUG REPRODUCED)")
        raise e

    assert new_channel in mq.pulled_set
    assert mq._last_notify_id == 1
