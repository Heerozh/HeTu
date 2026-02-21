import numpy as np
import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mysql, postgresql
from types import SimpleNamespace
from unittest.mock import Mock

from hetu.data.backend.sql import SQLBackendClient
from hetu.data.backend.sql.mq import MAX_CHANNELS_IN_FILTER, SQLMQClient
from hetu.common.multimap import MultiMap


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
    client._ensure_support_tables_sync = Mock()
    client._schema_checking_for_sql = Mock()

    client.post_configure()

    client._ensure_open.assert_called_once_with()
    client._ensure_support_tables_sync.assert_called_once_with()
    client._schema_checking_for_sql.assert_called_once_with()


def test_sql_post_configure_skips_support_table_ddl_on_servant():
    client = object.__new__(SQLBackendClient)
    client.is_servant = True
    client._ensure_open = Mock()
    client._ensure_support_tables_sync = Mock()
    client._schema_checking_for_sql = Mock()

    client.post_configure()

    client._ensure_open.assert_called_once_with()
    client._ensure_support_tables_sync.assert_not_called()
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
