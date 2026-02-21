import numpy as np
import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mysql, postgresql

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
