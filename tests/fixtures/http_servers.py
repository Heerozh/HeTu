"""
测试在哪几个 Web 服务器栈上跑：环境变量 HETU_TEST_HTTP_SERVERS 取 sanic,aiohttp 的子集（逗号分隔），不设时
两个都跑，写法同 HETU_TEST_BACKENDS。换 aiohttp 的过渡期用，删掉 Sanic 时一起删（设计稿
docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §4.3）。
"""

import os

import pytest

ALL_HTTP_SERVERS = ["sanic", "aiohttp"]

_env_servers = os.environ.get("HETU_TEST_HTTP_SERVERS")
if _env_servers:
    _allowed = [s.strip() for s in _env_servers.split(",")]
    HTTP_SERVERS = [s for s in ALL_HTTP_SERVERS if s in _allowed]
else:
    HTTP_SERVERS = list(ALL_HTTP_SERVERS)


@pytest.fixture(params=HTTP_SERVERS)
def http_server(request) -> str:
    """Web 服务器栈：sanic 或 aiohttp。用它的测试在每个栈上各跑一遍"""
    return request.param
