"""
从反向代理头里取客户端地址（hetu/server/forwarded.py）：规则和 Sanic 25.12 逐条对照（设计稿
docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §3.2 第 7 条）。
"""

from types import SimpleNamespace

import pytest
from multidict import CIMultiDict

from hetu.server.config import HeTuConfig
from hetu.server.forwarded import client_ip

# (配置, 请求头, 说明)。对端地址固定是 PEER
PEER = "203.0.113.9"
ADDRESS_CASES = [
    ({}, [], "没配代理：用对端地址"),
    ({}, [("X-Forwarded-For", "1.2.3.4")], "没配 PROXIES_COUNT：不信代理头"),
    ({"PROXIES_COUNT": 1}, [], "没有这个头"),
    ({"PROXIES_COUNT": 1}, [("X-Forwarded-For", "1.2.3.4, 10.0.0.1")], "取倒数第 1 个"),
    ({"PROXIES_COUNT": 2}, [("X-Forwarded-For", "1.2.3.4, 10.0.0.1")], "取倒数第 2 个"),
    ({"PROXIES_COUNT": 3}, [("X-Forwarded-For", "1.2.3.4, 10.0.0.1")], "条目不够"),
    (
        {"PROXIES_COUNT": 3},
        [("X-Forwarded-For", "1.1.1.1, 2.2.2.2"), ("X-Forwarded-For", "3.3.3.3")],
        "多个头合并",
    ),
    (
        {"PROXIES_COUNT": 2},
        [("X-Forwarded-For", " 1.1.1.1 ,, , 2.2.2.2 ")],
        "空项和空白",
    ),
    (
        {"PROXIES_COUNT": 1},
        [("X-Forwarded-For", "2001:DB8::1")],
        "IPv6 加方括号、转小写",
    ),
    ({"PROXIES_COUNT": 1}, [("X-Forwarded-For", "unknown")], "unknown 丢掉"),
    ({"PROXIES_COUNT": 1}, [("X-Forwarded-For", "_Hidden")], "混淆标识原样"),
    ({"PROXIES_COUNT": 1}, [("X-Forwarded-For", "Some.Host")], "转小写"),
    ({"PROXIES_COUNT": 1}, [("x-forwarded-for", "1.2.3.4")], "头名不分大小写"),
    (
        {"PROXIES_COUNT": 1, "FORWARDED_FOR_HEADER": "X-Client-Chain"},
        [("X-Client-Chain", "1.2.3.4"), ("X-Forwarded-For", "5.6.7.8")],
        "自定义 FORWARDED_FOR_HEADER",
    ),
    (
        {"REAL_IP_HEADER": "X-Real-IP", "PROXIES_COUNT": 1},
        [("X-Real-IP", "9.9.9.9"), ("X-Forwarded-For", "1.2.3.4")],
        "REAL_IP_HEADER 优先",
    ),
    (
        {"REAL_IP_HEADER": "X-Real-IP", "PROXIES_COUNT": 1},
        [("X-Forwarded-For", "1.2.3.4")],
        "没有 REAL_IP_HEADER 头就看 X-Forwarded-For",
    ),
    (
        {"REAL_IP_HEADER": "X-Real-IP", "PROXIES_COUNT": 1},
        [("X-Real-IP", "unknown"), ("X-Forwarded-For", "1.2.3.4")],
        "REAL_IP_HEADER 是 unknown：不再看 X-Forwarded-For，用对端地址",
    ),
    (
        {"REAL_IP_HEADER": "X-Real-IP"},
        [("X-Real-IP", "::1")],
        "REAL_IP_HEADER 里的 IPv6",
    ),
]


def _sanic_client_ip(config: dict, headers: list[tuple[str, str]]) -> str:
    """Sanic 25.12 的结果：parse_xforwarded 的 for，没有就是对端地址（request.client_ip）"""
    from sanic.compat import Header
    from sanic.headers import parse_xforwarded

    sanic_config = SimpleNamespace(
        REAL_IP_HEADER=config.get("REAL_IP_HEADER"),
        PROXIES_COUNT=config.get("PROXIES_COUNT"),
        FORWARDED_FOR_HEADER=config.get("FORWARDED_FOR_HEADER", "X-Forwarded-For"),
    )
    forwarded = parse_xforwarded(Header(headers), sanic_config) or {}
    return str(forwarded.get("for", "")) or PEER


@pytest.mark.parametrize(("config", "headers", "label"), ADDRESS_CASES)
def test_client_ip_matches_sanic(config, headers, label):
    """客户端地址的规则和 Sanic 逐条一致（设计稿 §3.2 第 7 条）"""
    ours = client_ip(CIMultiDict(headers), PEER, HeTuConfig(config))
    assert ours == _sanic_client_ip(config, headers), label


def test_client_ip_peer_address_kept_as_is():
    """对端地址原样返回：IPv6 不加方括号，同 Sanic 的 request.ip"""
    assert client_ip(CIMultiDict(), "::1", HeTuConfig()) == "::1"
