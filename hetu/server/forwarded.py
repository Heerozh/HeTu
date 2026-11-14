"""
从反向代理加的请求头里取客户端地址。规则移植自 Sanic 25.12 的 `parse_xforwarded` /
`fwd_normalize_address`（MIT License, Copyright (c) 2016-present Sanic Community），换掉 Sanic 后
结果保持不变（设计稿 docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §3.2 第 7 条、
§5.4.1）。

只支持 X-Forwarded-For 这一套（`REAL_IP_HEADER`、`FORWARDED_FOR_HEADER`、`PROXIES_COUNT`）。RFC 7239 的
`Forwarded` 头（Sanic 的 `FORWARDED_SECRET`）不支持。

注意：HeTu 被绕过反向代理直接访问时，这些头可以伪造，借此能绕过按 IP 限制的匿名连接数。要用防火墙
挡住直连。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

Resolve the client address from reverse-proxy headers, with the same rules as Sanic.
"""

import re
from collections.abc import Mapping
from typing import Any

from multidict import MultiMapping

# IPv6 地址（不带方括号）。和 Sanic 的一样宽松：只用来决定要不要加方括号
_IPV6_RE = re.compile("(?:[0-9A-Fa-f]{0,4}:){2,7}[0-9A-Fa-f]{0,4}")
DEFAULT_FORWARDED_FOR_HEADER = "X-Forwarded-For"


def normalize_address(addr: str) -> str | None:
    """
    规范化代理头里的地址："unknown" 丢掉（返回 None），`_` 开头的混淆标识原样返回，IPv6 加方括号，
    其余转小写。

    Normalize an address from a proxy header; returns None for "unknown".
    """
    if addr == "unknown":
        return None
    if addr.startswith("_"):
        return addr
    if _IPV6_RE.fullmatch(addr):
        addr = f"[{addr}]"
    return addr.lower()


def client_ip(
    headers: MultiMapping[str], peer_ip: str, config: Mapping[str, Any]
) -> str:
    """
    客户端地址：

    1. 配了 `REAL_IP_HEADER`、请求里也有这个头，就用它；
    2. 否则在 `PROXIES_COUNT` = N > 0 时，把所有 `FORWARDED_FOR_HEADER` 头合并，按逗号切开、去掉空白
       和空项，取倒数第 N 个；条目不够 N 个就当没有；
    3. 取到的地址经 `normalize_address` 规范化。取到的是 "unknown"，或者都没取到，就用直连对端的地址
       `peer_ip`（原样，IPv6 不加方括号，同 Sanic 的 `request.ip`）。

    headers 是大小写不敏感、一个名字可以有多个值的请求头（aiohttp 的 `request.headers`）。

    Resolve the client address like Sanic's `request.client_ip`.
    """
    real_ip_header = config.get("REAL_IP_HEADER")
    addr = headers.get(real_ip_header) if real_ip_header else None
    proxies_count = config.get("PROXIES_COUNT") or 0
    if not addr and proxies_count > 0:
        header = config.get("FORWARDED_FOR_HEADER") or DEFAULT_FORWARDED_FOR_HEADER
        entries = [
            entry
            for entry in (
                part.strip()
                for value in headers.getall(header, ())
                for part in value.split(",")
            )
            if entry
        ]
        if len(entries) >= proxies_count:
            addr = entries[-proxies_count]
    if addr:
        normalized = normalize_address(addr)
        if normalized is not None:
            return normalized
    return peer_ip
