"""
ws 连接的接口。HeTu 处理连接的代码（`serve_connection`、`client_handler`、`send_loop`）只经它收发，
不直接碰 Web 框架，由适配层实现（`adapters/aiohttp_app.py` 的 `AiohttpWsConnection`）。
设计稿 docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §5.3。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

WebSocket connection interface used by HeTu's connection handling, implemented by the
web framework adapter.
"""

import itertools
from typing import Any, Protocol


class ConnectionClosed(Exception):
    """
    对端已断开，或本端已经 fail 了连接，收发不了了。

    The peer is gone or the connection was failed locally.
    """


class Draining(ConnectionClosed):
    """
    服务器在关服排空（第 4b 步），这条连接不再收新消息：适配层要求停止接收之后，`recv` 抛它。连接还
    开着，发送照常，排在前面的回复发完之后由发送循环断开。不认识它的地方当成 ConnectionClosed 处理
    也安全：停下来、不再收。

    The server is draining: stop receiving, keep sending queued replies.
    """


class WsConnection(Protocol):
    """
    一个已经完成 ws 握手（HTTP upgrade）的连接。

    - `recv`：收一整条消息（分片已经重组），文本帧是 str、二进制帧是 bytes；给了 timeout 时超时返回
      None。适配层要求停止接收（关服排空）之后抛 `Draining`，正在等的 recv 也要醒来抛它；没读的消息
      不再交出去。连接断开之后在有限时间内抛 `ConnectionClosed`（本端 fail 之后对端又不读数据的，
      已经在等的 recv 要到 fail 的 abort 兜底时才醒）。
    - `send`：发一条消息，str 发文本帧、bytes 发二进制帧。连接已断开或已 fail 时抛 `ConnectionClosed`。
    - `fail`：同步调用，立即生效：返回时 close 码就已经定下，`serve_connection` 返回之后框架自己的
      关闭（aiohttp handler 返回时的 `write_eof()`）不能把它盖成 1000。尽力
      发出带 code、reason 的 close 帧后断开；可以重复调用，只有第一次的 code 生效；保证在有限时间内
      断开，正卡在发送上的 send 也要在有限时间内醒来（抛 `ConnectionClosed`），不受对端配合与否影响。
    - `protocol_error`：协议层的错误（帧格式错、消息超长等），没有就返回 None。对端正常掉线、直接断开
      不算。

    A WebSocket connection whose opening handshake is done.
    """

    conn_id: str
    """本进程内唯一的连接编号（见 `next_conn_id`），连接相关的任务名、日志用它"""

    client_ip: str
    """客户端地址，已经按 PROXIES_COUNT、REAL_IP_HEADER 等配置解析过"""

    request: Any
    """框架的原始请求，放进 ctx.request（unsafe）"""

    async def recv(self, timeout: float | None = None) -> str | bytes | None: ...

    async def send(self, data: str | bytes) -> None: ...

    def fail(self, code: int, reason: str) -> None: ...

    def protocol_error(self) -> BaseException | None: ...


# 本进程的连接序号。不能用客户端给的值（原来用过 Sanic 的 request.id，它取自 X-Request-ID 头）：
# 两个连接同名时任务表互相覆盖，一个连接拆掉时会取消掉另一个连接的接收协程
_conn_seq = itertools.count(1)


def next_conn_id() -> str:
    """
    分配一个本进程内唯一的连接编号。适配层建 `WsConnection` 时调用。

    Allocate a connection id that is unique within this process.
    """
    return str(next(_conn_seq))
