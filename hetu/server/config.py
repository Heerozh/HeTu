"""
HeTu 的服务器配置：读配置文件或命令行参数得到的 dict，可以按属性读键。与 Web 框架无关
（原来用的是 sanic.config.Config），设计稿 docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md
§5.2。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

HeTu server config: a dict with attribute access, independent of the web framework.
"""

from collections.abc import Mapping
from typing import Any

from ..i18n import _

# 原来由 Sanic 读取、迁移后仍保留的键，HeTu 自己给默认值（和 Sanic 25.12 的默认值等价，只有
# PROXIES_COUNT 用 0 代替 None，都表示不解析），免得换框架时默认值悄悄变掉。DEBUG 也给个默认值：
# 配日志、建事件循环时要按它。LISTEN_BACKLOG 是新加的，原来写死在代码里（Sanic 100，aiohttp 128）；
# DISCOVERY 也是新加的
DEFAULTS: dict[str, Any] = {
    "DEBUG": False,
    # 关服时等连接拆完、等后台任务停下各自的上限（秒）
    "GRACEFUL_SHUTDOWN_TIMEOUT": 15.0,
    # 关服排空的上限（秒）：各连接不再收新消息，已经收到的处理完、回复发出去再断开。超时还没排空的
    # 直接断开，在途的 RPC 随之取消（设计稿 §5.5）
    "SHUTDOWN_DRAIN_TIMEOUT": 5.0,
    # 一条 ws 消息的最大字节数（模板里设的是 0.5MB）
    "WEBSOCKET_MAX_SIZE": 2**20,
    # ws ping 的间隔（秒），等 pong 的时间是它的一半，0 为不发 ping
    "WEBSOCKET_PING_INTERVAL": 20,
    # 前置反向代理的层数、从哪个头取客户端真实 IP（0 / None 为不解析，直接用对端地址）
    "PROXIES_COUNT": 0,
    "REAL_IP_HEADER": None,
    "FORWARDED_FOR_HEADER": "X-Forwarded-For",
    # HTTP 请求体的最大字节数，只影响 define_route
    "REQUEST_MAX_SIZE": 100_000_000,
    # 监听队列长度（listen 的 backlog）：已经完成 TCP 握手、还没被 worker 接走的连接最多排多少个。
    # 大批客户端同时重连时，排不下的握手被内核丢掉，客户端要退避重传。内核会截到
    # net.core.somaxconn（设计稿 docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §12.4）
    "LISTEN_BACKLOG": 2048,
    # 节点发现：开好服后把本节点的地址登记进默认后端 Redis 的 hetu:nodes，网关上的 hetu dns 据此答
    # Caddy 的 DNS 查询（设计稿 docs/superpowers/specs/2026-11-15-node-discovery-design.md）。默认关闭：
    # 只支持 host 网络，现有的 bridge 网络部署照常能开
    "DISCOVERY": False,
}


def listen_backlog(config: Mapping[str, Any]) -> int:
    """
    配置里的监听队列长度 `LISTEN_BACKLOG`，不是正整数时抛 ValueError。

    The listen backlog (`LISTEN_BACKLOG`) from the config; raises ValueError unless it
    is a positive integer.
    """
    value = config.get("LISTEN_BACKLOG", DEFAULTS["LISTEN_BACKLOG"])
    # bool 是 int 的子类，`LISTEN_BACKLOG: true` 不能当成 1
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(
            _("LISTEN_BACKLOG 必须是正整数，现在是 {value!r}").format(value=value)
        )
    return value


class HeTuConfig(dict[str, Any]):
    """
    服务器配置。是一个 dict，键都是大写的，也可以按属性读（`config.NAMESPACE`）。没给的键用
    `DEFAULTS` 里的默认值。

    Server config: a dict of upper-case keys, readable as attributes, with defaults for
    the keys the web server reads.
    """

    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        super().__init__(DEFAULTS)
        if values is not None:
            self.update_config(values)

    def update_config(self, values: Mapping[str, Any]) -> None:
        """
        合并一份配置，只收大写的键（和原来 Sanic Config 一样：配置文件里小写的键是给 YAML 锚点
        之类用的，不算配置）。
        """
        self.update({key: value for key, value in values.items() if key.isupper()})

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    def __setattr__(self, name: str, value: Any) -> None:
        # 按属性写也写进 dict，不然属性读到新值、config["X"] / get 还是旧值
        self[name] = value
