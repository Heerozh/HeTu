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

# 原来由 Sanic 读取、迁移后仍保留的键，HeTu 自己给默认值（和 Sanic 25.12 的默认值等价，只有
# PROXIES_COUNT 用 0 代替 None，都表示不解析），免得换框架时默认值悄悄变掉。DEBUG 也给个默认值：
# worker_main 要按它设日志级别
DEFAULTS: dict[str, Any] = {
    "DEBUG": False,
    # 关服时等连接拆完、等后台任务停下各自的上限（秒）
    "GRACEFUL_SHUTDOWN_TIMEOUT": 15.0,
    # 关服排空的上限（秒）：各连接不再收新消息，已经收到的处理完、回复发出去再断开。超时还没排空的
    # 直接断开，在途的 RPC 随之取消。目前只在 aiohttp 栈上生效（设计稿 §5.5）
    "SHUTDOWN_DRAIN_TIMEOUT": 5.0,
    # 一条 ws 消息的最大字节数（模板里设的是 0.5MB）
    "WEBSOCKET_MAX_SIZE": 2**20,
    # ws ping 的间隔、等 pong 的超时（秒）
    "WEBSOCKET_PING_INTERVAL": 20,
    "WEBSOCKET_PING_TIMEOUT": 20,
    # 前置反向代理的层数、从哪个头取客户端真实 IP（0 / None 为不解析，直接用对端地址）
    "PROXIES_COUNT": 0,
    "REAL_IP_HEADER": None,
    "FORWARDED_FOR_HEADER": "X-Forwarded-For",
    # HTTP 请求体的最大字节数，只影响 define_route
    "REQUEST_MAX_SIZE": 100_000_000,
}


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
