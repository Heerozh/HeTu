"""
HeTu 服务端。Web 服务器（aiohttp）的适配层在 `hetu.server.adapters.aiohttp_app`，按需加载：只用
runtime、pipeline 等模块时不加载 Web 框架。

HeTu server package. The aiohttp adapter lives in `hetu.server.adapters.aiohttp_app` and
is imported on demand, so using `runtime`, `pipeline` and friends does not load a web
framework.
"""

from .runtime import StartupAborted, WorkerRuntime

__all__ = ["StartupAborted", "WorkerRuntime"]
