"""
HeTu 服务端。`worker_main`（Sanic 适配）按需加载：只用 runtime、pipeline 等模块时不加载 Web 框架。

HeTu server package. `worker_main` (the Sanic adapter) is imported lazily, so using
`runtime`, `pipeline` and friends does not load a web framework.
"""

from typing import TYPE_CHECKING, Any

from .runtime import StartupAborted, WorkerRuntime

if TYPE_CHECKING:
    from .adapters.sanic_app import worker_main

__all__ = ["StartupAborted", "WorkerRuntime", "worker_main"]


def __getattr__(name: str) -> Any:
    if name == "worker_main":
        from .adapters.sanic_app import worker_main

        return worker_main
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
