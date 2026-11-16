"""
Web 框架适配层（`aiohttp_app`）：只负责建框架的 app、把 `WorkerRuntime` 接到框架的生命周期上、把连接
交给 HeTu 处理。HeTu 自己的逻辑都在 `hetu.server.runtime` 等与框架无关的模块里。

Web framework adapter (`aiohttp_app`), wiring `WorkerRuntime` into the framework's
lifecycle.
"""
