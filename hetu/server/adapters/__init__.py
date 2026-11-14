"""
Web 框架适配层：每个框架一个模块，只负责建框架的 app、把 `WorkerRuntime` 接到框架的生命周期上、
把连接交给 HeTu 处理。HeTu 自己的逻辑都在 `hetu.server.runtime` 等与框架无关的模块里。

Web framework adapters: one module per framework, wiring `WorkerRuntime` into the
framework's lifecycle.
"""
