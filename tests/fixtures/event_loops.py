"""
测试用的事件循环。生产在 Linux / macOS 上跑 uvloop（Sanic 默认启用），本机（Windows）测试跑的是
标准事件循环，两者的 API 并不完全一样。
"""

import asyncio


class UvloopSignatureLoop(asyncio.EventLoop):
    """
    create_task 的签名与 uvloop 一致、只收 name / context 的标准事件循环。3.14 起标准循环的
    create_task 还收 eager_start 等参数，uvloop 0.22 不收（TypeError）：这类用法在标准循环上跑得
    好好的，上了生产的 uvloop 才出错。conftest 让所有异步用例都跑在它上面，本机就能发现
    """

    def create_task(self, coro, *, name=None, context=None):  # type: ignore[override]
        return super().create_task(coro, name=name, context=context)
