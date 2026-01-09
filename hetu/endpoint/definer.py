"""
供客户端SDK远程调用的函数接口
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import inspect
from inspect import signature
from types import FunctionType
from dataclasses import dataclass

from hetu.common import Singleton


ENDPOINT_NAME_MAX_LEN = 32


@dataclass
class EndpointDefine:
    func: FunctionType
    arg_count: int  # 全部参数个数（含默认参数）
    defaults_count: int  # 默认参数个数


class EndpointDefines(metaclass=Singleton):
    """
    储存所有Endpoint定义的信息。Endpoint是供客户端SDK远程调用的函数接口。
    c# sdk: Hetu.call("func", arg)

    此类只负责储存定义，调度器通过此类查询Endpoints信息。
    """

    def __init__(self):
        # 所有endpoint定义表，按namespace分类
        self._endpoint_map: dict[str, dict[str, EndpointDefine]] = {}
        # @endpoint(namespace="global") 定义的所有endpoint
        self._global_endpoint_map: dict[str, EndpointDefine] = {}
        # 方便快速访问主namespace的endpoint定义
        self._main_namespace: str = ""
        self._main_endpoint_map: dict[str, EndpointDefine] = {}

    def _clear(self):
        self._endpoint_map = {}

    def get_endpoint(
        self, endpoint_name: str, namespace: str | None = None
    ) -> EndpointDefine | None:
        if namespace:
            return self._endpoint_map[namespace].get(endpoint_name, None)
        else:
            return self._main_endpoint_map.get(endpoint_name, None)

    def get_endpoints(self, namespace: str) -> dict[str, EndpointDefine]:
        return self._endpoint_map[namespace]

    def add(self, namespace, func, force):
        sub_map = self._endpoint_map.setdefault(namespace, dict())

        if not force:
            assert func.__name__ not in sub_map, "Endpoint重复定义：" + func.__name__

        # 获取函数参数个数，存下来，要求客户端调用严格匹配
        arg_count = func.__code__.co_argcount
        defaults_count = len(func.__defaults__) if func.__defaults__ else 0

        sub_map[func.__name__] = EndpointDefine(
            func=func,
            arg_count=arg_count,
            defaults_count=defaults_count,
        )

        if namespace == "global":
            self._global_endpoint_map[func.__name__] = sub_map[func.__name__]


def endpoint(namespace: str = "global", force: bool = False):
    """
    把一个函数包装成可供客户端远程调用的接口。

    大部分情况不需要调用此包装器，你可以把逻辑代码直接写在System中。
    System在定义时，如果设置了permission，则会自动生成对应的Endpoint。
    如果你需要执行多次System，或不牵涉数据库操作的逻辑，可以使用Endpoint。

    Warnings
    --------
    如果在Endpoint中调用多次System，注意多次调用之间的事务是中断的。

    Examples
    --------
    >>> from hetu.endpoint import endpoint, Context, ResponseToClient
    >>>
    >>> @endpoint(namespace="example")
    ... async def pay(ctx: Context, order_id, paid):
    ...     await ctx.connection.excutor.call_system("SystemName", order_id, paid)
    ...     return ResponseToClient(['anything', 'blah blah'])

    Parameters
    ----------
    namespace: str
        是你的项目名，一个网络地址只能启动一个namespace下的Endpoint们。
        定义为"global"的namespace可以在所有项目下通用。
    force: bool
        遇到重复定义是否强制覆盖前一个, 单元测试用

    Notes
    -----
    **Endpoint函数要求：** ::

        async def pay(ctx: Context, order_id, paid)

    async:
        Endpoint必须是异步函数。
    ctx: Context
        上下文，具体见下述Context部分
    其他参数:
        为hetu client SDK调用时传入的参数。
    Endpoint返回值:
        如果调用方是hetu client SDK：
            - 返回值是 hetu.system.ResponseToClient(data)时，则把data发送给调用方sdk。
            - 其他返回值丢弃

    **Context部分：**
        连接/用户身份上下文，常用的有：

        ctx.caller: int
            调用者id，由你在登录System中调用 `elevate` 函数赋值，`None` 或 0 表示未登录用户

        具体参见 Context 类定义。

    See Also
    --------
    hetu.system.define_system : define_system装饰器定义System
    hetu.endpoint.Context : Context类定义
    """

    def warp(func):
        # warp只是在系统里记录下有这么个东西，实际不改变function

        # 严格要求第一个参数命名
        func_args = signature(func).parameters
        func_arg_names = list(func_args.keys())[:1]
        assert func_arg_names == ["ctx"], (
            f"Endpoint参数名定义错误，第一个参数必须为：ctx。你的：{func_arg_names}"
        )

        assert len(func.__name__) <= ENDPOINT_NAME_MAX_LEN, (
            f"Endpoint函数名过长，最大长度为{ENDPOINT_NAME_MAX_LEN}个字符"
        )

        assert inspect.iscoroutinefunction(func), (
            f"Endpoint {func.__name__} 必须是异步函数(`async def ...`)"
        )

        EndpointDefines().add(namespace, func, force)

        return func

    return warp
