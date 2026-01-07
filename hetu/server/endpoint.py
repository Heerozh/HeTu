"""
供客户端SDK远程调用的函数接口
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import copy
import functools
import inspect
from dataclasses import dataclass
from inspect import signature
from types import FunctionType
from typing import TYPE_CHECKING, Any

from ..common import Singleton

if TYPE_CHECKING:
    from ..data import BaseComponent

endpoint_NAME_MAX_LEN = 32


@dataclass
class EndpointDefine:
    func: FunctionType
    arg_count: int  # 全部参数个数（含默认参数）
    defaults_count: int  # 默认参数个数


class Endpoints(metaclass=Singleton):
    """
    储存所有Endpoint定义的信息。Endpoint是供客户端SDK远程调用的函数接口。
    c# sdk: Hetu.call("func", arg)

    此类只负责储存定义，调度器通过此类查询Endpoints信息。
    """

    def __init__(self):
        # ==== @define_endpoint(namespace=xxx) 定义的所有endpoint ====
        # 所有endpoint定义表，按namespace分类
        self._endpoint_map: dict[str, dict[str, EndpointDefine]] = {}
        # @define_endpoint(namespace="global") 定义的所有endpoint
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
        return {
            name: self.get_endpoint(name, namespace)
            for name in self._endpoint_map[namespace]
        }  # type: ignore

    def add(self, namespace, func, components, force, permission, depends, max_retry):
        sub_map = self._endpoint_map.setdefault(namespace, dict())
