"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from . import data
from . import system
from . import common
from . import endpoint
from importlib.metadata import version, PackageNotFoundError

# 常用核心对象导出到顶层命名空间
Permission = common.Permission
define_endpoint = endpoint.define_endpoint
define_system = system.define_system
define_component = data.define_component
elevate = endpoint.elevate
ResponseToClient = endpoint.ResponseToClient
# Context = endpoint.Context  Context和Endpoint有关，名字上看不出，不太合适放在顶层
# SystemContext = system.SystemContext


try:
    __version__ = version("hetu")
except PackageNotFoundError:
    __version__ = "hetu is not installed in a proper way"


__all__ = [
    "data",
    "system",
    "common",
    "endpoint",
    "Permission",
    "define_endpoint",
    "define_component",
    "define_system",
    "elevate",
    "ResponseToClient",
]
