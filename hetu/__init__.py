"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from . import data
from . import system
from . import common

from .manager import ComponentTableManager

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("hetu")
except PackageNotFoundError:
    __version__ = "hetu is not installed in a proper way"
