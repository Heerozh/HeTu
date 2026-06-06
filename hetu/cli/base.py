"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import argparse
import os

from ..i18n import _


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0", "None"):
        return False
    else:
        raise argparse.ArgumentTypeError(_("Boolean value expected."))


def resolve_app_file(app_file: str, config_file: str) -> str:
    """把 config 文件中的相对 APP_FILE 路径，按 config 文件所在目录解析为绝对路径。

    Resolve a relative ``APP_FILE`` path against the directory of the config
    file, not the process CWD. Absolute paths are returned unchanged.
    """
    config_dir = os.path.dirname(os.path.abspath(config_file))
    return os.path.join(config_dir, app_file)


class CommandInterface:
    @classmethod
    def name(cls):
        raise NotImplementedError("Subclasses should implement this method.")

    @classmethod
    def register(cls, subparsers):
        pass

    @classmethod
    def execute(cls, args):
        raise NotImplementedError("Subclasses should implement this method.")
