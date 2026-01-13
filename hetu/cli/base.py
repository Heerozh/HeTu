"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import argparse


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0", "None"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


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
