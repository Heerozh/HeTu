"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import argparse
import gettext

from .build import BuildCommand
from .migrate import MigrateCommand
from .start import StartCommand
from .unlock import UnlockCommand

args_loc = {
    "usage: ": "用法：",
    "the following arguments are required: %s": "以下参数是必须的： %s",
}
gettext.gettext = lambda x: args_loc.get(x, x)

# 把所有命令加入list
COMMANDS = [
    StartCommand,
    MigrateCommand,
    BuildCommand,
    UnlockCommand,
]


class CommandIndex:
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog="hetu", description="河图数据库")

    def register(self):
        command_parsers = self.parser.add_subparsers(
            dest="command", help="执行操作", required=True
        )

        for cmd in COMMANDS:
            cmd.register(command_parsers)

    def execute(self):
        args = self.parser.parse_args()

        rtn = None
        for cmd in COMMANDS:
            if cmd.name() == args.command:
                rtn = cmd.execute(args)
        return rtn
