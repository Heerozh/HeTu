import argparse
import gettext

from .base import CommandInterface
from .build import BuildCommand
from .unlock import UnlockCommand
from .migration import MigrationCommand
from .start import StartCommand

args_loc = {
    "usage: ": "用法：",
    "the following arguments are required: %s": "以下参数是必须的： %s",
}
gettext.gettext = lambda x: args_loc.get(x, x)

# 把所有命令加入list
COMMANDS = [
    StartCommand,
    MigrationCommand,
    BuildCommand,
    UnlockCommand,
]


class CommandIndex:
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='hetu', description='河图数据库')

    def register(self):
        command_parsers = self.parser.add_subparsers(dest='command', help='执行操作', required=True)

        for cmd in COMMANDS:
            cmd.register(command_parsers)

    def execute(self):
        args = self.parser.parse_args()

        for cmd in COMMANDS:
            if cmd.name() == args.command:
                cmd.execute(args)
