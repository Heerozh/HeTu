"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import argparse
import re
import sys
from typing import NoReturn

from ..i18n import _
from .build import BuildCommand
from .call import CallCommand
from .data import GetCommand, RangeCommand
from .init import InitCommand
from .migrate import MigrateCommand
from .shell import ShellCommand
from .start import StartCommand

# 把所有命令加入list
COMMANDS = [
    StartCommand,
    MigrateCommand,
    BuildCommand,
    InitCommand,
    CallCommand,
    GetCommand,
    RangeCommand,
    ShellCommand,
]


# argparse 当作位置参数（而不是选项）的负数
_NEGATIVE_NUMBER = re.compile(r"^-\d+$|^-\d*\.\d+$")


class _ArgumentError(Exception):
    def __init__(self, parser: argparse.ArgumentParser, message: str):
        super().__init__(message)
        self.parser = parser
        self.message = message


class _Parser(argparse.ArgumentParser):
    """用法错误时抛 `_ArgumentError` 而不是直接退出，由 `CommandIndex` 决定怎么报
    （add_subparsers 默认沿用父 parser 的类，子命令的 parser 也是它）"""

    def error(self, message: str) -> NoReturn:
        raise _ArgumentError(self, message)


class CommandIndex:
    def __init__(self):
        self.parser = _Parser(prog="hetu", description=_("河图数据库"))
        self.command_parsers: dict[str, argparse.ArgumentParser] = {}

    def register(self):
        command_parsers = self.parser.add_subparsers(
            dest="command", help=_("执行操作"), required=True
        )

        for cmd in COMMANDS:
            cmd.register(command_parsers)
        self.command_parsers = dict(command_parsers.choices)

    def parse(self, argv: list[str] | None = None) -> argparse.Namespace:
        """
        解析命令行。`hetu call` 允许选项夹在参数中间（``call add_gold 1001 --dry-run 500``）：
        argparse 把选项之后的参数当成多余的，这里接回 ARG 末尾。输出 JSON 的命令
        （call / get / range）的用法错误也输出一行 JSON、退出码 2。
        """
        args = None
        try:
            args, extras = self.parser.parse_known_args(argv)
            if extras:
                if (
                    args.command == "call"
                    and args.system is not None
                    and all(
                        not x.startswith("-") or _NEGATIVE_NUMBER.match(x)
                        for x in extras
                    )
                ):
                    args.args.extend(extras)
                else:
                    self.parser.error(
                        _("无法识别的参数：{extras}").format(extras=" ".join(extras))
                    )
            return args
        except _ArgumentError as e:
            name = getattr(args, "command", None) or next(
                (n for n, p in self.command_parsers.items() if p is e.parser), None
            )
            parser = self.command_parsers.get(name, e.parser) if name else e.parser
            self._usage_error(name, parser, e.message)

    @staticmethod
    def _usage_error(
        name: str | None, parser: argparse.ArgumentParser, message: str
    ) -> NoReturn:
        cmd = next((c for c in COMMANDS if c.name() == name), None)
        if cmd is None or not cmd.json_output:
            parser.print_usage(sys.stderr)
            parser.exit(2, f"{parser.prog}: error: {message}\n")
        from .base import UsageError
        from .console import run_json_command

        async def fail(_report: dict) -> dict:
            raise UsageError(f"{message}\n{parser.format_usage().strip()}")

        run_json_command(0, fail)

    def execute(self):
        args = self.parse()

        rtn = None
        for cmd in COMMANDS:
            if cmd.name() == args.command:
                rtn = cmd.execute(args)
        return rtn
