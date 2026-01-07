"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from hetu.cli.base import CommandInterface


class MigrateCommand(CommandInterface):
    @classmethod
    def name(cls):
        return "migrate"

    @classmethod
    def register(cls, subparsers):
        # parser_start = subparsers.add_parser(
        #     'schema_migration', help='如果Component定义发生改变，在数据库执行版本迁移(未完成）')
        pass

    @classmethod
    def execute(cls, args):
        raise NotImplementedError("还未实现")
