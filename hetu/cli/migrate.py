"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import yaml

from hetu.cli.base import CommandInterface
from typing import TYPE_CHECKING
from hetu.common import yamlloader


class MigrateCommand(CommandInterface):
    @classmethod
    def name(cls):
        return "migrate"

    @classmethod
    def register(cls, subparsers):
        parser_migrate = subparsers.add_parser(
            "migrate", help="在数据库执行Schema管理/迁移"
        )
        parser_migrate.add_argument(
            "--db",
            metavar="redis://127.0.0.1:6379/0",
            help="后端数据库地址",
            default="redis://127.0.0.1:6379/0",
        )
        parser_migrate.add_argument(
            "--config", help="通过yml配置文件读取后端数据库地址", metavar="config.yml"
        )

        pass

    @classmethod
    def run(cls, config: dict):
        # todo: 实现迁移逻辑
        # app.ctx.comp_mgr.create_or_migrate_all()
        # app.ctx.comp_mgr.flush_volatile()
        pass

    @classmethod
    def execute(cls, args):
        if args.config:
            config_file = args.config
            with open(config_file, "r", encoding="utf-8") as f:
                config_dict = yaml.load(f, yamlloader.Loader)
            config = config_dict
        else:
            config = {
                "BACKENDS": {
                    "Redis": {
                        "type": "Redis",
                        "master": args.db,
                    }
                },
            }
        return cls.run(config)
