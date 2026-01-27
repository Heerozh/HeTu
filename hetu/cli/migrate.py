"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import yaml
import importlib.util
import sys
import logging

from hetu.cli.base import CommandInterface
from hetu.common import yamlloader

logger = logging.getLogger("HeTu.root")
logger.setLevel(logging.DEBUG)
assert logging.lastResort
logging.lastResort.setLevel(logging.DEBUG)


class MigrateCommand(CommandInterface):
    @classmethod
    def name(cls):
        return "upgrade"

    @classmethod
    def register(cls, subparsers):
        parser_migrate = subparsers.add_parser(
            "upgrade",
            help="在数据库执行Schema升级脚本,如果没有则创建，如果无变更则跳过。同时还会进行临时表的清空。"
            "应该在CI/CD流程中，每次启动服务器前执行一次。",
        )
        parser_migrate.add_argument(
            "--db",
            metavar="redis://127.0.0.1:6379/0",
            help="后端数据库地址",
            default="redis://127.0.0.1:6379/0",
        )
        parser_migrate.add_argument(
            "--app-file",
            help="河图app的py文件",
            metavar=".app.py",
            default="app.py",
        )
        parser_migrate.add_argument(
            "--namespace", metavar="game1", help="启动app.py中哪个namespace下的System"
        )
        parser_migrate.add_argument(  # 不能require=True，因为有config参数
            "--instance", help="实例名称，每个实例是一个副本", metavar="server1"
        )

        parser_migrate.add_argument(
            "--config", help="通过yml配置文件读取后端数据库地址", metavar="config.yml"
        )
        parser_migrate.add_argument(
            "-y",
            action="store_true",
            default=False,
            help="自动确认数据备份提示",
        )
        parser_migrate.add_argument(
            "--drop-data",
            action="store_true",
            default=False,
            help="强制执行升级迁移，丢弃无法迁移的数据。请勿在生产环境使用此选项！",
        )

        pass

    @classmethod
    def run(cls, config: dict, yes, drop_data):
        # 创建后端连接池
        from ..data.backend import Backend
        from ..manager import ComponentTableManager

        backends: dict[str, Backend] = {}
        for name, db_cfg in config["BACKENDS"].items():
            backend = Backend(db_cfg)
            backends[name] = backend

            # 把config第一个设置为default后端
            if "default" not in backends:
                backends["default"] = backends[name]

        # 加载玩家的app文件
        spec = importlib.util.spec_from_file_location("HeTuApp", config["APP_FILE"])
        assert spec and spec.loader, f"无法加载app文件 {config['APP_FILE']}"
        module = importlib.util.module_from_spec(spec)
        sys.modules["HeTuApp"] = module
        spec.loader.exec_module(module)
        from hetu.system import SystemClusters

        SystemClusters().build_clusters(config["NAMESPACE"])

        if not yes:
            # cli提示用户先备份数据，按y继续
            user_input = input(
                "⚠️  升级数据库表结构可能会导致数据丢失，请确保已备份数据。"
                "确认继续请输 y ，取消请输其他键然后回车："
            )
            if user_input.lower() != "y":
                print("❌  升级迁移已取消。")
                return

        silence = False

        for instance_name in config["INSTANCES"]:
            comp_mgr = ComponentTableManager(
                config["NAMESPACE"],
                instance_name,
                backends,
            )

            # 先尝试普通迁移
            if not comp_mgr.create_or_migrate_all(config["APP_FILE"]):
                if not silence:
                    print(
                        "❗ Component有数据删除或类型变更，需要迁移脚本但未找到。"
                        "请添加对应的迁移脚本后重试。"
                    )
                    if not drop_data:
                        return
                    user_input = input(
                        "⚠️  确认强制迁移请输 y ，取消请输其他键然后回车："
                    )
                    if user_input.lower() != "y":
                        print("❌  升级迁移已取消。")
                        return
                    print(
                        f"⚠️  正在强制迁移 {instance_name} 服所有表结构，可能会丢失数据..."
                    )
                    silence = True
                comp_mgr.create_or_migrate_all(config["APP_FILE"], force=True)

            # 清除易失数据
            print(f"🧹 正在清除 {instance_name} 服易失数据...")
            comp_mgr.flush_volatile()

            print(f"✅  {instance_name} 服升级迁移完成！")
        print("🎉  恭喜！所有数据库表结构均已升级完成！")
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
                "APP_FILE": args.app_file,
                "NAMESPACE": args.namespace,
                "INSTANCES": [args.instance],
                "BACKENDS": {
                    "Redis": {
                        "type": "Redis",
                        "master": args.db,
                    }
                },
            }
            assert args.namespace, "namespace参数不能为空，建议用--config参数"
            assert args.instance, "instance参数不能为空，建议用--config参数"
            assert args.app_file, "app_file参数不能为空，建议用--config参数"
        return cls.run(config, args.y, args.drop_data)
