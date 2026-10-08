"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
import sys

from hetu.cli.base import (
    CommandInterface,
    ConfigError,
    config_from_flags,
    locate_config_file,
    read_config_file,
)
from hetu.i18n import _

logger = logging.getLogger("HeTu.root")

# 命令行参数模式下的默认值。参数本身默认为 None，才能判断是否显式给出（见 flag_mode_requested）
DEFAULT_APP_FILE = "app.py"
DEFAULT_DB = "redis://127.0.0.1:6379/0"


class MigrateCommand(CommandInterface):
    @classmethod
    def name(cls):
        return "upgrade"

    @classmethod
    def register(cls, subparsers):
        parser_migrate = subparsers.add_parser(
            "upgrade",
            help=_(
                "在数据库执行Schema升级脚本,如果没有则创建，如果无变更则跳过。同时还会进行临时表的清空。"
                "应该在CI/CD流程中，每次启动服务器前执行一次。"
            ),
        )
        parser_migrate.add_argument(
            "--db",
            metavar="redis://127.0.0.1:6379/0",
            help=_("后端数据库地址，默认 redis://127.0.0.1:6379/0"),
        )
        parser_migrate.add_argument(
            "--app-file",
            help=_("河图app的py文件，默认 app.py"),
            metavar=".app.py",
        )
        parser_migrate.add_argument(
            "--namespace",
            metavar="game1",
            help=_("启动app.py中哪个namespace下的System"),
        )
        parser_migrate.add_argument(  # 不能require=True，因为有config参数
            "--instance", help=_("实例名称，每个实例是一个副本"), metavar="server1"
        )

        parser_migrate.add_argument(
            "--config",
            help=_(
                "通过yml配置文件读取后端数据库地址。不给且没用命令行参数时，依次找环境变量"
                " HETU_CONFIG、当前目录的 config.yml"
            ),
            metavar="config.yml",
        )
        parser_migrate.add_argument(
            "-y",
            action="store_true",
            default=False,
            help=_("自动确认数据备份提示"),
        )
        parser_migrate.add_argument(
            "--drop-data",
            action="store_true",
            default=False,
            help=_("强制执行升级迁移，丢弃无法迁移的数据。请勿在生产环境使用此选项！"),
        )
        parser_migrate.add_argument(
            "--no-rebuild-index",
            action="store_true",
            default=False,
            help=_(
                "跳过重建索引。默认每次升级都按行数据重建持久组件的索引、修掉索引残留，"
                "数据量大时较慢"
            ),
        )

    @classmethod
    def run(cls, config: dict, yes, drop_data, rebuild_index=True):
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

        # 有服务器在跑时不能升级：迁移、清空易失表、重建索引在线执行都会写坏数据。
        # 靠 worker 租约判断，SQLite 后端没有租约，看不出来
        from ..data.backend import worker_keeper

        live: set[int] = set()
        for backend in set(backends.values()):
            live.update(worker_keeper.live_worker_ids(backend))
        if live:
            from ..common.snowflake_id import TOOL_NODE_PREFIX, WORKER_ID_EXPIRE_SEC

            print(
                _(
                    "❌ 检测到还有服务器在运行（持有 Worker ID 租约：{ids}），请先停服再升级："
                    "迁移、清空易失表、重建索引在服务器运行时执行都会写坏数据。服务器是异常"
                    "退出的，等租约过期（最多 {ttl} 秒）后再试。"
                ).format(ids=sorted(live), ttl=WORKER_ID_EXPIRE_SEC)
            )
            tools: list[int] = []
            for backend in set(backends.values()):
                try:
                    leases = worker_keeper.live_worker_leases(backend)
                except Exception:  # noqa: BLE001, S112 只是为了把提示说细，读不到就算了
                    continue
                tools += [
                    i
                    for i, owner in leases.items()
                    if owner.startswith(TOOL_NODE_PREFIX)
                ]
            if tools:
                print(
                    _(
                        "   其中 {ids} 是 hetu call / shell 进程，等它们结束即可"
                        "（异常退出的最多 {ttl} 秒过期）。"
                    ).format(ids=sorted(tools), ttl=WORKER_ID_EXPIRE_SEC)
                )
            sys.exit(1)

        # 加载玩家的app文件
        from hetu.local import load_app_module
        from hetu.system import SystemClusters

        load_app_module(config["APP_FILE"])

        SystemClusters().build_clusters(config["NAMESPACE"])

        if not yes:
            # cli提示用户先备份数据，按y继续
            user_input = input(
                _(
                    "⚠️  升级数据库表结构可能会导致数据丢失，请确保已备份数据。"
                    "确认继续请输 y ，取消请输其他键然后回车："
                )
            )
            if user_input.lower() != "y":
                print(_("❌  升级迁移已取消。"))
                return

        silence = False

        for instance_name in config["INSTANCES"]:
            tbl_mgr = ComponentTableManager(
                config["NAMESPACE"],
                instance_name,
                backends,
            )

            # 先尝试普通迁移
            if not tbl_mgr.create_or_migrate_all(config["APP_FILE"]):
                if not silence:
                    print(
                        _(
                            "❗ Component有数据删除或类型变更，请修改自动生成的迁移脚本，手动处理这些属性。"
                            "或使用--drop-data参数直接丢弃这些属性。"
                        )
                    )
                    if not drop_data:
                        return
                    user_input = input(
                        _("⚠️  确认强制迁移请输 y ，取消请输其他键然后回车：")
                    )
                    if user_input.lower() != "y":
                        print(_("❌  升级迁移已取消。"))
                        return
                    print(
                        _(
                            "⚠️  正在强制迁移 {instance_name} 服所有表结构，可能会丢失数据..."
                        ).format(instance_name=instance_name)
                    )
                    silence = True
                tbl_mgr.create_or_migrate_all(config["APP_FILE"], force=True)

            # 清除易失数据
            print(
                _("🧹 正在清除 {instance_name} 服易失数据...").format(
                    instance_name=instance_name
                )
            )
            tbl_mgr.flush_volatile()

            if rebuild_index:
                print(
                    _("🔧 正在重建 {instance_name} 服的索引...").format(
                        instance_name=instance_name
                    )
                )
                tbl_mgr.rebuild_index_all()

            print(
                _("✅  {instance_name} 服升级迁移完成！").format(
                    instance_name=instance_name
                )
            )
        print(_("🎉  恭喜！所有数据库表结构均已升级完成！"))

    @classmethod
    def execute(cls, args):
        # 迁移的每一步都要打出来：只在执行 upgrade 时打开 DEBUG（以前写在模块顶层，
        # import hetu.cli 就会改掉所有 hetu 命令的全局日志设置）
        logger.setLevel(logging.DEBUG)
        assert logging.lastResort
        logging.lastResort.setLevel(logging.DEBUG)

        try:
            if config_file := locate_config_file(args):
                config = read_config_file(config_file)
            else:
                config = config_from_flags(
                    args,
                    need_app=True,
                    need_db=True,
                    default_app_file=DEFAULT_APP_FILE,
                    default_db=DEFAULT_DB,
                )
        except ConfigError as e:
            print(e)
            sys.exit(2)
        return cls.run(config, args.y, args.drop_data, not args.no_rebuild_index)
