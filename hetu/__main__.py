"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import argparse
import sys
from hetu.server import start_webserver

FULL_COLOR_LOGO = """
\033[38;2;25;170;255m  ▀▄ ▄▄▄▄▄▄▄▄  \033[0m ▄▄▄▄▄▄▄▄▄▄▄  
\033[38;2;25;170;255m   ▀       █   \033[0m █  █▀▀▀█  █
\033[38;2;25;170;255m  █   ▄▄▄▄ █   \033[0m █  ▀▀█▀▀  █
\033[38;2;25;170;255m   ▀  █  █ █   \033[0m █ ▄▀▀ ▀▀▄ █
\033[38;2;25;170;255m   █  ▀▀▀▀ █   \033[0m █ █▀▀▀▀▀█ █
\033[38;2;25;170;255m  █        █   \033[0m █ █▄▄▄▄▄█ █
\033[38;2;25;170;255m  █     ▀▀▄█   \033[0m █▀▀▀▀▀▀▀▀▀█
"""


def start(start_args):
    from sanic.config import Config
    from sanic import Sanic
    from sanic.log import logger
    from sanic.worker.loader import AppLoader
    from functools import partial

    if start_args.config:
        config = Config()
        config.update_config(start_args.config)
        config_for_factory = start_args.config
    else:
        if not start_args.app_file or not start_args.namespace or not start_args.instance:
            print("使用--config指定配置文件启动，"
                  "或用--app-file, --namespace, --instance参数快捷启动，请参照--help")
            sys.exit(2)
        config_for_factory = {
            'APP_FILE': start_args.app_file,
            'NAMESPACE': start_args.namespace,
            'INSTANCE_NAME': start_args.instance,
            'HEAD_NODE': start_args.head,
            'LISTEN': f"0.0.0.0:{start_args.port}",
            'BACKENDS': {
                'Redis': {
                    "type": "Redis",
                    "addr": start_args.db,
                    "user": "root",
                    "pass": "",
                    "db": 0,
                }
            },
            'DEBUG': True,
            'WORKER_NUM': 4,
            'ACCESS_LOG': False,
        }
        config = Config(config_for_factory)
    # prepare用的配置
    fast = config.WORKER_NUM < 0
    workers = fast and 1 or config.WORKER_NUM
    # 加载app
    loader = AppLoader(factory=partial(start_webserver, f"Hetu-{config.NAMESPACE}",
                                       config_for_factory))
    app = loader.load()
    # 显示服务器信息
    logger.info(FULL_COLOR_LOGO)
    logger.info(f"ℹ️ {app.name}, {'Debug' if config.DEBUG else 'Production'}, {workers} workers")
    logger.info(f"ℹ️ Python {sys.version} on {sys.platform}")
    logger.info(f"📡 Listening on https://{config.LISTEN}")
    # 准备启动服务器

    app.prepare(debug=config.DEBUG,
                access_log=config.ACCESS_LOG,
                motd=False,
                host=config.LISTEN.split(':')[0],
                port=int(config.LISTEN.split(':')[1]),
                auto_tls=config.DEBUG,
                fast=fast,
                workers=workers)
    # 启动并堵塞
    Sanic.serve(primary=app, app_loader=loader)


def main():
    parser = argparse.ArgumentParser(prog='hetu', description='Hetu Data Server')
    command_parsers = parser.add_subparsers(dest='command', help='commands', required=True)

    # ============================================
    parser_start = command_parsers.add_parser(
        'start', help='启动河图服务')
    cli_group = parser_start.add_argument_group("通过命令行启动参数")
    cli_group.add_argument(
        "--app-file", help="河图app的py文件", metavar="app.py")
    cli_group.add_argument(
        "--namespace", metavar="game1", help="加载app中哪个命名空间")
    cli_group.add_argument(
        "--instance", help="河图实例名称，每个实例是一个副本",
        metavar="server1")
    cli_group.add_argument(
        "--port", metavar="2446", help="监听的Websocket端口", default='2466')
    cli_group.add_argument(
        "--db", metavar="127.0.0.1:6379", help="后端数据库地址", default='127.0.0.1:6379')
    cli_group.add_argument(
        "--head", help="是否为主节点，默认为True", default=True, metavar="True", type=bool)

    cfg_group = parser_start.add_argument_group("或 通过配置文件启动参数")
    cfg_group.add_argument(
        "--config", help="配置文件模板见CONFIG_TEMPLATE.py", metavar="config.py")

    # 开始执行
    args = parser.parse_args()
    command = globals().get(args.command)
    command(args)


if __name__ == "__main__":
    main()
