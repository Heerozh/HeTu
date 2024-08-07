"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import argparse
import os
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


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0', 'None'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


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
            'LISTEN': f"0.0.0.0:{start_args.port}",
            'CERT_CHAIN': start_args.cert,
            'PACKET_COMPRESSION_CLASS': 'zlib',
            'BACKENDS': {
                'Redis': {
                    "type": "Redis",
                    "master": start_args.db,
                }
            },
            'DEBUG': start_args.debug,
            'WORKER_NUM': start_args.workers,
            'ACCESS_LOG': False,
        }
        config = Config(config_for_factory)
    # 生成log目录
    os.mkdir('logs') if not os.path.exists('logs') else None
    # prepare用的配置
    fast = config.WORKER_NUM < 0
    workers = fast and 1 or config.WORKER_NUM
    # 加载app
    loader = AppLoader(factory=partial(start_webserver, f"Hetu-{config.NAMESPACE}",
                                       config_for_factory, os.getpid(), start_args.head))
    app = loader.load()
    # 显示服务器信息
    host, port = config.LISTEN.rsplit(':', 1)
    ssl = ('CERT_CHAIN' in config) and config.CERT_CHAIN or None

    logger.info(FULL_COLOR_LOGO)
    logger.info(f"ℹ️ {app.name}, {'Debug' if config.DEBUG else 'Production'}, {workers} workers")
    logger.info(f"ℹ️ Python {sys.version} on {sys.platform}")
    logger.info(f"📡 Listening on http{'s' if ssl else ''}://{host}:{port}")
    logger.info(f"ℹ️ 消息协议：压缩模块：{config.get('PACKET_COMPRESSION_CLASS')}, "
                f"加密模块：{config.get('PACKET_CRYPTOGRAPHY_CLASS')}")

    if config.DEBUG:
        logger.warning("⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️")
        logger.warning("⚠️⚠️⚠️ Debug模式开启  ⚠️⚠️⚠️   此模式下Python协程慢90%")
        logger.warning("⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️")

    # 准备启动服务器
    os.environ['SANIC_IGNORE_PRODUCTION_WARNING'] = '1'
    app.prepare(debug=config.DEBUG,
                access_log=config.ACCESS_LOG,
                motd=False,
                host=host,
                port=int(port),
                auto_tls= ssl == 'auto',
                auto_reload=config.DEBUG,
                ssl=ssl if ssl != 'auto' else None,
                fast=fast,
                workers=workers)
    # 启动并堵塞
    Sanic.serve(primary=app, app_loader=loader)


def main():
    parser = argparse.ArgumentParser(prog='hetu', description='Hetu Data Server')
    command_parsers = parser.add_subparsers(dest='command', help='commands', required=True)

    # ==================start==========================
    parser_start = command_parsers.add_parser(
        'start', help='启动河图服务')
    parser_start.add_argument(  # const意思如果--ind后不带参数，则默认打开
        "--head", type=str2bool, nargs='?', default=True, const=True,
        help="是否为Head Node，Head启动时会执行数据库初始化操作，比如清空临时数据，修改数据库表结构")

    cli_group = parser_start.add_argument_group("通过命令行启动参数")
    cli_group.add_argument(
        "--app-file", help="河图app的py文件", metavar="app.py")
    cli_group.add_argument(
        "--namespace", metavar="game1", help="启动app.py中哪个namespace下的System")
    cli_group.add_argument(
        "--instance", help="实例名称，每个实例是一个副本",
        metavar="server1")
    cli_group.add_argument(
        "--port", metavar="2446", help="监听的Websocket端口", default='2466')
    cli_group.add_argument(
        "--db", metavar="127.0.0.1:6379", help="后端数据库地址",
        default='redis://127.0.0.1:6379/0')
    cli_group.add_argument(
        "--workers", type=int, help="工作进程数，可设为 CPU * 1.2", default=4)
    cli_group.add_argument(
        "--debug", type=str2bool, nargs='?', const=True,
        help="启用debug模式，显示更多的log信息。因为也会开启Python协程的Debug模式，速度慢90%。",
        default=False)
    cli_group.add_argument(
        "--cert", metavar="/etc/letsencrypt/live/example.com/",
        help="证书目录，如果不设置则使用不安全的连接。也可以这里不设置，外加一层反向https代理。"
             "填入auto会生成自签https证书。",
        default='')

    cfg_group = parser_start.add_argument_group("或 通过配置文件启动参数")
    cfg_group.add_argument(
        "--config", help="配置文件模板见CONFIG_TEMPLATE.py", metavar="config.py")
    # ==================migration==========================
    # parser_start = command_parsers.add_parser(
    #     'schema_migration', help='如果Component定义发生改变，在数据库执行版本迁移(未完成）')
    # ==================build==========================
    # todo 增加个build c# class文件

    # 开始执行
    args = parser.parse_args()
    command = globals().get(args.command)
    command(args)


if __name__ == "__main__":
    main()
