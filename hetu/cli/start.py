"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
import logging.handlers
import os
import socket
import sys
import time
from functools import partial
from urllib.parse import urlparse

import yaml
from sanic import Sanic
from sanic.config import Config
from sanic.worker.loader import AppLoader

from ..common import yamlloader
from ..safelogging import handlers as log_handlers
from ..server import worker_main
from .base import CommandInterface, str2bool

logger = logging.getLogger("HeTu.root")

FULL_COLOR_LOGO = """
\033[38;2;25;170;255m  ▀▄ ▄▄▄▄▄▄▄▄  \033[0m ▄▄▄▄▄▄▄▄▄▄▄  
\033[38;2;25;170;255m   ▀       █   \033[0m █  █▀▀▀█  █
\033[38;2;25;170;255m  █   ▄▄▄▄ █   \033[0m █  ▀▀█▀▀  █
\033[38;2;25;170;255m   ▀  █  █ █   \033[0m █ ▄▀▀ ▀▀▄ █
\033[38;2;25;170;255m   █  ▀▀▀▀ █   \033[0m █ █▀▀▀▀▀█ █
\033[38;2;25;170;255m  █        █   \033[0m █ █▄▄▄▄▄█ █
\033[38;2;25;170;255m  █     ▀▀▄█   \033[0m █▀▀▀▀▀▀▀▀▀█
"""


def infer_backend_type_from_db_url(db_url: str) -> str:
    """根据db url推断后端类型。"""
    scheme = urlparse(db_url).scheme.lower()
    if scheme in {"redis", "rediss", "valkey", "valkeys"}:
        return "redis"
    if scheme in {"postgres", "postgresql", "sqlite", "mariadb", "mysql"}:
        return "sql"
    if scheme in {"file"}:
        return "sharedmemory"
    raise ValueError(
        f"不支持的数据库URL scheme: '{scheme}'。"
        "目前支持 redis/rediss/valkey/valkeys/postgres/postgresql/sqlite/mysql/mariadb"
    )


def wait_for_port(host, port, timeout=30):
    """轮询检测端口是否可用"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except socket.timeout, ConnectionRefusedError:
            time.sleep(0.5)
    return False


class StartCommand(CommandInterface):
    @classmethod
    def name(cls):
        return "start"

    @classmethod
    def register(cls, subparsers):
        parser_start = subparsers.add_parser("start", help="启动河图服务")

        cli_group = parser_start.add_argument_group("通过命令行启动参数")
        cli_group.add_argument(
            "--app-file",
            help="河图app的py文件",
            metavar=".app.py",
            default="/app/app.py",
        )
        cli_group.add_argument(
            "--namespace", metavar="game1", help="启动app.py中哪个namespace下的System"
        )
        cli_group.add_argument(  # 不能require=True，因为有config参数
            "--instance", help="实例名称，每个实例是一个副本", metavar="server1"
        )
        cli_group.add_argument(
            "--port", metavar="2446", help="监听的Websocket端口", default="2466"
        )
        cli_group.add_argument(
            "--db",
            metavar="redis://127.0.0.1:6379/0",
            help="后端数据库地址",
            default="redis://127.0.0.1:6379/0",
        )
        cli_group.add_argument(
            "--workers", type=int, help="工作进程数，可设为 CPU * 1.2", default=4
        )
        cli_group.add_argument(
            "--debug",
            type=int,
            nargs="?",
            const=1,
            help="开启debug模式，显示更多的log信息。如果设置为2，则对框架内核也启用debug模式，也会开启Python协程的Debug模式，速度慢90％。",
            default=0,
            metavar="0/1/2",
        )
        cli_group.add_argument(
            "--cert",
            metavar="/etc/letsencrypt/live/example.com/",
            help="证书目录，如果不设置则使用不安全的连接。不建议这里设置，请外加一层反向https代理。"
            "填入auto会生成自签https证书。",
            default="",
        )
        cli_group.add_argument(
            "--authkey",
            metavar="your-auth-key",
            help="默认crypto层的auth_key，用于握手签名校验。留空则不启用。",
            default="",
        )

        cfg_group = parser_start.add_argument_group("或 通过配置文件启动参数")
        cfg_group.add_argument(
            "--config", help="配置文件模板见CONFIG_TEMPLATE.yml", metavar="config.yml"
        )

    @classmethod
    def execute(cls, args):
        # 命令行转配置文件
        if args.config:
            config = Config()
            config_file = args.config
            with open(config_file, "r", encoding="utf-8") as f:
                config_dict = yaml.load(f, yamlloader.Loader)
            # update_config只会读取大写的值到config变量
            config.update_config(config_dict)
            config_for_factory = config
        else:
            if not args.app_file or not args.namespace or not args.instance:
                print(
                    "--app_file是必须参数，或者用--config"
                ) if not args.app_file else None
                print(
                    "--namespace是必须参数，或者用--config"
                ) if not args.namespace else None
                print(
                    "--instance是必须参数，或者用--config"
                ) if not args.instance else None
                sys.exit(2)
            backend_type = infer_backend_type_from_db_url(args.db)
            backend_name = backend_type.capitalize()

            config_for_factory = {
                "APP_FILE": args.app_file,
                "NAMESPACE": args.namespace,
                "INSTANCES": [args.instance],
                "LISTEN": f"0.0.0.0:{args.port}",
                "PACKET_LAYERS": [
                    {"type": "jsonb"},
                    {"type": "zlib"},
                    {"type": "crypto", "auth_key": args.authkey},
                ],
                "BACKENDS": {
                    backend_name: {
                        "type": backend_type,
                        "master": args.db,
                    },
                },
                "WORKER_NUM": args.workers,
                "CERT_CHAIN": args.cert,
                "DEBUG": args.debug,
                "ACCESS_LOG": False,
            }
            config = Config(config_for_factory)

        # 生成log目录
        os.mkdir("logs") if not os.path.exists("logs") else None
        # prepare用的配置
        fast = config.WORKER_NUM < 0
        workers = fast and 1 or config.WORKER_NUM
        # 加载app
        loader = AppLoader(
            factory=partial(worker_main, f"Hetu-{config.NAMESPACE}", config_for_factory)
        )
        app = loader.load()
        # 配置log，上面app.load()会自动调用logging.config.dictConfig(config.LOGGING)
        # 把dictConfig生成的queue实例存到config里，好传递到子进程
        if "LOGGING" in config:
            for hdl_name, handler_dict in config.LOGGING["handlers"].items():
                if isinstance(handler_dict.get("queue"), (str, dict, list)):
                    handler = logging.getHandlerByName(hdl_name)
                    if isinstance(handler, logging.handlers.QueueHandler):
                        handler_dict["queue"] = handler.queue

        # 启动日志Listener
        log_handlers.AutoListener.start_all()

        # 显示服务器信息
        host, port = config.LISTEN.rsplit(":", 1)
        ssl = ("CERT_CHAIN" in config) and config.CERT_CHAIN or None

        logger.info(FULL_COLOR_LOGO)
        logger.info(
            f"ℹ️ {app.name}, {'Debug' if config.DEBUG else 'Production'}, {workers} workers, "
            f"manager pid: {os.getpid()}"
        )
        logger.info(f"ℹ️ Python {sys.version} on {sys.platform}")
        logger.info(f"📡 Listening on http{'s' if ssl else ''}://{host}:{port}")
        layer_types = [layer.get("type") for layer in config.get("PACKET_LAYERS", [])]
        logger.info(f"ℹ️ 消息流协议：json -> {' -> '.join(layer_types)} -> Network")

        if int(config.DEBUG) > 1:
            logger.warning("⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️")
            logger.warning("⚠️⚠️⚠️ 深度Debug模式开启  ⚠️⚠️⚠️   此模式下Python协程慢90%")
            logger.warning("⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️")

        # 准备启动服务器
        os.environ["SANIC_IGNORE_PRODUCTION_WARNING"] = "1"
        app.prepare(
            debug=int(config.DEBUG) > 1,
            access_log=config.ACCESS_LOG,
            motd=False,
            host=host,
            port=int(port),
            auto_tls=ssl == "auto",
            auto_reload=config.DEBUG,
            ssl=ssl if ssl != "auto" else None,
            fast=fast,
            workers=workers,
            single_process=workers == 1,
        )
        # 启动并堵塞
        if workers == 1:
            Sanic.serve_single(primary=app)
        else:
            Sanic.serve(primary=app, app_loader=loader)

        # 退出log listener
        log_handlers.stop_all_logging_handlers()
