"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import logging
import os
import socket
import sys
import time

from ..i18n import _
from .base import (
    CommandInterface,
    ConfigError,
    infer_backend_type_from_db_url,
    locate_config_file,
    read_config_file,
)

# 命令行参数模式下的默认值。参数本身默认为 None，才能判断是否显式给出（见 flag_mode_requested）
DEFAULT_APP_FILE = "/app/app.py"
DEFAULT_DB = "redis://127.0.0.1:6379/0"

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


def resolve_worker_num(worker_num: int) -> int:
    """把配置的 WORKER_NUM 解析成真实的工作进程数，负数表示按CPU核心数自动。

    自己算才能让显示的进程数、是否走多进程、DEBUG 自动重载等判断都拿到真实值。
    process_cpu_count() 会考虑CPU亲和性(cgroup/taskset)，容器里更准。
    """
    if worker_num < 0:
        return os.process_cpu_count() or 1
    return worker_num


def log_server_info(config, name: str, workers: int) -> None:
    """开服前打一遍服务器信息：LOGO、模式、进程数、监听地址、消息流水线、auth_key 指纹"""
    from ..server.pipeline import CryptoLayer

    host, port = config.LISTEN.rsplit(":", 1)
    logger.info(FULL_COLOR_LOGO)
    logger.info(
        _("ℹ️ {name}, {mode}, {workers} 个工作进程, 管理进程 pid: {pid}").format(
            name=name,
            mode="Debug" if config.DEBUG else "Production",
            workers=workers,
            pid=os.getpid(),
        )
    )
    logger.info(
        _("ℹ️ Python {version} 平台 {platform}").format(
            version=sys.version, platform=sys.platform
        )
    )
    # TLS 由前面的反向代理（推荐 Caddy）终止，HeTu 自己只监听明文
    logger.info(_("📡 监听地址： http://{host}:{port}").format(host=host, port=port))
    layer_types = [layer.get("type") for layer in config.get("PACKET_LAYERS", [])]
    logger.info(
        _("ℹ️ 消息流协议：json -> {layers} -> Network").format(
            layers=" -> ".join(layer_types)
        )
    )
    # 打印 crypto 层 auth_key 的脱敏指纹，便于多服开服时核对各服/客户端密钥是否一致
    crypto_layer = next(
        (
            layer
            for layer in config.get("PACKET_LAYERS", [])
            if layer.get("type") == "crypto"
        ),
        None,
    )
    if crypto_layer is not None:
        masked = CryptoLayer.mask_auth_key(crypto_layer.get("auth_key"))
        if masked:
            logger.info(_("🔑 auth_key: {key}").format(key=masked))
        else:
            logger.warning(_("🔑 auth_key 未设置 ⚠️ 任意 key 都能连接"))

    if int(config.DEBUG) > 1:
        logger.warning("⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️")
        logger.warning(_("⚠️⚠️⚠️ 深度Debug模式开启  ⚠️⚠️⚠️   此模式下Python协程慢90%"))
        logger.warning("⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️⚠️")


def print_port_hint(err: OSError) -> None:
    """Windows 上端口落在系统保留段（excludedportrange）时 bind 报 10013，提示怎么解开"""
    if "10013" in str(err):
        print(
            _(
                "端口可能被系统 excludedportrange 禁用了，请用管理员执行："
                "net stop winnat"
                "net start winnat"
            )
        )


def wait_for_port(host, port, timeout=30):
    """轮询检测端口是否可用"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except TimeoutError, ConnectionRefusedError:
            time.sleep(0.5)
    return False


class StartCommand(CommandInterface):
    @classmethod
    def name(cls):
        return "start"

    @classmethod
    def register(cls, subparsers):
        parser_start = subparsers.add_parser("start", help=_("启动河图服务"))

        cli_group = parser_start.add_argument_group(_("通过命令行启动参数"))
        cli_group.add_argument(
            "--app-file",
            help=_("河图app的py文件，默认 /app/app.py"),
            metavar=".app.py",
        )
        cli_group.add_argument(
            "--namespace",
            metavar="game1",
            help=_("启动app.py中哪个namespace下的System"),
        )
        cli_group.add_argument(  # 不能require=True，因为有config参数
            "--instance", help=_("实例名称，每个实例是一个副本"), metavar="server1"
        )
        cli_group.add_argument(
            "--port", metavar="2446", help=_("监听的Websocket端口"), default="2466"
        )
        cli_group.add_argument(
            "--db",
            metavar="redis://127.0.0.1:6379/0",
            help=_("后端数据库地址，默认 redis://127.0.0.1:6379/0"),
        )
        cli_group.add_argument(
            "--workers",
            type=int,
            help=_("工作进程数，可设为 CPU * 1.2，-1为按CPU核心数自动"),
            default=4,
        )
        cli_group.add_argument(
            "--debug",
            type=int,
            nargs="?",
            const=1,
            help=_(
                "开启debug模式，这会启动代码热更新，并显示更多的log信息。"
                "如果设置为2，则对框架内核也启用debug模式，会开启Python协程的Debug模式，速度慢90%%。"
            ),
            default=0,
            metavar="0/1/2",
        )
        cli_group.add_argument(
            "--authkey",
            metavar="your-auth-key",
            help=_("默认crypto层的auth_key，用于握手签名校验。留空则不启用。"),
            default="",
        )

        cfg_group = parser_start.add_argument_group("或 通过配置文件启动参数")
        cfg_group.add_argument(
            "--config",
            help=_(
                "配置文件模板见CONFIG_TEMPLATE.yml。不给且没用命令行参数时，依次找环境变量"
                " HETU_CONFIG、当前目录的 config.yml"
            ),
            metavar="config.yml",
        )

    @classmethod
    def execute(cls, args):
        from ..common.helper import WORKER_INDEX_ENV
        from ..server.config import HeTuConfig
        from ..system.startup import ON_START_UUID_CONFIG_KEY, make_boot_uuid

        # worker 序号由多进程管理进程按 worker 设置。外面环境里残留的不能留着：单进程时会被当成本进程
        # 的序号，SQLite 后端上就是 Worker ID
        os.environ.pop(WORKER_INDEX_ENV, None)

        # 命令行转配置文件
        config_file = locate_config_file(args)
        if config_file:
            config = HeTuConfig()
            try:
                config_dict = read_config_file(config_file)
            except ConfigError as e:
                print(e)
                sys.exit(2)
            # update_config只会读取大写的值到config变量；APP_FILE、SQLite 库文件的相对路径
            # 已按配置文件所在目录解析，而非进程 CWD
            config.update_config(config_dict)
            config_for_factory = config
        else:
            args.app_file = args.app_file or DEFAULT_APP_FILE
            args.db = args.db or DEFAULT_DB
            if not args.app_file or not args.namespace or not args.instance:
                print(
                    _("--app_file是必须参数，或者用--config")
                ) if not args.app_file else None
                print(
                    _("--namespace是必须参数，或者用--config")
                ) if not args.namespace else None
                print(
                    _("--instance是必须参数，或者用--config")
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
                "DEBUG": args.debug,
            }
            config = HeTuConfig(config_for_factory)

        # 生成log目录
        os.mkdir("logs") if not os.path.exists("logs") else None
        # prepare用的配置
        workers = resolve_worker_num(config.WORKER_NUM)
        # per-boot uuid：main进程每次开服生成一次，经config_for_factory透传给每个worker，
        # 使 on_start System 在本次开服只跑一次（去重见 hetu.system.startup）
        config_for_factory[ON_START_UUID_CONFIG_KEY] = make_boot_uuid()
        cls.serve(config_for_factory, workers)

    @classmethod
    def serve(cls, config_for_factory, workers: int) -> None:
        """
        开服（aiohttp，设计稿 docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §5.4.3、
        §5.7）。单进程时在本进程里开服；多进程时本进程是管理进程，不加载 app 文件，交给 supervisor 起
        worker。两种都先配日志、启动日志 Listener，再 bind 监听 socket（端口冲突在这里就失败，不会先跑
        一遍开服）
        """
        from ..common.helper import enable_windows_console_ansi
        from ..safelogging import handlers as log_handlers
        from ..server import StartupAborted
        from ..server.adapters import aiohttp_app

        # Windows 控制台打开 ANSI 转义支持，彩色日志和启动 LOGO 才不会显示成裸转义码
        enable_windows_console_ansi()
        if workers == 1:
            config = aiohttp_app.prepare(config_for_factory)
        else:
            from ..server.config import HeTuConfig
            from ..server.runtime import setup_logging

            config = HeTuConfig(config_for_factory)
            # 管理进程只配日志（设计稿 §14 第 3 条）；建好的进程安全队列放回配置，传给 worker
            setup_logging(config)
            if "LOGGING" in config:
                log_handlers.share_queues(config.LOGGING)
        log_handlers.AutoListener.start_all()
        log_server_info(config, f"Hetu-{config.NAMESPACE}", workers)
        try:
            sock = aiohttp_app.bind_socket(config.LISTEN)
        except OSError as e:
            logger.error(
                _("❌ 监听 {listen} 失败：{err}").format(listen=config.LISTEN, err=e)
            )
            print_port_hint(e)
            log_handlers.stop_all_logging_handlers()
            sys.exit(1)
        try:
            if workers == 1:
                exit_code = aiohttp_app.run(config, sock)
            else:
                from ..server import supervisor

                exit_code = supervisor.run(config, sock, workers)
        except StartupAborted as e:
            logger.error(
                _("❌ 服务器启动失败，已中止，未对外提供服务：{err}").format(err=str(e))
            )
            exit_code = 1
        finally:
            sock.close()
            # 退出log listener，run() 抛别的异常时也要
            log_handlers.stop_all_logging_handlers()
        if exit_code:
            sys.exit(exit_code)
