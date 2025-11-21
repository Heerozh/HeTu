"""
Worker进程入口文件
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import importlib.util
import logging
import os
import sys

from sanic import Sanic

import hetu.server.websocket  # noqa: F401 (防止未使用警告)
import hetu.system.connection as connection
from hetu.common.helper import resolve_import
from hetu.data.backend import Backend, HeadLockFailed
from hetu.manager import ComponentTableManager
from hetu.safelogging import handlers as log_handlers
from hetu.safelogging.default import DEFAULT_LOGGING_CONFIG
from hetu.system import SystemClusters
from hetu.system.future import future_call_task
from hetu.web import APP_BLUEPRINT

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


def start_backends(app: Sanic):
    # 创建后端连接池
    backends = {}
    table_constructors = {}
    for name, db_cfg in app.config.BACKENDS.items():
        if db_cfg["type"] == "Redis":
            from ..data.backend import RedisBackend, RedisComponentTable

            backend = RedisBackend(db_cfg)
            backend.configure()
            backends[name] = backend
            table_constructors["Redis"] = RedisComponentTable
            app.ctx.__setattr__(name, backend)
        elif db_cfg["type"] == "PostgreSQL":
            # import sqlalchemy
            # app.ctx.__setattr__(name, sqlalchemy.create_engine(db_cfg["addr"]))
            raise NotImplementedError("PostgreSQL后端未实现")
        # 把config第一个设置为default后端
        if "default" not in backends:
            backends["default"] = backends[name]
            table_constructors["default"] = table_constructors[db_cfg["type"]]
            app.ctx.__setattr__("default_backend", backends["default"])

    # 初始化所有ComponentTable
    comp_mgr = ComponentTableManager(
        app.config["NAMESPACE"],
        app.config["INSTANCE_NAME"],
        backends,
        table_constructors,
    )
    app.ctx.__setattr__("comp_mgr", comp_mgr)


async def close_backends(app: Sanic):
    for attrib in dir(app.ctx):
        backend = app.ctx.__getattribute__(attrib)
        if isinstance(backend, Backend):
            logger.info(f"⌚ [📡Server] Closing backend {attrib}...")
            await backend.close()


async def worker_start(app: Sanic):
    start_backends(app)

    # 打印信息
    from pathlib import Path

    logger.info(
        f"ℹ️ 进程[{os.getpid()}] "
        f"加载 {Path(app.config.get('APP_FILE', None)).resolve(strict=False)} 完成"
    )
    logger.info(
        f"ℹ️ 进程[{os.getpid()}] "
        f"已启动 {app.config['NAMESPACE']} 应用 "
        f"{app.config['INSTANCE_NAME']} 服"
    )


async def worker_close(app):
    await close_backends(app)


def start_webserver(app_name, config, main_pid, head) -> Sanic:
    """
    此函数会执行 workers+1 次。但如果是单worker，则只会执行1次。
    多worker时，第一次是Main函数的进程，负责管理workers，执行完不会启动任何app.add_task或者注册的listener。
    后续Workers进程才会执行app.add_task和注册的listener。
    """

    # 加载玩家的app文件
    if (app_file := config.get("APP_FILE", None)) is not None:
        spec = importlib.util.spec_from_file_location("HeTuApp", app_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules["HeTuApp"] = module
        try:
            spec.loader.exec_module(module)
        except Exception as e:
            print(
                f"无法加载主启动文件({type(e).__name__})：{app_file}，检查以下可能性：\n"
                f"* 如果是命令行启动，检查--app-file参数路径是否正确\n"
                f"* 如果是通过Config启动，此文件由APP_FILE参数设置\n"
                f"* 如果由Docker启动，还需检查是否正确映射了/app目录\n"
            )
            raise e

    # 初始化SystemCluster
    SystemClusters().build_clusters(config["NAMESPACE"])

    # 传递配置
    connection.MAX_ANONYMOUS_CONNECTION_BY_IP = config.get(
        "MAX_ANONYMOUS_CONNECTION_BY_IP", 0
    )
    connection.SYSTEM_CALL_IDLE_TIMEOUT = config.get("SYSTEM_CALL_IDLE_TIMEOUT", 60 * 2)

    # 加载web服务器
    app = Sanic(app_name, log_config=config.get("LOGGING", DEFAULT_LOGGING_CONFIG))
    app.update_config(config)

    # 重定向logger，把sanic的重定向到hetu
    root_logger = logging.getLogger("sanic")
    root_logger.parent = logger
    if config["DEBUG"]:
        logger.setLevel(logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)

    # 加载协议
    app.ctx.compress, app.ctx.crypto = None, None
    compress = config.get("PACKET_COMPRESSION_CLASS")
    crypto = config.get("PACKET_CRYPTOGRAPHY_CLASS")
    if compress is not None:
        try:
            compress_module = resolve_import(compress)
        except ValueError as e:
            raise ValueError(
                f"该压缩模块无法解析，请使用可被import的字符串：{compress}"
            ) from e
        required = ("compress", "decompress")
        missing = [attr for attr in required if not hasattr(compress_module, attr)]
        if missing:
            raise ValueError(f"该压缩模块没有实现 {missing} 方法：{compress}")
        app.ctx.compress = compress_module
    if crypto is not None:
        try:
            crypto_module = resolve_import(crypto)
        except ValueError as e:
            raise ValueError(
                f"该加密模块无法解析，请使用可被import的字符串：{crypto}"
            ) from e
        required = ("encrypt", "decrypt")
        missing = [attr for attr in required if not hasattr(crypto_module, attr)]
        if missing:
            raise ValueError(f"该加密模块没有实现 {missing} 方法：{crypto}")
        app.ctx.crypto = crypto_module

    # 如果本app是Head Node，且本进程为main进程（非worker)，则额外启动一次backend清空非持久化表
    # 注意如果是单worker模式，则main进程也是worker进程，因此worker_start里会再次执行start_backends
    if head and os.getpid() == main_pid:
        start_backends(app)
        # 主进程+Head启动时执行检查schema, 清空所有非持久化表
        try:
            # is_worker = os.environ.get('SANIC_WORKER_IDENTIFIER').startswith('Srv ')
            logger.warning(
                "⚠️ [📡Server] 启动为Head node，开始检查schema并清空非持久化表..."
            )
            app.ctx.comp_mgr.create_or_migrate_all()
            app.ctx.comp_mgr.flush_volatile()
        except HeadLockFailed as e:
            message = (
                f"检测有其他head=True的node正在运行，只能启动一台head node。"
                f"如果上次Head服务器宕机了，可运行 "
                f"hetu unlock --db=redis://host:6379/0 "
                f"来强制删除此标记。"
            )
            logger.exception("❌ [📡Server] " + message)
            # 退出logger进程(主要是logger的Queue)，不然直接调用此函数的地方会卡死
            log_handlers.stop_all_logging_handlers()
            raise HeadLockFailed(message)
        finally:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                asyncio.run(close_backends(app))
            else:
                loop.run_until_complete(close_backends(app))
                loop.close()

    # 服务器main进程setup/teardown回调
    # app.main_process_start()
    # app.main_process_stop()
    # 服务器work进程setup/teardown回调
    app.before_server_start(worker_start)
    app.after_server_stop(worker_close)

    # 启动未来调用worker
    app.add_task(future_call_task(app))

    # 启动服务器监听
    app.blueprint(APP_BLUEPRINT)
    return app
