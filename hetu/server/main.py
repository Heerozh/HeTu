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

from redis.exceptions import ConnectionError as RedisConnectionError
from sanic import Sanic

from ..common.snowflake_id import SnowflakeID
from ..data.backend import Backend
from ..data.backend.worker_keeper import GeneralWorkerKeeper, WorkerLease
from ..endpoint import connection
from ..manager import ComponentTableManager
from ..safelogging.default import DEFAULT_LOGGING_CONFIG
from ..system import SystemClusters
from ..system.future import future_call_task
from . import pipeline
from . import websocket as _  # noqa: F401 (防止未使用警告)
from .web import HETU_BLUEPRINT

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


async def start_backends(app: Sanic):
    # 创建后端连接池
    backends: dict[str, Backend] = {}
    for name, db_cfg in app.config.BACKENDS.items():
        backend = Backend(db_cfg)
        backends[name] = backend
        app.ctx.__setattr__(name, backend)

        # 把config第一个设置为default后端
        if "default" not in backends:
            backends["default"] = backends[name]
            app.ctx.__setattr__("default_backend", backends["default"])

    # 初始化所有ComponentTable
    table_managers: dict[str, ComponentTableManager] = {}
    app.ctx.__setattr__("table_managers", table_managers)

    for instance_name in app.config.INSTANCES:
        table_managers[instance_name] = ComponentTableManager(
            app.config["NAMESPACE"],
            instance_name,
            backends,
        )

        # 检测表状态，创建所有不存在的表
        all_table_ok = table_managers[instance_name].check_and_create_new_tables()

        # 如果有迁移需求，则报错退出，让用户用cli migrate命令来迁移
        if not all_table_ok:
            msg = (
                "❌ [📡Server] 数据库表结构需要迁移，请使用迁移命令："
                "hetu upgrade --config <your_config_file>.yml"
            )
            logger.error(msg)
            app.stop()
            raise RuntimeError(msg)

    # 最后调用 backend config, 以防configure中需要之前初始化的东西
    for backend in backends.values():
        backend.post_configure()

    # 在backend初始化完毕后，启动WorkerKeeper，分配Worker ID，并把Worker ID和上次时间戳传给雪花ID生成器
    lease_tbl = table_managers[app.config.INSTANCES[0]].get_table(WorkerLease)
    assert lease_tbl is not None
    worker_keeper = GeneralWorkerKeeper(os.getpid(), lease_tbl)

    # 获得分配的worker id，如果KeyError，说明反复宕机导致分配满了，要等60秒过期
    while True:
        try:
            worker_id = await worker_keeper.get_worker_id()
            break
        except KeyError as e:
            logger.exception(e)
            logger.info(
                "⌚ [📡Server] Worker ID分配失败，可能是反复宕机导致Worker ID分配满了，等待1秒后重试..."
            )
            await asyncio.sleep(1)
    last_timestamp = await worker_keeper.get_last_timestamp()

    # 初始化雪花id生成器
    SnowflakeID().init(worker_id, last_timestamp)
    app.ctx.__setattr__("worker_keeper", worker_keeper)


async def close_backends(app: Sanic):
    # 释放worker id
    await app.ctx.worker_keeper.release_worker_id()

    # 关闭后端连接池
    for attrib in dir(app.ctx):
        backend = app.ctx.__getattribute__(attrib)
        if isinstance(backend, Backend):
            logger.info(f"⌚ [📡Server] Closing backend {attrib}...")
            await backend.close()


async def worker_start(app: Sanic):
    try:
        await start_backends(app)
    except Exception as e:
        logger.exception(f"❌ 进程[{os.getpid()}] 启动失败: {type(e).__name__}:{e}")
        app.stop()
        return

    # 打印信息
    from pathlib import Path

    logger.info(
        f"ℹ️ 进程[{os.getpid()}] "
        f"加载 {Path(app.config.get('APP_FILE', None)).resolve(strict=False)} 完成"
    )
    logger.info(
        f"ℹ️ 进程[{os.getpid()}] "
        f"已启动 {app.config['NAMESPACE']} 应用的"
        f" {app.config['INSTANCES']} 服"
    )


async def worker_close(app):
    # ctrl+c并不会触发此函数，sanic会直接退出进程
    await close_backends(app)


async def worker_keeper_renewal(app: Sanic):
    # 循环每5秒续约一次worker id
    while True:
        await asyncio.sleep(5)
        logger.info("⌚ [📡WorkerKeeper] 续约中... ")
        # todo sanic bug: 来新连接时，其他worker的task会被暂停，导致续约失败
        try:
            await app.ctx.worker_keeper.keep_alive(SnowflakeID().last_timestamp)
        except RedisConnectionError as e:
            logger.error(
                f"❌ [📡WorkerKeeper] 续约失败，将重试: {type(e).__name__}:{e}"
            )
            continue
        except SystemExit:
            app.m.restart()


def worker_main(app_name, config) -> Sanic:
    """
    此函数会执行 workers+1 次。但如果是单worker，则只会执行1次。
    多worker时，第一次是Main函数的进程，负责管理workers，执行完不会启动任何app.add_task或者注册的listener。
    后续Workers进程才会执行app.add_task和注册的listener。
    """

    # 加载玩家的app文件
    if (app_file := config.get("APP_FILE", None)) is not None:
        try:
            spec = importlib.util.spec_from_file_location("HeTuApp", app_file)
            assert spec and spec.loader
            module = importlib.util.module_from_spec(spec)
            sys.modules["HeTuApp"] = module
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
    SystemClusters().build_endpoints()

    # 传递配置
    connection.MAX_ANONYMOUS_CONNECTION_BY_IP = config.get(
        "MAX_ANONYMOUS_CONNECTION_BY_IP", 0
    )
    connection.ENDPOINT_CALL_IDLE_TIMEOUT = config.get(
        "ENDPOINT_CALL_IDLE_TIMEOUT", 60 * 2
    )

    # 加载web服务器
    app = Sanic(app_name, log_config=config.get("LOGGING", DEFAULT_LOGGING_CONFIG))
    app.update_config(config)

    # 重定向logger，把sanic的重定向到hetu
    root_logger = logging.getLogger("sanic")
    root_logger.parent = logger
    if config["DEBUG"]:
        logger.setLevel(logging.DEBUG)
    if int(config["DEBUG"]) > 1:
        logging.getLogger().setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)

    # 加载协议, 初始化消息处理流水线
    msg_pipe = pipeline.ServerMessagePipeline()
    msg_pipe.clean()  # msg_pipe是单件，防止test用例中多次调用worker_main导致重复添加layer
    for layer_cfg in config.get("PACKET_LAYERS", []):
        layer = pipeline.MessageProcessLayerFactory.create(**layer_cfg)
        msg_pipe.add_layer(layer)

    # 服务器main进程setup/teardown回调
    # app.main_process_start()
    # app.main_process_stop()
    # 服务器work进程setup/teardown回调
    app.before_server_start(worker_start)
    app.after_server_stop(worker_close)

    # 启动未来调用worker
    app.add_task(future_call_task(app))
    # 启动WorkerKeeper续约任务，保证自己的Worker ID不被回收
    app.add_task(worker_keeper_renewal(app))

    # 启动服务器监听
    app.blueprint(HETU_BLUEPRINT)
    return app
