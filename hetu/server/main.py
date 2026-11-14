"""
Worker进程入口文件
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
import os

from sanic import Sanic
from sanic.worker.manager import WorkerManager
from sanic.worker.process import WorkerProcess

from .. import webext
from ..data.backend import Backend
from ..data.backend.snowflake_lease import SnowflakeLease
from ..data.backend.worker_keeper import WorkerLease, create_worker_keeper
from ..endpoint import connection
from ..i18n import _
from ..local import load_app_module
from ..manager import ComponentTableManager
from ..safelogging.default import DEFAULT_LOGGING_CONFIG
from ..system import SystemClusters
from ..system import lock as system_lock
from ..system.future import future_call_task
from . import pipeline
from .threaded_accept import IS_WINDOWS, threaded_accept_listener
from .web import HETU_BLUEPRINT, web_root
from .websocket import wait_connections_closed  # 导入即注册 ws 路由

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")

# worker启动流程（连后端、建表、跑启动System）的总超时秒数配置键，0为不限制。
STARTUP_TIMEOUT_CONFIG_KEY = "STARTUP_TIMEOUT"
DEFAULT_STARTUP_TIMEOUT = 60.0
# 管理进程等worker ack的超时 = STARTUP_TIMEOUT + 这个余量。
# 两级超时不是冗余：我们自己的 asyncio 超时挡不住阻塞式代码（启动System里写了
# time.sleep/requests 之类把事件循环卡死），那时只有管理进程（另一个进程在数数）能兜底。
# 余量只需盖住我们量不到的部分：进程spawn、import app文件、bind端口。
ACK_TIMEOUT_MARGIN = 10.0
# "不限制"给sanic的阈值。sanic的单位是0.1秒，此值约3年，等同永不超时。
# 注意不能给它0，0是"第一次轮询(0.1秒)就判定worker启动失败"，意思正好相反
UNLIMITED_ACK_THRESHOLD = 10**9


def resolve_ack_threshold(config) -> int:
    """按 STARTUP_TIMEOUT 算出管理进程等worker ack的阈值（sanic单位为0.1秒）。

    ack超时必须晚于我们自己的启动超时，否则卡住时先被管理进程SIGKILL掉整个进程组，
    用户只能看到sanic那句没头没脑的"Not all workers acknowledged"，不知道卡在哪。
    所以它不单独配置，由 STARTUP_TIMEOUT 推算；STARTUP_TIMEOUT 为0(不限制)时它也不限制。

    Derive the manager's worker-ack threshold (in units of 0.1s) from STARTUP_TIMEOUT,
    so HeTu's own, more specific timeout always fires first.
    """
    startup_timeout = config.get(STARTUP_TIMEOUT_CONFIG_KEY, DEFAULT_STARTUP_TIMEOUT)
    if not startup_timeout:
        return UNLIMITED_ACK_THRESHOLD
    return max(1, int((float(startup_timeout) + ACK_TIMEOUT_MARGIN) * 10))


class StartupAborted(Exception):
    """worker启动流程失败，服务器必须中止，不能带病对外服务。

    必须以异常向上抛出（而不是app.stop()）才能真正中止：Sanic在
    ``worker/serve.py`` 里会捕获启动异常并向管理进程发 ``__TERMINATE_EARLY__``，
    多进程模式立刻中止（不用等30秒ack超时）；单进程模式则一路冒泡到CLI。
    两种模式的退出码都非0，docker/systemd/k8s的重启策略才能识别为失败。

    Raised when worker startup fails and the server must abort instead of serving in a
    broken state. It must propagate as an exception (not ``app.stop()``) so that Sanic
    aborts immediately and the process exits with a non-zero code in both single- and
    multi-process mode.
    """


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
            msg = _(
                "❌ [📡Server] 数据库表结构需要迁移，请使用迁移命令："
                "hetu upgrade --config <your_config_file>.yml"
            )
            logger.error(msg)
            app.stop()
            raise RuntimeError(msg)

    # 最后调用 backend config, 以防configure中需要之前初始化的东西
    for backend in backends.values():
        backend.post_configure()

    # 在backend初始化完毕后，租 Worker ID、接上时间戳高水位、初始化雪花ID生成器（见 SnowflakeLease）。
    # 分配器按后端类型自动选：Redis后端用真租约（多机安全），SQLite后端用本机进程序号
    # （开发模式，单机安全）。见 create_worker_keeper。
    # 水位表固定用 INSTANCES[0] 的：hetu call / shell 也认这一张
    lease_tbl = table_managers[app.config.INSTANCES[0]].get_table(WorkerLease)
    assert lease_tbl is not None
    lease = SnowflakeLease(
        create_worker_keeper(lease_tbl.backend, os.getpid()), lease_tbl
    )
    # 反复宕机导致 Worker ID 分配满了时每秒重试，等它们过期
    await lease.acquire(wait=True)
    app.ctx.__setattr__("snowflake_lease", lease)


async def close_backends(app: Sanic):
    # 关服写精确水位再释放 worker id。先停续约、水位预留两个循环：不然续约会对刚释放的 id 报
    # 租约丢失，水位预留会在精确水位之后再写一次。调用方保证连接已拆完、FutureCalls 已停（见
    # worker_close），不会再发号，最后用到的时间戳就是真实上界（见 SnowflakeLease.release）
    lease = app.ctx.snowflake_lease
    await lease.stop_loops()
    await lease.release()

    # 关闭后端连接池
    for attrib in dir(app.ctx):
        backend = app.ctx.__getattribute__(attrib)
        if isinstance(backend, Backend):
            logger.info(
                _("⌚ [📡Server] Closing backend {attrib}...").format(attrib=attrib)
            )
            await backend.close()


async def worker_start(app: Sanic):
    # 连后端、建表、跑启动System共用一个总超时预算，超时中止开服（见StartupAborted）。
    # 没有它的话，任意一步卡住（后端TCP不返回、启动System等一个永远不回的外部服务）在
    # 单进程模式下会让服务器永久静默挂起，多进程模式下则是被管理进程SIGKILL，看不出卡在哪。
    startup_timeout = app.config.get(
        STARTUP_TIMEOUT_CONFIG_KEY, DEFAULT_STARTUP_TIMEOUT
    )
    deadline = (
        asyncio.get_running_loop().time() + float(startup_timeout)
        if startup_timeout
        else None
    )

    def timeout_aborted(err: str) -> StartupAborted:
        """超时的统一报错：日志给排查线索，异常给CLI/管理进程做中止"""
        logger.error(
            _(
                "❌ 进程[{pid}] 启动超时({timeout}秒): {err}\n"
                "    启动流程卡住会让服务器一直起不来，已中止。"
                "如果它本来就需要跑这么久，请调大配置 STARTUP_TIMEOUT（0为不限制）"
            ).format(pid=os.getpid(), timeout=startup_timeout, err=err)
        )
        return StartupAborted(
            _("启动超时({timeout}秒): {err}").format(timeout=startup_timeout, err=err)
        )

    try:
        async with asyncio.timeout_at(deadline):
            await start_backends(app)
    except TimeoutError as e:
        raise timeout_aborted(_("卡在连接后端/建表阶段")) from e
    except Exception as e:
        logger.exception(
            _("❌ 进程[{pid}] 启动失败: {err}").format(
                pid=os.getpid(), err=f"{type(e).__name__}:{e}"
            )
        )
        raise StartupAborted(
            _("后端启动失败: {err}").format(err=f"{type(e).__name__}:{e}")
        ) from e

    # 打印信息
    from pathlib import Path

    logger.info(
        _("ℹ️ 进程[{pid}] 加载 {app_file} 完成").format(
            pid=os.getpid(),
            app_file=Path(app.config.get("APP_FILE", None)).resolve(strict=False),
        )
    )
    logger.info(
        _("ℹ️ 进程[{pid}] 已启动 {namespace} 应用的 {instances} 服").format(
            pid=os.getpid(),
            namespace=app.config["NAMESPACE"],
            instances=app.config["INSTANCES"],
        )
    )

    # 启动钩子：在开始收连接前，对每个instance执行所有 @define_system(on_start=True) 的System
    # 每次hetu start执行一次：boot uuid 由main进程经config传入，本次开服所有worker共享同一值以去重；
    # 缺失时（直接调用worker_main的测试/嵌入场景，均为单进程）退化为本进程生成。
    # 执行失败或超时则中止整个服务器启动（抛StartupAborted），避免带病对外服务。
    from ..system.startup import (
        ON_START_UUID_CONFIG_KEY,
        make_boot_uuid,
        run_startup_systems,
    )

    boot_uuid = app.config.get(ON_START_UUID_CONFIG_KEY) or make_boot_uuid()
    try:
        await run_startup_systems(
            app.config["NAMESPACE"],
            app.ctx.table_managers,
            boot_uuid,
            deadline=deadline,
        )
    except TimeoutError as e:
        raise timeout_aborted(str(e)) from e
    except Exception as e:
        logger.exception(
            _("❌ 进程[{pid}] 启动System执行失败: {err}").format(
                pid=os.getpid(), err=f"{type(e).__name__}:{e}"
            )
        )
        raise StartupAborted(
            _("启动System执行失败: {err}").format(err=f"{type(e).__name__}:{e}")
        ) from e


async def worker_close(app):
    # Windows 多 worker 时不会执行：sanic 停 worker（Ctrl+C、DEBUG 自动重载）是
    # TerminateProcess 硬杀，留下的租约由 live_worker_ids 认出来（见 redis/worker_keeper.py）
    # 先等连接拆完（断线System、删Connection行）再关后端，见 wait_connections_closed
    await wait_connections_closed(app.config.GRACEFUL_SHUTDOWN_TIMEOUT)
    # FutureCalls 执行的 System 会发号，要在释放租约之前停掉
    await stop_future_calls(app)
    await close_backends(app)


async def start_lease_loops(app: Sanic):
    """after_server_start：起续约、水位预留两个循环，由 close_backends 停掉"""
    lease = app.ctx.snowflake_lease
    # app.m 只在丢租约时才去取，和原来的写法一致
    lease.on_lost = lambda: app.m.restart()
    lease.start_loops()


def _report_task_death(task: asyncio.Task) -> None:
    """后台循环本应一直跑到关服。自己持有引用的 task 异常退出时 asyncio 不会提示，这里马上打出来"""
    if task.cancelled() or (exc := task.exception()) is None:
        return
    logger.error(
        _("❌ [📡Server] 后台任务 {name} 异常退出：{err}").format(
            name=task.get_name(), err=f"{type(exc).__name__}:{exc}"
        ),
        exc_info=exc,
    )


async def start_future_calls(app: Sanic):
    """after_server_start：开始执行到期的未来调用，由 stop_future_calls 停掉"""
    task = asyncio.create_task(future_call_task(app), name="hetu:future_calls")
    task.add_done_callback(_report_task_death)
    app.ctx.future_call_task = task


async def stop_future_calls(app: Sanic):
    """
    关服时在释放租约之前停掉 FutureCalls：它执行的 System 会发号，释放之后再发，就可能和
    接手这个 worker id 的进程撞号。
    """
    task = getattr(app.ctx, "future_call_task", None)
    if task is None or task.done():
        return
    task.cancel()
    timeout = app.config.GRACEFUL_SHUTDOWN_TIMEOUT
    _done, pending = await asyncio.wait({task}, timeout=timeout)
    if pending:
        logger.warning(
            _("⚠️ [⚙️Future] 关服时 FutureCalls 没在{timeout}秒内停下，不再等待").format(
                timeout=timeout
            )
        )


def worker_main(app_name, config) -> Sanic:
    """
    此函数会执行 workers+1 次。但如果是单worker，则只会执行1次。
    多worker时，第一次是Main函数的进程，负责管理workers，执行完不会启动任何注册的listener。
    后续Workers进程才会执行注册的listener。
    """

    # 加载玩家的app文件
    if (app_file := config.get("APP_FILE", None)) is not None:
        try:
            load_app_module(app_file)
        except Exception as e:
            print(
                _(
                    "无法加载主启动文件({err})：{app_file}，检查以下可能性：\n"
                    "* 如果是命令行启动，检查--app-file参数路径是否正确\n"
                    "* 如果是通过Config启动，此文件由APP_FILE参数设置\n"
                    "* 如果由Docker启动，还需检查是否正确映射了/app目录\n"
                ).format(err=type(e).__name__, app_file=app_file)
            )
            raise

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
    connection.CONNECTION_ALIVE_RECHECK_INTERVAL = config.get(
        "CONNECTION_ALIVE_RECHECK_INTERVAL", 5
    )
    system_lock.CALL_LOCK_RETENTION = config.get(
        "CALL_LOCK_RETENTION", system_lock.CALL_LOCK_RETENTION
    )

    # 加载web服务器
    app = Sanic(app_name, log_config=config.get("LOGGING", DEFAULT_LOGGING_CONFIG))
    app.update_config(config)

    # 设置管理进程等ack的超时。此函数在管理进程也会执行一次（AppLoader.load），
    # 所以在这里改类属性就能生效。注意WorkerManager.THRESHOLD是模块导入时从
    # WorkerProcess复制的，只改WorkerProcess的没用，两个都要设。
    ack_threshold = resolve_ack_threshold(config)
    WorkerManager.THRESHOLD = ack_threshold  # type: ignore
    WorkerProcess.THRESHOLD = ack_threshold  # type: ignore

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
    # Windows 多 worker 共享监听socket，accept 会把事件循环堵死，改到专用线程里接，
    # 见 threaded_accept.py
    if IS_WINDOWS:
        app.before_server_start(threaded_accept_listener)

    # 开始监听之后启动续约、水位预留两个循环和 FutureCalls。不用 app.add_task：loop 启动前加的
    # 任务不进 Sanic 的任务表，关服时不会被取消，会和释放租约并发；自己持有引用，由 worker_close
    # 在释放租约之前停掉
    app.after_server_start(start_lease_loops)
    app.after_server_start(start_future_calls)

    # 启动服务器监听
    app.blueprint(HETU_BLUEPRINT)

    # 挂载用户自定义的HTTP端点（@hetu.define_route / @hetu.on_server_setup）
    webext.apply_to(app)

    # 最后补上HeTu的默认欢迎页，用户自己注册了"/"就让给用户，不去撞RouteExists
    if not any(route.path in ("", "/") for route in app.router.routes):
        app.route("/")(web_root)
    return app
