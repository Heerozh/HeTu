"""
Sanic 适配：建 Sanic app，把它的生命周期接到 `WorkerRuntime` 上。换成 aiohttp 后删除
（设计稿 docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §5.1、§5.8）。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

Sanic adapter: builds the Sanic app and wires its lifecycle into `WorkerRuntime`.
"""

import asyncio
import logging
from collections.abc import Mapping
from typing import Any

from sanic import Sanic
from sanic.worker.manager import WorkerManager
from sanic.worker.process import WorkerProcess

from ... import webext
from ...i18n import _
from ...safelogging.default import DEFAULT_LOGGING_CONFIG
from .. import websocket as _websocket  # noqa: F401  导入即注册 ws 路由
from ..runtime import (
    DEFAULT_STARTUP_TIMEOUT,
    LEASE_LOST_EXIT_CODE,
    STARTUP_TIMEOUT_CONFIG_KEY,
    WorkerRuntime,
    prepare_worker,
    setup_message_pipeline,
)
from ..threaded_accept import IS_WINDOWS, threaded_accept_listener
from ..web import HETU_BLUEPRINT, web_root

logger = logging.getLogger("HeTu.root")

# 管理进程等worker ack的超时 = STARTUP_TIMEOUT + 这个余量。
# 两级超时不是冗余：我们自己的 asyncio 超时挡不住阻塞式代码（启动System里写了
# time.sleep/requests 之类把事件循环卡死），那时只有管理进程（另一个进程在数数）能兜底。
# 余量只需盖住我们量不到的部分：进程spawn、import app文件、bind端口。
ACK_TIMEOUT_MARGIN = 10.0
# "不限制"给sanic的阈值。sanic的单位是0.1秒，此值约3年，等同永不超时。
# 注意不能给它0，0是"第一次轮询(0.1秒)就判定worker启动失败"，意思正好相反
UNLIMITED_ACK_THRESHOLD = 10**9


def resolve_ack_threshold(config: Mapping[str, Any]) -> int:
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


def on_lease_lost(app: Sanic, runtime: WorkerRuntime) -> None:
    """
    续约发现 worker id 已经不属于本进程：再发号就可能和接手的进程撞号（发号围栏会拦住，但
    本进程从此发不出号），必须换一个新进程重新租。

    多进程交给 Sanic 管理进程只重启本 worker。单进程没有管理进程（没有 app.multiplexer，
    app.m 会抛 AttributeError）：停服并把 runtime.exit_code 置为非 0，由 hetu start 退出时
    带出，交给 docker/systemd/k8s 重启。
    """
    if hasattr(app, "multiplexer"):
        app.multiplexer.restart()
        return
    logger.error(
        _(
            "❌ [📡WorkerKeeper] Worker ID 租约已丢失，单进程模式没法只重启 worker，停止服务器"
        )
    )
    runtime.exit_code = LEASE_LOST_EXIT_CODE
    if app.state.is_running:
        app.stop()
    else:
        # 还在开服：Sanic 用 run_until_complete 跑各个启动钩子，这时 app.stop() 停掉 loop 会让
        # 开服报错。进入正常服务再停；登记进 Sanic 的任务表，没等到就关服时由它取消
        app.add_task(_stop_when_serving(app), name="hetu:stop_on_lease_lost")


async def _stop_when_serving(app: Sanic) -> None:
    while not app.state.is_running:
        await asyncio.sleep(0.1)
    app.stop()


async def _runtime_start(app: Sanic) -> None:
    """before_server_start：每次开服新建一个 runtime（Sanic 的测试客户端会对同一个 app 反复起停）"""
    runtime = WorkerRuntime(
        app.config, on_lease_lost=lambda: on_lease_lost(app, runtime)
    )
    app.ctx.runtime = runtime
    await runtime.start()


async def _runtime_start_background(app: Sanic) -> None:
    """after_server_start：开始监听之后再启动 FutureCalls"""
    app.ctx.runtime.start_background()


async def _runtime_stop(app: Sanic) -> None:
    """after_server_stop：Sanic 已停止监听、对各 ws 发了 1001，这时按 runtime 的顺序关服"""
    # Windows 多 worker 时不会执行：sanic 停 worker（Ctrl+C、DEBUG 自动重载）是
    # TerminateProcess 硬杀，留下的租约由 live_worker_ids 认出来（见 redis/worker_keeper.py）
    await app.ctx.runtime.stop()


def worker_main(app_name: str, config: Mapping[str, Any]) -> Sanic:
    """
    此函数会执行 workers+1 次。但如果是单worker，则只会执行1次。
    多worker时，第一次是Main函数的进程，负责管理workers，执行完不会启动任何注册的listener。
    后续Workers进程才会执行注册的listener。
    """
    # 加载玩家的app文件、建簇、把配置传给各模块
    prepare_worker(config)

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
    setup_message_pipeline(config)

    # 服务器main进程setup/teardown回调
    # app.main_process_start()
    # app.main_process_stop()
    # 服务器work进程setup/teardown回调：开服、关服的顺序都由 runtime 管
    app.before_server_start(_runtime_start)
    app.after_server_stop(_runtime_stop)
    # Windows 多 worker 共享监听socket，accept 会把事件循环堵死，改到专用线程里接，
    # 见 threaded_accept.py
    if IS_WINDOWS:
        app.before_server_start(threaded_accept_listener)

    # 开始监听之后启动 FutureCalls，要注册在用户的钩子（webext.apply_to）之前。续约、水位预留
    # 两个循环在 runtime.start() 拿到租约时就起了
    app.after_server_start(_runtime_start_background)

    # 启动服务器监听
    app.blueprint(HETU_BLUEPRINT)

    # 挂载用户自定义的HTTP端点（@hetu.define_route / @hetu.on_server_setup）
    webext.apply_to(app)

    # 最后补上HeTu的默认欢迎页，用户自己注册了"/"就让给用户，不去撞RouteExists
    if not any(route.path in ("", "/") for route in app.router.routes):
        app.route("/")(web_root)
    return app
