"""
一个 worker 里 HeTu 的运行时：持有后端、组件表、雪花号租约、任务表、连接表，负责开服、关服的
先后顺序。与 Web 框架无关，各框架的适配层（`hetu.server.adapters`）只负责把它接到自己的生命
周期上（设计稿 docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §5.1）。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

HeTu's per-worker runtime: backends, component tables, the snowflake lease, task and
connection registries, and the ordering of startup and shutdown. It does not depend on
any web framework; each framework adapter only wires it into its own lifecycle.
"""

import asyncio
import logging
import os
from collections.abc import Callable, Coroutine, Mapping
from pathlib import Path
from typing import Any

from ..data.backend import Backend
from ..data.backend.snowflake_lease import SnowflakeLease
from ..data.backend.worker_keeper import WorkerLease, create_worker_keeper
from ..endpoint import connection
from ..i18n import _
from ..local import load_app_module
from ..manager import ComponentTableManager
from ..system import SystemClusters
from ..system import lock as system_lock
from ..system.future import future_call_task
from . import pipeline

logger = logging.getLogger("HeTu.root")

# worker启动流程（连后端、建表、跑启动System）的总超时秒数配置键，0为不限制。
STARTUP_TIMEOUT_CONFIG_KEY = "STARTUP_TIMEOUT"
DEFAULT_STARTUP_TIMEOUT = 60.0
# 关服时等连接拆完、等后台任务停下各自的上限（秒），配置键 GRACEFUL_SHUTDOWN_TIMEOUT
DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT = 15.0
# 单进程模式丢租约后 hetu start 的退出码（见适配层的 on_lease_lost）。框架正常停服的退出码是 0，
# docker/systemd/k8s 会当成正常退出、不重启
LEASE_LOST_EXIT_CODE = 1
# 开服失败时停续约循环、释放租约的限时（秒）：开服失败多半是后端出了问题，释放卡住不能挡住中止
LEASE_ABANDON_TIMEOUT = 5.0


class StartupAborted(Exception):
    """worker启动流程失败，服务器必须中止，不能带病对外服务。

    必须以异常向上抛出（而不是让框架停服）才能真正中止：Sanic在 ``worker/serve.py`` 里会捕获
    启动异常并向管理进程发 ``__TERMINATE_EARLY__``，多进程模式立刻中止（不用等30秒ack超时）；
    单进程模式则一路冒泡到CLI。两种模式的退出码都非0，docker/systemd/k8s的重启策略才能识别为失败。

    Raised when worker startup fails and the server must abort instead of serving in a
    broken state. It must propagate as an exception (not a framework stop) so that the
    server aborts immediately and the process exits with a non-zero code in both single-
    and multi-process mode.
    """


def prepare_worker(config: Mapping[str, Any]) -> None:
    """
    worker 进程的一次性准备：加载玩家的 app 文件、建簇、把配置传给各模块。与 Web 框架无关；
    Sanic 多进程时管理进程也会跑一遍。

    One-time, framework-agnostic preparation of a worker process: load the app file, build
    the system clusters and pass config values to the modules that read them.
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


def setup_message_pipeline(config: Mapping[str, Any]) -> None:
    """按配置的 PACKET_LAYERS 初始化服务端消息处理流水线（进程内单件）"""
    msg_pipe = pipeline.ServerMessagePipeline()
    msg_pipe.clean()  # msg_pipe是单件，防止test用例中多次调用worker_main导致重复添加layer
    for layer_cfg in config.get("PACKET_LAYERS", []):
        layer = pipeline.MessageProcessLayerFactory.create(**layer_cfg)
        msg_pipe.add_layer(layer)


class TaskRegistry:
    """
    HeTu 自己的任务表，不交给 Web 框架：持有强引用（asyncio 只弱引用 task），结束后自动移除，
    异常退出时马上打日志（自己持有引用的 task 异常退出时 asyncio 不会提示）。有名字的 task
    名字唯一，可以按名取消。

    HeTu's own task registry instead of the web framework's: keeps strong references,
    drops finished tasks, logs tasks that die with an exception, and supports cancelling
    by name.
    """

    def __init__(self) -> None:
        self._tasks: set[asyncio.Task] = set()
        self._named: dict[str, asyncio.Task] = {}

    def __len__(self) -> int:
        return len(self._tasks)

    def spawn(
        self, coro: Coroutine[Any, Any, Any], *, name: str | None = None
    ) -> asyncio.Task:
        """起一个 task 并登记。名字已被占用时抛 ValueError（不会起这个 task）"""
        if name is not None and name in self._named:
            coro.close()
            raise ValueError(f"task {name!r} is already registered")
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        if name is not None:
            self._named[name] = task
        task.add_done_callback(self._on_done)
        return task

    def _on_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        name = task.get_name()
        if self._named.get(name) is task:
            del self._named[name]
        if task.cancelled() or (exc := task.exception()) is None:
            return
        logger.error(
            _("❌ [📡Server] 后台任务 {name} 异常退出：{err}").format(
                name=name, err=f"{type(exc).__name__}:{exc}"
            ),
            exc_info=exc,
        )

    async def cancel(self, name: str) -> None:
        """按名取消并等它结束；没有这个名字（已经结束或从没起过）就什么也不做"""
        task = self._named.get(name)
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise  # 是调用方自己被取消了，往外传
        except Exception:  # noqa: BLE001, S110 异常已由 _on_done 打出
            pass

    async def cancel_all(self, timeout: float | None) -> bool:
        """取消全部并等它们结束，至多 timeout 秒。返回是否都结束了"""
        tasks = list(self._tasks)
        if not tasks:
            return True
        for task in tasks:
            task.cancel()
        _done, pending = await asyncio.wait(tasks, timeout=timeout)
        return not pending


class WorkerRuntime:
    """
    一个服务实例一个（适配层每次开服新建一个），持有 HeTu 在一个 worker 里的运行时状态，负责
    开服、关服的先后顺序。适配层在框架的生命周期钩子里调用：

    - 开服：`start()`（连后端、建表、拿租约并马上开始续约、跑启动 System）→ 框架开始监听 →
      `start_background()`（FutureCalls）；
    - 关服：框架停止监听、开始拆连接 → `stop()`（等连接拆完 → 停后台任务 → 释放租约 → 关后端）。

    `on_lease_lost` 由适配层提供：续约发现 Worker ID 已经不属于本进程时调用，要换一个新进程
    重新租（多进程时只重启本 worker，单进程时停服并把 `exit_code` 置为非 0）。

    One per server instance (adapters create a new one on every start). Holds HeTu's
    runtime state in a worker and owns the ordering of startup and shutdown.
    """

    def __init__(
        self, config: Mapping[str, Any], *, on_lease_lost: Callable[[], None]
    ) -> None:
        self.config = config
        self.on_lease_lost = on_lease_lost
        # 按配置名登记的后端，另有 "default" 指向配置里的第一个
        self.backends: dict[str, Backend] = {}
        self.table_managers: dict[str, ComponentTableManager] = {}
        self.snowflake_lease: SnowflakeLease | None = None
        # 连接相关的任务：各连接的接收协程、顶号核查
        self.tasks = TaskRegistry()
        # 后台任务：FutureCalls。续约、水位预留两个循环由 SnowflakeLease 自己管
        self.background = TaskRegistry()
        # Connection 行已落库的连接协程、拆连接的清理任务，结束后自动移除，关服时要等它们
        # 都结束（见 wait_connections_closed）
        self.live_connections: set[asyncio.Task] = set()
        self.cleanup_tasks: set[asyncio.Task] = set()
        # 进程该以什么码退出：单进程模式丢租约时由适配层置为 LEASE_LOST_EXIT_CODE
        self.exit_code = 0

    @property
    def default_backend(self) -> Backend:
        return self.backends["default"]

    @property
    def graceful_shutdown_timeout(self) -> float:
        return float(
            self.config.get(
                "GRACEFUL_SHUTDOWN_TIMEOUT", DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT
            )
        )

    # ---------------------------------------------------------------- 开服

    async def start(self) -> None:
        """
        开服：连后端、建表、拿租约（拿到就开始续约）、跑启动 System，共用一个 STARTUP_TIMEOUT
        预算。失败或超时抛 StartupAborted；拿到租约之后才失败的，先停续约循环并释放租约。
        """
        try:
            await self._start()
        except StartupAborted:
            await self.abandon_lease()
            raise

    async def _start(self) -> None:
        # 连后端、建表、跑启动System共用一个总超时预算，超时中止开服（见StartupAborted）。
        # 没有它的话，任意一步卡住（后端TCP不返回、启动System等一个永远不回的外部服务）在
        # 单进程模式下会让服务器永久静默挂起，多进程模式下则是被管理进程SIGKILL，看不出卡在哪。
        startup_timeout = self.config.get(
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
                _("启动超时({timeout}秒): {err}").format(
                    timeout=startup_timeout, err=err
                )
            )

        try:
            async with asyncio.timeout_at(deadline):
                await self.start_backends()
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
        logger.info(
            _("ℹ️ 进程[{pid}] 加载 {app_file} 完成").format(
                pid=os.getpid(),
                app_file=Path(self.config.get("APP_FILE", None)).resolve(strict=False),
            )
        )
        logger.info(
            _("ℹ️ 进程[{pid}] 已启动 {namespace} 应用的 {instances} 服").format(
                pid=os.getpid(),
                namespace=self.config["NAMESPACE"],
                instances=self.config["INSTANCES"],
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

        boot_uuid = self.config.get(ON_START_UUID_CONFIG_KEY) or make_boot_uuid()
        try:
            await run_startup_systems(
                self.config["NAMESPACE"],
                self.table_managers,
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

    async def start_backends(self) -> None:
        """创建后端连接池、建表，拿 Worker ID 租约并马上开始续约、预留水位"""
        # 创建后端连接池
        backends = self.backends
        for name, db_cfg in self.config["BACKENDS"].items():
            backend = Backend(db_cfg)
            backends[name] = backend
            # 把config第一个设置为default后端
            if "default" not in backends:
                backends["default"] = backend

        # 初始化所有ComponentTable
        for instance_name in self.config["INSTANCES"]:
            self.table_managers[instance_name] = ComponentTableManager(
                self.config["NAMESPACE"],
                instance_name,
                backends,
            )

            # 检测表状态，创建所有不存在的表
            all_table_ok = self.table_managers[
                instance_name
            ].check_and_create_new_tables()

            # 如果有迁移需求，则报错退出，让用户用cli migrate命令来迁移
            if not all_table_ok:
                msg = _(
                    "❌ [📡Server] 数据库表结构需要迁移，请使用迁移命令："
                    "hetu upgrade --config <your_config_file>.yml"
                )
                logger.error(msg)
                raise RuntimeError(msg)

        # 最后调用 backend config, 以防configure中需要之前初始化的东西
        for backend in backends.values():
            backend.post_configure()

        # 在backend初始化完毕后，租 Worker ID、接上时间戳高水位、初始化雪花ID生成器（见
        # SnowflakeLease）。分配器按后端类型自动选：Redis后端用真租约（多机安全），SQLite后端
        # 用本机进程序号（开发模式，单机安全）。见 create_worker_keeper。
        # 水位表固定用 INSTANCES[0] 的：hetu call / shell 也认这一张
        lease_tbl = self.table_managers[self.config["INSTANCES"][0]].get_table(
            WorkerLease
        )
        assert lease_tbl is not None
        lease = SnowflakeLease(
            create_worker_keeper(lease_tbl.backend, os.getpid()), lease_tbl
        )
        # 反复宕机导致 Worker ID 分配满了时每秒重试，等它们过期
        await lease.acquire(wait=True)
        self.snowflake_lease = lease
        # 拿到租约立刻开始续约、预留水位，不能等开始监听：发号围栏在拿到租约 45 秒后跳闸，
        # 而之后的启动 System 最长能跑 STARTUP_TIMEOUT（默认 60）秒
        lease.on_lost = lambda: self.on_lease_lost()
        lease.start_loops()

    def start_background(self) -> None:
        """开始监听之后调用：开始执行到期的未来调用（FutureCalls），由 stop() 在释放租约之前停掉"""
        self.background.spawn(future_call_task(self), name="hetu:future_calls")

    async def abandon_lease(self) -> None:
        """开服失败时停掉续约、水位预留循环并释放租约（拿到租约之后才失败的情况），限时"""
        lease = self.snowflake_lease
        if lease is None:
            return
        try:
            async with asyncio.timeout(LEASE_ABANDON_TIMEOUT):
                await lease.stop_loops()
                await lease.release()
        except TimeoutError:
            logger.warning(
                _("⚠️ [📡Server] 开服失败后释放 Worker ID 租约超时，等它自己过期")
            )

    # ---------------------------------------------------------------- 关服

    async def wait_connections_closed(self, timeout: float) -> bool:
        """
        关服时等本服务的连接都拆完：连接协程结束、清理任务（断线 System、删 Connection 行）
        跑完。超时返回 False。

        清理任务不在任何框架的任务表里，框架关服不等它；直接关后端的话，它可能正停在写库事务
        中途，loop 一关就永远挂住：断线 System 没跑完、Connection 行漏删。

        Wait until every connection of this server is torn down (on_disconnect called,
        Connection row deleted) before the backends are closed. Returns False on timeout.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        # 连接协程被框架强关后才进 finally 建清理任务，所以每轮重新收集，直到两边都空
        while pending := {
            task
            for task in (*self.live_connections, *self.cleanup_tasks)
            if not task.done()
        }:
            remaining = deadline - loop.time()
            if remaining <= 0:
                logger.warning(
                    _(
                        "⚠️ [📡Server] 关服时还有{count}个连接没拆完，已等待{timeout}秒，"
                        "不再等待直接关闭后端"
                    ).format(count=len(pending), timeout=timeout)
                )
                return False
            await asyncio.wait(pending, timeout=remaining)
        return True

    async def stop(self) -> None:
        """
        关服。调用方（适配层）保证此时已停止监听、框架已经开始拆连接。顺序：

        1. 等连接拆完（断线 System、删 Connection 行）；
        2. 取消还没结束的连接相关任务；
        3. 停 FutureCalls：它执行的 System 会发号，释放租约之后再发，就可能和接手这个
           Worker ID 的进程撞号；
        4. 停续约、水位预留循环，释放租约并写精确水位；
        5. 关后端。

        Shut down in a fixed order: wait for connections, cancel leftover connection
        tasks, stop FutureCalls, release the lease, close backends.
        """
        timeout = self.graceful_shutdown_timeout
        await self.wait_connections_closed(timeout)
        await self.tasks.cancel_all(timeout)
        if not await self.background.cancel_all(timeout):
            logger.warning(
                _(
                    "⚠️ [⚙️Future] 关服时 FutureCalls 没在{timeout}秒内停下，不再等待"
                ).format(timeout=timeout)
            )
        await self.close_backends()

    async def close_backends(self) -> None:
        """释放租约（写精确水位）后关闭所有后端连接池"""
        # 先停续约、水位预留两个循环：不然续约会对刚释放的 id 报租约丢失，水位预留会在精确
        # 水位之后再写一次。调用方保证连接已拆完、FutureCalls 已停（见 stop），不会再发号，
        # 最后用到的时间戳就是真实上界（见 SnowflakeLease.release）
        lease = self.snowflake_lease
        if lease is not None:
            await lease.stop_loops()
            await lease.release()

        # 关闭后端连接池。"default" 和第一个后端是同一个，只关一次
        closed: set[int] = set()
        for name, backend in self.backends.items():
            if id(backend) in closed:
                continue
            closed.add(id(backend))
            logger.info(
                _("⌚ [📡Server] Closing backend {attrib}...").format(attrib=name)
            )
            await backend.close()
