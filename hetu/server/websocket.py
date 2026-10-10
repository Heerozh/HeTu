"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from typing import Any

from sanic import Request, Websocket
from sanic.exceptions import WebsocketClosed

from ..data.sub import SubscriptionBroker
from ..endpoint import connection
from ..endpoint.executor import EndpointExecutor
from ..i18n import _
from ..system.caller import SystemCaller
from ..system.context import SystemContext
from .pipeline import ServerMessagePipeline
from .receiver import PUSH_CLOSE, client_handler
from .web import HETU_BLUEPRINT

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")
DISCONNECT_SYSTEM = "on_disconnect"
# 塞进 push_queue 叫醒空闲等在 get() 上的发送循环去取待发区：hub 把订阅推送交到门面时由它的
# 叫醒函数塞进来（见 SubscriptionBroker.bind_sender_）
PUSH_UPDATES = object()
# 订阅推送最多被排着的回复压住多久（秒，约一个合批间隔），见 send_loop
PUSH_MAX_HOLD = 0.1


class PushQueue(asyncio.Queue[Any]):
    """
    连接的 push_queue：按请求顺序排着要发的回复（RPC 回复、订阅回复的占位）和几个哨兵。另记着排在
    里面的订阅回复占位（future）有几个：一个都没有时，登记过的订阅的回复都已发出（占位先于登记入队），
    发送循环据此判断推送能不能插到排着的回复前面（见 send_loop）
    """

    def _init(self, maxsize: int) -> None:
        super()._init(maxsize)
        self.placeholders = 0

    def _put(self, item: Any) -> None:
        if isinstance(item, asyncio.Future):
            self.placeholders += 1
        super()._put(item)

    def _get(self) -> Any:
        item = super()._get()
        if isinstance(item, asyncio.Future):
            self.placeholders -= 1
        return item


@HETU_BLUEPRINT.websocket("/hetu/<db_name>")
async def websocket_connection(request: Request, ws: Websocket, db_name: str) -> None:
    """ws连接处理器，运行在worker主协程下"""
    # 获取当前协程任务, 自身算是一个协程1
    current_task = asyncio.current_task()
    assert current_task, "Must be called in an asyncio task"
    logger.info(
        _("🔗 [📡WSConnect] 新连接：{db_name}: {task_name}").format(
            db_name=db_name, task_name=current_task.get_name()
        )
    )

    # 获得客户端握手消息
    msg_pipe = ServerMessagePipeline()
    handshake_msg = await ws.recv(timeout=10)
    if not isinstance(handshake_msg, (bytes, bytearray)):
        logger.info("New Connect Error: Invalid handshake message type")
        ws.fail_connection()
        return
    handshake_msg = msg_pipe.decode(None, handshake_msg)
    if not isinstance(handshake_msg, list):
        logger.info("New Connect Error: Invalid handshake message format")
        ws.fail_connection()
        return

    # 进行握手处理，获得连接上下文
    if len(handshake_msg) != msg_pipe.num_handshake_layers:
        logger.info(
            "New Connect Error: client pipeline layers count "
            "does not match server pipeline"
        )
        ws.fail_connection()
        return

    # 在客户端握手后，才返回实例是否存在的错误，防止暴露实例信息给扫描器
    instance = db_name
    if instance not in request.app.ctx.table_managers:
        logger.info(
            _("New Connect Error: 错误的路径，实例名不存在: {instance}").format(
                instance=instance
            )
        )
        ws.fail_connection()
        return
    tbl_mgr = request.app.ctx.table_managers[instance]

    # 返回握手结果
    try:
        pipe_ctx, reply = msg_pipe.handshake(handshake_msg)
        await ws.send(reply)
    except Exception as e:  # noqa: BLE001 握手包来自客户端，出什么异常都只断开该连接
        logger.info(f"New Connect Error: handshake failed: {e}")
        try:
            await ws.send(msg_pipe.encode(None, []))
        finally:
            ws.fail_connection()
        return

    # 初始化Context，一个连接一个Context
    context = SystemContext(
        caller=0,
        connection_id=0,
        address=request.client_ip,
        group="guest",
        user_data={},
        timestamp=0,
        request=request,
        systems=None,  # type: ignore
    )
    default_limits = []  # [[10, 1], [27, 5], [100, 50], [300, 300]]
    context.configure(
        client_limits=request.app.config.get("CLIENT_SEND_LIMITS", default_limits),
        server_limits=request.app.config.get("SERVER_SEND_LIMITS", default_limits),
        max_row_sub=request.app.config.get("MAX_ROW_SUBSCRIPTION", 1000),
        max_index_sub=request.app.config.get("MAX_INDEX_SUBSCRIPTION", 50),
        max_table_sub=request.app.config.get("MAX_TABLE_SUBSCRIPTION", 20),
    )

    # 初始化System执行器，一个连接一个执行器
    namespace = request.app.config["NAMESPACE"]
    system_caller = SystemCaller(namespace, tbl_mgr, context)
    context.systems = system_caller

    # 初始化Endpoint执行器，一个连接一个执行器
    endpoint_executor = EndpointExecutor(namespace, tbl_mgr, context)
    await endpoint_executor.initialize(request.client_ip)

    # 从这里起本连接的 Connection 行已经落库：之后不管哪一步失败或被取消（客户端在初始化期间
    # 断线、pubsub 订阅失败……），都必须走到 finally 里的 terminate() 把它删掉。这行没有 TTL
    # 也没有清理任务，漏掉就永远留在库里，而匿名连接数是按 IP 计数的，攒够几次同一出口的
    # 客户端就再也连不上了
    recv_task_id = f"client_handler:{request.id}"
    broker: SubscriptionBroker | None = None
    closing = False  # 已进入拆连接流程：之后收到的"被顶号"核查结果不作数
    # 关服时要等本连接拆完再关后端，见 wait_connections_closed
    _live_connections.add(current_task)
    current_task.add_done_callback(_live_connections.discard)
    try:
        # 初始化订阅管理器，一个连接一个订阅管理器
        broker = SubscriptionBroker(
            request.app.ctx.default_backend,
            max_table_rows=request.app.config.get(
                "MAX_TABLE_SUBSCRIPTION_ROWS", 100_000
            ),
        )

        # 被顶号通知：登录后订阅 Connection 表 "owner == 本用户" 这个索引值频道（owner 声明了
        # point_sub），收到通知才重查，RPC 路径上不再每次读库；收到通知还主动从 master 核一次，
        # 被顶号就立刻断连，不用等它下次调用。订索引值频道而不是本连接那行的行频道：行频道会被
        # 本连接自己的心跳 HSET(last_active) 每 ENDPOINT_CALL_IDLE_TIMEOUT/5 秒触发一次，白白
        # 重查；值频道只在有行"进入"本用户时才有消息（commit 只发进入），心跳碰不到它。顶号正是
        # 这样：别处登录的 elevate() 在同一个事务里把本行 owner 改成 0（离开，不发）、把新连接
        # 那行 owner 改成本用户（进入，发）。只把本行 owner 改走、或删掉本行而没有行进入本用户，
        # 不会有通知，要等下次调用时按 CONNECTION_ALIVE_RECHECK_INTERVAL 兜底重查。
        # 频道名与 hub 必须是同一个后端，否则保持每次都查
        conn_tbl = tbl_mgr.get_table(connection.Connection)
        if conn_tbl is not None and conn_tbl.backend is request.app.ctx.default_backend:
            alive_checker = endpoint_executor.alive_checker
            mark_dirty = alive_checker.enable_notify_mode()

            async def recheck_alive():
                try:
                    kicked = await alive_checker.kicked(context)
                    if closing:
                        # 正常拆连接时自己删了本行，kicked() 读到 None 也算"被顶号"；
                        # 核查发起于拆连接之前、读回来时已在拆的，别记假的顶号日志
                        return
                    if kicked:
                        close_msg = _(
                            "⛓️ [📡WSConnect] 连接已被顶号，主动断开：{ctx}"
                        ).format(ctx=context)
                        replay.info(close_msg)
                        logger.info(close_msg)
                        # 先发带原因的 close 再断开，客户端据此提示"账号已在别处登录"
                        ws.fail_connection(
                            connection.CLOSE_KICKED, connection.CLOSE_KICKED_REASON
                        )
                except Exception as e:  # noqa: BLE001 读库失败不致命：兜底间隔和下次调用还会再查
                    logger.warning(
                        _("⚠️ [📡WSConnect] 顶号核查读库失败：{err}").format(
                            err=f"{type(e).__name__}:{e}"
                        )
                    )

            def on_owner_changed():
                # 在 hub 的监听协程里同步调用：只置标记 + 起短任务，不能 await
                mark_dirty()
                request.app.add_task(recheck_alive())

            async def watch_owner(user_id: int):
                # 登录的那次调用返回前订上；订阅生效之前就被顶号的话收不到通知，订上后主动核一次
                assert broker is not None
                await broker.watch_channel(
                    conn_tbl.backend.servant.index_value_channel(
                        conn_tbl, "owner", user_id
                    ),
                    on_owner_changed,
                )
                await recheck_alive()

            endpoint_executor.on_elevated = watch_owner

        # 初始化push消息队列
        push_queue = PushQueue(1024)

        # 初始化发送/接受计数器
        flood_checker = connection.ConnectionFloodChecker()

        # 创建接受客户端消息的协程2
        receiver_task = client_handler(
            ws,
            pipe_ctx,
            endpoint_executor,
            broker,
            push_queue,
            flood_checker,
            int(request.app.config.get("DEBUG", 0)),
        )
        _forget = request.app.add_task(receiver_task, name=recv_task_id)

        # 订阅推送不另开协程：hub 交到门面的待发区时，发送循环空闲就塞 PUSH_UPDATES 叫醒它，由它
        # 直接取走发送。队列满时不塞：那时发送循环不会等在 get() 上，发完队列会回来取待发区
        def wake_sender() -> None:
            with contextlib.suppress(asyncio.QueueFull):
                push_queue.put_nowait(PUSH_UPDATES)

        broker.bind_sender_(wake_sender)

        # 删除当前长连接用不上的临时变量
        del namespace
        del default_limits
    except Exception as e:
        # 初始化失败只能断开，finally 里会把已落库的 Connection 行清掉
        err_msg = _("❌ [📡WSConnect] 连接初始化异常：{err}").format(
            err=f"{type(e).__name__}:{e}"
        )
        replay.info(err_msg)
        logger.exception(err_msg)
        ws.fail_connection()
    else:
        assert broker is not None

        def pack(reply) -> bytes:
            # 如果关闭了replay，为了速度不执行下面的字符串序列化
            if replay.isEnabledFor(logging.DEBUG):
                replay.debug(">>> " + str(reply))
            return msg_pipe.encode(pipe_ctx, reply)

        def flooded() -> bool:
            """记一次发送，到了发送上限就断开"""
            flood_checker.sent()
            if flood_checker.send_limit_reached(context, "Coroutines(Websocket.push)"):
                ws.fail_connection()
                return True
            return False

        # 这里循环发送，保证总是第一时间Push
        try:
            await send_loop(ws, broker, push_queue, pack, flooded)
        except asyncio.CancelledError:
            if ws.ws_proto.parser_exc and not isinstance(
                ws.ws_proto.parser_exc, EOFError
            ):
                err_msg = _("❌ [📡WSSender] WS协议异常：{exc}").format(
                    exc=ws.ws_proto.parser_exc
                )
                replay.info(err_msg)
                logger.exception(err_msg, exc_info=ws.ws_proto.parser_exc)
            # print(executor.context, 'websocket_connection normal canceled', ws.ws_proto.parser_exc)
        except WebsocketClosed:
            pass
        except BaseException as e:
            err_msg = _("❌ [📡WSSender] 发送数据异常：{err}").format(
                err=f"{type(e).__name__}:{e}"
            )
            replay.info(err_msg)
            logger.exception(err_msg)
    finally:
        # 连接断开，强制关闭此协程时也会调用。
        # 清理放进独立 task 并 shield：ws.fail_connection() 之后传输层一关，Sanic 会取消本协程
        # （connection_lost → 取消连接 task），要是正落在清理里的某个 await 上，后面的
        # terminate() 就跑不到，Connection 行同样泄漏；shield 让本协程被取消时清理照常跑完。
        # 不登记进 app 的任务表：关服时 shutdown_tasks 会取消表里的任务，loop 却已经不转了，
        # 没法结束的任务会让它空转不退出。关服由 wait_connections_closed 等它跑完
        closing = True
        cleanup_task = asyncio.create_task(
            _cleanup_connection(
                request,
                current_task.get_name(),
                recv_task_id,
                system_caller,
                context,
                endpoint_executor,
                broker,
            ),
            name=f"ws_cleanup:{request.id}",
        )
        _cleanup_tasks.add(cleanup_task)  # 保持引用免得被 gc
        cleanup_task.add_done_callback(_cleanup_tasks.discard)
        await asyncio.shield(cleanup_task)


async def send_loop(
    ws: Websocket,
    broker: SubscriptionBroker,
    push_queue: PushQueue,
    pack: Callable[[Any], bytes],
    flooded: Callable[[], bool],
) -> None:
    """
    连接的发送循环（跑在连接协程自己里）：按顺序发 push_queue 里的回复（RPC 回复、订阅回复的占位），
    队列空了就取门面的待发区发订阅推送。订阅推送不进队列，留在待发区里按 sub_id / row_id 合并。
    订阅回复的占位先于登记放进队列，队列空了就说明登记之前放的占位都已发出，推送不会抢到它的回复
    前面（设计稿 2026-09-29-push-path-and-gc §2）。
    回复优先，但推送不能一直等队列空：客户端连着发 RPC、链路又慢时队列可能一直不空。推送被排着的回复
    压住超过 PUSH_MAX_HOLD 秒，而队列里一个订阅回复的占位都没有（登记过的订阅的回复都已发出）时，
    先插一轮推送。在等占位的期间不插：那个占位是哪个订阅的还不知道。
    等推送的调用（rpcs）的 ["sync", id] 随推送一起取、排在推送后面发（门面的 take_synced_，设计稿
    2026-10-10-rpcs-sync §4.5），先后规则与推送相同。
    pack 把一条回复 / 推送编成要发的帧；flooded 记一次发送，到了发送上限就断开连接、返回真。
    返回时连接已在断开：收到 PUSH_CLOSE（接收协程已经拆了连接）、发送超限、订阅初始化失败
    """
    loop = asyncio.get_running_loop()
    # 推送开始被排着的回复压住的时刻；待发区空了、或推送发出去了就清掉
    held_since: float | None = None
    while True:
        drain = push_queue.empty()
        if not drain:
            if not (broker.has_updates_() or broker.has_synced_()):
                held_since = None
            elif held_since is None:
                held_since = loop.time()
            elif (
                push_queue.placeholders == 0
                and loop.time() - held_since >= PUSH_MAX_HOLD
            ):
                drain = True
        if drain:
            held_since = None
            # 同一个同步段里先取待发区、再取 sync：栅栏在它那个 tick 交付之后才触发，应排在 sync 前面的
            # 推送要么已经发出，要么就在这次取走的待发区里
            updates = broker.take_updates_()
            synced = broker.take_synced_()
            if updates or synced:
                for sub_id, data in updates.items():
                    await ws.send(pack(["updt", sub_id, data]))
                    if flooded():
                        return
                for sync_id in synced:
                    await ws.send(pack(["sync", sync_id]))
                    if flooded():
                        return
                continue
        if push_queue.empty():
            # 空闲等着：这期间交来的推送不算卡住，hub 会塞 PUSH_UPDATES 叫醒这里
            broker.idle_(True)
            try:
                reply = await push_queue.get()
            finally:
                broker.idle_(False)
        else:
            reply = push_queue.get_nowait()
        if reply is PUSH_UPDATES:
            continue
        # 接收协程结束时会塞这个哨兵进来（它已经把连接拆了）：返回去跑连接协程 finally 里的清理，
        # 否则会一直阻塞在 get() 上，连接半死不活地挂着
        if reply is PUSH_CLOSE:
            return
        if isinstance(reply, asyncio.Future):
            # 在后台完成的订阅占住的回复位：等它填好再发。回复没有请求 id、SDK 按顺序对应，排在它
            # 后面的回复都得跟着等。等的期间推送不算卡住：hub 照常读、合并进待发区，回复发出之后再推
            broker.idle_(True)
            try:
                reply = await reply
            except Exception as e:
                err_msg = _("❌ [📡WSSender] 订阅初始化异常：{err}").format(
                    err=f"{type(e).__name__}:{e}"
                )
                replay.info(err_msg)
                logger.exception(err_msg)
                ws.fail_connection()
                return
            finally:
                broker.idle_(False)
        await ws.send(pack(reply))
        if flooded():
            return


# 拆连接的清理任务：保持引用免得被 gc，跑完自动移除
_cleanup_tasks: set[asyncio.Task] = set()
# Connection 行已落库的连接协程，协程结束自动移除
_live_connections: set[asyncio.Task] = set()


async def wait_connections_closed(timeout: float) -> bool:
    """
    关服时等本进程的连接都拆完：连接协程结束、清理任务（断线 System、删 Connection 行）
    跑完。超时返回 False。

    清理任务没登记进 app 的任务表（见 websocket_connection 的 finally），Sanic 关服不等它；
    直接关后端的话，它可能正停在写库事务中途，loop 一关就永远挂住：断线 System 没跑完、
    Connection 行漏删。

    Wait until every connection of this process is torn down (on_disconnect called,
    Connection row deleted) before the backends are closed. Returns False on timeout.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    # 连接协程被 Sanic 强关后才进 finally 建清理任务，所以每轮重新收集，直到两边都空。
    # 别的 loop 上的任务（测试里之前起过的服务器）永远等不到，不算
    while pending := {
        task
        for task in (*_live_connections, *_cleanup_tasks)
        if not task.done() and task.get_loop() is loop
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


async def _cleanup_connection(
    request: Request,
    task_name: str,
    recv_task_id: str,
    system_caller: SystemCaller,
    context: SystemContext,
    endpoint_executor: EndpointExecutor,
    broker: SubscriptionBroker | None,
) -> None:
    """拆连接：停协程、调断线 System、删 Connection 行、退订。任何一步失败都不能跳过后面的"""
    close_msg = _("⛓️ [📡WSConnect] 连接断开：{task_name}").format(task_name=task_name)
    replay.info(close_msg)
    logger.info(close_msg)
    await request.app.cancel_task(recv_task_id, raise_exception=False)
    # 先退订再删本连接的 Connection 行（在 endpoint_executor.terminate 里）。删行对 owner 值
    # 频道是"离开"，commit 不发（值频道只发进入），被顶号的 watcher 不会因此收到通知；此前
    # 已发起、读回时已在拆连接的核查结果也不作数（见 closing）。
    # 退订等到 UNSUBSCRIBE ack 才返回，之后的通知 Redis 不会再投给本进程
    if broker is not None:
        await broker.close()
    try:
        system_caller.call_check(DISCONNECT_SYSTEM)
    except ValueError:
        pass
    else:
        # 断线System不走Endpoint，要自己打时间戳，否则是上一次rpc的时间（可能很久以前）
        context.timestamp = time.time()
        try:
            # 被顶号的连接已经不代表这个用户：账号在新连接上，新连接的登录逻辑可能已经跑过
            # （比如把用户标成在线），再以用户身份跑断线 System 会把它覆盖掉。改以匿名身份跑，
            # 和库里一致（顶号时本行 owner 已被改成 0）。顶号通知可能丢了、还在路上，这里从
            # master 再核一次：刚被顶号时副本可能还是旧的。
            # 核查和断线 System 不在一个事务里（Connection 在自己的簇），核查之后、断线 System
            # 提交之前才被顶号的，仍会以用户身份跑
            if context.caller and await endpoint_executor.alive_checker.kicked(context):
                context.caller = 0
            await system_caller.call(DISCONNECT_SYSTEM)
        except BaseException as e:
            err_msg = _(
                "❌ [📡WSDisconnectHook] 断线System调用异常: {system} | {err}"
            ).format(system=DISCONNECT_SYSTEM, err=f"{type(e).__name__}:{e}")
            replay.info(err_msg)
            logger.exception(err_msg)
    try:
        await endpoint_executor.terminate()
    finally:
        request.app.purge_tasks()
