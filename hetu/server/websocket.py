"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
import time

from sanic import Request, Websocket
from sanic.exceptions import WebsocketClosed

from ..data.sub import SubscriptionBroker
from ..endpoint import connection
from ..endpoint.executor import EndpointExecutor
from ..i18n import _
from ..system.caller import SystemCaller
from ..system.context import SystemContext
from .pipeline import ServerMessagePipeline
from .receiver import PUSH_CLOSE, client_handler, subscription_handler
from .web import HETU_BLUEPRINT

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")
DISCONNECT_SYSTEM = "on_disconnect"


@HETU_BLUEPRINT.websocket("/hetu/<db_name>")  # noqa
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
    except Exception as e:
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
    subs_task_id = f"subs_receiver:{request.id}"
    broker: SubscriptionBroker | None = None
    closing = False  # 已进入拆连接流程：之后收到的"被顶号"核查结果不作数
    try:
        # 初始化订阅管理器，一个连接一个订阅管理器
        broker = SubscriptionBroker(
            request.app.ctx.default_backend,
            max_table_rows=request.app.config.get(
                "MAX_TABLE_SUBSCRIPTION_ROWS", 100_000
            ),
        )

        # 被顶号通知：登录后订阅 Connection 表 "owner == 本用户" 这个索引值频道，收到通知才重查，
        # RPC 路径上不再每次读库；收到通知还主动从 master 核一次，被顶号就立刻断连，不用等它
        # 下次调用。订索引值频道而不是本连接那行的行频道：行频道会被本连接自己的心跳
        # HSET(last_active) 每 ENDPOINT_CALL_IDLE_TIMEOUT/5 秒触发一次，白白重查；而 owner 值
        # 频道只在某行的 owner 从/到本用户变化、或带本用户的行增删时才有消息——正是被顶号
        # （本行 owner 被改成 0）和别处登录本用户这两件事，心跳碰不到它。
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
                        ws.fail_connection()
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
        push_queue = asyncio.Queue(1024)

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

        # 创建获得订阅推送通知的协程3（通知由本进程共享的 pubsub 分发器直接塞进 broker 的本地队列）
        subscript_task = subscription_handler(ws, broker, push_queue)
        _forget = request.app.add_task(subscript_task, name=subs_task_id)

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
        # 这里循环发送，保证总是第一时间Push
        try:
            while True:
                reply = await push_queue.get()
                # 接收协程结束时会塞这个哨兵进来（它已经把连接拆了）：跳出去跑 finally
                # 的清理，否则本协程会一直阻塞在 get() 上，连接半死不活地挂着
                if reply is PUSH_CLOSE:
                    break
                # 如果关闭了replay，为了速度不执行下面的字符串序列化
                if replay.level < logging.ERROR:
                    replay.debug(">>> " + str(reply))
                # print(executor.context, 'got', reply)
                await ws.send(msg_pipe.encode(pipe_ctx, reply))
                # 检查发送上限
                flood_checker.sent()
                if flood_checker.send_limit_reached(
                    context, "Coroutines(Websocket.push)"
                ):
                    ws.fail_connection()
                    break
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
        # 没法结束的任务会让它空转不退出
        closing = True
        cleanup_task = asyncio.create_task(
            _cleanup_connection(
                request,
                current_task.get_name(),
                recv_task_id,
                subs_task_id,
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


# 拆连接的清理任务：保持引用免得被 gc，跑完自动移除
_cleanup_tasks: set[asyncio.Task] = set()


async def _cleanup_connection(
    request: Request,
    task_name: str,
    recv_task_id: str,
    subs_task_id: str,
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
    await request.app.cancel_task(subs_task_id, raise_exception=False)
    # 先退订再删本连接的 Connection 行：删行会向 owner 索引值频道 PUBLISH（带本用户的行没了），
    # 被顶号的 watcher 还挂着的话会收到它，主动核查读到行不存在就记一条假的"已被顶号"。
    # 退订等到 UNSUBSCRIBE ack 才返回，之后的 DEL 通知 Redis 不会再投给本进程
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
