"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging

from sanic import Request, Websocket
from sanic.exceptions import WebsocketClosed

from ..data.sub import SubscriptionBroker
from ..endpoint import connection
from ..endpoint.executor import EndpointExecutor
from ..system.caller import SystemCaller
from ..system.context import SystemContext
from .pipeline import ServerMessagePipeline
from .receiver import client_handler, mq_puller, subscription_handler
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
    logger.info(f"🔗 [📡WSConnect] 新连接：{db_name}: {current_task.get_name()}")

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
        logger.info(f"New Connect Error: 错误的路径，实例名不存在: {instance}")
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
    )

    # 初始化System执行器，一个连接一个执行器
    namespace = request.app.config["NAMESPACE"]
    system_caller = SystemCaller(namespace, tbl_mgr, context)
    context.systems = system_caller

    # 初始化Endpoint执行器，一个连接一个执行器
    endpoint_executor = EndpointExecutor(namespace, tbl_mgr, context)
    await endpoint_executor.initialize(request.client_ip)

    # 初始化订阅管理器，一个连接一个订阅管理器
    broker = SubscriptionBroker(request.app.ctx.default_backend)

    # 初始化push消息队列
    push_queue = asyncio.Queue(1024)

    # 初始化发送/接受计数器
    flood_checker = connection.ConnectionFloodChecker()

    # 创建接受客户端消息的协程2
    recv_task_id = f"client_handler:{request.id}"
    receiver_task = client_handler(
        ws,
        pipe_ctx,
        endpoint_executor,
        broker,
        push_queue,
        flood_checker,
        int(request.app.config.get("DEBUG", 0)),
    )
    _ = request.app.add_task(receiver_task, name=recv_task_id)

    # 创建获得订阅推送通知的协程3,4,还有内部pubsub协程5
    subs_task_id = f"subs_receiver:{request.id}"
    subscript_task = subscription_handler(ws, broker, push_queue)
    _ = request.app.add_task(subscript_task, name=subs_task_id)
    puller_task_id = f"mq_puller:{request.id}"
    puller_task = mq_puller(ws, broker)
    _ = request.app.add_task(puller_task, name=puller_task_id)

    # 删除当前长连接用不上的临时变量
    del namespace
    del default_limits

    # 这里循环发送，保证总是第一时间Push
    try:
        while True:
            reply = await push_queue.get()
            # 如果关闭了replay，为了速度不执行下面的字符串序列化
            if replay.level < logging.ERROR:
                replay.debug(">>> " + str(reply))
            # print(executor.context, 'got', reply)
            await ws.send(msg_pipe.encode(pipe_ctx, reply))
            # 检查发送上限
            flood_checker.sent()
            if flood_checker.send_limit_reached(context, "Coroutines(Websocket.push)"):
                ws.fail_connection()
                break
    except asyncio.CancelledError:
        if ws.ws_proto.parser_exc:
            err_msg = f"❌ [📡WSSender] WS协议异常：{ws.ws_proto.parser_exc}"
            replay.info(err_msg)
            logger.exception(err_msg, exc_info=ws.ws_proto.parser_exc)
        # print(executor.context, 'websocket_connection normal canceled', ws.ws_proto.parser_exc)
    except WebsocketClosed:
        pass
    except BaseException as e:
        err_msg = f"❌ [📡WSSender] 发送数据异常：{type(e).__name__}:{e}"
        replay.info(err_msg)
        logger.exception(err_msg)
    finally:
        # 连接断开，强制关闭此协程时也会调用
        close_msg = f"⛓️ [📡WSConnect] 连接断开：{current_task.get_name()}"
        replay.info(close_msg)
        logger.info(close_msg)
        await request.app.cancel_task(recv_task_id, raise_exception=False)
        await request.app.cancel_task(subs_task_id, raise_exception=False)
        await request.app.cancel_task(puller_task_id, raise_exception=False)
        try:
            system_caller.call_check(DISCONNECT_SYSTEM)
        except ValueError:
            pass
        else:
            try:
                await system_caller.call(DISCONNECT_SYSTEM)
            except BaseException as e:
                err_msg = (
                    f"❌ [📡WSDisconnectHook] 断线System调用异常: "
                    f"{DISCONNECT_SYSTEM} | {type(e).__name__}:{e}"
                )
                replay.info(err_msg)
                logger.exception(err_msg)
        await endpoint_executor.terminate()
        await broker.close()
        request.app.purge_tasks()
