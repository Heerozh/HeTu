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

from ..data.backend import Subscriptions
from ..endpoint import connection
from ..endpoint.executor import EndpointExecutor
from ..system.context import SystemContext
from ..system.caller import SystemCaller
from .message import encode_message
from .receiver import client_receiver, mq_puller, subscription_receiver
from .web import HETU_BLUEPRINT

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


@HETU_BLUEPRINT.websocket("/hetu")  # noqa
async def websocket_connection(request: Request, ws: Websocket):
    """ws连接处理器，运行在worker主协程下"""
    # 获取当前协程任务, 自身算是一个协程1
    current_task = asyncio.current_task()
    assert current_task, "Must be called in an asyncio task"
    logger.info(f"🔗 [📡WSConnect] 新连接：{current_task.get_name()}")
    comp_mgr = request.app.ctx.comp_mgr

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
    system_caller = SystemCaller(namespace, comp_mgr, context)
    context.systems = system_caller

    # 初始化Endpoint执行器，一个连接一个执行器
    endpoint_executor = EndpointExecutor(namespace, comp_mgr, context)
    await endpoint_executor.initialize(request.client_ip)

    # 初始化订阅管理器，一个连接一个订阅管理器
    subscriptions = Subscriptions(request.app.ctx.default_backend)

    # 初始化push消息队列
    push_queue = asyncio.Queue(1024)

    # 初始化发送/接受计数器
    flood_checker = connection.ConnectionFloodChecker()

    # 创建接受客户端消息的协程2
    protocol = dict(compress=request.app.ctx.compress, crypto=request.app.ctx.crypto)
    recv_task_id = f"client_receiver:{request.id}"
    receiver_task = client_receiver(
        ws, protocol, endpoint_executor, subscriptions, push_queue, flood_checker
    )
    _ = request.app.add_task(receiver_task, name=recv_task_id)

    # 创建获得订阅推送通知的协程3,4,还有内部pubsub协程5
    subs_task_id = f"subs_receiver:{request.id}"
    subscript_task = subscription_receiver(ws, subscriptions, push_queue)
    _ = request.app.add_task(subscript_task, name=subs_task_id)
    puller_task_id = f"mq_puller:{request.id}"
    puller_task = mq_puller(ws, subscriptions)
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
            await ws.send(encode_message(reply, protocol))
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
        await endpoint_executor.terminate()
        await subscriptions.close()
        request.app.purge_tasks()
