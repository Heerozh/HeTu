"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from redis.exceptions import ConnectionError as RedisConnectionError
from sanic import SanicException
from sanic.exceptions import WebsocketClosed

import hetu  # for obtaining __version__

from ..endpoint import connection
from ..endpoint.response import ResponseToClient
from .pipeline.pipeline import PipeContext, ServerMessagePipeline

if TYPE_CHECKING:
    from sanic import Websocket

    from ..data.sub import SubscriptionBroker
    from ..endpoint.executor import EndpointExecutor

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


def check_length(name, data: list, left, right):
    if left > len(data) > right:
        raise ValueError(f"Invalid {name} message")


async def rpc(data: list, executor: EndpointExecutor, push_queue: asyncio.Queue):
    """处理Client SDK调用Endpoint的命令"""
    # print(executor.context, 'rpc', data)
    check_length("rpc", data, 2, 100)
    ok, res = await executor.execute(data[1], *data[2:])
    # 如果关闭了replay，为了速度，不执行下面的字符串序列化
    if replay.level < logging.ERROR:
        replay.info(f"[EndpointResult][{data[1]}]({ok}, {str(res)})")

    if not ok:
        # 关闭连接
        return False

    if isinstance(res, ResponseToClient):
        await push_queue.put(["rsp", res.message])
    else:
        # 无视返回值，直接返回ok，如果不返回，Request无法对应
        await push_queue.put(["rsp", "ok"])
    return True


async def sub_call(
    data: list,
    executor: EndpointExecutor,
    broker: SubscriptionBroker,
    push_queue: asyncio.Queue,
):
    """处理Client SDK调用订阅的命令"""
    ctx = executor.context
    # print(executor.context, 'sub', data)
    check_length("sub", data, 4, 100)
    table = executor.tbl_mgr.get_table(data[1])
    if table is None:
        raise ValueError(
            f" [非法操作] subscribe了不存在的Component名，注意大小写：{data[1]}"
        )

    sub_id = None
    sub_data: dict[str, Any] | list[dict] | None = None
    match data[2]:
        case "get":
            check_length("get", data, 5, 5)
            sub_id, sub_data = await broker.subscribe_get(table, ctx, *data[3:])
        case "range":
            check_length("range", data, 5, 8)
            sub_id, sub_data = await broker.subscribe_range(table, ctx, *data[3:])
        case "logic_query":
            # todo 逻辑订阅，query后再通过脚本进行二次筛选，再发送到客户端，更新时也会调用筛选代码
            pass
        case _:
            raise ValueError(f" [非法操作] 未知订阅操作：{data[2]}")

    reply = ["sub", sub_id, sub_data]
    await push_queue.put(reply)

    num_row_sub, num_idx_sub = broker.count()
    if num_row_sub > ctx.max_row_sub or num_idx_sub > ctx.max_index_sub:
        raise ValueError(
            f" [非法操作] 订阅数超过限制："
            f"{num_row_sub}个行订阅，{num_idx_sub}个索引订阅"
        )


async def client_handler(
    ws: Websocket,
    pipe_ctx: PipeContext,
    executor: EndpointExecutor,
    broker: SubscriptionBroker,
    push_queue: asyncio.Queue,
    flood_checker: connection.ConnectionFloodChecker,
):
    """ws接受消息循环，是一个asyncio的task，由loop.call_soon方法添加到worker主协程的执行队列"""
    pipe = ServerMessagePipeline()
    ctx = executor.context
    last_data = None
    try:
        async for message in ws:
            if not message:
                break
            if type(message) is not bytes:
                break  # if not byte frame, close connection
            # 转换消息到array
            last_data = pipe.decode(pipe_ctx, message)
            if type(last_data) is not list:
                raise ValueError("Invalid message format")
            # 如果关闭了replay，为了速度，不执行下面的字符串序列化
            if replay.level < logging.ERROR:
                replay.debug("<<< " + str(last_data))
            # 检查接受上限
            flood_checker.received()
            if flood_checker.recv_limit_reached(
                ctx, "Coroutines(Websocket.client_handler)"
            ):
                return ws.fail_connection()
            # 执行消息
            match last_data[0]:
                case "rpc":  # rpc endpoint_name args ...
                    rpc_ok = await rpc(last_data, executor, push_queue)
                    if not rpc_ok:
                        print(executor.context, "call failed, close connection...")
                        return ws.fail_connection()
                case "sub":  # sub component_name get/range args ...
                    await sub_call(last_data, executor, broker, push_queue)
                case "unsub":  # unsub sub_id
                    check_length("unsub", last_data, 2, 2)
                    await broker.unsubscribe(last_data[1])
                case "motd":
                    await ws.send(f"👋 Welcome to HeTu Database! v{hetu.__version__}")
                case _:
                    raise ValueError(f" [非法操作] 未知消息类型：{last_data[0]}")
    except asyncio.CancelledError:
        # print(ctx, 'client_handler normal canceled')
        pass
    except WebsocketClosed:
        pass
    except RedisConnectionError as e:
        err_msg = (
            f"❌ [📡WSReceiver] Redis ConnectionError，断开连接: {type(e).__name__}:{e}"
        )
        replay.info(err_msg)
        logger.error(err_msg)
        return ws.fail_connection()
    except (SanicException, BaseException) as e:
        err_msg = (
            f"❌ [📡WSReceiver] 执行异常，封包：{last_data}，"
            f"异常：{type(e).__name__}:{e}"
        )
        replay.info(err_msg)
        logger.exception(err_msg)
        ws.fail_connection()
    finally:
        # print(ctx, 'client_handler closed')
        pass


async def mq_puller(ws: Websocket, broker: SubscriptionBroker):
    """消息队列拉取器，需要持续拉取，防止消息在队列服务器中积压"""
    try:
        while True:
            await broker.mq_pull()
    except asyncio.CancelledError:
        pass
    except RedisConnectionError as e:
        logger.error(
            f"❌ [📡WSMQPuller] Redis ConnectionError，断开连接: "
            f"{type(e).__name__}:{e}"
            f"网络故障外的可能原因：连接来不及接受pubsub消息，积攒过多断开。"
        )
        return ws.fail_connection()
    except BaseException as e:
        logger.exception(
            f"❌ [📡WSMQPuller] 数据库Pull MQ消息时异常，异常：{type(e).__name__}:{e}"
        )
        return ws.fail_connection()
    finally:
        pass


async def subscription_handler(
    ws: Websocket, broker: SubscriptionBroker, push_queue: asyncio.Queue
):
    """订阅消息获取循环，是一个asyncio的task，由loop.call_soon方法添加到worker主协程的执行队列"""
    last_updates = {}
    try:
        while True:
            last_updates = await broker.get_updates()
            for sub_id, data in last_updates.items():
                reply = ["updt", sub_id, data]
                await push_queue.put(reply)
    except asyncio.CancelledError:
        # print('subscription_handler normal canceled')
        pass
    except RedisConnectionError as e:
        logger.error(
            f"❌ [📡WSSubscription] Redis ConnectionError，断开连接: {type(e).__name__}:{e}"
            f"上次接受了：{len(last_updates)}条消息。"
        )
        return ws.fail_connection()
    except BaseException as e:
        logger.exception(
            f"❌ [📡WSSubscription] 数据库获取订阅消息时异常，"
            f"上条消息：{last_updates}，异常：{type(e).__name__}:{e}"
        )
        return ws.fail_connection()
    finally:
        # print('subscription_handler closed')
        pass
