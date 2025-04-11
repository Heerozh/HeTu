"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging

from redis.exceptions import ConnectionError as RedisConnectionError
from sanic import SanicException
from sanic import Websocket
from sanic.exceptions import WebsocketClosed

import hetu
import hetu.system.connection as connection
from hetu.data.backend import Subscriptions
from hetu.system import SystemExecutor, SystemCall, ResponseToClient
from .message import decode_message

logger = logging.getLogger('HeTu.root')
replay = logging.getLogger('HeTu.replay')


def check_length(name, data: list, left, right):
    if left > len(data) > right:
        raise ValueError(f"Invalid {name} message")


async def sys_call(data: list, executor: SystemExecutor, push_queue: asyncio.Queue):
    """处理Client SDK调用System的命令"""
    # print(executor.context, 'sys', data)
    check_length('sys', data, 2, 100)
    call = SystemCall(data[1], tuple(data[2:]))
    ok, res = await executor.execute(call)
    if replay.level < logging.ERROR:  # 如果关闭了replay，为了速度不执行下面的字符串序列化
        replay.info(f"[SystemResult][{data[1]}]({ok}, {str(res)})")
    if ok and isinstance(res, ResponseToClient):
        await push_queue.put(['rsp', res.message])
    return ok


async def sub_call(data: list, executor: SystemExecutor, subs: Subscriptions,
                   push_queue: asyncio.Queue):
    """处理Client SDK调用订阅的命令"""
    ctx = executor.context
    # print(executor.context, 'sub', data)
    check_length('sub', data, 4, 100)
    table = executor.comp_mgr.get_table(data[1])
    if table is None:
        raise ValueError(f" [非法操作] subscribe了不存在的Component名，注意大小写：{data[1]}")

    if ctx.group and ctx.group.startswith("admin"):
        caller = 'admin'
    else:
        caller = ctx.caller

    match data[2]:
        case 'select':
            check_length('select', data, 5, 5)
            sub_id, data = await subs.subscribe_select(table, caller, *data[3:])
        case 'query':
            check_length('query', data, 5, 8)
            sub_id, data = await subs.subscribe_query(table, caller, *data[3:])
        case 'logic_query':
            # todo 逻辑订阅，query后再通过脚本进行二次筛选，再发送到客户端，更新时也会调用筛选代码
            pass
        case _:
            raise ValueError(f" [非法操作] 未知订阅操作：{data[2]}")

    reply = ['sub', sub_id, data]
    await push_queue.put(reply)

    num_row_sub, num_idx_sub = subs.count()
    if num_row_sub > ctx.max_row_sub or num_idx_sub > ctx.max_index_sub:
        raise ValueError(f" [非法操作] 订阅数超过限制："
                         f"{num_row_sub}个行订阅，{num_idx_sub}个索引订阅")


async def client_receiver(
        ws: Websocket, protocol: dict,
        executor: SystemExecutor,
        subs: Subscriptions,
        push_queue: asyncio.Queue,
        flood_checker: connection.ConnectionFloodChecker
):
    """ws接受消息循环，是一个asyncio的task，由loop.call_soon方法添加到worker主协程的执行队列"""
    ctx = executor.context
    last_data = None
    try:
        async for message in ws:
            if not message:
                break
            # 转换消息到array
            last_data = decode_message(message, protocol)
            if replay.level < logging.ERROR:  # 如果关闭了replay，为了速度不执行下面的字符串序列化
                replay.debug("<<< " + str(last_data))
            # 检查接受上限
            flood_checker.received()
            if flood_checker.recv_limit_reached(ctx, "Coroutines(Websocket.client_receiver)"):
                return ws.fail_connection()
            # 执行消息
            match last_data[0]:
                case 'sys':  # sys system_name args ...
                    sys_ok = await sys_call(last_data, executor, push_queue)
                    if not sys_ok:
                        print(executor.context, 'call failed, close connection...')
                        return ws.fail_connection()
                case 'sub':  # sub component_name select/query args ...
                    await sub_call(last_data, executor, subs, push_queue)
                case 'unsub':  # unsub sub_id
                    check_length('unsub', last_data, 2, 2)
                    await subs.unsubscribe(last_data[1])
                case 'motd':
                    await ws.send(f"👋 Welcome to HeTu Database! v{hetu.__version__}")
                case _:
                    raise ValueError(f" [非法操作] 未知消息类型：{last_data[0]}")
    except asyncio.CancelledError:
        # print(ctx, 'client_receiver normal canceled')
        pass
    except WebsocketClosed:
        pass
    except RedisConnectionError as e:
        err_msg = f"❌ [📡WSReceiver] Redis ConnectionError，断开连接: {e}"
        replay.info(err_msg)
        logger.error(err_msg)
        return ws.fail_connection()
    except (SanicException, BaseException) as e:
        err_msg = f"❌ [📡WSReceiver] 执行异常，封包：{last_data}，异常：{e}"
        replay.info(err_msg)
        logger.exception(err_msg)
        ws.fail_connection()
    finally:
        # print(ctx, 'client_receiver closed')
        pass


async def mq_puller(ws: Websocket, subscriptions: Subscriptions):
    """消息队列拉取器，需要持续拉取，防止消息在队列服务器中积压"""
    try:
        while True:
            await subscriptions.mq_pull()
    except asyncio.CancelledError:
        pass
    except RedisConnectionError as e:
        logger.error(f"❌ [📡WSMQPuller] Redis ConnectionError，断开连接: {e}"
                     f"网络故障外的可能原因：连接来不及接受pubsub消息，积攒过多断开。")
        return ws.fail_connection()
    except BaseException as e:
        logger.exception(f"❌ [📡WSMQPuller] 数据库Pull MQ消息时异常，异常：{e}")
        return ws.fail_connection()
    finally:
        pass


async def subscription_receiver(
        ws: Websocket,
        subscriptions: Subscriptions,
        push_queue: asyncio.Queue
):
    """订阅消息获取循环，是一个asyncio的task，由loop.call_soon方法添加到worker主协程的执行队列"""
    last_updates = None
    try:
        while True:
            last_updates = await subscriptions.get_updates()
            for sub_id, data in last_updates.items():
                reply = ['updt', sub_id, data]
                await push_queue.put(reply)
    except asyncio.CancelledError:
        # print('subscription_receiver normal canceled')
        pass
    except RedisConnectionError as e:
        logger.error(f"❌ [📡WSSubscription] Redis ConnectionError，断开连接: {e}"
                     f"上次接受了：{len(last_updates)}条消息。")
        return ws.fail_connection()
    except BaseException as e:
        logger.exception(f"❌ [📡WSSubscription] 数据库获取订阅消息时异常，"
                         f"上条消息：{last_updates}，异常：{e}")
        return ws.fail_connection()
    finally:
        # print('subscription_receiver closed')
        pass
