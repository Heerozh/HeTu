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
from ..endpoint.response import RejectResponse, ResponseToClient
from ..i18n import _
from .pipeline.pipeline import PipeContext, ServerMessagePipeline

if TYPE_CHECKING:
    from sanic import Websocket

    from ..data.sub import SubscriptionBroker
    from ..endpoint.executor import EndpointExecutor

logger = logging.getLogger("HeTu.root")
replay = logging.getLogger("HeTu.replay")


# 塞进 push_queue 用来叫醒外层 websocket_connection 的发送循环。接收协程一旦结束，
# 外层还会永远阻塞在 push_queue.get() 上，靠它退出去跑 finally 里的清理。
PUSH_CLOSE = object()

BAD_SUBS = [
    "sub",
    "fail",
    [{"id": 0, "info": "DEBUG MODE: server rejected the request."}],
]


def check_length(name, data: list, left, right):
    if left > len(data) > right:
        raise ValueError(f"Invalid {name} message")


async def rpc(
    data: list,
    executor: EndpointExecutor,
    push_queue: asyncio.Queue,
    debug: int = 0,
):
    """处理Client SDK调用Endpoint的命令"""
    # print(executor.context, 'rpc', data)
    check_length("rpc", data, 2, 100)
    ok, res = await executor.execute(data[1], *data[2:])
    # 如果关闭了replay，为了速度，不执行下面的字符串序列化
    if replay.level < logging.ERROR:
        replay.info(f"[EndpointResult][{data[1]}]({ok}, {str(res)})")

    if not ok:
        # 执行失败/非法调用。debug 模式下发独立的 err 帧（区别于成功的 rsp 信封），
        # 把失败原因回传客户端 SDK，便于开发期定位且不污染 ToDict 等反序列化；连接保持。
        # release 模式下直接关闭连接，不向客户端泄露任何原因。
        if debug:
            reason = res if isinstance(res, str) else "server rejected the request."
            await push_queue.put(["err", data[1], reason])
            return True
        return False

    if isinstance(res, RejectResponse):
        # 软拒绝：发 rej 帧，连接保持
        await push_queue.put(["rej", data[1], res.code])
    elif isinstance(res, ResponseToClient):
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
) -> bool:
    """处理Client SDK调用订阅的命令"""
    ctx = executor.context
    # print(executor.context, 'sub', data)
    check_length("sub", data, 3, 100)
    table = executor.tbl_mgr.get_table(data[1])
    if table is None:
        err_msg = _(
            " [非法操作] subscribe了不存在的Component名，注意大小写：{name}"
        ).format(name=data[1])
        replay.info(err_msg)
        logger.warning(err_msg)
        return False

    sub_id = None
    sub_data: dict[str, Any] | list[dict] | None = None
    match data[2]:
        case "get":
            check_length("get", data, 5, 5)
            sub_id, sub_data = await broker.subscribe_get(table, ctx, *data[3:])
        case "range":
            check_length("range", data, 5, 8)
            sub_id, sub_data = await broker.subscribe_range(table, ctx, *data[3:])
        case "table":  # sub component_name table
            check_length("table", data, 3, 3)
            sub_id, sub_data = await broker.subscribe_table(table, ctx)
        case "logic_query":
            # todo 逻辑订阅，query后再通过脚本进行二次筛选，再发送到客户端，更新时也会调用筛选代码
            pass
        case _:
            raise ValueError(_(" [非法操作] 未知订阅操作：{op}").format(op=data[2]))

    reply = ["sub", sub_id, sub_data]
    await push_queue.put(reply)

    num_row_sub, num_idx_sub, num_tbl_sub = broker.count()
    if (
        num_row_sub > ctx.max_row_sub
        or num_idx_sub > ctx.max_index_sub
        or num_tbl_sub > ctx.max_table_sub
    ):
        err_msg = _(
            " [非法操作] 订阅数超过限制：{num_row_sub}个行订阅，{num_idx_sub}个索引订阅，"
            "{num_tbl_sub}个整表订阅"
        ).format(
            num_row_sub=num_row_sub, num_idx_sub=num_idx_sub, num_tbl_sub=num_tbl_sub
        )
        replay.info(err_msg)
        logger.warning(err_msg)
        return False
    return True


async def client_handler(
    ws: Websocket,
    pipe_ctx: PipeContext,
    executor: EndpointExecutor,
    broker: SubscriptionBroker,
    push_queue: asyncio.Queue,
    flood_checker: connection.ConnectionFloodChecker,
    debug: int,
):
    """ws接受消息循环，是一个asyncio的task，由loop.call_soon方法添加到worker主协程的执行队列"""
    pipe = ServerMessagePipeline()
    ctx = executor.context
    last_data = None
    cancelled = False
    # async for 正常跑完 = 对端把连接关了；其余出口在各自分支里改写此原因
    exit_reason = _("对端关闭了连接")
    try:
        async for message in ws:
            if not message:
                exit_reason = _("收到空帧")
                break
            if type(message) is not bytes:
                exit_reason = _("收到非二进制帧：{kind}").format(
                    kind=type(message).__name__
                )
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
                exit_reason = _("超出接收频率上限")
                return ws.fail_connection()
            # 执行消息
            match last_data[0]:
                case "rpc":  # rpc endpoint_name args ...
                    # rpc() 内部按 debug 决定失败时发 err 帧（保持连接）还是关连接
                    if not await rpc(last_data, executor, push_queue, debug):
                        exit_reason = _("rpc 调用失败")
                        return ws.fail_connection()
                case "sub":  # sub component_name get/range args ...
                    sub_ok = await sub_call(last_data, executor, broker, push_queue)
                    if not sub_ok:
                        if debug:
                            await push_queue.put(BAD_SUBS)
                        else:
                            exit_reason = _("订阅请求非法")
                            return ws.fail_connection()
                case "unsub":  # unsub sub_id
                    check_length("unsub", last_data, 2, 2)
                    await broker.unsubscribe(last_data[1])
                case "motd":
                    await ws.send(f"👋 Welcome to HeTu Database! v{hetu.__version__}")
                case _:
                    raise ValueError(
                        _(" [非法操作] 未知消息类型：{msg_type}").format(
                            msg_type=last_data[0]
                        )
                    )
    except asyncio.CancelledError:
        # print(ctx, 'client_handler normal canceled')
        # 唯一合法的静默出口：连接本来就在被 websocket_connection 拆，别再插手
        cancelled = True
    except WebsocketClosed:
        exit_reason = _("WebSocket 已关闭")
    except RedisConnectionError as e:
        exit_reason = _("Redis ConnectionError")
        err_msg = _("❌ [📡WSReceiver] Redis ConnectionError，断开连接: {err}").format(
            err=f"{type(e).__name__}:{e}"
        )
        replay.info(err_msg)
        logger.error(err_msg)
        return ws.fail_connection()
    except (SanicException, BaseException) as e:
        exit_reason = f"{type(e).__name__}:{e}"
        err_msg = _("❌ [📡WSReceiver] 执行异常，封包：{data}，异常：{err}").format(
            data=last_data, err=f"{type(e).__name__}:{e}"
        )
        replay.info(err_msg)
        logger.exception(err_msg)
        return ws.fail_connection()
    finally:
        # 除了被取消（连接本来就在拆），任何退出路径都必须把连接一起拆掉并留下日志。
        # 否则连接会变成"半死"：外层 websocket_connection 还阻塞在 push_queue.get()
        # 上，谁也不知道接收协程没了 —— TCP 仍 ESTABLISHED、协议层 ping/pong 照常、
        # 客户端 socket 还是 Open，但客户端之后发的每一帧都没人消费，请求永远等不到
        # 回应，而且两端一行错误日志都没有（实测：卡在 WatchRange 上无限等待）。
        if not cancelled:
            close_msg = _("⛓️ [📡WSReceiver] 接收协程结束，关闭连接：{reason}").format(
                reason=exit_reason
            )
            replay.info(close_msg)
            logger.info(close_msg)
            ws.fail_connection()
            try:
                push_queue.put_nowait(PUSH_CLOSE)
            except asyncio.QueueFull:
                pass  # 队列满时外层自然会被 Sanic 的连接关闭取消掉


async def mq_puller(ws: Websocket, broker: SubscriptionBroker):
    """消息队列拉取器，需要持续拉取，防止消息在队列服务器中积压"""
    try:
        while True:
            await broker.mq_pull()
    except asyncio.CancelledError:
        pass
    except RedisConnectionError as e:
        logger.error(
            _(
                "❌ [📡WSMQPuller] Redis ConnectionError，断开连接: {err}"
                "网络故障外的可能原因：连接来不及接受pubsub消息，积攒过多断开。"
            ).format(err=f"{type(e).__name__}:{e}")
        )
        return ws.fail_connection()
    except BaseException as e:
        logger.exception(
            _("❌ [📡WSMQPuller] 数据库Pull MQ消息时异常，异常：{err}").format(
                err=f"{type(e).__name__}:{e}"
            )
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
            _(
                "❌ [📡WSSubscription] Redis ConnectionError，断开连接: {err}"
                "上次接受了：{count}条消息。"
            ).format(err=f"{type(e).__name__}:{e}", count=len(last_updates))
        )
        return ws.fail_connection()
    except BaseException as e:
        logger.exception(
            _(
                "❌ [📡WSSubscription] 数据库获取订阅消息时异常，"
                "上条消息：{updates}，异常：{err}"
            ).format(updates=last_updates, err=f"{type(e).__name__}:{e}")
        )
        return ws.fail_connection()
    finally:
        # print('subscription_handler closed')
        pass
