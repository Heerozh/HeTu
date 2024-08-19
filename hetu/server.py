"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import asyncio
import importlib.util
import json
import logging
import os
import sys
import zlib

from redis.exceptions import ConnectionError as RedisConnectionError
from sanic import Blueprint
from sanic import Request, Websocket, text
from sanic import Sanic
from sanic import SanicException
from sanic.exceptions import WebsocketClosed

import hetu
import hetu.system.connection as connection
from hetu.data.backend import Subscriptions, Backend, HeadLockFailed
from hetu.logging.default import DEFAULT_LOGGING_CONFIG
from hetu.manager import ComponentTableManager
from hetu.system import SystemClusters, SystemExecutor, SystemCall, ResponseToClient

logger = logging.getLogger('HeTu.root')
replay = logging.getLogger('HeTu.replay')

hetu_bp = Blueprint("my_blueprint")
_ = zlib  # 标记使用，下方globals()['zlib']会使用


def decode_message(message: bytes, protocol: dict):
    if len(message) > 10240:
        raise ValueError("Message too long，为了防止性能攻击限制长度")
    if crypto := protocol['crypto']:
        message = crypto.decrypt(message)
    if compress := protocol['compress']:
        message = compress.decompress(message)
    return json.loads(message.decode())


def encode_message(message: list | dict, protocol: dict):
    message = json.dumps(message).encode()
    if compress := protocol['compress']:
        message = compress.compress(message)
    if crypto := protocol['crypto']:
        message = crypto.encrypt(message)
    return message


def check_length(name, data: list, left, right):
    if left > len(data) > right:
        raise ValueError(f"Invalid {name} message")


async def sys_call(data: list, executor: SystemExecutor, push_queue: asyncio.Queue):
    """处理Client SDK调用System的命令"""
    # print(executor.context, 'sys', data)
    check_length('sys', data, 2, 100)
    call = SystemCall(data[1], tuple(data[2:]))
    ok, res = await executor.execute(call)
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
        case _:
            raise ValueError(f" [非法操作] 未知订阅操作：{data[2]}")

    reply = ['sub', sub_id, data]
    await push_queue.put(reply)

    num_row_sub, num_idx_sub = subs.count()
    if num_row_sub > ctx.max_row_sub or num_idx_sub > ctx.max_index_sub:
        raise ValueError(f" [非法操作] 订阅数超过限制："
                         f"{num_row_sub}个行订阅，{num_idx_sub}个索引订阅")


@hetu_bp.route("/")
async def web_root(request):
    return text(f"Powered by HeTu(v{hetu.__version__}) Database! ")


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


@hetu_bp.websocket("/hetu")  # noqa
async def websocket_connection(request: Request, ws: Websocket):
    """ws连接处理器，运行在worker主协程下"""
    # 初始化执行器，一个连接一个执行器
    comp_mgr = request.app.ctx.comp_mgr
    executor = SystemExecutor(request.app.config['NAMESPACE'], comp_mgr)
    await executor.initialize(request.client_ip)
    ctx = executor.context
    logger.info(f"🔗 [📡WSConnect] 新连接：{asyncio.current_task().get_name()}")
    # 初始化订阅管理器，一个连接一个订阅管理器
    subscriptions = Subscriptions(request.app.ctx.default_backend)
    # 初始化push消息队列
    push_queue = asyncio.Queue(1024)
    # 初始化发送/接受计数器
    flood_checker = connection.ConnectionFloodChecker()

    # 传递默认配置参数到ctx
    default_limits = []  # [[10, 1], [27, 5], [100, 50], [300, 300]]
    ctx.configure(
        idle_timeout=request.app.config.get('SYSTEM_CALL_IDLE_TIMEOUT', 60 * 2),
        client_limits=request.app.config.get('CLIENT_SEND_LIMITS', default_limits),
        server_limits=request.app.config.get('SERVER_SEND_LIMITS', default_limits),
        max_row_sub=request.app.config.get('MAX_ROW_SUBSCRIPTION', 1000),
        max_index_sub=request.app.config.get('MAX_INDEX_SUBSCRIPTION', 50),
    )

    # 创建接受客户端消息的协程
    protocol = dict(compress=request.app.ctx.compress,
                    crypto=request.app.ctx.crypto)
    recv_task_id = f"client_receiver:{request.id}"
    receiver_task = client_receiver(
        ws, protocol, executor, subscriptions, push_queue, flood_checker)
    _ = request.app.add_task(receiver_task, name=recv_task_id)

    # 创建获得订阅推送通知的协程
    subs_task_id = f"subs_receiver:{request.id}"
    subscript_task = subscription_receiver(ws, subscriptions, push_queue)
    _ = request.app.add_task(subscript_task, name=subs_task_id)
    puller_task_id = f"mq_puller:{request.id}"
    puller_task = mq_puller(ws, subscriptions)
    _ = request.app.add_task(puller_task, name=puller_task_id)

    # 这里循环发送，保证总是第一时间Push
    try:
        while True:
            reply = await push_queue.get()
            replay.debug(">>> " + str(reply))
            # print(executor.context, 'got', reply)
            await ws.send(encode_message(reply, protocol))
            # 检查发送上限
            flood_checker.sent()
            if flood_checker.send_limit_reached(ctx, "Coroutines(Websocket.push)"):
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
        err_msg = f"❌ [📡WSSender] 发送数据异常：{e}"
        replay.info(err_msg)
        logger.exception(err_msg)
    finally:
        # 连接断开，强制关闭此协程时也会调用
        close_msg = f"⛓️ [📡WSConnect] 连接断开：{asyncio.current_task().get_name()}"
        replay.info(close_msg)
        logger.info(close_msg)
        await request.app.cancel_task(recv_task_id, raise_exception=False)
        await request.app.cancel_task(subs_task_id, raise_exception=False)
        await request.app.cancel_task(puller_task_id, raise_exception=False)
        await executor.terminate()
        await subscriptions.close()
        request.app.purge_tasks()


async def server_close(app):
    for attrib in dir(app.ctx):
        backend = app.ctx.__getattribute__(attrib)
        if isinstance(backend, Backend):
            logger.info(f"⌚ [📡Server] Closing backend {attrib}...")
            await backend.close()


def start_webserver(app_name, config, main_pid, head) -> Sanic:
    """config： dict或者py目录"""
    # 加载玩家的app文件
    if (app_file := config.get('APP_FILE', None)) is not None:
        spec = importlib.util.spec_from_file_location('HeTuApp', app_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules['HeTuApp'] = module
        spec.loader.exec_module(module)

    # 传递配置
    connection.MAX_ANONYMOUS_CONNECTION_BY_IP = config.get('MAX_ANONYMOUS_CONNECTION_BY_IP', 10)

    # 加载web服务器
    app = Sanic(app_name, log_config=config.get('LOGGING', DEFAULT_LOGGING_CONFIG))
    app.update_config(config)

    # 重定向logger，把sanic的重定向到hetu
    root_logger = logging.getLogger("sanic")
    root_logger.parent = logger
    if config['DEBUG']:
        logger.setLevel(logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)

    # 加载协议
    app.ctx.compress, app.ctx.crypto = None, None
    compress = config.get('PACKET_COMPRESSION_CLASS')
    crypto = config.get('PACKET_CRYPTOGRAPHY_CLASS')
    if compress is not None:
        if compress not in globals():
            raise ValueError(f"该压缩模块未在全局变量中找到：{compress}")
        app.ctx.compress = globals()[compress]
    if crypto is not None:
        if crypto not in globals():
            raise ValueError(f"该加密模块未在全局变量中找到：{crypto}")
        app.ctx.crypto = globals()[crypto]

    # 创建后端连接池
    backends = {}
    table_constructors = {}
    for name, db_cfg in app.config.BACKENDS.items():
        if db_cfg["type"] == "Redis":
            from .data.backend import RedisBackend, RedisComponentTable
            backend = RedisBackend(db_cfg)
            backend.configure()
            backends[name] = backend
            table_constructors['Redis'] = RedisComponentTable
            app.ctx.__setattr__(name, backend)
        elif db_cfg["type"] == "SQL":
            # import sqlalchemy
            # app.ctx.__setattr__(name, sqlalchemy.create_engine(db_cfg["addr"]))
            raise NotImplementedError(
                "SQL后端未实现，实现SQL后端还需要redis或zmq在前面一层负责推送，较复杂")
        # 把config第一个设置为default后端
        if 'default' not in backends:
            backends['default'] = backends[name]
            table_constructors['default'] = table_constructors[db_cfg["type"]]
            app.ctx.__setattr__('default_backend', backends['default'])

    # 初始化SystemCluster
    SystemClusters().build_clusters(config['NAMESPACE'])

    # 初始化所有ComponentTable
    comp_mgr = ComponentTableManager(
        config['NAMESPACE'], config['INSTANCE_NAME'], backends, table_constructors)
    app.ctx.__setattr__('comp_mgr', comp_mgr)
    # 主进程+Head启动时执行检查schema, 清空所有非持久化表
    try:
        # is_worker = os.environ.get('SANIC_WORKER_IDENTIFIER').startswith('Srv ')
        if head and os.getpid() == main_pid:
            logger.warning("⚠️ [📡Server] 启动为Head node，开始检查schema并清空非持久化表...")
            comp_mgr.create_or_migrate_all()
            comp_mgr.flush_volatile()
    except HeadLockFailed as e:
        message = (f"检测有其他head=True的node正在运行，只能启动一台head node。"
                   f"此标记位于{e}，如果之前服务器未正常关闭，请手动删除该键值")
        logger.exception("❌ [📡Server] " + message)
        raise HeadLockFailed(message)

    # 服务器work和main关闭回调
    app.after_server_stop(server_close)
    app.main_process_stop(server_close)

    # 启动服务器监听
    app.blueprint(hetu_bp)
    return app
