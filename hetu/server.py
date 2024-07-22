"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import asyncio
import importlib.util
import json
import os
import sys
import traceback
import zlib

from sanic import Blueprint
from sanic import Request, Websocket, text
from sanic import Sanic
from sanic import SanicException
from sanic.log import logger

import hetu
from hetu.data.backend import Subscriptions, Backend, HeadLockFailed
from hetu.manager import ComponentTableManager
from hetu.system import SystemClusters, SystemExecutor, SystemCall, SystemResponse

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
    print(executor.context, 'sys', data)
    check_length('sys', data, 2, 100)
    call = SystemCall(data[1], tuple(data[2:]))
    ok, res = await executor.execute(call)
    if ok and isinstance(res, SystemResponse):
        await push_queue.put(res.message)
    return ok


async def sub_call(data: list, executor: SystemExecutor, subs: Subscriptions,
                   push_queue: asyncio.Queue):
    """处理Client SDK调用订阅的命令"""
    print(executor.context, 'sub', data)
    check_length('sub', data, 4, 100)
    table = executor.comp_mgr.get_table(data[1])
    if table is None:
        raise ValueError(f"subscribe了不存在的Component名，注意大小写：{data[1]}")

    if executor.context.group and executor.context.group.startswith("admin"):
        caller = 'admin'
    else:
        caller = executor.context.caller

    match data[2]:
        case 'select':
            check_length('select', data, 5, 5)
            sub_id, data = await subs.subscribe_select(table, caller, *data[3:])
        case 'query':
            check_length('query', data, 5, 8)
            sub_id, data = await subs.subscribe_query(table, caller, *data[3:])
        case _:
            raise ValueError(f"Invalid sub message")
    if sub_id is not None:
        reply = ['sub', sub_id, data]
        await push_queue.put(reply)


@hetu_bp.route("/")
async def web_root(request):
    return text(f"Powered by HeTu(v{hetu.__version__}) Database! ")


async def client_receiver(
        ws: Websocket, protocol: dict,
        executor: SystemExecutor, subs: Subscriptions,
        push_queue: asyncio.Queue
):
    """ws接受消息循环，是一个asyncio的task，由loop.call_soon方法添加到worker主协程的执行队列"""
    last_data = None
    try:
        async for message in ws:
            if not message:
                break
            # 转换消息到array
            last_data = decode_message(message, protocol)
            # print('recv', last_data)
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
                    print('motd')
                    await ws.send(f"👋 Welcome to HeTu Database! v{hetu.__version__}")
                case _:
                    raise ValueError(f"Invalid message")
    except asyncio.CancelledError:
        print(executor.context, 'client_receiver normal canceled')
    except (SanicException, BaseException) as e:
        logger.exception(f"❌ [📡Websocket] 执行异常，连接{executor.context}，"
                         f"封包：{last_data}，异常：{e}")
        ws.fail_connection()
    finally:
        print(executor.context, 'client_receiver closed')


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
            # todo 备注，客户端要注意内部避免掉重复订阅
            # 客户端通过查询参数组合成查询字符串，来判断是否重复订阅，管理器注册对应的callback，重复注册只
            # 是callback增加并不会去服务器请求
    except asyncio.CancelledError:
        print('subscription_receiver normal canceled')
    except BaseException as e:
        logger.exception(f"❌ [📡Websocket] 数据库获取订阅消息时异常，上条消息：{last_updates}，异常：{e}")
        return ws.fail_connection()
    finally:
        print('subscription_receiver closed')


@hetu_bp.websocket("/hetu")
async def websocket_connection(request: Request, ws: Websocket):
    """ws连接处理器，运行在worker主协程下"""
    # 初始化执行器，一个连接一个执行器
    comp_mgr = request.app.ctx.comp_mgr
    executor = SystemExecutor(request.app.config['NAMESPACE'], comp_mgr)
    await executor.initialize(request.client_ip)
    # 初始化订阅管理器，一个连接一个订阅管理器
    subscriptions = Subscriptions(request.app.ctx.default_backend)
    # 初始化push消息队列
    push_queue = asyncio.Queue(128)

    # 创建接受客户端消息的协程
    protocol = dict(compress=request.app.ctx.compress,
                    crypto=request.app.ctx.crypto)
    recv_task_id = f"client_receiver:{request.id}"
    receiver_task = client_receiver(ws, protocol, executor, subscriptions, push_queue)
    _ = request.app.add_task(receiver_task, name=recv_task_id)

    # 创建获得订阅推送通知的协程
    subs_task_id = f"subs_receiver:{request.id}"
    subscript_task = subscription_receiver(ws, subscriptions, push_queue)
    _ = request.app.add_task(subscript_task, name=subs_task_id)

    # 这里循环发送，保证总是第一时间Push
    try:
        while True:
            reply = await push_queue.get()
            # todo 增加replay log file，把recv和send的消息都记录，以及事务执行的结果等
            print(executor.context, 'got', reply)
            await ws.send(encode_message(reply, protocol))
    except asyncio.CancelledError:
        print(executor.context, 'websocket_connection normal canceled')
    except BaseException as e:
        logger.exception(f"❌ [📡Websocket] 发送数据异常：{e}")
    finally:
        # 连接断开，强制关闭此协程时也会调用
        print(executor.context, asyncio.current_task().get_name(), 'closed')
        await request.app.cancel_task(recv_task_id)
        await request.app.cancel_task(subs_task_id)
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

    # 重定向logger
    import logging
    hetu_logger = logging.getLogger('HeTu')
    hetu_logger.parent = logger

    # 加载web服务器
    app = Sanic(app_name)
    app.update_config(config)

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
            backends['Redis'] = backend
            table_constructors['Redis'] = RedisComponentTable
            app.ctx.__setattr__(name, backend)
        elif db_cfg["type"] == "SQL":
            # import sqlalchemy
            # app.ctx.__setattr__(name, sqlalchemy.create_engine(db_cfg["addr"]))
            raise NotImplementedError(
                "SQL后端未实现，实现SQL后端还需要redis或zmq在前面一层负责推送，较复杂")
    # 把config第一个设置为default后端
    backends['default'] = backends[next(iter(app.config.BACKENDS.keys()))]
    table_constructors['default'] = table_constructors[next(iter(app.config.BACKENDS.keys()))]
    app.ctx.__setattr__('default_backend', backends['default'])

    # 初始化SystemCluster
    SystemClusters().build_clusters(config['NAMESPACE'])

    # 初始化所有ComponentTable
    comp_mgr = ComponentTableManager(
        config['NAMESPACE'], config['INSTANCE_NAME'], backends, table_constructors)
    app.ctx.__setattr__('comp_mgr', comp_mgr)
    # 主进程+Head启动时执行检查schema, 清空所有非持久化表
    try:
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
