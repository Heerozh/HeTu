"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""
import json
import zlib
from sanic import Sanic
from sanic import Request, Websocket, text
from sanic import SanicException
from sanic import Blueprint
from sanic.log import logger
import asyncio
import importlib.util
import sys
from hetu.data.backend import Subscriptions
from hetu.system import SystemClusters, SystemExecutor, SystemCall, SystemResponse
from hetu.manager import ComponentTableManager

hetu_bp = Blueprint("my_blueprint")


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


async def sys_call(data: list, protocol: dict, executor: SystemExecutor, push_queue: asyncio.Queue):
    """处理Client SDK调用System的命令"""
    check_length('sys', data, 2, 100)
    call = SystemCall(data[1], tuple(data[2:]))
    ok, res = await executor.run(call)  # 非ok就是无权限调用，参数不对，重试超出
    if ok and isinstance(res, SystemResponse):
        await push_queue.put(encode_message(res.message, protocol))


async def sub_call(data: list, protocol: dict, subs: Subscriptions, push_queue: asyncio.Queue):
    """处理Client SDK调用订阅的命令"""
    check_length('sub', data, 4, 100)
    table = ComponentTableManager().get_table(data[1])
    match data[2]:
        case 'select':
            check_length('select', data, 5, 5)
            sub_id, data = await subs.subscribe_select(table, *data[3:])
        case 'query':
            check_length('query', data, 5, 8)
            sub_id, data = await subs.subscribe_query(table, *data[3:])
        case _:
            raise ValueError(f"Invalid sub message")
    if sub_id is not None:
        reply = ['sub', sub_id, data]
        await push_queue.put(encode_message(reply, protocol))


@hetu_bp.route("/")
async def web_root(request):
    import hetu
    return text(f"Powered by HeTu(v{hetu.__version__}) Logic Database! ")


async def client_receiver(
        ws: Websocket, protocol: dict,
        executor: SystemExecutor, subs: Subscriptions,
        push_queue: asyncio.Queue
):
    try:
        async for message in ws:
            if not message:
                break
            # 转换消息到array
            data = decode_message(message, protocol)
            # 执行消息
            match data[0]:
                case 'sys':  # sys system_name args ...
                    await sys_call(data, protocol, executor, push_queue)
                case 'sub':  # sub component_name select/query args ...
                    await sub_call(data, protocol, subs, push_queue)
                case 'unsub':  # unsub sub_id
                    check_length('unsub', data, 2, 2)
                    await subs.unsubscribe(data[1])
                case _:
                    raise ValueError(f"Invalid message")
    except SanicException:
        # 不用断开连接，ws断了时主线程会自动结束
        pass


async def subscription_receiver(subscriptions: Subscriptions, push_queue: asyncio.Queue):
    try:
        while True:
            updates = await subscriptions.get_updates()
            # todo 备注，客户端要注意内部避免掉重复订阅
            # 客户端通过查询参数组合成查询字符串，来判断是否重复订阅，管理器注册对应的callback，重复注册只
            # 是callback增加并不会去服务器请求
    finally:
        # 这里需要关闭ws连接，不然主线程会无障碍运行
        pass


@hetu_bp.websocket("/hetu")
async def websocket_connection(request: Request, ws: Websocket):
    # 初始化执行器，一个连接一个执行器
    executor = SystemExecutor(request.app.config['NAMESPACE'])
    await executor.initialize(request.client_ip)
    # 初始化订阅管理器，一个连接一个订阅管理器
    subscriptions = Subscriptions(request.app.ctx.default_backend)
    # 初始化push消息队列
    push_queue = asyncio.Queue(128)

    # 创建接受客户端消息的协程
    protocol = dict(compress=request.app.ctx.compress,
                    crypto=request.app.ctx.crypto)
    task_name = f"client_receiver:{request.id}"
    receiver_task = client_receiver(ws, protocol, executor, subscriptions, push_queue)
    _ = request.app.add_task(receiver_task, name=task_name)

    # 创建获得订阅推送通知的协程
    task_name = f"subs_receiver:{request.id}"
    receiver_task = subscription_receiver(subscriptions, push_queue)
    _ = request.app.add_task(receiver_task, name=task_name)
    # todo 测试subscription_receiver报错退出了连接是否推出

    # 这里循环发送，保证总是第一时间Push
    try:
        while True:
            print('wait queue')
            data = await push_queue.get()
            print('getted')
            await ws.send(data)
    finally:
        # 连接断开，强制关闭此协程时也会调用
        print('closed')
        await request.app.cancel_task(task_name)
        request.app.purge_tasks()


def start_webserver(app_name, config) -> Sanic:
    """config： dict或者py目录"""
    # 加载玩家的app文件
    if (app_file := config.get('APP_FILE', None)) is not None:
        spec = importlib.util.spec_from_file_location('HeTuApp', app_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules['HeTuApp'] = module
        spec.loader.exec_module(module)

    # 加载web服务器
    app = Sanic(app_name)
    app.update_config(config)

    # 加载协议
    app.ctx.compress, app.ctx.crypto = None, None
    compress = config.get('WEBSOCKET_COMPRESSION_CLASS')
    crypto = config.get('WEBSOCKET_CRYPTOGRAPHY_CLASS')
    if compress is not None:
        if compress not in globals():
            raise ValueError(f"该压缩模块未在全局变量中找到：{compress}")
        app.ctx.compress = globals()[compress]
    if crypto is not None:
        if crypto not in globals():
            raise ValueError(f"该加密模块未在全局变量中找到：{crypto}")
        app.ctx.crypto = globals()[crypto]

    # 创建数据库连接池
    backends = {}
    table_classes = {}
    for name, db_cfg in app.config.BACKENDS.items():
        if db_cfg["type"] == "Redis":
            from .data.backend import RedisBackend, RedisComponentTable
            backend = RedisBackend(db_cfg)
            backends['Redis'] = backend
            table_classes['Redis'] = RedisComponentTable
            app.ctx.__setattr__(name, backend)
            # 调用某个函数，让他把define的所有component的table都创建出来
        elif db_cfg["type"] == "SQL":
            # import sqlalchemy
            # app.ctx.__setattr__(name, sqlalchemy.create_engine(db_cfg["addr"]))
            raise NotImplementedError("SQL后端未实现，SQL后端还是需要redis或zmq在前面一层负责推送，不一定必要")
    # 把default后端设置为config第一个
    backends['default'] = backends[next(iter(app.config.BACKENDS.keys()))]
    table_classes['default'] = table_classes[next(iter(app.config.BACKENDS.keys()))]
    app.ctx.__setattr__('default_backend', backends['default'])

    # 初始化SystemCluster
    SystemClusters().build_clusters(config['NAMESPACE'])
    # 初始化所有ComponentTable
    ComponentTableManager().build(
        config['NAMESPACE'], config['INSTANCE_NAME'], backends, table_classes)

    # 启动时清空所有非持久化表数据
    for comp, tbl in ComponentTableManager().items():
        if not comp.persist_:
            tbl.flush()

    # 启动服务器监听
    app.blueprint(hetu_bp)
    return app


