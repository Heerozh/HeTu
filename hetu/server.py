"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from sanic import Sanic
from sanic import Request, Websocket, text, json
from sanic import Blueprint
from sanic.log import logger


hetu_bp = Blueprint("my_blueprint")


@hetu_bp.route("/")
async def web_root(request):
    return text("It's alive!")


@hetu_bp.websocket("/api")
async def ws_api(request: Request, ws: Websocket):
    # todo 直接调用内部的各个模块
    while True:
        data = "hello!"
        print("Sending: " + data)
        await ws.send(data)
        data = await ws.recv()
        print("Received: " + data)


def start_webserver(app_name, config) -> Sanic:
    """config： dict或者py目录"""
    # todo 加载玩家的app文件

    # 加载web服务器
    app = Sanic(app_name)
    app.update_config(config)

    for name, db_cfg in app.config.BACKENDS.items():
        if db_cfg["type"] == "Redis":
            import redis
            # app.ctx[name] = redis.Redis(**db_cfg)
        elif db_cfg["type"] == "SQL":
            # import sqlalchemy
            # app.ctx[name] = sqlalchemy.create_engine(db_cfg["addr"])
            raise NotImplementedError("SQL 后端未实现")

    app.blueprint(hetu_bp)
    return app


