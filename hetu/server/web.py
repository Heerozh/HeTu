from sanic import Blueprint, Request, text

import hetu

HETU_BLUEPRINT = Blueprint("HeTuDB")


async def web_root(request: Request):
    """HeTu的默认欢迎页。

    没有挂在HETU_BLUEPRINT上，而是由worker_main在最后按需注册到app：用户可以用
    `hetu.define_route("/")` 或 `hetu.on_server_setup` 定义自己的首页顶掉它。
    """
    return text(f"Powered by HeTu(v{hetu.__version__}) Database! ")
