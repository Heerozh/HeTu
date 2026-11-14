from sanic import Request, text

import hetu


async def web_root(request: Request):
    """HeTu的默认欢迎页。

    由worker_main在最后按需注册到app：用户可以用 `hetu.define_route("/")` 或
    `hetu.on_server_setup` 定义自己的首页顶掉它。
    """
    return text(f"Powered by HeTu(v{hetu.__version__}) Database! ")
