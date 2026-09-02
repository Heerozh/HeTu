"""
自定义HTTP端点的注册表，用于把普通web路由挂到HeTu内置的web服务器上。

Registry for plain HTTP endpoints served by HeTu's built-in web server.

这些端点就是普通的Sanic路由（文件下载、后台页面、健康检查等），和HeTu本身无关：
不经过客户端Endpoint的权限体系，也不提供数据库访问。

本模块刻意不在顶层import sanic，只保存"声明"；真正的注册由
`hetu.server.main.worker_main` 在创建Sanic app之后重放（见 `apply_to`）。这样app文件
在同一进程内被重复exec时（单元测试、嵌入场景）不会重复注册路由——Sanic的Blueprint
一旦注册到某个app上，之后再往它加路由会立刻抛RouteExists。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2025, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from .i18n import _

if TYPE_CHECKING:
    from sanic import Sanic

# HeTu自己占用的路径前缀，`/hetu/<instance>`是客户端SDK的WebSocket入口，不可占用。
# 根路径"/"不在此列：它只是个默认欢迎页，用户注册了同名路由就让给用户。
RESERVED_URI_PREFIX = "/hetu"

# 用户自定义路由所在的Blueprint名，路由名会是 "HeTuUser.<函数名>"，和HeTu自己的隔离。
USER_BLUEPRINT_NAME = "HeTuUser"

# 声明表。key为函数的全限定名：app文件被重复exec时是覆盖而不是追加。
_ROUTES: dict[str, tuple[str, dict[str, Any], Callable]] = {}
_SETUPS: dict[str, Callable] = {}


def _registry_key(func: Callable) -> str:
    return f"{func.__module__}.{func.__qualname__}"


def define_route(uri: str, **sanic_kwargs: Any):
    """
    把一个async函数注册为HTTP端点，挂载到HeTu的web服务器上。

    用于文件下载、后台页面、健康检查等和HeTu数据无关的普通web请求。它们是纯粹的
    Sanic路由，`hetu`只负责帮你挂上去：不经过客户端的权限体系（`Permission`），
    也不提供数据库/`System`访问，鉴权、限流等都需要你自己在函数内处理。

    需要客户端SDK调用的游戏逻辑请用 `define_system` / `define_endpoint`，不是这个。

    Register an async function as a plain HTTP endpoint on HeTu's web server, for
    things unrelated to HeTu's data (file downloads, admin pages, health checks).
    These routes bypass HeTu's `Permission` system entirely and get no database
    access, so any authentication is up to you.

    Examples
    --------
    >>> import hetu
    >>> from sanic import Request, response
    >>>
    >>> @hetu.define_route("/download/<name:str>")
    ... async def download(request: Request, name: str):
    ...     return await response.file(f"/data/{name}")

    Parameters
    ----------
    uri: str
        路由路径，必须以"/"开头，可使用Sanic的路径参数语法（如 `/file/<name:str>`）。
        不能占用HeTu保留的 `/hetu` 前缀。注册"/"会覆盖HeTu的默认欢迎页。
    **sanic_kwargs
        原样透传给Sanic的 `Blueprint.route`，常用的有 `methods=["POST"]`、
        `stream=True`、`websocket=True` 等。

    Notes
    -----
    - 被装饰的函数签名就是Sanic handler的签名：第一个参数是 `sanic.Request`，
      后面是路径参数。返回值需要是 `sanic.HTTPResponse`。
    - 路由在**每个worker进程**中都会注册，处理函数应保持无状态。大文件下载会和
      WebSocket游戏流量抢同一个事件循环，生产环境建议交给前置的nginx/CDN。
    - 装饰器必须在app文件被引擎加载时执行（即模块顶层），worker启动后再注册无效。

    See Also
    --------
    hetu.on_server_setup : 直接拿到Sanic app做任意配置（静态目录、中间件等）
    """
    if not uri.startswith("/"):
        raise ValueError(
            _("define_route的uri必须以'/'开头，你的：{uri}").format(uri=uri)
        )
    if uri == RESERVED_URI_PREFIX or uri.startswith(RESERVED_URI_PREFIX + "/"):
        raise ValueError(
            _(
                "define_route的uri不能占用HeTu保留的'{prefix}'路径（客户端SDK的"
                "WebSocket入口），你的：{uri}"
            ).format(prefix=RESERVED_URI_PREFIX, uri=uri)
        )

    def wrapper(func: Callable) -> Callable:
        _ROUTES[_registry_key(func)] = (uri, sanic_kwargs, func)
        return func

    return wrapper


def on_server_setup(func: Callable) -> Callable:
    """
    注册一个web服务器配置回调，在Sanic app创建后调用，参数为app本身。

    `define_route` 覆盖不到的场景用它：静态文件目录、中间件、异常处理器、自建带
    `url_prefix` 的Blueprint等，都可以在回调里直接对app操作。

    Register a callback invoked with the Sanic app right after it is created, for
    anything `define_route` cannot express: static directories, middleware, error
    handlers, your own prefixed Blueprint, and so on.

    Examples
    --------
    >>> import hetu
    >>> from sanic import Sanic
    >>>
    >>> @hetu.on_server_setup
    ... def setup(app: Sanic):
    ...     app.static("/download", "/data/files")

    Notes
    -----
    - 回调在**管理进程和每个worker进程**中各执行一次，且此时worker还没启动、后端也
      还没连接。所以只做注册类的操作，需要每worker初始化的东西请在回调里用
      `app.before_server_start(...)` 挂钩子。
    - 回调在 `define_route` 的路由注册**之后**执行，HeTu的默认欢迎页"/"则在最后
      按需补上：你在这里注册了"/"，默认欢迎页就自动让位。

    See Also
    --------
    hetu.define_route : 直接注册单条HTTP路由的快捷方式
    """
    _SETUPS[_registry_key(func)] = func
    return func


def apply_to(app: Sanic) -> None:
    """把注册表中的声明应用到Sanic app上，由worker_main在创建app后调用。

    每次调用都新建Blueprint（而不是复用一个模块级单件），这样同一进程内多次
    worker_main + 重复exec app文件也不会撞RouteExists。
    """
    from sanic import Blueprint

    if _ROUTES:
        user_bp = Blueprint(USER_BLUEPRINT_NAME)
        for uri, sanic_kwargs, func in _ROUTES.values():
            user_bp.route(uri, **sanic_kwargs)(func)
        app.blueprint(user_bp)

    for setup in list(_SETUPS.values()):
        setup(app)


def clear() -> None:
    """清空注册表，单元测试用。"""
    _ROUTES.clear()
    _SETUPS.clear()
