"""
自定义HTTP端点的注册表，用于把普通web路由挂到HeTu内置的web服务器（aiohttp）上。

Registry for plain HTTP endpoints served by HeTu's built-in web server (aiohttp).

这些端点就是普通的aiohttp路由（文件下载、后台页面、健康检查等），和HeTu本身无关：
不经过客户端Endpoint的权限体系，也不提供数据库访问。

本模块只保存"声明"，不在顶层 import aiohttp（`import hetu` 不加载 Web 框架）；真正的注册由
aiohttp 适配层（`hetu.server.adapters.aiohttp_app.build_app`）在每次建 app 时重放（见
`apply_to`）。声明按函数的全限定名登记：app文件在同一进程内被重复exec时（单元测试、嵌入场景）
是覆盖而不是追加，不会把同一条路由注册两遍。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

from .i18n import _

if TYPE_CHECKING:
    from aiohttp import web

# HeTu自己占用的路径前缀，`/hetu/{instance}`是客户端SDK的WebSocket入口，不可占用。
# 根路径"/"不在此列：它只是个默认欢迎页，用户注册了同名路由就让给用户。
RESERVED_URI_PREFIX = "/hetu"

# 声明表。key为函数的全限定名：app文件被重复exec时是覆盖而不是追加。
# 路由的值是 (uri, HTTP 方法, 透传给 add_route 的参数, 处理函数)
_ROUTES: dict[str, tuple[str, tuple[str, ...], dict[str, Any], Callable]] = {}
_SETUPS: dict[str, Callable] = {}


def _registry_key(func: Callable) -> str:
    return f"{func.__module__}.{func.__qualname__}"


def define_route(uri: str, *, methods: Iterable[str] = ("GET",), **aiohttp_kwargs: Any):
    """
    把一个async函数注册为HTTP端点，挂载到HeTu的web服务器上。

    用于文件下载、后台页面、健康检查等和HeTu数据无关的普通web请求。它们是纯粹的
    aiohttp路由，`hetu`只负责帮你挂上去：不经过客户端的权限体系（`Permission`），
    也不提供数据库/`System`访问，鉴权、限流等都需要你自己在函数内处理。

    需要客户端SDK调用的游戏逻辑请用 `define_system` / `define_endpoint`，不是这个。

    Register an async function as a plain HTTP endpoint on HeTu's web server
    (aiohttp), for things unrelated to HeTu's data (file downloads, admin pages,
    health checks). These routes bypass HeTu's `Permission` system entirely and get
    no database access, so any authentication is up to you.

    Examples
    --------
    >>> import hetu
    >>> from aiohttp import web
    >>>
    >>> @hetu.define_route("/hello/{name}")
    ... async def hello(request: web.Request) -> web.StreamResponse:
    ...     return web.Response(text=f"hello {request.match_info['name']}")

    Parameters
    ----------
    uri: str
        路由路径，必须以"/"开头，路径参数用aiohttp的语法（如 `/file/{name}`，也可以带正则：
        `/item/{id:\\d+}`），在处理函数里从 `request.match_info` 取。不能占用HeTu保留的
        `/hetu` 前缀。注册"/"会覆盖HeTu的默认欢迎页。aiohttp 严格区分尾斜杠：`/ping/` 不会命中
        `/ping`。
    methods: Iterable[str]
        接受哪些HTTP方法，默认只有 GET，不能为空。用别的方法请求这个路径会得到 405。
    **aiohttp_kwargs
        原样透传给aiohttp的 `UrlDispatcher.add_route`，比如 `name`、`expect_handler`。

    Notes
    -----
    - 被装饰的函数就是aiohttp的handler：唯一的参数是 `aiohttp.web.Request`，返回
      `aiohttp.web.StreamResponse`（`web.Response`、`web.json_response(...)`、
      `web.FileResponse` 等）。
    - 路径参数已经 URL 解码过（`%2F` 会变成 `/`），拿它拼文件路径就会被路径穿越读到别的文件。提供
      文件下载请在 `hetu.on_server_setup` 里用 `app.router.add_static`，它会规范化路径、不出目录。
    - 请求体的上限是配置项 `REQUEST_MAX_SIZE`（默认 100MB）。处理函数没有总时长的上限（原来 Sanic 的
      RESPONSE_TIMEOUT 是 60 秒），需要的话自己加 `asyncio.timeout`。
    - 路由在**每个worker进程**中都会注册，处理函数应保持无状态。大文件下载会和
      WebSocket游戏流量抢同一个事件循环，生产环境建议交给前置的Caddy/CDN。
    - 装饰器必须在app文件被引擎加载时执行（即模块顶层），worker启动后再注册无效。

    See Also
    --------
    hetu.on_server_setup : 直接拿到aiohttp的app做任意配置（静态目录、中间件等）
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
    if isinstance(methods, str):
        methods = (methods,)
    # 统一大写、去重：同一个方法在一个路径上注册两次，aiohttp 会报错
    method_list = tuple(dict.fromkeys(method.upper() for method in methods))
    if not method_list:
        raise ValueError(
            _("define_route的methods不能为空，你的uri：{uri}").format(uri=uri)
        )

    def wrapper(func: Callable) -> Callable:
        _ROUTES[_registry_key(func)] = (uri, method_list, aiohttp_kwargs, func)
        return func

    return wrapper


def on_server_setup(func: Callable) -> Callable:
    """
    注册一个web服务器配置回调，参数是aiohttp的 `web.Application`。

    `define_route` 覆盖不到的场景用它：静态文件目录、中间件、子应用、开服/关服时要做的事等，
    都可以在回调里直接对app操作。

    Register a callback invoked with the aiohttp `web.Application`, for anything
    `define_route` cannot express: static directories, middleware, sub-applications,
    startup / cleanup hooks, and so on.

    Examples
    --------
    >>> import hetu
    >>> from aiohttp import web
    >>>
    >>> @hetu.on_server_setup
    ... def setup(app: web.Application):
    ...     app.router.add_static("/download", "/data/files")

    Notes
    -----
    - 回调在**每个worker进程**里、每次开服建app时执行一次，此时后端还没连接、也还没开始监听；
      多进程时管理进程不执行。回调出错就中止开服。要在开服、关服时做事（比如建一个连接池），在回调里
      往 `app.on_startup` / `app.on_cleanup` 里挂。
    - 回调在 `define_route` 的路由注册**之后**执行，HeTu的默认欢迎页"/"则在最后
      按需补上：你在这里注册了"/"，默认欢迎页就自动让位。

    See Also
    --------
    hetu.define_route : 直接注册单条HTTP路由的快捷方式
    """
    _SETUPS[_registry_key(func)] = func
    return func


def apply_to(app: web.Application) -> None:
    """
    把注册表中的声明应用到aiohttp的app上：先挂 `define_route` 的路由，再按注册顺序调
    `on_server_setup` 的回调。由aiohttp适配层在每次建app时调用（app 还没冻结）。

    Apply the registered routes and setup callbacks to an aiohttp app.
    """
    for uri, methods, kwargs, func in _ROUTES.values():
        # 同一个路径的几个方法连着加，aiohttp 会复用刚建的那个 resource
        for method in methods:
            app.router.add_route(method, uri, func, **kwargs)

    for setup in list(_SETUPS.values()):
        setup(app)


def clear() -> None:
    """清空注册表，单元测试用。"""
    _ROUTES.clear()
    _SETUPS.clear()
