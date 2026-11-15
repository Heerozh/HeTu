"""
自定义HTTP端点（@hetu.define_route / @hetu.on_server_setup）：真起服务、发真实的请求。后端用 SQLite，
不需要 Docker。
"""

from typing import Any

import pytest
from fixtures.server_harness import ServerHarness

import hetu
from hetu import webext
from hetu.endpoint.definer import EndpointDefines
from hetu.safelogging.default import DEFAULT_LOGGING_CONFIG
from hetu.system import SystemClusters

NAMESPACE = "webext_test"


@pytest.fixture(autouse=True)
def clean_registry():
    """webext 的注册表、簇、Endpoint 的定义都是进程级的，用例之间必须清干净，否则会漏到别的测试文件。"""
    webext.clear()
    SystemClusters()._clear()
    EndpointDefines()._clear()
    yield
    webext.clear()
    SystemClusters()._clear()
    EndpointDefines()._clear()


# app 文件会被重复 exec，所以定义都带 force=True。SystemClusters 要求 namespace 下至少有一个 System
# 才能建簇，这里放个空的
APP_PREAMBLE = """
import hetu
import numpy as np
from aiohttp import web


@hetu.define_component(namespace="{ns}", force=True)
class Dummy(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)


@hetu.define_system(namespace="{ns}", components=(Dummy,), force=True)
async def noop(ctx):
    pass


"""

DOWNLOAD_APP = """
@hetu.define_route("/download/{name}")
async def download(request: web.Request) -> web.StreamResponse:
    return web.Response(text=f"downloaded {request.match_info['name']}")
"""


def make_server(tmp_path, body: str, **config: Any) -> ServerHarness:
    """把 body 接在 APP_PREAMBLE 后面写成 app 文件，建 harness（会加载一遍 app 文件）"""
    app_file = tmp_path / "app.py"
    app_file.write_text(APP_PREAMBLE.format(ns=NAMESPACE) + body, "utf-8")
    return ServerHarness(
        {
            "APP_FILE": str(app_file),
            "NAMESPACE": NAMESPACE,
            "INSTANCES": ["webext_1"],
            "LISTEN": "127.0.0.1:0",
            "PACKET_LAYERS": [{"type": "jsonb"}],
            "BACKENDS": {
                "main": {
                    "type": "sqlite",
                    "master": f"sqlite:///{tmp_path / 'webext.db'}",
                }
            },
            "LOGGING": DEFAULT_LOGGING_CONFIG,
            "DEBUG": False,
            "WORKER_NUM": 1,
            **config,
        }
    )


def test_define_route_serves_requests(tmp_path):
    """@define_route 的路由能访问到，路径参数从 request.match_info 取；HeTu 自己的入口都还在"""
    server = make_server(tmp_path, DOWNLOAD_APP)

    response = server.get("/download/abc.zip")
    assert (response.status, response.text) == (200, "downloaded abc.zip")
    assert "Powered by HeTu" in server.get("/").text
    # ws 入口还是 HeTu 的：普通 GET 不是 ws 握手，回 400
    assert server.get("/hetu/webext_1").status == 400


def test_methods_limit_and_request_size(tmp_path):
    """
    methods 限制生效：只注册了 POST 的路径，用 GET 请求得到 405；不写 methods 就只有 GET。methods 可以
    只给一个字符串，大小写重复的只注册一次（aiohttp 上同一方法加两次会报错）。请求体的上限是
    REQUEST_MAX_SIZE，超过回 413
    """
    server = make_server(
        tmp_path,
        """
@hetu.define_route("/upload", methods=["POST"])
async def upload(request):
    return web.Response(text=f"got {len(await request.read())} bytes")


@hetu.define_route("/ping")
async def ping(request):
    return web.Response(text="pong")


@hetu.define_route("/delete", methods="DELETE")
async def delete(request):
    return web.Response(text="deleted")


@hetu.define_route("/both", methods=["get", "GET", "put"])
async def both(request):
    return web.Response(text=request.method)
""",
        REQUEST_MAX_SIZE=10,
    )

    assert server.request("POST", "/upload", data=b"x" * 10).text == "got 10 bytes"
    assert server.request("POST", "/upload", data=b"x" * 11).status == 413
    assert server.get("/upload").status == 405
    assert server.get("/ping").text == "pong"
    assert server.request("POST", "/ping").status == 405
    assert server.request("DELETE", "/delete").text == "deleted"
    assert server.get("/delete").status == 405
    assert server.get("/both").text == "GET"
    assert server.request("PUT", "/both").text == "PUT"


def test_user_route_replaces_default_welcome_page(tmp_path):
    """
    用户注册"/"时，HeTu 的默认欢迎页整个让位：用户的路由只收 GET，HEAD 也不会落到默认欢迎页上（aiohttp
    按注册顺序匹配，默认欢迎页要是照挂，GET 虽然轮不到它，HEAD 还会由它回）
    """
    server = make_server(
        tmp_path,
        """
@hetu.define_route("/")
async def home(request):
    return web.Response(text="my home page")
""",
    )

    response = server.get("/")
    assert (response.status, response.text) == (200, "my home page")
    assert server.request("HEAD", "/").status == 405


def test_on_server_setup_receives_app(tmp_path):
    """
    on_server_setup 的回调拿到 aiohttp 的 app：能加路由，往 app.on_startup 挂的钩子在开服时跑；它注册的
    "/" 同样顶掉默认欢迎页
    """
    server = make_server(
        tmp_path,
        """
STARTED = web.AppKey("started", bool)


@hetu.on_server_setup
def setup(app: web.Application):
    async def on_startup(app):
        app[STARTED] = True

    async def ping(request):
        return web.Response(text=f"pong started={request.app.get(STARTED, False)}")

    async def home(request):
        return web.Response(text="setup home")

    app.on_startup.append(on_startup)
    app.router.add_get("/admin/ping", ping)
    app.router.add_get("/", home)
""",
    )

    assert server.get("/admin/ping").text == "pong started=True"
    assert server.get("/").text == "setup home"


def test_reserved_and_invalid_uri_rejected():
    """HeTu的WebSocket入口前缀不能被占用，uri也必须是绝对路径，methods 不能为空。"""
    with pytest.raises(ValueError, match="hetu"):

        @hetu.define_route("/hetu/{db_name}")
        async def ws_conflict(request):
            pass

    with pytest.raises(ValueError, match="hetu"):

        @hetu.define_route("/hetu")
        async def prefix_conflict(request):
            pass

    with pytest.raises(ValueError, match="'/'"):

        @hetu.define_route("download")
        async def not_absolute(request):
            pass

    with pytest.raises(ValueError, match="methods"):

        @hetu.define_route("/nothing", methods=[])
        async def no_methods(request):
            pass

    # 只是前缀相似的路径不受影响
    @hetu.define_route("/hetumeta")
    async def similar(request):
        pass


COUNT_APP = (
    DOWNLOAD_APP
    + """

def _count(app):
    return sum(1 for r in app.router.resources() if r.canonical == "/download/{name}")


@hetu.on_server_setup
def count_routes(app: web.Application):
    # 回调在 define_route 的路由挂好之后才执行：这时就数得到
    at_setup = _count(app)

    async def count(request):
        return web.Response(text=f"{at_setup} {_count(request.app)}")

    app.router.add_get("/route-count", count)
"""
)


def test_app_file_exec_twice_registers_once(tmp_path):
    """
    app 文件在同一进程里被 exec 两遍（嵌入场景、测试）：注册表按函数的全限定名覆盖，路由只挂一次、配置
    回调只调一次；回调执行时 define_route 的路由已经挂好了。aiohttp 只拦挨着的重复注册（同一路径、
    同一方法连着加会报错），隔开的照样登记、先注册的那条生效，所以由回调数路由表
    """
    make_server(tmp_path, COUNT_APP)
    # 簇、Endpoint 的定义是单件，生产中第二次加载在新进程里，这里手动还原成新进程的状态。webext 的
    # 注册表**不清**，本用例要的就是它带着上一轮的声明再来一次
    SystemClusters()._clear()
    EndpointDefines()._clear()
    server = make_server(tmp_path, COUNT_APP)

    assert server.get("/route-count").text == "1 1"
    assert server.get("/download/x").text == "downloaded x"


def test_on_server_setup_error_aborts_before_backends(tmp_path):
    """
    on_server_setup 的回调出错就中止开服，而且是在连后端、跑启动 System 之前：配错的路由不用等开完服
    （最长 STARTUP_TIMEOUT）才报出来，启动 System 也不会已经写过数据。先确认回调正常时启动 System
    确实会跑（会写标记文件），不然这个用例是空测
    """
    marker = tmp_path / "on_start_ran"
    seed_app = f"""
from pathlib import Path


@hetu.define_system(namespace="{NAMESPACE}", components=(Dummy,), on_start=True, force=True)
async def seed(ctx):
    Path({str(marker)!r}).write_text("ran")
"""
    server = make_server(tmp_path, seed_app)
    assert server.get("/").status == 200
    assert marker.exists()

    marker.unlink()
    webext.clear()
    SystemClusters()._clear()
    EndpointDefines()._clear()
    server = make_server(
        tmp_path,
        seed_app
        + """

@hetu.on_server_setup
def broken(app):
    raise RuntimeError("broken setup")
""",
    )
    with pytest.raises(RuntimeError, match="broken setup"):
        server.get("/")
    assert not marker.exists()
