"""自定义HTTP端点（@hetu.define_route / @hetu.on_server_setup）的注册测试。

不需要数据库：worker_main只负责创建Sanic app并挂路由，连后端是在
before_server_start里做的，这里不启动服务器。
"""

import itertools

import pytest

import hetu
from hetu import webext
from hetu.endpoint.definer import EndpointDefines
from hetu.safelogging.default import DEFAULT_LOGGING_CONFIG
from hetu.server import worker_main
from hetu.system import SystemClusters

NAMESPACE = "webext_test"


@pytest.fixture(autouse=True)
def clean_registry():
    """webext的注册表是模块级全局的，用例之间必须清干净，否则会漏到别的测试文件。"""
    webext.clear()
    SystemClusters()._clear()
    EndpointDefines()._clear()
    yield
    webext.clear()
    SystemClusters()._clear()
    EndpointDefines()._clear()


# app文件会被worker_main重复exec（管理进程+worker），所以定义都带force=True。
# 另外SystemClusters要求namespace下至少有一个System才能build_clusters，这里放个空的。
APP_PREAMBLE = """
import hetu
import numpy as np
from sanic import text


@hetu.define_component(namespace="{ns}", force=True)
class Dummy(hetu.BaseComponent):
    owner: np.int64 = hetu.property_field(0, unique=True)


@hetu.define_system(namespace="{ns}", components=(Dummy,), force=True)
async def noop(ctx):
    pass


"""


def make_app_file(tmp_path, body: str):
    app_file = tmp_path / "app.py"
    app_file.write_text(APP_PREAMBLE.format(ns=NAMESPACE) + body, "utf-8")
    return app_file


_app_seq = itertools.count()


def build_server(app_file):
    # Sanic不允许同名app共存于一个进程（生产中每个worker是独立进程），测试里给个唯一名
    return worker_main(
        f"Hetu-{NAMESPACE}-{next(_app_seq)}",
        {
            "APP_FILE": str(app_file),
            "NAMESPACE": NAMESPACE,
            "INSTANCES": ["webext_1"],
            "LISTEN": "0.0.0.0:875",
            "PACKET_LAYERS": [{"type": "jsonb"}],
            "BACKENDS": {},
            "LOGGING": DEFAULT_LOGGING_CONFIG,
            "DEBUG": False,
            "WORKER_NUM": 1,
            "ACCESS_LOG": False,
        },
    )


def route_paths(app) -> set[str]:
    # sanic的route.path不带开头的"/"，根路径是空字符串
    return {route.path for route in app.router.routes}


DOWNLOAD_APP = """
@hetu.define_route("/download/<name:str>")
async def download(request, name: str):
    return text(name)
"""


def test_define_route_mounts_on_web_server(tmp_path):
    """@define_route的路由应该出现在服务器上，且不影响HeTu自己的入口。"""
    app = build_server(make_app_file(tmp_path, DOWNLOAD_APP))

    paths = route_paths(app)
    assert "download/<name:str>" in paths
    # HeTu自己的两个入口都还在
    assert "" in paths
    assert "hetu/<db_name:str>" in paths


def test_route_can_declare_sanic_kwargs(tmp_path):
    """kwargs原样透传给sanic，比如限定methods。"""
    app = build_server(
        make_app_file(
            tmp_path,
            """
@hetu.define_route("/upload", methods=["POST"])
async def upload(request):
    return text("ok")
""",
        )
    )

    route = next(r for r in app.router.routes if r.path == "upload")
    assert set(route.methods) == {"POST"}


def test_worker_main_twice_does_not_conflict(tmp_path):
    """同一进程内重复调用worker_main（管理进程+worker、或测试）不能撞RouteExists。

    app文件每次都会被重新exec一遍，注册表按函数全限定名去重；路由则是每次新建一个
    Blueprint重放上去——Blueprint一旦注册到某个app就不能再加路由了。
    """
    app_file = make_app_file(tmp_path, DOWNLOAD_APP)

    first = build_server(app_file)
    # 簇是单件，生产中第二次worker_main在新进程里，这里手动还原成新进程的状态。
    # 注意webext的注册表**不清**，本用例要的就是它带着上一轮的注册再来一次。
    SystemClusters()._clear()
    EndpointDefines()._clear()
    second = build_server(app_file)

    assert "download/<name:str>" in route_paths(first)
    assert "download/<name:str>" in route_paths(second)


def test_user_route_replaces_default_welcome_page(tmp_path):
    """用户注册"/"时，HeTu的默认欢迎页让位，而不是报路由冲突。"""
    app = build_server(
        make_app_file(
            tmp_path,
            """
@hetu.define_route("/")
async def home(request):
    return text("my home page")
""",
        )
    )

    roots = [r for r in app.router.routes if r.path == ""]
    assert len(roots) == 1
    assert webext.USER_BLUEPRINT_NAME in roots[0].name


def test_on_server_setup_receives_app(tmp_path):
    """setup回调能拿到Sanic app，可自建blueprint/静态目录/中间件。"""
    app = build_server(
        make_app_file(
            tmp_path,
            """
from sanic import Blueprint

@hetu.on_server_setup
def setup(app):
    bp = Blueprint("MyAdmin", url_prefix="/admin")

    @bp.route("/ping")
    async def ping(request):
        return text("pong")

    app.blueprint(bp)
    app.ctx.setup_called = True
""",
        )
    )

    assert app.ctx.setup_called is True
    assert "admin/ping" in route_paths(app)


def test_reserved_and_invalid_uri_rejected():
    """HeTu的WebSocket入口前缀不能被占用，uri也必须是绝对路径。"""
    with pytest.raises(ValueError, match="hetu"):

        @hetu.define_route("/hetu/<db_name>")
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

    # 只是前缀相似的路径不受影响
    @hetu.define_route("/hetumeta")
    async def similar(request):
        pass
