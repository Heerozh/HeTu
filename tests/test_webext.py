"""自定义HTTP端点（@hetu.define_route / @hetu.on_server_setup）的注册测试。"""

import pytest

import hetu
from hetu import webext


@pytest.fixture(autouse=True)
def clean_registry():
    """webext的注册表是模块级全局的，用例之间必须清干净，否则会漏到别的测试文件。"""
    webext.clear()
    yield
    webext.clear()


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
