import importlib.util
from pathlib import Path
from types import SimpleNamespace

from hetu.system.context import SystemContext


def _load_chat_app():
    app_path = Path(__file__).resolve().parents[1] / "examples/chat/server/src/app.py"
    spec = importlib.util.spec_from_file_location("chat_example_app", app_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHAT_APP = _load_chat_app()


class _FakeOnlineRepo:
    def __init__(self, row=None):
        self.row = row
        self.deleted_ids = []

    async def get(self, **_kwargs):
        return self.row

    def delete(self, row_id):
        self.deleted_ids.append(row_id)


def test_chat_example_exports_required_systems():
    assert callable(CHAT_APP.user_login)
    assert callable(CHAT_APP.user_quit)
    assert callable(CHAT_APP.user_chat)
    assert callable(CHAT_APP.on_disconnect)


async def test_chat_example_disconnect_calls_user_quit():
    repo = _FakeOnlineRepo(row=SimpleNamespace(id=7))
    ctx = SystemContext(
        caller=1001,
        connection_id=1,
        address="127.0.0.1",
        group="",
        user_data={},
        timestamp=0.0,
        request=None,  # type: ignore[arg-type]
        systems=None,  # type: ignore[arg-type]
        repo={CHAT_APP.OnlineUser: repo},
        depend={
            "on_disconnect": CHAT_APP.on_disconnect.__wrapped__,
            "user_quit": CHAT_APP.user_quit.__wrapped__,
        },
    )

    await CHAT_APP.on_disconnect(ctx)

    assert repo.deleted_ids == [7]
