import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

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
        self.upsert_rows = []
        self.deleted_ids = []

    async def get(self, **_kwargs):
        return self.row

    def upsert(self, **kwargs):
        row = self.row or SimpleNamespace(id=1, owner=kwargs.get("owner", 0), name="")
        self.row = row

        class _Ctx:
            async def __aenter__(inner_self):
                return row

            async def __aexit__(inner_self, _exc_type, _exc, _tb):
                return False

        self.upsert_rows.append(kwargs)
        return _Ctx()

    def delete(self, row_id):
        self.deleted_ids.append(row_id)
        if self.row and getattr(self.row, "id", None) == row_id:
            self.row = None


class _FakeMessageRepo:
    def __init__(self):
        self.inserted = []

    async def insert(self, row):
        self.inserted.append(
            {
                "owner": row.owner,
                "name": row.name,
                "text": row.text,
                "kind": row.kind,
                "created_at_ms": row.created_at_ms,
            }
        )


def _build_ctx(caller, online_repo, message_repo=None):
    repo = {CHAT_APP.OnlineUser: online_repo}
    if message_repo is not None:
        repo[CHAT_APP.ChatMessage] = message_repo

    return SystemContext(
        caller=caller,
        connection_id=1,
        address="127.0.0.1",
        group="",
        user_data={},
        timestamp=0.0,
        request=None,  # type: ignore[arg-type]
        systems=None,  # type: ignore[arg-type]
        repo=repo,
        depend={
            "on_disconnect": CHAT_APP.on_disconnect.__wrapped__,
            "user_quit": CHAT_APP.user_quit.__wrapped__,
        },
    )


def _patch_chat_new_row(monkeypatch):
    counter = {"id": 1000}

    def _fake_new_row():
        counter["id"] += 1
        return SimpleNamespace(
            id=counter["id"],
            owner=0,
            name="",
            text="",
            kind="",
            created_at_ms=0,
        )

    monkeypatch.setattr(CHAT_APP.ChatMessage, "new_row", staticmethod(_fake_new_row))


def test_chat_example_exports_required_systems():
    assert callable(CHAT_APP.user_login)
    assert callable(CHAT_APP.user_quit)
    assert callable(CHAT_APP.user_chat)
    assert callable(CHAT_APP.on_disconnect)


async def test_chat_example_disconnect_calls_user_quit():
    online_repo = _FakeOnlineRepo(row=SimpleNamespace(id=7, owner=1001, name="alice", online=True))
    message_repo = _FakeMessageRepo()
    ctx = _build_ctx(1001, online_repo, message_repo)

    monkeypatch = pytest.MonkeyPatch()
    _patch_chat_new_row(monkeypatch)
    try:
        await CHAT_APP.on_disconnect(ctx)
    finally:
        monkeypatch.undo()

    assert online_repo.row is not None and online_repo.row.online is False
    assert message_repo.inserted[-1]["kind"] == "system"
    assert message_repo.inserted[-1]["text"] == "alice left the chat"


async def test_chat_example_login_emits_join_system_message():
    online_repo = _FakeOnlineRepo()
    message_repo = _FakeMessageRepo()
    ctx = _build_ctx(1001, online_repo, message_repo)

    elevate_called = {}

    async def _fake_elevate(_ctx, user_id, kick_logged_in):
        elevate_called["user_id"] = user_id
        elevate_called["kick_logged_in"] = kick_logged_in

    monkeypatch = pytest.MonkeyPatch()
    _patch_chat_new_row(monkeypatch)
    monkeypatch.setattr(CHAT_APP.hetu, "elevate", _fake_elevate)
    try:
        await CHAT_APP.user_login.__wrapped__(ctx, 1001, "alice")
    finally:
        monkeypatch.undo()

    assert elevate_called == {"user_id": 1001, "kick_logged_in": True}
    assert online_repo.row.name == "alice"
    assert online_repo.row.online is True
    assert message_repo.inserted[-1]["kind"] == "system"
    assert message_repo.inserted[-1]["text"] == "alice joined the chat"
    assert message_repo.inserted[-1]["created_at_ms"] > 0


async def test_chat_example_user_chat_emits_timestamped_chat_message():
    online_repo = _FakeOnlineRepo(row=SimpleNamespace(id=2, owner=1001, name="alice", online=True))
    message_repo = _FakeMessageRepo()
    ctx = _build_ctx(1001, online_repo, message_repo)

    monkeypatch = pytest.MonkeyPatch()
    _patch_chat_new_row(monkeypatch)
    try:
        await CHAT_APP.user_chat.__wrapped__(ctx, "hello world")
    finally:
        monkeypatch.undo()

    inserted = message_repo.inserted[-1]
    assert inserted["owner"] == 1001
    assert inserted["name"] == "alice"
    assert inserted["text"] == "hello world"
    assert inserted["kind"] == "chat"
    assert inserted["created_at_ms"] > 0


async def test_chat_example_user_quit_with_no_user_does_not_emit_message():
    online_repo = _FakeOnlineRepo(row=None)
    message_repo = _FakeMessageRepo()
    ctx = _build_ctx(1001, online_repo, message_repo)

    await CHAT_APP.user_quit(ctx)

    assert message_repo.inserted == []


async def test_chat_example_user_chat_requires_online_user():
    online_repo = _FakeOnlineRepo(row=None)
    message_repo = _FakeMessageRepo()
    ctx = _build_ctx(1001, online_repo, message_repo)

    with pytest.raises(AssertionError, match="call user_login first"):
        await CHAT_APP.user_chat.__wrapped__(ctx, "hello")

    assert message_repo.inserted == []
