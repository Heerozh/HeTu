import importlib.util
from pathlib import Path

import pytest

import hetu
from hetu.common.snowflake_id import SnowflakeID
from hetu.endpoint.executor import EndpointExecutor

SnowflakeID().init(1, 0)


@pytest.fixture(scope="module")
def chat_app():
    hetu.data.ComponentDefines().clear_()
    hetu.endpoint.definer.EndpointDefines()._clear()
    hetu.system.SystemClusters()._clear()

    app_path = Path(__file__).resolve().parents[1] / "examples/chat/server/src/app.py"
    spec = importlib.util.spec_from_file_location("chat_example_app", app_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    hetu.system.SystemClusters().build_clusters("Chat")
    hetu.system.SystemClusters().build_endpoints()
    return module


@pytest.fixture(scope="function")
def tbl_mgr(mod_auto_backend, chat_app):
    backends = {"default": mod_auto_backend()}
    from hetu.manager import ComponentTableManager

    tbl_mgr = ComponentTableManager("Chat", "server1", backends)
    tbl_mgr._flush_all(force=True)
    return tbl_mgr


@pytest.fixture(scope="function")
async def executor(tbl_mgr, chat_app):
    from hetu.system import SystemContext
    from hetu.system.caller import SystemCaller

    ctx = SystemContext(
        caller=0,
        connection_id=0,
        address="127.0.0.1",
        group="",
        user_data={},
        timestamp=0,
        request=None,  # type: ignore[arg-type]
        systems=None,  # type: ignore[arg-type]
    )
    systems = SystemCaller("Chat", tbl_mgr, ctx)
    ctx.systems = systems

    executor = EndpointExecutor("Chat", tbl_mgr, ctx)
    await executor.initialize("")
    yield executor
    await executor.terminate()


def test_chat_example_exports_required_systems(chat_app):
    assert callable(chat_app.user_login)
    assert callable(chat_app.user_quit)
    assert callable(chat_app.user_chat)
    assert callable(chat_app.on_disconnect)


async def test_chat_example_login_emits_join_system_message(
    chat_app, executor, tbl_mgr
):
    ok, _ = await executor.execute("user_login", 1001, "alice")
    assert ok

    OnlineUser = chat_app.OnlineUser
    tbl = tbl_mgr.get_table(OnlineUser)
    async with tbl.session() as session:
        user = await session.using(OnlineUser).get(owner=1001)
        assert user is not None
        assert user.name == "alice"
        assert bool(user.online) is True

    ChatMessage = chat_app.ChatMessage
    tbl = tbl_mgr.get_table(ChatMessage)
    async with tbl.session() as session:
        messages = await session.using(ChatMessage).range(
            created_at_ms=(0, float("inf")), limit=1, desc=True
        )
        assert len(messages) == 1
        msg = messages[0]
        assert msg.kind == "system"
        assert msg.text == "alice joined the chat"
        assert msg.created_at_ms > 0


async def test_chat_example_user_chat_emits_timestamped_chat_message(
    chat_app, executor, tbl_mgr
):
    ok, _ = await executor.execute("user_login", 1001, "alice")
    assert ok

    ok, _ = await executor.execute("user_chat", "hello world")
    assert ok

    ChatMessage = chat_app.ChatMessage
    tbl = tbl_mgr.get_table(ChatMessage)
    async with tbl.session() as session:
        messages = await session.using(ChatMessage).range(
            created_at_ms=(0, float("inf")), limit=1, desc=True
        )
        msg = messages[0]
        assert msg.owner == 1001
        assert msg.name == "alice"
        assert msg.text == "hello world"
        assert msg.kind == "chat"
        assert msg.created_at_ms > 0


async def test_chat_example_user_quit_with_no_user_does_not_emit_message(
    chat_app, executor, tbl_mgr
):
    ok, _ = await executor.execute("user_quit")
    assert not ok

    ChatMessage = chat_app.ChatMessage
    tbl = tbl_mgr.get_table(ChatMessage)
    async with tbl.session() as session:
        messages = await session.using(ChatMessage).range(
            created_at_ms=(0, float("inf")), limit=1, desc=True
        )
        assert len(messages) == 0


async def test_chat_example_user_chat_requires_online_user(chat_app, executor, tbl_mgr):
    ok, _ = await executor.execute("user_chat", "hello")
    assert not ok

    ChatMessage = chat_app.ChatMessage
    tbl = tbl_mgr.get_table(ChatMessage)
    async with tbl.session() as session:
        messages = await session.using(ChatMessage).range(
            created_at_ms=(0, float("inf")), limit=1, desc=True
        )
        assert len(messages) == 0


async def test_chat_example_disconnect_calls_user_quit(chat_app, executor, tbl_mgr):
    ok, _ = await executor.execute("user_login", 1001, "alice")
    assert ok

    await executor.context.systems.call("on_disconnect")

    OnlineUser = chat_app.OnlineUser
    tbl = tbl_mgr.get_table(OnlineUser)
    async with tbl.session() as session:
        user = await session.using(OnlineUser).get(owner=1001)
        assert user is not None
        assert bool(user.online) is False

    ChatMessage = chat_app.ChatMessage
    tbl = tbl_mgr.get_table(ChatMessage)
    async with tbl.session() as session:
        messages = await session.using(ChatMessage).range(
            created_at_ms=(0, float("inf")), limit=1, desc=True
        )
        msg = messages[0]
        assert msg.kind == "system"
        assert msg.text == "alice left the chat"
