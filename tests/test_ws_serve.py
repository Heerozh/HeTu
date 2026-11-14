"""
serve_connection 的单元测试：用内存里的假连接（FakeWsConnection），不走网络、不起 Web 框架，直接驱动
握手、收发和拆连接（设计稿 docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §5.3）。

假连接按 aiohttp 的语义实现：fail 之后、对端断开之后，收发立即抛 ConnectionClosed，不靠框架取消
连接的协程。走真网络、跑在 Sanic 上的测试在 test_websocket.py。
"""

import asyncio
import logging
from typing import Any

import pytest
from fixtures.backends import REDIS_BACKENDS, SQLITE_BACKENDS

from hetu.endpoint import connection
from hetu.endpoint.connection import Connection
from hetu.server import websocket as websocket_server
from hetu.server.conn import ConnectionClosed, next_conn_id
from hetu.server.pipeline import JSONBinaryLayer, MessagePipeline, ServerMessagePipeline
from hetu.server.runtime import WorkerRuntime, setup_message_pipeline

# serve_connection 的逻辑与后端无关，Redis、SQLite 各跑一遍就够了
pytestmark = pytest.mark.parametrize(
    "backend_name",
    [b for b in ("redis", "sqlite") if b in REDIS_BACKENDS + SQLITE_BACKENDS],
    indirect=True,
)

# 独立 instance：别的模块在 server1、pytest_1 等 instance 上按各自的簇建过表
INSTANCE = "ws_serve"
_CLOSED = object()


class FakeWsConnection:
    """
    内存里的 WsConnection。服务端一侧是 WsConnection 的四个方法；客户端一侧用 `client_send_raw`、
    `client_recv_raw`、`disconnect`。
    """

    def __init__(self, client_ip: str = "10.0.0.1"):
        self.conn_id = next_conn_id()
        self.client_ip = client_ip
        self.request = None
        # 第一次 fail 的 code / reason，也就是客户端收到的 close 帧
        self.close_code: int | None = None
        self.close_reason: str | None = None
        # 对端不读数据：send 一直卡在 drain 上，fail 之后也不返回
        self.stuck = False
        # protocol_error() 的返回值
        self.error: BaseException | None = None
        self._closed = False
        self._inbox: asyncio.Queue[Any] = asyncio.Queue()
        self._outbox: asyncio.Queue[Any] = asyncio.Queue()

    # ---- WsConnection ----

    async def recv(self, timeout: float | None = None) -> str | bytes | None:
        if self._closed:
            raise ConnectionClosed
        try:
            async with asyncio.timeout(timeout):
                item = await self._inbox.get()
        except TimeoutError:
            return None
        if item is _CLOSED:
            raise ConnectionClosed
        return item

    async def send(self, data: str | bytes) -> None:
        if self._closed:
            raise ConnectionClosed
        if self.stuck:
            await asyncio.Event().wait()
        self._outbox.put_nowait(data)

    def fail(self, code: int, reason: str) -> None:
        if not self._closed:
            self.close_code, self.close_reason = code, reason
        self._close()

    def protocol_error(self) -> BaseException | None:
        return self.error

    # ---- 客户端一侧 ----

    def _close(self) -> None:
        if not self._closed:
            self._closed = True
            self._inbox.put_nowait(_CLOSED)

    def client_send_raw(self, data: str | bytes) -> None:
        self._inbox.put_nowait(data)

    async def client_recv_raw(self, timeout: float = 5) -> Any:
        return await asyncio.wait_for(self._outbox.get(), timeout)

    def disconnect(self) -> None:
        """对端断开（没有 close 帧）"""
        self._close()


class FakeClient:
    """假连接的客户端一侧：握手、按服务端的流水线（只有 jsonb）编解码"""

    def __init__(self, conn: FakeWsConnection):
        self.conn = conn
        self.pipe = MessagePipeline()
        self.pipe.add_layer(JSONBinaryLayer())
        self.pipe_ctx: Any = None

    async def handshake(self) -> None:
        self.conn.client_send_raw(self.pipe.encode(None, []))
        reply = self.pipe.decode(None, await self.conn.client_recv_raw())
        assert isinstance(reply, list)
        self.pipe_ctx, _reply = self.pipe.handshake(reply)

    def send(self, message: Any) -> None:
        self.conn.client_send_raw(self.pipe.encode(self.pipe_ctx, message))

    async def recv(self, timeout: float = 5) -> Any:
        return self.pipe.decode(self.pipe_ctx, await self.conn.client_recv_raw(timeout))

    async def call(self, *message: Any) -> Any:
        self.send(["rpc", *message])
        return await self.recv()


@pytest.fixture(scope="module")
async def mod_runtime(mod_test_app, mod_backend_config):
    """只连后端、拿租约的 runtime，流水线只有 jsonb。连接相关的全局配置按服务器的默认值设"""
    pipe = ServerMessagePipeline()
    saved_layers = (list(pipe._layers), list(pipe._disabled))
    setup_message_pipeline({"PACKET_LAYERS": [{"type": "jsonb"}]})
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(connection, "MAX_ANONYMOUS_CONNECTION_BY_IP", 0)
        mp.setattr(connection, "ENDPOINT_CALL_IDLE_TIMEOUT", 120)
        mp.setattr(connection, "CONNECTION_ALIVE_RECHECK_INTERVAL", 5)
        runtime = WorkerRuntime(
            {
                "NAMESPACE": "pytest",
                "INSTANCES": [INSTANCE],
                "BACKENDS": {"main": mod_backend_config},
            },
            on_lease_lost=lambda: None,
        )
        await runtime.start_backends()
        try:
            yield runtime
        finally:
            await runtime.stop()
            pipe._layers[:], pipe._disabled[:] = saved_layers


def _serve(runtime: WorkerRuntime, conn: FakeWsConnection, instance: str = INSTANCE):
    return asyncio.create_task(
        websocket_server.serve_connection(runtime, conn, instance),
        name=f"serve:{conn.conn_id}",
    )


async def _connect(runtime: WorkerRuntime) -> tuple[FakeClient, asyncio.Task]:
    conn = FakeWsConnection()
    task = _serve(runtime, conn)
    client = FakeClient(conn)
    await client.handshake()
    return client, task


async def _finished(task: asyncio.Task, timeout: float = 5) -> None:
    """
    等 serve_connection 自己结束，超时算失败。不能用 asyncio.wait_for：它超时会取消 task，而
    serve_connection 被取消时会照常拆完连接、正常返回，卡住了也测不出来
    """
    done, _pending = await asyncio.wait({task}, timeout=timeout)
    if not done:
        task.cancel()
        await asyncio.wait({task}, timeout=timeout)
        pytest.fail(f"serve_connection 没有在 {timeout} 秒内自己结束")
    task.result()  # 有异常就抛出来


async def _connection_rows(runtime: WorkerRuntime) -> int:
    table = runtime.table_managers[INSTANCE].get_table(Connection)
    assert table is not None
    return len(await table.servant_range("id", 0, float("inf"), limit=1000))


async def _wait_rows(runtime: WorkerRuntime, expected: int, timeout: float = 5) -> None:
    """等本 instance 的 Connection 行数变成 expected（servant 读，要等复制）"""
    async with asyncio.timeout(timeout):
        while await _connection_rows(runtime) != expected:
            await asyncio.sleep(0.02)


async def _disconnect_count(runtime: WorkerRuntime, user_id: int) -> int:
    """另开一个连接查断线 System 记的次数"""
    client, task = await _connect(runtime)
    count = (await client.call("get_disconnect_count", user_id))[1]
    client.conn.disconnect()
    await _finished(task, 5)
    return count


# ==== 握手 ====


async def test_handshake_timeout(monkeypatch, mod_runtime):
    """客户端连上来不发握手消息：超时断开，不落 Connection 行"""
    monkeypatch.setattr(websocket_server, "HANDSHAKE_TIMEOUT", 0.05)
    before = await _connection_rows(mod_runtime)
    conn = FakeWsConnection()
    await _finished(_serve(mod_runtime, conn), 5)
    assert conn.close_code == connection.CLOSE_REJECTED
    assert await _connection_rows(mod_runtime) == before


@pytest.mark.parametrize(
    ("label", "frame"),
    [
        ("文本帧", "hello"),
        ("解不开", b"\xff\xfe"),
        ("不是 list", JSONBinaryLayer().encode(None, {"a": 1})),
        ("层数不对", JSONBinaryLayer().encode(None, [b"x"])),
    ],
)
async def test_bad_handshake_rejected(mod_runtime, label, frame):
    """握手消息格式不对：以 CLOSE_REJECTED 断开，不落 Connection 行"""
    before = await _connection_rows(mod_runtime)
    conn = FakeWsConnection()
    task = _serve(mod_runtime, conn)
    conn.client_send_raw(frame)
    await _finished(task, 5)
    assert conn.close_code == connection.CLOSE_REJECTED, label
    assert await _connection_rows(mod_runtime) == before


async def test_unknown_instance_rejected_after_handshake(mod_runtime):
    """路径里的实例名不存在：握手消息收到之后才以 CLOSE_REJECTED 断开（不给扫描器暴露实例信息）"""
    conn = FakeWsConnection()
    task = _serve(mod_runtime, conn, instance="no_such_instance")
    conn.client_send_raw(FakeClient(conn).pipe.encode(None, []))
    await _finished(task, 5)
    assert conn.close_code == connection.CLOSE_REJECTED


async def test_disconnect_before_handshake(mod_runtime):
    """握手之前对端就断了：安静地结束，不抛异常"""
    conn = FakeWsConnection()
    task = _serve(mod_runtime, conn)
    conn.disconnect()
    await _finished(task, 5)
    assert conn.close_code is None


# ==== 各种断开原因的 close 码 ====


async def test_recv_flooding_closes_with_rejected(monkeypatch, mod_runtime):
    """收到的消息超过频率上限：CLOSE_REJECTED，Connection 行删掉"""
    monkeypatch.setitem(mod_runtime.config, "CLIENT_SEND_LIMITS", [[2, 60]])
    before = await _connection_rows(mod_runtime)
    client, task = await _connect(mod_runtime)
    for _ in range(3):
        client.send(["rpc", "report_ctx_timestamp"])
    await _finished(task, 5)
    assert client.conn.close_code == connection.CLOSE_REJECTED
    await _wait_rows(mod_runtime, before)


async def test_send_flooding_closes_with_rejected(monkeypatch, mod_runtime):
    """发出的消息超过频率上限：CLOSE_REJECTED，Connection 行删掉"""
    monkeypatch.setitem(mod_runtime.config, "SERVER_SEND_LIMITS", [[2, 60]])
    before = await _connection_rows(mod_runtime)
    client, task = await _connect(mod_runtime)
    for _ in range(3):
        client.send(["rpc", "report_ctx_timestamp"])
    await _finished(task, 5)
    assert client.conn.close_code == connection.CLOSE_REJECTED
    await _wait_rows(mod_runtime, before)


async def test_kicked_closes_with_kicked(mod_runtime):
    """同一用户在别的连接登录：旧连接收到顶号通知后以 CLOSE_KICKED 断开，新连接照常可用"""
    user_id = 299001
    old, old_task = await _connect(mod_runtime)
    assert await old.call("login", user_id) == ["rsp", {"id": user_id}]
    new, new_task = await _connect(mod_runtime)
    assert await new.call("login", user_id) == ["rsp", {"id": user_id}]
    await _finished(old_task, 10)
    assert old.conn.close_code == connection.CLOSE_KICKED
    assert old.conn.close_reason == connection.CLOSE_KICKED_REASON
    assert await new.call("echo_response", ["alive"]) == ["rsp", ["alive"]]
    new.conn.disconnect()
    await _finished(new_task, 5)


async def test_setup_failure_closes_with_internal_error(monkeypatch, mod_runtime):
    """Connection 行落库之后初始化失败：CLOSE_INTERNAL_ERROR，已落库的行要删掉"""

    class BrokenBroker:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("broker boom")

    monkeypatch.setattr(websocket_server, "SubscriptionBroker", BrokenBroker)
    before = await _connection_rows(mod_runtime)
    client, task = await _connect(mod_runtime)
    await _finished(task, 5)
    assert client.conn.close_code == connection.CLOSE_INTERNAL_ERROR
    await _wait_rows(mod_runtime, before)


async def test_send_error_closes_with_internal_error(monkeypatch, mod_runtime):
    """发送循环出了意外的异常：以 CLOSE_INTERNAL_ERROR 断开（不留给框架按正常关闭 1000 断开）"""

    def broken_encode(self, pipe_ctx, message, until=-1):
        if pipe_ctx is not None:  # 握手的回复照常发
            raise RuntimeError("encode boom")
        return MessagePipeline.encode(self, pipe_ctx, message, until)

    # 只换服务端的，客户端用的是 MessagePipeline
    monkeypatch.setattr(ServerMessagePipeline, "encode", broken_encode)
    before = await _connection_rows(mod_runtime)
    client, task = await _connect(mod_runtime)
    client.send(["rpc", "report_ctx_timestamp"])
    await _finished(task, 5)
    assert client.conn.close_code == connection.CLOSE_INTERNAL_ERROR
    await _wait_rows(mod_runtime, before)


# ==== 收发途中断开 ====


async def test_peer_disconnect_runs_cleanup(mod_runtime):
    """收的途中对端断开：拆连接跑完，断线 System 以用户身份跑，Connection 行删掉"""
    user_id = 299002
    before = await _connection_rows(mod_runtime)
    client, task = await _connect(mod_runtime)
    assert await client.call("login", user_id) == ["rsp", {"id": user_id}]
    client.conn.disconnect()
    await _finished(task, 5)
    await _wait_rows(mod_runtime, before)
    assert await _disconnect_count(mod_runtime, user_id) == 1


async def test_peer_gone_while_sending_runs_cleanup(mod_runtime):
    """发的途中发现对端已断开（send 抛 ConnectionClosed）：拆连接跑完，Connection 行删掉"""
    before = await _connection_rows(mod_runtime)
    client, task = await _connect(mod_runtime)
    conn = client.conn

    async def gone(data):
        raise ConnectionClosed

    conn.send = gone  # 只有发送这一侧发觉了，接收协程还在等下一条消息
    client.send(["rpc", "report_ctx_timestamp"])
    await _finished(task, 5)
    assert conn.close_code is None
    await _wait_rows(mod_runtime, before)


async def test_cleanup_runs_when_queue_full_and_send_stuck(monkeypatch, mod_runtime):
    """
    对端不读数据，发送循环卡在 send 上；接收协程这时出错结束，队列满着，PUSH_CLOSE 塞不进去。
    以前只等发送循环，两边都等不到对方，清理永远不执行（aiohttp 上不会有框架来取消连接协程）。
    现在收发任意一边结束就拆：Connection 行要删掉
    """
    monkeypatch.setattr(websocket_server, "PUSH_QUEUE_SIZE", 2)
    before = await _connection_rows(mod_runtime)
    client, task = await _connect(mod_runtime)
    client.conn.stuck = True
    # 第一条的回复被发送循环取走、卡在 send 上，后两条的回复把队列塞满
    for _ in range(3):
        client.send(["rpc", "report_ctx_timestamp"])
    client.conn.client_send_raw("not binary")  # 接收协程以 CLOSE_REJECTED 结束
    await _finished(task, 5)
    assert client.conn.close_code == connection.CLOSE_REJECTED
    await _wait_rows(mod_runtime, before)


async def test_framework_cancel_stops_both_sides_before_unsubscribe(
    monkeypatch, mod_runtime
):
    """
    框架取消连接协程（Sanic：传输层断开、关服）时，照常拆完连接，取消不往外抛。asyncio.wait 被取消
    时不会取消它在等的收发协程，清理要先把这两个停下来，再退订、删行
    """
    from hetu.data.sub import SubscriptionBroker

    running_at_close: list[list[str]] = []
    close = SubscriptionBroker.close

    async def spying_close(self):
        names = {f"client_handler:{conn_id}", f"ws_send:{conn_id}"}
        running_at_close.append(
            [t.get_name() for t in asyncio.all_tasks() if t.get_name() in names]
        )
        await close(self)

    monkeypatch.setattr(SubscriptionBroker, "close", spying_close)
    user_id = 299003
    before = await _connection_rows(mod_runtime)
    client, task = await _connect(mod_runtime)
    conn_id = client.conn.conn_id
    assert await client.call("login", user_id) == ["rsp", {"id": user_id}]
    task.cancel()
    await _finished(task, 5)
    assert not task.cancelled()
    assert running_at_close == [[]]
    await _wait_rows(mod_runtime, before)
    assert await _disconnect_count(mod_runtime, user_id) == 1


async def test_protocol_error_is_logged(mod_runtime, caplog):
    """连接上的协议错误（帧格式错、消息超长等）要打出来；对端正常掉线不打"""
    caplog.set_level(logging.INFO, logger="HeTu.root")

    def logged() -> list[logging.LogRecord]:
        return [
            r
            for r in caplog.records
            if r.name == "HeTu.root" and "WS协议异常" in r.getMessage()
        ]

    error = ValueError("bad frame")
    client, task = await _connect(mod_runtime)
    client.conn.error = error
    client.conn.disconnect()
    await _finished(task, 5)
    records = logged()
    assert len(records) == 1
    assert records[0].exc_info is not None and records[0].exc_info[1] is error

    caplog.clear()
    client, task = await _connect(mod_runtime)
    client.conn.disconnect()
    await _finished(task, 5)
    assert logged() == []


async def test_motd_is_sent_as_text(mod_runtime):
    """motd 回一条明文文本帧（不走流水线），之后连接照常可用"""
    import hetu

    client, task = await _connect(mod_runtime)
    client.send(["motd"])
    assert await client.conn.client_recv_raw() == (
        f"👋 Welcome to HeTu Database! v{hetu.__version__}"
    )
    assert await client.call("echo_response", ["after motd"]) == ["rsp", ["after motd"]]
    client.conn.disconnect()
    await _finished(task, 5)
