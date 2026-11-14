"""
serve_connection 的单元测试：用内存里的假连接（FakeWsConnection），不走网络、不起 Web 框架，直接驱动
握手、收发和拆连接（设计稿 docs/superpowers/specs/2026-11-14-aiohttp-migration-design.md §5.3）。

假连接按 aiohttp 的语义实现：fail 之后、对端断开之后，新的收发立即抛 ConnectionClosed，不靠框架
取消连接的协程；正卡在发送上的 send 要等 abort 兜底（ABORT_DELAY 秒）才醒。走真网络、跑在 Sanic 上的
测试在 test_websocket.py。
"""

import asyncio
import logging
from typing import Any

import pytest
from fixtures.backends import REDIS_BACKENDS, SQLITE_BACKENDS

from hetu.endpoint import connection
from hetu.endpoint.connection import Connection
from hetu.server import websocket as websocket_server
from hetu.server.conn import ConnectionClosed, Draining, next_conn_id
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
# 连接关了之后，正卡在发送上的 send 过多久被 abort 叫醒（对应 aiohttp 适配层 fail 的 abort 兜底）
ABORT_DELAY = 3.0
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
        # 对端不读数据：send 一直卡在 drain 上，连接关了之后要到 abort 兜底才醒
        self.stuck = False
        # 每次 send 要花多久（秒），模拟慢链路：发送协程落在接收协程后面
        self.send_delay = 0.0
        # protocol_error() 的返回值
        self.error: BaseException | None = None
        self._closed = False
        self._aborted = asyncio.Event()
        self._inbox: asyncio.Queue[Any] = asyncio.Queue()
        self._outbox: asyncio.Queue[Any] = asyncio.Queue()
        # 关服排空，照 AiohttpWsConnection 的做法：stop_receiving 用取消叫醒正在等的 recv
        self._draining = False
        self._receiving: asyncio.Task | None = None
        self._wakeup_pending = False

    # ---- WsConnection ----

    async def recv(self, timeout: float | None = None) -> str | bytes | None:
        if self._closed:
            raise ConnectionClosed
        if self._draining:
            raise Draining
        task = asyncio.current_task()
        self._receiving = task
        try:
            async with asyncio.timeout(timeout):
                item = await self._inbox.get()
        except TimeoutError:
            return None
        except asyncio.CancelledError:
            if not self._wakeup_pending:
                raise
            self._wakeup_pending = False
            if task is not None and task.uncancel() > 0:
                raise
            raise Draining from None
        finally:
            self._receiving = None
        if item is _CLOSED:
            raise ConnectionClosed
        return item

    def stop_receiving(self) -> None:
        """关服排空：之后不再交出消息（排在 inbox 里的留着不读），正在等的 recv 醒来抛 Draining"""
        if self._draining:
            return
        self._draining = True
        if self._receiving is not None and not self._wakeup_pending:
            self._wakeup_pending = True
            self._receiving.cancel()

    async def send(self, data: str | bytes) -> None:
        if self._closed:
            raise ConnectionClosed
        if self.stuck:
            await self._aborted.wait()
            raise ConnectionClosed
        if self.send_delay:
            await asyncio.sleep(self.send_delay)
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
            asyncio.get_running_loop().call_later(ABORT_DELAY, self._aborted.set)

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


async def _finished(
    task: asyncio.Task, timeout: float = 5, *, cancelled: bool = False
) -> None:
    """
    等 serve_connection 自己结束，超时算失败。不能用 asyncio.wait_for：它超时会取消 task，而
    serve_connection 被取消时会照常拆完连接、正常返回，卡住了也测不出来。cancelled 为真时要求它
    以取消结束，否则要求正常返回
    """
    done, _pending = await asyncio.wait({task}, timeout=timeout)
    if not done:
        task.cancel()
        await asyncio.wait({task}, timeout=timeout)
        pytest.fail(f"serve_connection 没有在 {timeout} 秒内自己结束")
    if cancelled:
        assert task.cancelled()
    else:
        task.result()  # 有异常就抛出来


def _spy_disconnect_system(monkeypatch) -> list[int]:
    """记下拆连接时跑了几次断线 System（记的是 connection_id），照常调原来的"""
    calls: list[int] = []
    call = websocket_server._call_disconnect_system

    async def spy(system_caller, context, endpoint_executor):
        calls.append(context.connection_id)
        await call(system_caller, context, endpoint_executor)

    monkeypatch.setattr(websocket_server, "_call_disconnect_system", spy)
    return calls


def _receiver_exit_logs(caplog) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == "HeTu.root" and "接收协程结束" in r.getMessage()
    ]


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


async def test_kicked_closes_with_kicked(mod_runtime, caplog):
    """
    同一用户在别的连接登录：旧连接收到顶号通知后以 CLOSE_KICKED 断开、拆完（Connection 行删掉），
    新连接照常可用。旧连接的接收协程发现连接已关，安静地退出（不再记"接收协程结束"）
    """
    caplog.set_level(logging.INFO, logger="HeTu.root")
    user_id = 299001
    before = await _connection_rows(mod_runtime)
    old, old_task = await _connect(mod_runtime)
    assert await old.call("login", user_id) == ["rsp", {"id": user_id}]
    new, new_task = await _connect(mod_runtime)
    assert await new.call("login", user_id) == ["rsp", {"id": user_id}]
    await _finished(old_task, 10)
    assert old.conn.close_code == connection.CLOSE_KICKED
    assert old.conn.close_reason == connection.CLOSE_KICKED_REASON
    await _wait_rows(mod_runtime, before + 1)  # 只剩新连接那一行
    assert await new.call("echo_response", ["alive"]) == ["rsp", ["alive"]]
    assert _receiver_exit_logs(caplog) == []
    new.conn.disconnect()
    await _finished(new_task, 5)
    await _wait_rows(mod_runtime, before)


async def test_setup_failure_closes_with_internal_error(monkeypatch, mod_runtime):
    """Connection 行落库之后初始化失败：CLOSE_INTERNAL_ERROR，已落库的行要删掉，断线 System 照常跑"""

    class BrokenBroker:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("broker boom")

    monkeypatch.setattr(websocket_server, "SubscriptionBroker", BrokenBroker)
    disconnects = _spy_disconnect_system(monkeypatch)
    before = await _connection_rows(mod_runtime)
    client, task = await _connect(mod_runtime)
    await _finished(task, 5)
    assert client.conn.close_code == connection.CLOSE_INTERNAL_ERROR
    await _wait_rows(mod_runtime, before)
    assert len(disconnects) == 1  # initialize 走完了，连接算建立过


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


async def test_peer_disconnect_runs_cleanup(mod_runtime, caplog):
    """
    收的途中对端断开：拆连接跑完，断线 System 以用户身份跑，Connection 行删掉。正常掉线不算接收
    协程出错，不记"接收协程结束"（拆连接时会记"连接断开"）
    """
    caplog.set_level(logging.INFO, logger="HeTu.root")
    user_id = 299002
    before = await _connection_rows(mod_runtime)
    client, task = await _connect(mod_runtime)
    assert await client.call("login", user_id) == ["rsp", {"id": user_id}]
    client.conn.disconnect()
    await _finished(task, 5)
    await _wait_rows(mod_runtime, before)
    assert await _disconnect_count(mod_runtime, user_id) == 1
    assert _receiver_exit_logs(caplog) == []
    assert any(
        f"连接断开：{client.conn.conn_id}" in r.getMessage() for r in caplog.records
    )


async def test_peer_gone_while_sending_runs_cleanup(mod_runtime):
    """
    发的途中发现对端已断开（send 抛 ConnectionClosed）：拆连接跑完，断线 System 以用户身份跑，
    Connection 行删掉
    """
    user_id = 299004
    before = await _connection_rows(mod_runtime)
    client, task = await _connect(mod_runtime)
    assert await client.call("login", user_id) == ["rsp", {"id": user_id}]
    conn = client.conn

    async def gone(data):
        raise ConnectionClosed

    conn.send = gone  # 只有发送这一侧发觉了，接收协程还在等下一条消息
    client.send(["rpc", "report_ctx_timestamp"])
    await _finished(task, 5)
    assert conn.close_code is None
    await _wait_rows(mod_runtime, before)
    assert await _disconnect_count(mod_runtime, user_id) == 1


async def test_cleanup_is_prompt_when_queue_full_and_send_stuck(
    monkeypatch, mod_runtime
):
    """
    对端不读数据，发送循环卡在 send 上；接收协程这时出错结束，队列满着，PUSH_CLOSE 塞不进去。
    以前只等发送循环：aiohttp 上没有框架来取消连接协程，要等 fail 的 abort 兜底把卡住的 send
    叫醒才能拆连接，还会多一条发送异常。现在收发任意一边结束就拆，远早于 abort 兜底
    """
    monkeypatch.setattr(websocket_server, "PUSH_QUEUE_SIZE", 2)
    before = await _connection_rows(mod_runtime)
    client, task = await _connect(mod_runtime)
    client.conn.stuck = True
    # 第一条的回复被发送循环取走、卡在 send 上，后两条的回复把队列塞满
    for _ in range(3):
        client.send(["rpc", "report_ctx_timestamp"])
    client.conn.client_send_raw("not binary")  # 接收协程以 CLOSE_REJECTED 结束
    await _finished(task, ABORT_DELAY / 2)
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
    assert task.cancelling() == 0  # 吞掉的取消请求已经撤销
    assert running_at_close == [[]]
    await _wait_rows(mod_runtime, before)
    assert await _disconnect_count(mod_runtime, user_id) == 1


async def test_cancel_during_initialize_still_deletes_connection_row(
    monkeypatch, mod_runtime
):
    """
    Connection 行已经提交、initialize 还在等副本同步时，连接协程被取消（Sanic：客户端握手完立刻
    断开）：拆连接照样要把这一行删掉，删之前先等副本同步；连接没建立起来，不跑断线 System。以前
    initialize 在拆连接的 try 外面、行 id 也是返回之后才记下，这一行就永远留在库里
    """
    from hetu.data.backend import Backend

    entered = asyncio.Event()
    calls: list[int] = []
    wait_for_synced = Backend.wait_for_synced

    async def blocked_once(self):
        calls.append(1)
        if not entered.is_set():
            entered.set()
            await asyncio.Event().wait()  # 卡在这里，直到被取消
        await wait_for_synced(self)

    monkeypatch.setattr(Backend, "wait_for_synced", blocked_once)
    disconnects = _spy_disconnect_system(monkeypatch)
    before = await _connection_rows(mod_runtime)
    conn = FakeWsConnection()
    task = _serve(mod_runtime, conn)
    await FakeClient(conn).handshake()
    await asyncio.wait_for(entered.wait(), 5)
    await _wait_rows(mod_runtime, before + 1)  # 行已经提交（副本上也看得到了）
    task.cancel()
    await _finished(task, 5, cancelled=True)
    await _wait_rows(mod_runtime, before)
    # 删行之前又等了一次副本同步：这一行刚提交时副本上可能还没有，随机读到副本就删不掉
    assert len(calls) == 2
    # 连接没建立起来（initialize 没走完），游戏逻辑从没见过它，不跑断线 System
    assert disconnects == []


async def test_anonymous_limit_rejection_does_no_cleanup_work(monkeypatch, mod_runtime):
    """
    同一 IP 的匿名连接数到了上限，新连接被拒：以 1011 断开。拆连接不碰库：不跑断线 System、不等
    副本同步、不删行。这个限制是防刷连接的，被拒的连接不能让库（master）多干活
    """
    from hetu.data.backend import Backend
    from hetu.endpoint import executor as executor_module

    monkeypatch.setattr(connection, "MAX_ANONYMOUS_CONNECTION_BY_IP", 1)
    first = FakeWsConnection(client_ip="10.9.9.9")
    first_task = _serve(mod_runtime, first)
    first_client = FakeClient(first)
    await first_client.handshake()
    # 回了 rsp 说明 initialize 已经走完：这条匿名连接已经落库、副本也同步了
    assert await first_client.call("report_ctx_timestamp") == ["rsp", "ok"]

    work: list[str] = []
    wait_for_synced = Backend.wait_for_synced

    async def counting_synced(self):
        work.append("wait_for_synced")
        await wait_for_synced(self)

    async def counting_del(*args, **kwargs):
        work.append("del_connection")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(Backend, "wait_for_synced", counting_synced)
        mp.setattr(executor_module, "del_connection", counting_del)
        disconnects = _spy_disconnect_system(mp)
        second = FakeWsConnection(client_ip="10.9.9.9")
        second_task = _serve(mod_runtime, second)
        await FakeClient(second).handshake()
        await _finished(second_task, 5)
    assert second.close_code == connection.CLOSE_INTERNAL_ERROR
    assert work == []
    assert disconnects == []

    first.disconnect()
    await _finished(first_task, 5)


# ==== 关服排空（第 4b 步）====


async def _wait_for(predicate, timeout: float = 5) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def _received_until_closed(client: FakeClient, timeout: float = 5) -> list[Any]:
    """收客户端这边的消息，直到连接被服务端关掉"""
    received: list[Any] = []
    async with asyncio.timeout(timeout):
        while not (client.conn._closed and client.conn._outbox.empty()):
            try:
                received.append(await client.recv(timeout=0.05))
            except TimeoutError:
                pass
    return received


async def test_drain_idle_connection_closes_with_going_away(mod_runtime):
    """排空时空闲的连接：正在等的 recv 醒来，马上以 1001 断开，照常拆完（断线 System 以用户身份跑）"""
    user_id = 299101
    before = await _connection_rows(mod_runtime)
    client, task = await _connect(mod_runtime)
    assert await client.call("login", user_id) == ["rsp", {"id": user_id}]
    client.conn.stop_receiving()
    await _finished(task, 2)
    assert client.conn.close_code == connection.CLOSE_GOING_AWAY
    await _wait_rows(mod_runtime, before)
    assert await _disconnect_count(mod_runtime, user_id) == 1


async def test_drain_finishes_inflight_rpc_and_skips_unread(monkeypatch, mod_runtime):
    """
    排空时正在执行的 RPC 照常执行完、回复发出去，再以 1001 断开；排在后面、还没读的消息不执行（客户端
    重连后重发也不会重复执行）
    """
    from hetu.server import receiver

    calls: list[Any] = []
    entered, release = asyncio.Event(), asyncio.Event()
    rpc = receiver.rpc

    async def gated_rpc(data, *args, **kwargs):
        calls.append(data[2])
        if not entered.is_set():
            entered.set()
            await release.wait()
        return await rpc(data, *args, **kwargs)

    monkeypatch.setattr(receiver, "rpc", gated_rpc)
    client, task = await _connect(mod_runtime)
    # 慢链路：回复还在发的时候接收协程就已经停下了，不能这时就拆连接（发送协程会被取消，回复就丢了）
    client.conn.send_delay = 0.2
    client.send(["rpc", "echo_response", ["first"]])
    client.send(["rpc", "echo_response", ["second"]])
    async with asyncio.timeout(5):
        await entered.wait()
    client.conn.stop_receiving()
    release.set()
    assert await _received_until_closed(client) == [["rsp", ["first"]]]
    await _finished(task, 5)
    assert client.conn.close_code == connection.CLOSE_GOING_AWAY
    assert calls == [["first"]]


async def test_drain_with_full_queue_keeps_all_replies(monkeypatch, mod_runtime):
    """
    排空时队列是满的（链路慢）：PUSH_DRAINED 也要等着塞进去，不能丢。丢了的话接收协程带着异常退出、
    连接直接拆掉，已经执行的调用的回复就都发不出去了
    """
    from hetu.server import receiver

    monkeypatch.setattr(websocket_server, "PUSH_QUEUE_SIZE", 2)
    calls: list[Any] = []
    rpc = receiver.rpc

    async def counting_rpc(data, *args, **kwargs):
        calls.append(data[2])
        return await rpc(data, *args, **kwargs)

    monkeypatch.setattr(receiver, "rpc", counting_rpc)
    client, task = await _connect(mod_runtime)
    client.conn.send_delay = 0.2
    for i in range(6):
        client.send(["rpc", "echo_response", [i]])
    await _wait_for(lambda: len(calls) >= 4)  # 队列满着，接收协程卡在塞回复上
    client.conn.stop_receiving()
    received = await _received_until_closed(client, timeout=10)
    await _finished(task, 5)
    assert len(calls) >= 4
    assert received == [["rsp", c] for c in calls]
    assert client.conn.close_code == connection.CLOSE_GOING_AWAY


async def test_drain_waits_for_pending_subscription_reply(monkeypatch, mod_runtime):
    """排空时在后台等着的订阅照常做完，订阅的回复发出去之后才以 1001 断开"""
    from hetu.data import sub as sub_module

    reading, release = asyncio.Event(), asyncio.Event()
    real_read = sub_module.read_whole_table_

    async def gated_read(*args, **kwargs):
        reading.set()
        await release.wait()
        return await real_read(*args, **kwargs)

    monkeypatch.setattr(sub_module, "read_whole_table_", gated_read)
    client, task = await _connect(mod_runtime)
    client.send(["sub", "PublicNames", "table"])
    async with asyncio.timeout(5):
        await reading.wait()
    client.conn.stop_receiving()
    await asyncio.sleep(0.1)
    assert not task.done()  # 在等订阅
    release.set()
    received = await _received_until_closed(client)
    await _finished(task, 5)
    assert [msg[0] for msg in received] == ["sub"]
    assert client.conn.close_code == connection.CLOSE_GOING_AWAY


async def test_drain_before_handshake_ends_quietly(mod_runtime):
    """握手之前就开始排空：安静地结束，不落 Connection 行（断开由适配层收尾）"""
    before = await _connection_rows(mod_runtime)
    conn = FakeWsConnection()
    conn.stop_receiving()
    await _finished(_serve(mod_runtime, conn), 2)
    assert await _connection_rows(mod_runtime) == before


async def test_drain_fallback_unblocks_stuck_sender(mod_runtime):
    """
    排空时发送卡住（对端不读数据）：serve_connection 一直等发送那一边。适配层的兜底（fail 再取消收发
    协程，见 AiohttpServer.stop）照样能把它收掉，Connection 行删掉
    """
    before = await _connection_rows(mod_runtime)
    client, task = await _connect(mod_runtime)
    client.conn.stuck = True
    client.send(["rpc", "report_ctx_timestamp"])  # 回复卡在发送上
    await asyncio.sleep(0.1)
    client.conn.stop_receiving()
    await asyncio.sleep(0.3)
    assert not task.done()
    client.conn.fail(connection.CLOSE_GOING_AWAY, "")
    await mod_runtime.tasks.cancel_all(2)
    await _finished(task, 2)
    await _wait_rows(mod_runtime, before)


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
