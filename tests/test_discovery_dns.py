"""
网关上的 DNS 桥 hetu dns（hetu/discovery/dns.py，设计稿 docs/superpowers/specs/2026-11-15-node-discovery-design.md
§6）：报文（用 dnspython 构造查询、解析回答，当对照实现）、答哪些地址、探测、启动检查，以及真跑起来的 hetu dns。

报文、选地址、探测的用例不需要 Docker；启动检查和真跑的用例要 Redis 一主一从（原生集群那条要集群）。
"""

import asyncio
import contextlib
import os
import socket
import struct
import subprocess
import sys
import time
from typing import Any

import dns.exception
import dns.flags
import dns.message
import dns.opcode
import dns.query
import dns.rcode
import dns.rdataclass
import dns.rdatatype
import pytest
from fixtures.backends import REDIS_BACKENDS

from hetu.discovery import dns as hdns
from hetu.discovery.dns import (
    ACL_COMMAND,
    DnsCheckFailed,
    DnsService,
    NodeAddress,
    Prober,
    Selector,
    check_anonymous_access,
    check_listen,
    check_network,
    check_redis,
    parse_query,
)
from hetu.discovery.registry import (
    REGISTRY_KEY,
    STATE_DRAINING,
    STATE_READY,
    NodeEntry,
    encode_entry,
)

NAME = "hetu.internal"


def addresses(count: int) -> list[str]:
    return [f"10.0.{i // 250}.{i % 250 + 1}" for i in range(count)]


def make_service(addrs: list[str] | None = None, **kwargs: Any) -> DnsService:
    service = DnsService(None, namespace="game", **kwargs)
    service.addresses = tuple(socket.inet_aton(a) for a in addrs or [])
    return service


def ask(service: DnsService, query: dns.message.Message) -> dns.message.Message:
    wire = service.respond(query.to_wire())
    assert wire is not None
    response = dns.message.from_wire(wire)
    # 从不设 TC：Caddy 不会改用 TCP 重问，截断的回答会被照单全收
    assert not response.flags & dns.flags.TC
    return response


def answers(response: dns.message.Message) -> list[str]:
    return [rdata.to_text() for rrset in response.answer for rdata in rrset]


def a_query(name: str = NAME, rdtype: str = "A", **kwargs: Any) -> dns.message.Message:
    kwargs.setdefault("use_edns", False)
    return dns.message.make_query(name, rdtype, **kwargs)


@contextlib.contextmanager
def hetu_logs(caplog, level: str = "WARNING"):
    """收 HeTu.root 的日志：起服类测试会把它的 propagate 关掉，caplog 收不到"""
    propagate, hdns.logger.propagate = hdns.logger.propagate, True
    try:
        with caplog.at_level(level, logger="HeTu.root"):
            yield
    finally:
        hdns.logger.propagate = propagate


# ---------------------------------------------------------------- 报文


def test_answers_the_name_with_every_address():
    service = make_service(["10.0.3.17", "10.0.3.18"])
    query = a_query()
    response = ask(service, query)
    assert response.id == query.id
    assert response.rcode() == dns.rcode.NOERROR
    assert response.flags & dns.flags.QR
    assert response.flags & dns.flags.AA
    assert response.flags & dns.flags.RD
    assert not response.flags & dns.flags.RA
    assert answers(response) == ["10.0.3.17", "10.0.3.18"]
    assert response.answer[0].ttl == 0
    assert response.question[0].name.to_text() == NAME + "."
    assert response.edns == -1  # 查询没带 EDNS，回答也不带


def test_name_is_case_insensitive_and_echoed_as_asked():
    response = ask(make_service(["10.0.3.17"]), a_query("HeTu.Internal"))
    assert answers(response) == ["10.0.3.17"]
    assert response.question[0].name.to_text() == "HeTu.Internal."


def test_rd_is_echoed():
    query = a_query()
    query.flags &= ~dns.flags.RD
    assert not ask(make_service(["10.0.3.17"]), query).flags & dns.flags.RD


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({}, 30),  # 没带 EDNS：512 字节
        ({"use_edns": 0, "payload": 1232}, 74),  # Go 解析器声明的就是 1232
        ({"use_edns": 0, "payload": 4096}, 74),  # 声明得再大也按 1232
        ({"use_edns": 0, "payload": 600}, 34),
        ({"use_edns": 0, "payload": 100}, 29),  # 小于 512 的按 512，还要放 OPT
    ],
)
def test_answer_always_fits_one_packet(kwargs, expected):
    service = make_service(addresses(200))
    query = a_query(**kwargs)
    wire = service.respond(query.to_wire())
    assert wire is not None
    response = dns.message.from_wire(wire)
    assert len(answers(response)) == expected
    assert len(wire) <= max(512, min(kwargs.get("payload", 512), 1232))
    assert not response.flags & dns.flags.TC
    if kwargs:
        assert response.edns == 0
        assert response.payload == 1232


def test_any_gets_the_addresses_other_types_get_nodata():
    service = make_service(["10.0.3.17"])
    assert answers(ask(service, a_query(rdtype="ANY"))) == ["10.0.3.17"]
    for rdtype in ("AAAA", "MX", "TXT"):
        response = ask(service, a_query(rdtype=rdtype))
        assert response.rcode() == dns.rcode.NOERROR
        assert answers(response) == []


def test_other_names_get_nxdomain(caplog):
    service = make_service(["10.0.3.17"])
    hetu_logger = hdns.logger
    propagate, hetu_logger.propagate = hetu_logger.propagate, True
    try:
        with caplog.at_level("WARNING", logger="HeTu.root"):
            for _i in range(2):
                assert (
                    ask(service, a_query("game.internal")).rcode() == dns.rcode.NXDOMAIN
                )
            # Go 解析器答空时会拼上 resolv.conf 的搜索域再问：答 NXDOMAIN，不告警
            searched = a_query(NAME + ".ec2.internal")
            assert ask(service, searched).rcode() == dns.rcode.NXDOMAIN
    finally:
        hetu_logger.propagate = propagate
    warnings = [r.getMessage() for r in caplog.records if "别的名字" in r.getMessage()]
    assert len(warnings) == 1
    assert "game.internal" in warnings[0]


def test_other_classes_and_opcodes_get_notimp():
    service = make_service(["10.0.3.17"])
    chaos = dns.message.make_query(NAME, "A", rdclass=dns.rdataclass.CH, use_edns=False)
    assert ask(service, chaos).rcode() == dns.rcode.NOTIMP
    notify = a_query()
    notify.set_opcode(dns.opcode.NOTIFY)
    response = ask(service, notify)
    assert response.rcode() == dns.rcode.NOTIMP
    assert response.opcode() == dns.opcode.NOTIFY


def _header(qdcount: int = 1, *, flags: int = 0x0100, arcount: int = 0) -> bytes:
    return struct.pack("!6H", 0x1234, flags, qdcount, 0, 0, arcount)


QUESTION = b"\x04hetu\x08internal\x00" + struct.pack("!HH", 1, 1)


@pytest.mark.parametrize(
    "wire",
    [
        _header(2) + QUESTION + QUESTION,  # 两个问题
        _header(0),  # 没有问题
        _header() + b"\xc0\x0c" + struct.pack("!HH", 1, 1),  # 问题里的压缩指针
        _header() + b"\x04hetu\x08inte",  # 名字不完整
        _header() + b"\x04hetu\x08internal\x00\x00",  # 类型、类别不完整
        _header(arcount=1) + QUESTION + b"\x00\x00\x29",  # OPT 不完整
    ],
)
def test_malformed_queries_get_formerr(wire):
    response = make_service(["10.0.3.17"]).respond(wire)
    assert response is not None
    qid, flags, _qd, ancount, _ns, _ar = struct.unpack_from("!6H", response)
    assert qid == 0x1234
    assert flags & 0x8000
    assert flags & 0xF == dns.rcode.FORMERR
    assert ancount == 0


OPT = b"\x00" + struct.pack("!HHIH", 41, 1232, 0, 0)


def _name(*lengths: int) -> bytes:
    return b"".join(bytes([n]) + b"a" * n for n in lengths) + b"\x00"


def test_rfc_details_of_edns_and_name_length():
    """
    RFC 6891：查询里有两条 OPT 回 FORMERR，EDNS 版本不是 0 回 BADVERS；RFC 1035：名字连长度字节和结尾的 0
    最多 255 字节
    """
    service = make_service(["10.0.3.17"])
    two_opts = service.respond(_header(arcount=2) + QUESTION + OPT + OPT)
    assert two_opts is not None
    assert struct.unpack_from("!6H", two_opts)[1] & 0xF == dns.rcode.FORMERR
    response = ask(service, a_query(use_edns=1, payload=1232))
    assert response.rcode() == dns.rcode.BADVERS
    assert answers(response) == []
    longest = _header() + _name(63, 63, 63, 61) + struct.pack("!HH", 1, 1)  # 255 字节
    too_long = _header() + _name(63, 63, 63, 62) + struct.pack("!HH", 1, 1)  # 256 字节
    assert struct.unpack_from("!6H", service.respond(longest) or b"")[1] & 0xF == (
        dns.rcode.NXDOMAIN
    )
    assert struct.unpack_from("!6H", service.respond(too_long) or b"")[1] & 0xF == (
        dns.rcode.FORMERR
    )


@pytest.mark.parametrize(
    "wire",
    [b"", b"\x12\x34\x01", _header(flags=0x8000) + QUESTION],  # 报头不完整、本身是回答
)
def test_garbage_and_responses_are_dropped(wire):
    assert make_service(["10.0.3.17"]).respond(wire) is None


def test_parse_query_reads_the_edns_payload():
    wire = a_query(use_edns=0, payload=1232).to_wire()
    query = parse_query(wire)
    assert query is not None
    assert (query.name, query.qtype, query.edns) == (b"hetu.internal", 1, 1232)


# ---------------------------------------------------------------- 答哪些地址


class Clock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


WALL = 1_763_190_000.0


def entry(**overrides: Any) -> bytes:
    fields: dict[str, Any] = {
        "node": "m1:0123456789abcdef:w0:1",
        "ns": "game",
        "inst": ["s1"],
        "addr": "10.0.3.17",
        "port": 2466,
        "state": STATE_READY,
        "conns": 5,
        "ts": WALL,
        "up": WALL - 100,
        "ver": "1.4.0",
    }
    fields.update(overrides)
    return encode_entry(NodeEntry(**fields))


def make_selector() -> tuple[Selector, Clock, Clock]:
    mono, wall = Clock(100.0), Clock(WALL)
    return Selector("game", 2466, clock=mono, wall=wall), mono, wall


def always(_addr: str) -> bool:
    return True


def answered(selection) -> list[str]:
    return [node.addr for node in selection.addresses]


def test_staleness_follows_progress_not_clocks(caplog):
    """
    过期只看 ts 有没有往前走：节点的时钟慢了 100 秒也照答，只告警一次；15 秒没往前走就过期。另放一个一直在写
    的节点，免得触发"全都过期就沿用上一次的答案"
    """
    selector, mono, wall = make_selector()

    def update(ts: float):
        mono.now += 5
        wall.now += 5
        live = entry(addr="10.0.3.99", ts=wall.now)
        return selector.update({b"0": entry(ts=ts), b"9": live}, always)

    with hetu_logs(caplog):
        # 第一次见到：按 ts 算它 100 秒没写了
        selection = update(WALL - 95)
        assert "10.0.3.17" not in answered(selection)
        assert "100 秒" in selection.excluded["10.0.3.17"]
        # 之后每 5 秒写一次，ts 往前走：照答，时钟差多少都没关系
        for step in range(1, 5):
            assert "10.0.3.17" in answered(update(WALL - 95 + 5 * step))
        # 不写了：15 秒没往前走就过期
        for _i in range(4):
            selection = update(WALL - 75)
        assert "10.0.3.17" not in answered(selection)
        assert "没更新" in selection.excluded["10.0.3.17"]
    skews = [r.getMessage() for r in caplog.records if "时钟" in r.getMessage()]
    assert len(skews) == 1, skews
    assert "10.0.3.17" in skews[0]


def test_dead_node_flipping_between_two_replicas_still_goes_stale():
    """
    经代理轮询两台副本，其中一台和 master 断开了还在给旧数据：死节点的值在新旧两份之间来回变。只看值变没变的话
    它永远"活着"；按 ts 有没有往前走判断，照样过期
    """
    selector, mono, wall = make_selector()
    old, last = entry(ts=WALL - 4), entry(ts=WALL)
    selection = None
    for step in range(20):
        mono.now += 1
        wall.now += 1
        live = entry(addr="10.0.3.99", ts=wall.now)
        selection = selector.update(
            {b"0": old if step % 2 else last, b"9": live}, always
        )
    assert selection is not None
    assert answered(selection) == ["10.0.3.99"]


def test_new_process_counts_as_a_write_even_with_an_older_ts():
    """同一个 worker id 换了一个进程（重启、被接手）：算一次新的写入，哪怕它的时钟慢、ts 比原来的小"""
    selector, mono, wall = make_selector()
    selector.update({b"0": entry(ts=WALL)}, always)
    mono.now += 20
    wall.now += 20
    restarted = entry(ts=WALL - 10, up=WALL - 12, node="m1:0123456789abcdef:w0:2")
    assert answered(selector.update({b"0": restarted}, always)) == ["10.0.3.17"]


def test_old_entries_are_not_mistaken_for_clock_skew(caplog):
    """
    死掉的 worker 的登记会一直留着：第一次见到时按 ts 算出它多久没写了，不当成时钟差告警。别的 namespace 的、
    5 秒多以前刚写过的也不报
    """
    selector, _mono, _wall = make_selector()
    raw = {
        b"0": entry(ts=WALL - 600),
        b"1": entry(addr="10.0.9.9", ns="other", ts=WALL - 3600),
        b"2": entry(addr="10.0.3.18", ts=WALL - 5.2),
    }
    with hetu_logs(caplog):
        selection = selector.update(raw, always)
    assert "600 秒" in selection.excluded["10.0.3.17"]
    assert answered(selection) == ["10.0.3.18"]
    assert not [r for r in caplog.records if "时钟" in r.getMessage()]


def test_entries_seen_first_long_after_their_last_write_are_stale():
    selector, _mono, _wall = make_selector()
    selection = selector.update({b"0": entry(ts=WALL - 60)}, always)
    assert answered(selection) == []
    assert "60 秒没更新" in selection.excluded["10.0.3.17"]
    assert selection.kept_previous  # 本 namespace 的全过期了：沿用上一次（空的）


def test_clock_skew_is_reported_once(caplog):
    selector, mono, _wall = make_selector()
    hetu_logger = hdns.logger
    propagate, hetu_logger.propagate = hetu_logger.propagate, True
    try:
        with caplog.at_level("WARNING", logger="HeTu.root"):
            for step in range(3):
                mono.now += 5
                selector.update({b"0": entry(ts=WALL + 30 + step)}, always)
    finally:
        hetu_logger.propagate = propagate
    skews = [r.getMessage() for r in caplog.records if "时钟" in r.getMessage()]
    assert len(skews) == 1
    assert "10.0.3.17" in skews[0]


def test_only_ready_entries_of_this_namespace_are_answered():
    selector, _mono, _wall = make_selector()
    raw = {
        b"0": entry(addr="10.0.3.17"),
        b"1": entry(addr="10.0.3.18", state=STATE_DRAINING),
        b"2": entry(addr="10.0.3.19", ns="other"),
        b"3": b"garbage",
        b"4": entry(addr="not-an-ip"),
    }
    selection = selector.update(raw, always)
    assert answered(selection) == ["10.0.3.17"]
    assert "关服中" in selection.excluded["10.0.3.18"]
    assert "10.0.3.19" not in selection.excluded
    assert selection.namespaces == {"game": 2, "other": 1}  # 解析不了的不算
    assert set(selection.invalid) == {"3", "4"}
    assert selection.entries == 2


def test_workers_on_one_machine_are_one_address():
    selector, _mono, _wall = make_selector()
    raw = {
        b"0": entry(conns=10),
        b"1": entry(conns=20),
        b"2": entry(state=STATE_DRAINING, conns=30),  # 同一台上有个 worker 在重启
    }
    (node,) = selector.update(raw, always).addresses
    assert node == NodeAddress("10.0.3.17", workers=2, conns=30)


def test_wrong_port_is_excluded_and_reported_once(caplog):
    selector, mono, _wall = make_selector()
    hetu_logger = hdns.logger
    propagate, hetu_logger.propagate = hetu_logger.propagate, True
    try:
        with caplog.at_level("ERROR", logger="HeTu.root"):
            for step in range(3):
                mono.now += 1
                selection = selector.update(
                    {b"0": entry(port=2467, ts=WALL + step)}, always
                )
                assert answered(selection) == []
    finally:
        hetu_logger.propagate = propagate
    assert "端口 2467" in selection.excluded["10.0.3.17"]
    errors = [r.getMessage() for r in caplog.records if "端口是 2467" in r.getMessage()]
    assert len(errors) == 1


def test_least_loaded_addresses_come_first():
    """一个包装不下时取最闲的：按每个 worker 的平均连接数（机器规格可能不一样）"""
    selector, _mono, _wall = make_selector()
    raw = {
        b"0": entry(addr="10.0.3.17", conns=10),  # 2 个 worker，平均 5
        b"1": entry(addr="10.0.3.17", conns=0),
        b"2": entry(addr="10.0.3.18", conns=3),  # 1 个 worker，平均 3
        b"3": entry(addr="10.0.3.19", conns=40),
    }
    assert answered(selector.update(raw, always)) == [
        "10.0.3.18",
        "10.0.3.17",
        "10.0.3.19",
    ]


def test_unprobed_addresses_wait():
    selector, _mono, _wall = make_selector()
    selection = selector.update({b"0": entry()}, lambda addr: False)
    assert answered(selection) == []
    assert selection.pending == ["10.0.3.17"]


def test_all_stale_keeps_the_previous_answer_but_empty_registry_does_not():
    """
    全都过期多半是副本落后、或者和 master 断开了：沿用上一次的答案；注册表空了说明节点都正常停服了，照实答空
    """
    selector, mono, _wall = make_selector()
    raw = {b"0": entry(addr="10.0.3.17"), b"1": entry(addr="10.0.3.18")}
    assert len(selector.update(raw, always).addresses) == 2
    mono.now += 20
    selection = selector.update(raw, always)
    assert selection.kept_previous
    assert sorted(answered(selection)) == ["10.0.3.17", "10.0.3.18"]
    empty = selector.update({}, always)
    assert not empty.kept_previous
    assert answered(empty) == []


# ---------------------------------------------------------------- 探测


async def test_prober_probes_new_addresses_and_retries_failures(monkeypatch):
    monkeypatch.setattr(hdns, "PROBE_RETRY", 10.0)
    clock = Clock(0.0)
    reachable: set[str] = {"10.0.3.17"}
    calls: list[str] = []

    async def connect(addr: str, port: int, timeout: float) -> str | None:
        calls.append(addr)
        return None if addr in reachable else "ConnectionRefusedError"

    prober = Prober(2466, connect=connect, clock=clock)
    prober.track(["10.0.3.17", "10.0.3.18"])
    await prober.wait()
    assert prober.ok == {"10.0.3.17"}
    assert "ConnectionRefusedError" in prober.reason("10.0.3.18")
    # 正在答的不再探测，失败的到时间才重试
    prober.track(["10.0.3.17", "10.0.3.18"])
    await prober.wait()
    assert calls == ["10.0.3.17", "10.0.3.18"]
    reachable.add("10.0.3.18")
    clock.now += 10
    prober.track(["10.0.3.17", "10.0.3.18"])
    await prober.wait()
    assert prober.ok == {"10.0.3.17", "10.0.3.18"}
    # 去掉之后又回来的要重新探测（回收的实例的 IP 会分给新实例）
    prober.track(["10.0.3.18"])
    prober.track(["10.0.3.17", "10.0.3.18"])
    await prober.wait()
    assert calls.count("10.0.3.17") == 2


class FakeClient:
    def __init__(self, raw: dict[bytes, bytes] | None = None) -> None:
        self.raw = raw or {}
        self.fail = False

    async def hgetall(self, key: str) -> dict[bytes, bytes]:
        assert key == REGISTRY_KEY
        if self.fail:
            raise ConnectionError("redis down")
        return self.raw


async def ok_probe(addr: str, port: int, timeout: float) -> None:
    return None


async def test_refresh_answers_new_nodes_after_probing_and_survives_read_errors():
    client = FakeClient({b"0": entry(ts=time.time())})
    service = DnsService(
        client, namespace="game", prober=Prober(2466, connect=ok_probe)
    )
    # 第一次重算等探测做完：开起来马上就有答案
    await service.refresh(wait_probes=True)
    assert answers(ask(service, a_query())) == ["10.0.3.17"]
    # 读 Redis 失败：接着答上一次的结果
    client.fail = True
    assert await service.refresh() is None
    assert answers(ask(service, a_query())) == ["10.0.3.17"]
    # 节点都正常停服、删掉了登记：答空
    client.fail = False
    client.raw = {}
    await service.refresh()
    assert answers(ask(service, a_query())) == []


class _PingClient:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.closed = False

    async def ping(self) -> bool:
        if self.error is not None:
            raise self.error
        return True

    async def aclose(self) -> None:
        self.closed = True


async def test_no_password_in_the_url_is_warned_without_another_connection():
    """hetu dns 自己就是不带口令连上的：直接告警，不用另连一次"""

    def connect(url: str) -> Any:
        raise AssertionError("不该另连")

    warnings = await check_anonymous_access("redis://10.0.1.20:6379/0", connect=connect)
    assert len(warnings) == 1
    assert "10.0.1.20:6379 不带口令也能连上" in warnings[0]


async def test_anonymous_probe_drops_the_credentials():
    """去掉地址里的用户名和口令另连一次：要口令（NOAUTH）就不告警，PING 得通就告警；告警里不带口令"""
    import redis.exceptions

    url = "redis://hetu-dns:dns-secret@10.0.1.20:6379/0?protocol=2"
    seen: list[str] = []
    clients: list[_PingClient] = []

    def connect_with(error: BaseException | None):
        def connect(probe_url: str) -> Any:
            seen.append(probe_url)
            clients.append(_PingClient(error))
            return clients[-1]

        return connect

    noauth = redis.exceptions.AuthenticationError("Authentication required.")
    assert await check_anonymous_access(url, connect=connect_with(noauth)) == []
    warnings = await check_anonymous_access(url, connect=connect_with(None))
    assert seen == ["redis://10.0.1.20:6379/0?protocol=2"] * 2
    assert all(client.closed for client in clients)
    assert len(warnings) == 1
    assert "dns-secret" not in warnings[0]
    assert await check_anonymous_access("unix:///run/redis.sock") == []
    # redis-py 也认查询串里的 username / password：同样算带了口令，另连时去掉
    seen.clear()
    query_url = (
        "redis://10.0.1.20:6379/0?username=hetu-dns&password=dns-secret&protocol=2"
    )
    warnings = await check_anonymous_access(query_url, connect=connect_with(None))
    assert seen == ["redis://10.0.1.20:6379/0?protocol=2"]
    assert "dns-secret" not in warnings[0]


class _ErrorClient:
    """每条命令都抛同一个异常"""

    def __init__(self, error: BaseException) -> None:
        self.error = error

    async def hgetall(self, key: str) -> Any:
        raise self.error

    async def execute_command(self, *args: Any) -> Any:
        raise self.error


MASTER_DOWN = "Link with MASTER is down and replica-serve-stale-data is set to 'no'."


async def test_master_down_is_not_a_native_cluster():
    """
    副本和 master 断开、配了 replica-serve-stale-data no 时读命令回 MASTERDOWN。redis-py 把它归成集群错误的子类，
    但它是环境未就绪（3），不是原生集群（2）
    """
    import redis.exceptions as errors

    url = "redis://10.0.1.20:6379/0"
    with pytest.raises(DnsCheckFailed, match="和 master 断开") as e:
        await check_redis(_ErrorClient(errors.MasterDownError(MASTER_DOWN)), url)
    assert e.value.code == 3
    assert "原生集群" not in str(e.value)
    with pytest.raises(DnsCheckFailed, match="原生集群") as e:
        await check_redis(_ErrorClient(errors.MovedError("1234 10.0.1.21:6379")), url)
    assert e.value.code == 2


@pytest.mark.parametrize(
    "error,words,not_words",
    [
        ("master_down", "和 master 断开", "原生集群"),
        ("moved", "原生集群", "和 master 断开"),
    ],
)
async def test_read_errors_at_run_time_say_why(caplog, error, words, not_words):
    """运行时读注册表失败（副本断开、集群的槽位迁走了）：接着答上一次的结果，日志说清是哪种"""
    import redis.exceptions as errors

    exc = (
        errors.MasterDownError(MASTER_DOWN)
        if error == "master_down"
        else errors.MovedError("1234 10.0.1.21:6379")
    )
    service = DnsService(_ErrorClient(exc), namespace="game")
    with hetu_logs(caplog, "ERROR"):
        assert await service.refresh() is None
    (message,) = [
        r.getMessage() for r in caplog.records if "读注册表失败" in r.getMessage()
    ]
    assert words in message
    assert not_words not in message


DEEP = b'{"v": 1, "x": ' + b"[" * 100_000 + b"]" * 100_000 + b"}"


async def test_deeply_nested_entry_is_just_invalid():
    """解析器对嵌套很深的 JSON 抛 RecursionError：当成解析不了的登记跳过，别的照答"""
    client = FakeClient({b"0": entry(ts=time.time()), b"1": DEEP})
    service = DnsService(
        client, namespace="game", prober=Prober(2466, connect=ok_probe)
    )
    selection = await service.refresh(wait_probes=True)
    assert selection is not None
    assert set(selection.invalid) == {"1"}
    assert answers(ask(service, a_query())) == ["10.0.3.17"]


async def test_refresh_loop_survives_unexpected_errors(monkeypatch, caplog):
    """重算出了意料之外的错：记下来接着转。循环要是停了，进程还活着、一直答旧的答案，谁也发现不了"""
    monkeypatch.setattr(hdns, "REFRESH_INTERVAL", 0.01)
    client = FakeClient({b"0": entry(ts=time.time())})
    service = DnsService(
        client, namespace="game", prober=Prober(2466, connect=ok_probe)
    )
    real_update = service.selector.update
    calls = 0

    def flaky_update(raw, probed):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")
        return real_update(raw, probed)

    monkeypatch.setattr(service.selector, "update", flaky_update)
    with hetu_logs(caplog, "ERROR"):
        loop = asyncio.create_task(service.run_refresh_loop())
        try:
            async with asyncio.timeout(5):
                while calls < 3:
                    await asyncio.sleep(0.01)
        finally:
            loop.cancel()
            await asyncio.gather(loop, return_exceptions=True)
    assert any("重算答案出错" in r.getMessage() for r in caplog.records)


async def test_unreachable_nodes_are_not_answered():
    """新地址先从网关连一次：连不上的先不答，原因写进 excluded"""

    async def connect(addr: str, port: int, timeout: float) -> str | None:
        return None if addr == "10.0.3.17" else "ConnectionRefusedError"

    now = time.time()
    client = FakeClient({b"0": entry(ts=now), b"1": entry(addr="10.0.3.18", ts=now)})
    service = DnsService(client, namespace="game", prober=Prober(2466, connect=connect))
    selection = await service.refresh(wait_probes=True)
    assert selection is not None
    assert answers(ask(service, a_query())) == ["10.0.3.17"]
    assert selection.pending == ["10.0.3.18"]
    assert "连不上 10.0.3.18:2466" in selection.excluded["10.0.3.18"]


def test_listen_outside_loopback_is_warned():
    assert check_listen("127.0.0.1") == []
    assert check_listen("localhost") == []  # 主机名按解析出的地址判断
    assert check_listen("0.0.0.0")


async def test_once_reports_a_failed_read_instead_of_keeping_the_old_answer():
    """--once 没有上一次的结果可答：读注册表失败就以 3 退出，如实说读失败了"""
    client = FakeClient()
    client.fail = True
    service = DnsService(client, namespace="game")
    with pytest.raises(DnsCheckFailed, match="读注册表失败") as e:
        await service.refresh(keep_previous=False)
    assert e.value.code == 3


def test_bridge_network_is_rejected(tmp_path):
    """hetu dns 自己也只支持 host 网络：Caddy 查的 127.0.0.1 是宿主机的回环"""
    device = tmp_path / "net" / "eth0"
    device.mkdir(parents=True)
    (device / "ifindex").write_text("2\n", encoding="ascii")
    (device / "iflink").write_text("12\n", encoding="ascii")
    (device / "uevent").write_text("INTERFACE=eth0\n", encoding="ascii")
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect(("192.0.2.1", 9))
        except OSError:
            pytest.skip("本机没有到外网的路由")
        src = s.getsockname()[0]
    with pytest.raises(DnsCheckFailed, match="只支持 host 网络") as e:
        check_network(
            "redis://192.0.2.1:6379/0",
            sysfs=str(tmp_path / "net"),
            interfaces={src: "eth0"},
        )
    assert e.value.code == 2
    assert "--network host" in str(e.value)


# ---------------------------------------------------------------- 对真 Redis


@pytest.fixture
def replica(request):
    """(master 的 io, 副本的地址)，用 12 号库，用例前清空；用例里在副本上建的 ACL 用户用完删掉"""
    import redis as sync_redis

    if "redis" not in REDIS_BACKENDS:
        pytest.skip("HETU_TEST_BACKENDS 里没有 redis")
    ses_redis_service = request.getfixturevalue("ses_redis_service")

    master_url, replica_url = (
        url.rsplit("/", 1)[0] + "/12" for url in ses_redis_service
    )
    master = sync_redis.Redis.from_url(master_url)
    master.flushdb()
    replica_io = sync_redis.Redis.from_url(replica_url)
    try:
        yield master, replica_url, replica_io
    finally:
        for user in ("hetu-dns", "hetu-dns-noperm", "hetu-dns-admin"):
            replica_io.acl_deluser(user)
        replica_io.close()
        master.flushdb()
        master.close()


def acl_url(replica_url: str, user: str, password: str) -> str:
    """把副本地址换成 ACL 用户的"""
    scheme, rest = replica_url.split("://", 1)
    rest = rest.split("@", 1)[-1]
    return f"{scheme}://{user}:{password}@{rest}"


def create_dns_user(replica_io, replica_url: str) -> str:
    """按文档里的命令（ACL_COMMAND）在副本上建只读用户，返回它的地址。不是 0 号库要加 +select"""
    # 去掉开头的 ACL SETUSER hetu-dns
    rules = ACL_COMMAND.replace(">口令", ">dns-secret").split()[3:]
    replica_io.execute_command("ACL", "SETUSER", "hetu-dns", *rules, "+select")
    return acl_url(replica_url, "hetu-dns", "dns-secret")


async def _connect(url: str) -> Any:
    return hdns._connect_redis(url)


@pytest.mark.timeout(60)
async def test_documented_acl_user_works_without_warnings(replica):
    _master, replica_url, replica_io = replica
    url = create_dns_user(replica_io, replica_url)
    client = await _connect(url)
    try:
        assert await check_redis(client, url) == []
    finally:
        await client.aclose()


@pytest.mark.timeout(60)
async def test_default_user_and_master_are_warned(replica, ses_redis_service):
    """default 用户什么都能读：网关暴露在公网，告警；连的是 master 也告警（读都该走副本）"""
    _master, replica_url, _replica_io = replica
    client = await _connect(replica_url)
    try:
        warnings = await check_redis(client, replica_url)
    finally:
        await client.aclose()
    (too_powerful,) = [w for w in warnings if "权限太大" in w]
    assert "别的 key 也能读" in too_powerful
    assert "管理命令" in too_powerful
    assert not any("连的是 master" in w for w in warnings), warnings
    master_url = ses_redis_service[0].rsplit("/", 1)[0] + "/12"
    client = await _connect(master_url)
    try:
        warnings = await check_redis(client, master_url)
    finally:
        await client.aclose()
    assert any("连的是 master" in w for w in warnings), warnings


@pytest.mark.timeout(60)
async def test_user_limited_only_by_key_is_still_too_powerful(replica):
    """只限了 key 的用户（~hetu:nodes +@all）照样能 FLUSHALL、CONFIG SET、REPLICAOF：也算权限太大"""
    _master, replica_url, replica_io = replica
    replica_io.execute_command(
        "ACL",
        "SETUSER",
        "hetu-dns-admin",
        "on",
        ">x",
        "resetkeys",
        "~hetu:nodes",
        "+@all",
    )
    url = acl_url(replica_url, "hetu-dns-admin", "x")
    client = await _connect(url)
    try:
        warnings = await check_redis(client, url)
    finally:
        await client.aclose()
    (too_powerful,) = [w for w in warnings if "权限太大" in w]
    assert "管理命令" in too_powerful
    assert "别的 key 也能读" not in too_powerful


@pytest.mark.timeout(60)
async def test_denied_hgetall_explains_the_grant(replica):
    _master, replica_url, replica_io = replica
    replica_io.execute_command(
        "ACL",
        "SETUSER",
        "hetu-dns-noperm",
        "on",
        ">x",
        "~*",
        "-@all",
        "+ping",
        "+select",
    )
    url = acl_url(replica_url, "hetu-dns-noperm", "x")
    client = await _connect(url)
    try:
        with pytest.raises(DnsCheckFailed) as e:
            await check_redis(client, url)
    finally:
        await client.aclose()
    assert e.value.code == 3
    assert "ACL SETUSER hetu-dns" in str(e.value)
    assert "x@" not in str(e.value)  # 口令打码


@pytest.mark.timeout(60)
async def test_replica_without_a_default_password_is_warned(replica):
    """
    测试用的副本 default 用户没设口令：hetu dns 用的只读用户权限再小也没用，告警；给 default 设上口令之后不告警
    """
    import redis as sync_redis

    _master, replica_url, replica_io = replica
    url = create_dns_user(replica_io, replica_url)
    warnings = await check_anonymous_access(url)
    assert len(warnings) == 1, warnings
    assert "不带口令也能连上" in warnings[0]
    replica_io.execute_command(
        "ACL", "SETUSER", "default", "resetpass", ">default-secret"
    )
    try:
        assert await check_anonymous_access(url) == []
    finally:
        admin = sync_redis.Redis.from_url(replica_url, password="default-secret")
        try:
            admin.execute_command("ACL", "SETUSER", "default", "nopass")
        finally:
            admin.close()


@pytest.mark.timeout(60)
@pytest.mark.skipif(
    "redis_cluster" not in REDIS_BACKENDS,
    reason="HETU_TEST_BACKENDS 里没有 redis_cluster",
)
async def test_native_cluster_is_rejected(ses_redis_cluster_service):
    """连到不持有 hetu:nodes 的集群节点：收到 MOVED，第一版不支持原生集群，以 2 退出"""
    import redis as sync_redis
    from redis.cluster import key_slot

    with sync_redis.Redis.from_url(ses_redis_cluster_service) as io:
        slots = io.execute_command("CLUSTER", "SLOTS")
    slot = key_slot(REGISTRY_KEY.encode())
    owner = next(int(node[1]) for start, end, node, *_ in slots if start <= slot <= end)
    other = next(
        int(node[1]) for start, end, node, *_ in slots if int(node[1]) != owner
    )
    url = f"redis://127.0.0.1:{other}"
    client = await _connect(url)
    try:
        with pytest.raises(DnsCheckFailed, match="原生集群") as e:
            await check_redis(client, url)
    finally:
        await client.aclose()
    assert e.value.code == 2


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _run_dns(*args: str, env: dict[str, str], timeout: float = 60):
    full_env = {k: v for k, v in os.environ.items() if k != "HETU_DNS_REDIS"}
    full_env.update(env)
    return subprocess.run(
        [sys.executable, "-m", "hetu", "dns", *args],
        capture_output=True,
        timeout=timeout,
        env=full_env,
        check=False,
    )


def _query(port: int, timeout: float = 2.0) -> dns.message.Message:
    query = dns.message.make_query(NAME, "A", use_edns=0, payload=1232)
    return dns.query.udp(query, "127.0.0.1", port=port, timeout=timeout)


def _wait_answer(port: int, expected: list[str], timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        try:
            last = sorted(answers(_query(port)))
        except (dns.exception.Timeout, OSError) as e:
            last = e
        if last == sorted(expected):
            return
        time.sleep(0.2)
    raise AssertionError(
        f"hetu dns 没在 {timeout} 秒内答 {expected}，最后答的是 {last}"
    )


@pytest.mark.timeout(120)
def test_hetu_dns_end_to_end(replica):
    """
    真跑 hetu dns（受限的 ACL 用户、读副本）：节点开好服登记之后答它的地址；改成 draining 之后不答；探测不通
    的地址不答
    """
    master, replica_url, replica_io = replica
    url = create_dns_user(replica_io, replica_url)
    node = socket.create_server(("127.0.0.1", 0))  # 假装是节点：探测连得上
    node_port = node.getsockname()[1]
    dns_port = _free_udp_port()
    master.hset(
        REGISTRY_KEY,
        mapping={
            "0": entry(addr="127.0.0.1", port=node_port, ts=time.time()),
            "1": entry(addr="10.255.255.1", port=node_port, ts=time.time(), ns="other"),
            # 同一个 namespace、探测不通：假节点只监听 127.0.0.1
            "2": entry(addr="127.0.0.2", port=node_port, ts=time.time()),
        },
    )
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "hetu",
            "dns",
            "--namespace",
            "game",
            "--port",
            str(node_port),
            "--listen",
            f"127.0.0.1:{dns_port}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={**os.environ, "HETU_DNS_REDIS": url},
    )
    try:
        _wait_answer(dns_port, ["127.0.0.1"])
        # 节点关服第一步改成 draining：不再答它
        master.hset(
            REGISTRY_KEY,
            "0",
            entry(
                addr="127.0.0.1", port=node_port, ts=time.time(), state=STATE_DRAINING
            ),
        )
        _wait_answer(dns_port, [])
    finally:
        proc.terminate()
        output, _err = proc.communicate(timeout=30)
        node.close()
    log = output.decode("utf-8", "replace")
    assert "答案变了" in log, log
    assert "收到第一个查询" in log, log
    assert "权限太大" not in log, log
    assert "从网关连不上节点 127.0.0.2" in log, log
    # 测试用的副本 default 用户没设口令
    assert "不带口令也能连上" in log, log


@pytest.mark.timeout(120)
def test_once_reports_what_would_be_answered(replica):
    master, replica_url, replica_io = replica
    url = create_dns_user(replica_io, replica_url)
    node = socket.create_server(("127.0.0.1", 0))
    node_port = node.getsockname()[1]
    now = time.time()
    try:
        master.hset(
            REGISTRY_KEY,
            mapping={
                "0": entry(addr="127.0.0.1", port=node_port, ts=now, conns=12),
                "1": entry(addr="127.0.0.2", port=node_port, ts=now - 120),
                "2": entry(
                    addr="127.0.0.3", port=node_port, ts=now, state=STATE_DRAINING
                ),
                "3": entry(addr="127.0.0.4", port=node_port + 1, ts=now),
                "4": entry(addr="127.0.0.5", port=node_port, ts=now),
            },
        )
        proc = _run_dns(
            "--namespace",
            "game",
            "--port",
            str(node_port),
            "--listen",
            "0.0.0.0:11053",
            "--once",
            env={"HETU_DNS_REDIS": url},
        )
    finally:
        node.close()
    report = proc.stdout.decode("utf-8", "replace")
    stderr = proc.stderr.decode("utf-8", "replace")
    assert proc.returncode == 0, (report, stderr)
    # 告警只打在报告里，不再进 stderr 的日志（测试用的副本 default 用户没设口令）
    assert "不带口令也能连上" in report, report
    assert "不带口令也能连上" not in stderr, stderr
    assert "--listen 0.0.0.0 不是回环地址" in report, report
    assert "会答 1 个地址" in report, report
    assert "127.0.0.1  1 个 worker，12 个连接" in report, report
    assert "127.0.0.2  登记" in report, report
    assert "127.0.0.3  关服中" in report, report
    assert f"127.0.0.4  端口 {node_port + 1}" in report, report
    assert f"127.0.0.5  从网关连不上 127.0.0.5:{node_port}" in report, report
    assert "dns-secret" not in report  # 口令打码


@pytest.mark.timeout(120)
def test_listen_port_in_use_exits_with_3(replica):
    """--listen 的端口被占了：以 3 退出（环境未就绪），说明是监听失败"""
    _master, replica_url, replica_io = replica
    url = create_dns_user(replica_io, replica_url)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as taken:
        taken.bind(("127.0.0.1", 0))
        port = taken.getsockname()[1]
        proc = _run_dns(
            "--namespace",
            "game",
            "--listen",
            f"127.0.0.1:{port}",
            env={"HETU_DNS_REDIS": url},
        )
    stderr = proc.stderr.decode("utf-8", "replace")
    assert proc.returncode == 3, stderr
    assert "监听" in stderr


@pytest.mark.parametrize("url", ["http://10.0.1.20:6379/0", "localhost:6379"])
def test_malformed_redis_url_is_a_usage_error(url):
    """Redis 地址写错（scheme 不对、没写 scheme）：以 2 退出，说明怎么写，不打 traceback"""
    proc = _run_dns("--namespace", "game", "--once", env={"HETU_DNS_REDIS": url})
    stderr = proc.stderr.decode("utf-8", "replace")
    assert proc.returncode == 2, stderr
    assert "Traceback" not in stderr
    assert "redis://" in stderr


@pytest.mark.timeout(120)
def test_exit_codes(replica):
    _master, replica_url, replica_io = replica
    # 没给 Redis 地址：用法错误
    assert _run_dns("--namespace", "game", "--once", env={}).returncode == 2
    # 被 ACL 拒绝：环境未就绪
    replica_io.execute_command(
        "ACL",
        "SETUSER",
        "hetu-dns-noperm",
        "on",
        ">x",
        "~*",
        "-@all",
        "+ping",
        "+select",
    )
    url = acl_url(replica_url, "hetu-dns-noperm", "x")
    proc = _run_dns("--namespace", "game", "--once", env={"HETU_DNS_REDIS": url})
    assert proc.returncode == 3
    assert "ACL SETUSER hetu-dns" in proc.stderr.decode("utf-8", "replace")
