"""
网关上的 DNS 桥 `hetu dns`（设计稿 docs/superpowers/specs/2026-11-15-node-discovery-design.md §6）：每秒从
Redis（副本、只读 ACL 用户）读一次注册表 hetu:nodes，把本项目里还活着、开好了服、从网关连得通的节点答成
`--name` 的 A 记录，给 Caddy 的 `dynamic a`（`resolvers 127.0.0.1:1053`）用。

只做 Caddy 用得到的那点 DNS（§6.2）：UDP、一个名字的 A 记录，报文用 struct 手写，不加运行时依赖。**从不设
TC、不做 TCP**：Caddy 配了 resolvers 之后它的 Dial 总是拨 UDP，Go 解析器收到 TC 后按"tcp"重试，拿到的还是
UDP 连接，第二次的回答即使截断了也照单全收，所以回答必须一个包就装下：带 EDNS（Go 声明 1232 字节）最多 74 条，
不带（512 字节）最多 30 条。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

The gateway's DNS bridge: serves the node registry to Caddy as A records.
"""

import asyncio
import contextlib
import ipaddress
import logging
import random
import signal
import socket
import struct
import sys
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse

from ..i18n import _
from .netcheck import host_network_hint, host_network_problem
from .registry import (
    REGISTRY_KEY,
    STALE_SEC,
    STATE_READY,
    EntryError,
    NodeEntry,
    decode_entry,
)

logger = logging.getLogger("HeTu.root")

DEFAULT_NAME = "hetu.internal"
DEFAULT_PORT = 2466
DEFAULT_LISTEN = "127.0.0.1:1053"
# Redis 地址的环境变量：放在命令行里，口令会出现在进程列表里
REDIS_ENV = "HETU_DNS_REDIS"
# 多久读一次注册表、重算一次答案（秒）。节点注销后等 4 秒才停止监听，是按它和 Caddy 的 refresh 2s 算的
REFRESH_INTERVAL = 1.0
# 新出现的地址从网关 TCP 连一次的超时、连不上之后多久重试（秒）
PROBE_TIMEOUT = 1.0
PROBE_RETRY = 10.0
# 节点的时钟和本机差多少秒就告警（雪花 ID 本来就要求 NTP 同步）
CLOCK_SKEW_WARN = 5.0
# 一个 field 最多记几个进程见过的最大 ts（同一个 worker id 被先后几个进程用过）
MAX_INSTANCES_PER_FIELD = 4
# 连 Redis、每条命令的超时（秒）
REDIS_TIMEOUT = 2.0
# 只告警一次的名字最多记多少个（别人拿各种名字来扫时不至于无限增长）
MAX_WARNED_NAMES = 100

# 只读 ACL 用户：只能 HGETALL hetu:nodes。以实测为准，测试里用它建用户跑一遍；{password} 是口令的占位，
# 运维文档（中英）里写的命令和它一致，也有测试对照
ACL_COMMAND_TEMPLATE = (
    "ACL SETUSER hetu-dns on >{password} resetkeys ~hetu:nodes resetchannels -@all "
    "+hgetall +ping"
)


def acl_command() -> str:
    """
    建 hetu dns 用的只读用户的命令，口令的占位按当前语言写

    The command that creates the read-only user for hetu dns.
    """
    return ACL_COMMAND_TEMPLATE.format(password=_("口令"))


EXIT_USAGE = 2
EXIT_NOT_READY = 3

# ---------------------------------------------------------------- 报文

CLASSIC_UDP_PAYLOAD = 512
MAX_UDP_PAYLOAD = 1232
TYPE_A = 1
TYPE_OPT = 41
TYPE_ANY = 255
CLASS_IN = 1
NOERROR = 0
FORMERR = 1
NXDOMAIN = 3
NOTIMP = 4
# EDNS 版本不认识（扩展 RCODE，高 8 位放在 OPT 的 TTL 里）
BADVERS = 16

_HEADER = struct.Struct("!6H")
_FLAG_QR = 0x8000
_FLAG_AA = 0x0400
_FLAG_RD = 0x0100
# 一条 A 记录：名字（指向问题里名字的压缩指针 0xC00C）、类型、类别、TTL、长度，再 4 字节地址
_A_RECORD = struct.Struct("!HHHIH")
_A_RECORD_SIZE = _A_RECORD.size + 4
_NAME_POINTER = 0xC000 | _HEADER.size
# 回答里的 EDNS OPT：根名字、类型、我们的 UDP 大小、扩展 RCODE / 版本 / 标志都是 0、没有选项
_OPT_RECORD = b"\x00" + struct.pack("!HHIH", TYPE_OPT, MAX_UDP_PAYLOAD, 0, 0)


def _opt_record(rcode: int) -> bytes:
    """回答里的 OPT：TTL 的最高 8 位是扩展 RCODE（RCODE 的高位），版本、标志都是 0"""
    return b"\x00" + struct.pack(
        "!HHIH", TYPE_OPT, MAX_UDP_PAYLOAD, (rcode >> 4) << 24, 0
    )


@dataclass
class Query:
    """解析出来的一个查询"""

    id: int
    rd: bool
    opcode: int = 0
    # 问题段原样（名字、类型、类别），回答里照抄；解析出错时为空
    question: bytes = b""
    # 问的名字：标签用点连起来、ASCII 小写、不带末尾的点
    name: bytes = b""
    qtype: int = 0
    qclass: int = 0
    # 查询带了 EDNS OPT：它声明的 UDP 大小
    edns: int | None = None
    # 查询的 EDNS 版本
    edns_version: int = 0
    # 解析出错时要回的 RCODE（FORMERR、NOTIMP），NOERROR 为没出错
    error: int = NOERROR


def _read_name(
    data: bytes, offset: int, *, allow_pointer: bool
) -> tuple[int, list[bytes]]:
    """从 offset 读一个名字，返回 (名字之后的位置, 标签)。压缩指针只跳过、不追"""
    labels: list[bytes] = []
    total = 0
    while True:
        length = data[offset]
        if length == 0:
            return offset + 1, labels
        if length & 0xC0:
            if not allow_pointer or length & 0xC0 != 0xC0 or offset + 2 > len(data):
                raise ValueError("compression pointer")
            return offset + 2, labels
        label = data[offset + 1 : offset + 1 + length]
        total += length + 1
        # 名字连长度字节和结尾的 0 最多 255 字节
        if len(label) != length or total + 1 > 255:
            raise ValueError("bad label")
        labels.append(label)
        offset += 1 + length


def parse_query(data: bytes) -> Query | None:
    """
    解析一个 UDP 查询。连报头都不完整的、本身是回答的（QR=1）返回 None，直接丢掉；其余的返回 Query：不是标准
    查询（opcode 不是 QUERY）的 error 是 NOTIMP，问题不是正好一个、问题里带压缩指针、包不完整的是 FORMERR。

    Parse a UDP query; None means drop it.
    """
    if len(data) < _HEADER.size:
        return None
    qid, flags, qdcount, ancount, nscount, arcount = _HEADER.unpack_from(data)
    if flags & _FLAG_QR:
        return None
    query = Query(id=qid, rd=bool(flags & _FLAG_RD), opcode=(flags >> 11) & 0xF)
    if query.opcode != 0:
        query.error = NOTIMP
        return query
    if qdcount != 1 or ancount or nscount:
        query.error = FORMERR
        return query
    try:
        offset, labels = _read_name(data, _HEADER.size, allow_pointer=False)
        query.qtype, query.qclass = struct.unpack_from("!HH", data, offset)
        offset += 4
        query.question = data[_HEADER.size : offset]
        query.name = b".".join(labels).lower()
        for _i in range(arcount):
            offset, _labels = _read_name(data, offset, allow_pointer=True)
            rtype, rclass, ttl, rdlength = struct.unpack_from("!HHIH", data, offset)
            offset += 10 + rdlength
            if offset > len(data):
                raise ValueError("truncated rdata")
            if rtype == TYPE_OPT:
                if query.edns is not None:
                    raise ValueError("more than one OPT")  # RFC 6891 §6.1.1
                query.edns = rclass
                query.edns_version = (ttl >> 16) & 0xFF
    except IndexError, ValueError, struct.error:
        query.error = FORMERR
        query.question = b""
    return query


def payload_limit(query: Query) -> int:
    """这个查询的回答最多多大：没带 EDNS 是 512 字节，带了按它声明的大小，最多 1232"""
    if query.edns is None:
        return CLASSIC_UDP_PAYLOAD
    return max(CLASSIC_UDP_PAYLOAD, min(query.edns, MAX_UDP_PAYLOAD))


def max_answers(query: Query) -> int:
    """一个包最多装几条 A 记录"""
    room = payload_limit(query) - _HEADER.size - len(query.question)
    if query.edns is not None:
        room -= len(_OPT_RECORD)
    return max(0, room // _A_RECORD_SIZE)


def build_response(
    query: Query, rcode: int = NOERROR, addresses: Sequence[bytes] = ()
) -> bytes:
    """
    回答：回显 ID、opcode、RD 和问题，带 AA、不带 RA，从不设 TC。addresses 是打包好的 IPv4 地址（4 字节），
    TTL 0，一个包装不下的部分不答。查询带 EDNS OPT 时回答也带。

    Build a response that always fits one UDP packet; TC is never set.
    """
    flags = _FLAG_QR | (query.opcode << 11) | _FLAG_AA | (rcode & 0xF)
    if query.rd:
        flags |= _FLAG_RD
    answers = addresses[: max_answers(query)] if query.question else ()
    parts = [
        _HEADER.pack(
            query.id,
            flags,
            1 if query.question else 0,
            len(answers),
            0,
            0 if query.edns is None else 1,
        ),
        query.question,
    ]
    for address in answers:
        parts.append(_A_RECORD.pack(_NAME_POINTER, TYPE_A, CLASS_IN, 0, 4))
        parts.append(address)
    if query.edns is not None:
        parts.append(_opt_record(rcode))
    return b"".join(parts)


# ---------------------------------------------------------------- 答哪些地址


@dataclass
class NodeAddress:
    """一个要答的地址：同一台机器上的几个 worker 合成一个"""

    addr: str
    workers: int = 0
    conns: int = 0

    @property
    def load(self) -> float:
        """每个 worker 的平均连接数（伸缩组里可能混着不同规格的机器）"""
        return self.conns / max(1, self.workers)


@dataclass
class Selection:
    """一次重算的结果"""

    # 要答的地址，按每个 worker 的平均连接数从少到多，平均数相同的随机
    addresses: list[NodeAddress]
    # 活着、开好了服、端口对，但还没探测通的地址
    pending: list[str]
    # 不答的地址 → 原因
    excluded: dict[str, str]
    # 本 namespace 的登记数
    entries: int
    # 注册表里各 namespace 的登记数
    namespaces: dict[str, int]
    # 解析不了的登记：field → 原因
    invalid: dict[str, str]
    # 本 namespace 的登记都过期了：沿用了上一次的答案
    kept_previous: bool = False


class Selector:
    """
    把每次读到的注册表变成要答的地址（§6.3、§6.4）。

    过期按登记有没有往前走判断，不比较时钟：记住每个 field 上见过的最大 ts 和它前进的时刻（本机单调时钟），
    STALE_SEC 秒没前进就算过期。节点每 5 秒写一次，ts 一次比一次大。只看值变没变不够：经代理按请求轮询几台
    副本、其中一台和 master 断开了还在给旧数据时，死节点的值会在新旧两份之间来回变。换了一个进程（持有者
    标识或就绪时刻变了：重启、worker id 被接手）算一次新的写入，ts 不用比原来的大。第一次见到的 field 没有
    历史，按本机时钟和 ts 的差判断：在 STALE_SEC 以内的算活着，更早的按 ts 算出它多久没写了。

    Turns registry snapshots into the addresses to answer, judging staleness by progress.
    """

    def __init__(
        self,
        namespace: str,
        port: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        wall: Callable[[], float] = time.time,
    ) -> None:
        self.namespace = namespace
        self.port = port
        self._clock = clock
        self._wall = wall
        # field → ({进程（持有者标识, 就绪时刻）: 见过的最大 ts}, 最近一次前进的时刻)
        self._history: dict[str, tuple[dict[tuple[str, float], float], float]] = {}
        self.previous: list[NodeAddress] = []
        # 已经报过时钟差的 field
        self._warned_skew: set[str] = set()
        self._warned_port: set[tuple[str, int]] = set()
        self._warned_invalid: set[str] = set()

    def _progress(self, field: str, entry: NodeEntry, now: float) -> tuple[float, bool]:
        """这条登记最近一次往前走的时刻（本机单调时钟），以及这一次是不是看到了一次新的写入"""
        instance = (entry.node, entry.up)
        history = self._history.get(field)
        if history is not None:
            seen, progressed = history
            last = seen.pop(instance, None)
            if last is not None and entry.ts <= last:
                seen[instance] = last
                return progressed, False
            if last is None:
                self._warned_skew.discard(field)  # 换了一个进程，时钟也可能不一样了
            seen[instance] = entry.ts
            while len(seen) > MAX_INSTANCES_PER_FIELD:
                del seen[next(iter(seen))]
            self._history[field] = (seen, now)
            return now, True
        age = self._wall() - entry.ts
        if age > STALE_SEC:
            progressed = now - age
        elif age < -STALE_SEC:
            # ts 在未来太远：先当它过期，等它再写一次
            progressed = now - STALE_SEC - 1
        else:
            progressed = now - max(0.0, age)
        self._history[field] = ({instance: entry.ts}, progressed)
        return progressed, False

    def _check_skew(self, field: str, entry: NodeEntry, wrote: bool) -> None:
        """
        节点的时钟和本机差得太多就告警，每个 field 一次。只在看到一次新的写入时判断：那时 ts 刚写下不久（最多
        一次重算的间隔加上副本延迟）。第一次见到的只认 ts 在未来的：ts 在过去可能只是它很久没写了，死掉的
        worker 的登记会一直留着
        """
        age = self._wall() - entry.ts
        skewed = abs(age) > CLOCK_SKEW_WARN if wrote else age < -CLOCK_SKEW_WARN
        if not skewed or field in self._warned_skew:
            return
        self._warned_skew.add(field)
        logger.warning(
            _(
                "⚠️ [📣DNS] 节点 {addr}（worker {field}）的时钟和本机差 {skew:.1f} 秒，请检查两边的 NTP"
                "（雪花 ID 本来就要求时钟同步）"
            ).format(addr=entry.addr, field=field, skew=age)
        )

    def update(
        self, raw: Mapping[Any, Any], probed: Callable[[str], bool]
    ) -> Selection:
        """
        按这次读到的注册表（HGETALL 的结果）重算。probed(地址) 为真的才答（探测通了）。

        1. 留下本 namespace 的、没过期的、ready 的登记；
        2. 端口不等于 --port 的排除（每个地址报一次错：网关按 --port 去连，连不到它）；
        3. 按地址去重：同一台机器上的几个 worker 合成一个，有一个 ready 就答；
        4. 没探测通的先不答；
        5. 按每个 worker 的平均连接数从少到多排好（平均数相同的随机），查询时取一个包装得下的前缀。

        本 namespace 有登记、但全都过期了：多半是副本落后或者和 master 断开了，而不是节点同时死了，沿用上一次的
        答案。
        """
        now = self._clock()
        # 本 namespace 的、解析得了的 field：只记它们的历史
        tracked: set[str] = set()
        namespaces: dict[str, int] = {}
        invalid: dict[str, str] = {}
        candidates: dict[str, NodeAddress] = {}
        excluded: dict[str, str] = {}
        ours = fresh = 0
        for key, value in raw.items():
            field = (
                key.decode("utf-8", "replace") if isinstance(key, bytes) else str(key)
            )
            value = value if isinstance(value, bytes) else str(value).encode()
            try:
                entry = decode_entry(value)
                ipaddress.IPv4Address(entry.addr)
            except (EntryError, ValueError) as e:
                invalid[field] = str(e)
                if field not in self._warned_invalid:
                    self._warned_invalid.add(field)
                    logger.warning(
                        _(
                            "⚠️ [📣DNS] 注册表里 worker {field} 的登记解析不了，跳过：{err}"
                        ).format(field=field, err=e)
                    )
                continue
            self._warned_invalid.discard(field)
            namespaces[entry.ns] = namespaces.get(entry.ns, 0) + 1
            if entry.ns != self.namespace:
                continue
            tracked.add(field)
            progressed_at, wrote = self._progress(field, entry, now)
            self._check_skew(field, entry, wrote)
            ours += 1
            idle = now - progressed_at
            if idle > STALE_SEC:
                excluded.setdefault(
                    entry.addr,
                    _("登记 {idle:.0f} 秒没更新，节点多半已经没了").format(idle=idle),
                )
                continue
            fresh += 1
            if entry.state != STATE_READY:
                excluded.setdefault(
                    entry.addr, _("关服中（{state}）").format(state=entry.state)
                )
                continue
            if entry.port != self.port:
                excluded.setdefault(
                    entry.addr,
                    _("端口 {port} 和 --port {expected} 不一致").format(
                        port=entry.port, expected=self.port
                    ),
                )
                if (entry.addr, entry.port) not in self._warned_port:
                    self._warned_port.add((entry.addr, entry.port))
                    logger.error(
                        _(
                            "❌ [📣DNS] 节点 {addr} 登记的端口是 {port}，和 --port {expected} 不一致，"
                            "网关按 {expected} 连不到它，不答。所有节点的 LISTEN 端口、--port、Caddyfile "
                            "里 dynamic a 的 port 要一致"
                        ).format(addr=entry.addr, port=entry.port, expected=self.port)
                    )
                continue
            node = candidates.setdefault(entry.addr, NodeAddress(entry.addr))
            node.workers += 1
            node.conns += entry.conns
        for field in list(self._history):
            if field not in tracked:
                del self._history[field]
        self._warned_skew &= tracked
        for addr in candidates:
            excluded.pop(addr, None)
        pending = [addr for addr in candidates if not probed(addr)]
        addresses = [node for node in candidates.values() if probed(node.addr)]
        random.shuffle(addresses)
        addresses.sort(key=lambda node: node.load)
        kept = bool(ours) and not fresh
        if kept:
            addresses = list(self.previous)
        self.previous = addresses
        return Selection(addresses, pending, excluded, ours, namespaces, invalid, kept)


# ---------------------------------------------------------------- 探测


async def tcp_probe(addr: str, port: int, timeout: float) -> str | None:
    """从本机 TCP 连一次 addr:port，连得上返回 None，连不上返回原因"""
    try:
        async with asyncio.timeout(timeout):
            _reader, writer = await asyncio.open_connection(addr, port)
    except TimeoutError:
        return _("{timeout:g} 秒内没连上").format(timeout=timeout)
    except OSError as e:
        return f"{type(e).__name__}: {e}"
    writer.close()
    with contextlib.suppress(OSError):
        await writer.wait_closed()
    return None


class Prober:
    """
    地址从不答变成要答时（第一次出现，或者去掉之后又回来：云上回收的实例的内网 IP 会分给新实例），先从网关
    TCP 连一次（§6.6）。连得上才答；连不上打错误日志、提示检查安全组，之后每 PROBE_RETRY 秒重试。正在答的
    地址不再探测，掉线交给 Caddy 的被动健康检查。

    Probes newly seen addresses from the gateway before they are answered.
    """

    def __init__(
        self,
        port: int,
        *,
        connect: Callable[[str, int, float], Any] = tcp_probe,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.port = port
        self._connect = connect
        self._clock = clock
        self.ok: set[str] = set()
        self.errors: dict[str, str] = {}
        self._retry_at: dict[str, float] = {}
        self._tasks: dict[str, asyncio.Task] = {}

    def is_ok(self, addr: str) -> bool:
        return addr in self.ok

    def reason(self, addr: str) -> str:
        """没探测通的原因"""
        if addr in self.errors:
            return _("从网关连不上 {addr}:{port}（{err}）").format(
                addr=addr, port=self.port, err=self.errors[addr]
            )
        return _("正在探测")

    def track(self, addrs: Iterable[str]) -> None:
        """
        只记着这些地址（要答的和等探测的）：别的忘掉，回来时重新探测；没探测过、到了重试时间的开始探测
        """
        wanted = set(addrs)
        self.ok &= wanted
        for addr in list(self.errors):
            if addr not in wanted:
                del self.errors[addr]
                self._retry_at.pop(addr, None)
        for addr in list(self._tasks):
            if addr not in wanted:
                self._tasks.pop(addr).cancel()
        now = self._clock()
        for addr in sorted(wanted):
            if addr in self.ok or addr in self._tasks:
                continue
            if now < self._retry_at.get(addr, 0.0):
                continue
            self._tasks[addr] = asyncio.create_task(self._probe(addr))

    async def _probe(self, addr: str) -> None:
        try:
            error = await self._connect(addr, self.port, PROBE_TIMEOUT)
        finally:
            self._tasks.pop(addr, None)
        if error is None:
            self.ok.add(addr)
            if self.errors.pop(addr, None) is not None:
                self._retry_at.pop(addr, None)
                logger.info(
                    _("ℹ️ [📣DNS] 从网关连得上 {addr}:{port} 了，开始答它").format(
                        addr=addr, port=self.port
                    )
                )
            return
        if addr not in self.errors:
            logger.error(
                _(
                    "❌ [📣DNS] 从网关连不上节点 {addr}:{port}（{err}），先不答。请检查安全组是否放行"
                    "网关到节点的 {port} 端口、节点的 LISTEN 和网络模式（只支持 host 网络）；每 "
                    "{retry:g} 秒重试一次"
                ).format(addr=addr, port=self.port, err=error, retry=PROBE_RETRY)
            )
        self.errors[addr] = error
        self._retry_at[addr] = self._clock() + PROBE_RETRY

    async def wait(self) -> None:
        """等正在进行的探测都结束（第一次重算、--once 用）"""
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)

    def cancel(self) -> None:
        for task in self._tasks.values():
            task.cancel()
        self._tasks.clear()


# ---------------------------------------------------------------- 服务


def normalize_name(name: str) -> bytes:
    """--name → 查询里比较用的形式：ASCII 小写、不带末尾的点"""
    return name.strip().rstrip(".").encode("ascii").lower()


class DnsService:
    """
    `hetu dns` 的核心：定时重算答案（`refresh`），回答查询（`respond`，直接用算好的答案，不碰 Redis）。

    The DNS bridge: refreshes the answer from Redis and answers queries from memory.
    """

    def __init__(
        self,
        client: Any,
        *,
        namespace: str,
        name: str = DEFAULT_NAME,
        port: int = DEFAULT_PORT,
        selector: Selector | None = None,
        prober: Prober | None = None,
    ) -> None:
        self.client = client
        self.namespace = namespace
        self.name = normalize_name(name)
        self.port = port
        self.selector = selector or Selector(namespace, port)
        self.prober = prober or Prober(port)
        self.selection: Selection | None = None
        # 打包好的地址，查询时取一个包装得下的前缀
        self.addresses: tuple[bytes, ...] = ()
        self._answered: list[str] | None = None
        self._read_failing = False
        self._kept_previous = False
        self._warned_namespace = False
        self._warned_names: set[bytes] = set()
        self._first_query = True

    # ------------------------------------------------------------ 重算

    async def refresh(
        self, *, wait_probes: bool = False, keep_previous: bool = True
    ) -> Selection | None:
        """
        读一次注册表、重算答案。读失败时一直答上一次的结果并打错误日志，返回 None；keep_previous 为假时
        （--once）抛 DnsCheckFailed（3）。wait_probes：等新地址的探测做完再算（第一次重算、--once 用），不然
        探测在后台做、下一次重算时生效

        Re-read the registry and recompute the answer.
        """
        try:
            raw = await self.client.hgetall(REGISTRY_KEY)
        except Exception as e:  # 读失败：常驻时接着答上一次的，--once 时如实报
            if not keep_previous:
                raise DnsCheckFailed(
                    EXIT_NOT_READY,
                    _("❌ 读注册表失败：{err}").format(err=f"{type(e).__name__}: {e}"),
                ) from e
            self._read_failing = True
            if is_master_down(e):
                hint = _(
                    "副本和 master 断开了（配了 replica-serve-stale-data no，断开期间不给读）。"
                )
            elif is_cluster_error(e):
                hint = _(
                    "连到的是原生集群（槽位迁走了？），hetu dns 第一版不支持原生集群，请经代理层连。"
                )
            else:
                hint = ""
            logger.error(
                _("❌ [📣DNS] 读注册表失败，接着答上一次的结果。{hint}{err}").format(
                    hint=hint, err=f"{type(e).__name__}: {e}"
                )
            )
            return None
        if self._read_failing:
            self._read_failing = False
            logger.info(_("ℹ️ [📣DNS] 读注册表恢复正常"))
        raw = raw or {}
        selection = self._select(raw)
        if wait_probes and selection.pending:
            await self.prober.wait()
            selection = self._select(raw)
        for addr in selection.pending:
            selection.excluded[addr] = self.prober.reason(addr)
        self._apply(raw, selection)
        return selection

    def _select(self, raw: Mapping[Any, Any]) -> Selection:
        selection = self.selector.update(raw, self.prober.is_ok)
        if not selection.kept_previous:
            self.prober.track(
                [*(node.addr for node in selection.addresses), *selection.pending]
            )
        return selection

    def _apply(self, raw: Mapping[Any, Any], selection: Selection) -> None:
        self.selection = selection
        if selection.kept_previous and not self._kept_previous:
            logger.error(
                _(
                    "❌ [📣DNS] namespace {ns} 的 {count} 条登记全都过期了，多半是副本落后或者和 master "
                    "断开了，接着答上一次的结果。节点要是真的都没了，答上一次的结果也只是让 Caddy 连不上"
                ).format(ns=self.namespace, count=selection.entries)
            )
        elif self._kept_previous and not selection.kept_previous:
            logger.info(_("ℹ️ [📣DNS] 注册表的登记又在更新了"))
        self._kept_previous = selection.kept_previous
        if raw and not selection.entries and not self._warned_namespace:
            self._warned_namespace = True
            logger.warning(
                _(
                    "⚠️ [📣DNS] 注册表里没有 namespace {ns} 的登记，有的是：{found}。请检查 --namespace"
                    "（和节点配置的 NAMESPACE 一致）"
                ).format(ns=self.namespace, found=sorted(selection.namespaces))
            )
        answered = [node.addr for node in selection.addresses]
        if self._answered is None or set(answered) != set(self._answered):
            before = set(self._answered or ())
            added = sorted(set(answered) - before)
            removed = sorted(before - set(answered))
            logger.info(
                _(
                    "📣 [📣DNS] 答案变了：加了 {added}，去了 {removed}，一共 {count} 台"
                ).format(
                    added=added or "-", removed=removed or "-", count=len(answered)
                )
            )
        self._answered = answered
        self.addresses = tuple(socket.inet_aton(addr) for addr in answered)

    async def run_refresh_loop(self) -> None:
        """
        每 REFRESH_INTERVAL 秒重算一次，直到被取消。出了意料之外的错也接着转：循环要是停了，进程照样活着、
        一直答最后一次的答案，`restart: always` 也救不回来
        """
        while True:
            await asyncio.sleep(REFRESH_INTERVAL)
            try:
                await self.refresh()
            except Exception:  # 记下来，下一次接着算
                logger.exception(
                    _(
                        "❌ [📣DNS] 重算答案出错，接着答上一次的结果，{interval:g} 秒后再算"
                    ).format(interval=REFRESH_INTERVAL)
                )

    # ------------------------------------------------------------ 回答

    def respond(self, data: bytes, peer: Any = None) -> bytes | None:
        """回答一个 UDP 查询，要丢掉的返回 None"""
        query = parse_query(data)
        if query is None:
            return None
        if self._first_query:
            self._first_query = False
            logger.info(
                _(
                    "ℹ️ [📣DNS] 收到第一个查询（来自 {peer}），Caddy 已经在用 hetu dns 了"
                ).format(peer=peer)
            )
        if query.error:
            return build_response(query, query.error)
        if query.edns is not None and query.edns_version != 0:
            return build_response(query, BADVERS)
        if query.name != self.name:
            self._warn_unknown_name(query.name)
            return build_response(query, NXDOMAIN)
        if query.qclass != CLASS_IN:
            return build_response(query, NOTIMP)
        if query.qtype in (TYPE_A, TYPE_ANY):
            return build_response(query, NOERROR, self.addresses)
        return build_response(query, NOERROR)

    def _warn_unknown_name(self, name: bytes) -> None:
        # Go 解析器在答空时会按 resolv.conf 的搜索域拼出 <name>.<后缀> 再问，这些不算
        if name.startswith(self.name + b"."):
            return
        if name in self._warned_names or len(self._warned_names) >= MAX_WARNED_NAMES:
            return
        self._warned_names.add(name)
        logger.warning(
            _(
                "⚠️ [📣DNS] 收到别的名字的查询：{name}，只答 {expected}。请检查 Caddyfile 里 dynamic a "
                "的 name 和 --name 是否一致"
            ).format(
                name=name.decode("ascii", "replace"),
                expected=self.name.decode("ascii"),
            )
        )


class _DnsProtocol(asyncio.DatagramProtocol):
    def __init__(self, service: DnsService) -> None:
        self.service = service
        self.transport: Any = None

    def connection_made(self, transport: Any) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: Any) -> None:
        try:
            response = self.service.respond(data, addr)
        except Exception:
            logger.exception(_("❌ [📣DNS] 处理查询出错"))
            return
        if response is not None and self.transport is not None:
            self.transport.sendto(response, addr)

    def error_received(self, exc: Exception) -> None:
        # Windows 上往已经关掉的端口回包会收到 ICMP 端口不可达（WSAECONNRESET），不用管
        logger.debug("hetu dns: %r", exc)


async def serve_udp(service: DnsService, host: str, port: int) -> Any:
    """在 host:port 上开始应答，返回 transport（关掉它就停止应答）"""
    loop = asyncio.get_running_loop()
    # 写的是主机名（localhost）时只按 IPv4 解析：有的系统先解析出 ::1，Caddy 的 resolvers 写的是 127.0.0.1
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    transport, _protocol = await loop.create_datagram_endpoint(
        lambda: _DnsProtocol(service), local_addr=(host, port), family=family
    )
    return transport


# ---------------------------------------------------------------- 启动检查


class DnsCheckFailed(Exception):
    """`hetu dns` 的启动检查没过：code 是退出码（2 用法或部署错误，3 环境未就绪），消息带改法"""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code


def mask_url(url: str) -> str:
    from ..cli.console import mask_url as _mask

    return _mask(url)


def parse_listen_arg(listen: str) -> tuple[str, int]:
    host, sep, port = listen.rpartition(":")
    try:
        number = int(port) if sep else 0
    except ValueError:
        number = 0
    if not host or not 0 < number < 65536:
        raise DnsCheckFailed(
            EXIT_USAGE,
            _("❌ --listen 要写成 IP:端口，比如 {default}，现在是 {listen!r}").format(
                default=DEFAULT_LISTEN, listen=listen
            ),
        )
    return host.strip("[]"), number


def _route_source(host: str, port: int) -> str | None:
    """连 host:port 时本机用的源地址，推不出来（解析不了、unix socket……）返回 None"""
    try:
        target = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_DGRAM)[0][4]
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(target)
            return s.getsockname()[0]
    except OSError, IndexError:
        return None


def check_network(
    redis_url: str,
    *,
    sysfs: str | None = None,
    interfaces: Mapping[str, str] | None = None,
) -> None:
    """
    只支持 host 网络（同节点，§5.4）：看连 Redis 用的网卡。Caddy 用 host 网络时查的 127.0.0.1 是宿主机的
    回环，bridge 网络里的 hetu dns 收不到
    """
    url = urlparse(redis_url)
    if url.scheme == "unix" or not url.hostname:
        return
    try:
        port = url.port or 6379
    except ValueError:
        port = 6379
    src = _route_source(url.hostname, port)
    if src is None or ipaddress.ip_address(src).is_loopback:
        return
    kwargs: dict[str, Any] = {"interfaces": interfaces}
    if sysfs is not None:
        kwargs["sysfs"] = sysfs
    problem = host_network_problem(src, **kwargs)
    if problem is None or problem.kind != "veth":
        return
    raise DnsCheckFailed(
        EXIT_USAGE,
        _(
            "❌ hetu dns 只支持 host 网络：连 Redis 用的地址 {src} 在 {device} 上，{device} 是容器网络的"
            "虚拟网卡（veth 之类：iflink {iflink} ≠ ifindex {ifindex}）。Caddy 用 host 网络时查的 "
            "127.0.0.1 是宿主机的回环，bridge 网络里的 hetu dns 收不到。{hint}"
        ).format(
            src=src,
            device=problem.device,
            iflink=problem.iflink,
            ifindex=problem.ifindex,
            hint=host_network_hint(),
        ),
    )


def is_master_down(error: BaseException) -> bool:
    """
    副本和 master 断开了、又配了 replica-serve-stale-data no：读命令回 MASTERDOWN。redis-py 把它归成
    ClusterDownError 的子类，但它和原生集群无关，单机副本也会回

    Whether the error is MASTERDOWN (a replica cut off from its master).
    """
    import redis.exceptions as errors

    return isinstance(error, errors.MasterDownError)


def is_cluster_error(error: BaseException) -> bool:
    """
    连到的是原生集群：MOVED、ASK、CLUSTERDOWN 这些（不含 MASTERDOWN，见 `is_master_down`）。redis-py 解析
    错误时会去掉开头的错误码，只能按异常类型认

    Whether the error means a native Redis Cluster (MOVED, ASK, CLUSTERDOWN, ...).
    """
    import redis.exceptions as errors

    if is_master_down(error):
        return False
    return isinstance(
        error,
        (
            errors.AskError,  # MovedError 是它的子类
            errors.ClusterDownError,
            errors.ClusterError,
            errors.TryAgainError,
            errors.ClusterCrossSlotError,
        ),
    )


def _acl_hint() -> str:
    return _(
        "请用只能读 hetu:nodes 的只读用户（ACL 不随复制同步，要在 hetu dns 连的那台副本上建；用的不是 0 "
        "号库时还要加 +select）：{command}"
    ).format(command=acl_command())


async def check_redis(client: Any, redis_url: str) -> list[str]:
    """
    连得上 Redis、HGETALL hetu:nodes 没被拒绝（§6.5），不过抛 DnsCheckFailed；返回要告警的问题：权限过大、
    连的是 master
    """
    import redis.exceptions as errors

    masked = mask_url(redis_url)
    try:
        await client.hgetall(REGISTRY_KEY)
    except errors.AuthenticationError as e:
        raise DnsCheckFailed(
            EXIT_NOT_READY,
            _("❌ 连 Redis {url} 时用户名或口令不对：{err}").format(url=masked, err=e),
        ) from e
    except errors.NoPermissionError as e:
        raise DnsCheckFailed(
            EXIT_NOT_READY,
            _("❌ Redis {url} 拒绝了 HGETALL hetu:nodes（{err}）。{hint}").format(
                url=masked, err=e, hint=_acl_hint()
            ),
        ) from e
    except errors.ResponseError as e:
        if is_master_down(e):
            raise DnsCheckFailed(
                EXIT_NOT_READY,
                _(
                    "❌ Redis {url} 是和 master 断开了的副本（{err}）：配了 replica-serve-stale-data "
                    "no，断开期间不给读。等它重新连上 master，或者指向别的副本"
                ).format(url=masked, err=e),
            ) from e
        if is_cluster_error(e):
            raise DnsCheckFailed(
                EXIT_USAGE,
                _(
                    "❌ Redis {url} 是原生集群（{err}），hetu dns 第一版不支持原生集群，请经代理层连"
                ).format(url=masked, err=e),
            ) from e
        raise DnsCheckFailed(
            EXIT_NOT_READY,
            _("❌ Redis {url} 返回错误：{err}").format(url=masked, err=e),
        ) from e
    except (errors.ConnectionError, errors.TimeoutError, OSError) as e:
        raise DnsCheckFailed(
            EXIT_NOT_READY,
            _("❌ 连不上 Redis {url}：{err}").format(
                url=masked, err=f"{type(e).__name__}: {e}"
            ),
        ) from e
    warnings = []
    too_powerful = []
    try:
        await client.hgetall("hetu:dns-acl-probe")
    except errors.ResponseError:
        pass
    else:
        too_powerful.append(_("除了 hetu:nodes 别的 key 也能读"))
    # 只限了 key 的用户（~hetu:nodes +@all）照样能 FLUSHALL、CONFIG SET、REPLICAOF：拿一条无害的管理命令试
    try:
        await client.execute_command("CONFIG", "GET", "maxmemory")
    except errors.ResponseError:
        pass
    else:
        too_powerful.append(_("能执行管理命令（CONFIG、REPLICAOF 这些）"))
    if too_powerful:
        warnings.append(
            _("⚠️ Redis 用户的权限太大：{what}。网关暴露在公网，{hint}").format(
                what=_("；").join(too_powerful), hint=_acl_hint()
            )
        )
    try:
        role = await client.execute_command("ROLE")
    except errors.ResponseError:
        role = None
    if role and (role[0] in (b"master", "master")):
        warnings.append(
            _(
                "⚠️ hetu dns 连的是 master（{url}）：每秒一条 HGETALL 不大，但架构上读都走副本，请指向副本"
            ).format(url=masked)
        )
    return warnings


async def check_anonymous_access(
    redis_url: str, *, connect: Callable[[str], Any] | None = None
) -> list[str]:
    """
    不带口令也能连上 Redis 就告警（§6.5）。网关是唯一暴露在公网的机器，它被攻破的话，攻击者不用 hetu dns 的
    只读用户，直接用没设口令的 default 用户就能读走全部数据，还能改副本的主从配置（REPLICAOF）、篡改 HeTu 从
    副本读到的数据。hetu dns 自己用的用户权限再小也挡不住这个，只能去掉口令另连一次试试：PING 得通就告警，
    要口令、被拒、连不上都不告警。地址里本来就没有口令时，hetu dns 自己就是这么连上的，直接告警。

    Warn when Redis accepts a connection without a password (an unprotected default user).
    """
    url = urlparse(redis_url)
    if url.scheme == "unix":
        return []
    host = url.netloc.rsplit("@", 1)[-1]
    warning = _(
        "⚠️ Redis {host} 不带口令也能连上：default 用户没设口令。网关暴露在公网，它被攻破的话，攻击者不用 "
        "hetu dns 的只读用户，直接用 default 用户连；default 没收紧权限的话（默认什么都能做），能读走全部数据，"
        "还能改副本的主从配置（REPLICAOF）、篡改 HeTu 从副本读到的数据。请给网关连得到的每台副本的 default "
        "用户设口令（节点 BACKENDS 里 servants 的地址带上口令），或者禁用它（ACL SETUSER default off，节点改用"
        "别的用户）；ACL、requirepass 都不随复制同步，每台都要设。要给 master 也设的话，先在各副本上配好 "
        "masterauth（禁用了 default 的还要配 masteruser），不然主从复制会断"
    ).format(host=host)
    # redis-py 也认查询串里的 username / password
    pairs = parse_qsl(url.query, keep_blank_values=True)
    credential_keys = {"username", "password"}
    has_credentials = (
        url.password is not None
        or bool(url.username)
        or any(key.lower() in credential_keys for key, _value in pairs)
    )
    if not has_credentials:
        return [warning]
    query = urlencode([(k, v) for k, v in pairs if k.lower() not in credential_keys])
    probe_url = url._replace(netloc=host, query=query).geturl()
    client = (connect or _connect_redis)(probe_url)
    try:
        await client.ping()
    except Exception:  # noqa: BLE001 要口令（NOAUTH）、被拒、连不上：都不算能匿名连上
        return []
    finally:
        await client.aclose()
    return [warning]


def check_listen(host: str) -> list[str]:
    """
    --listen 不是回环地址就告警：别人也能查到节点地址。写的是主机名（localhost）时按解析出的地址判断

    Warn unless --listen is a loopback address.
    """
    try:
        addresses = {ipaddress.ip_address(host)}
    except ValueError:
        try:
            addresses = {
                ipaddress.ip_address(str(info[4][0]).split("%", 1)[0])
                for info in socket.getaddrinfo(host, None, type=socket.SOCK_DGRAM)
            }
        except OSError, ValueError:
            addresses = set()
    if addresses and all(address.is_loopback for address in addresses):
        return []
    return [
        _(
            "⚠️ --listen {host} 不是回环地址，别的机器也能查到节点的内网地址。Caddy 和 hetu dns 在同一台机器上，"
            "用 127.0.0.1 就够了"
        ).format(host=host)
    ]


# ---------------------------------------------------------------- 命令


def _setup_logging(verbosity: int) -> None:
    level = logging.DEBUG if verbosity else logging.INFO
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root = logging.getLogger()
    for old in list(root.handlers):
        root.removeHandler(old)
    root.addHandler(handler)
    root.setLevel(level)
    hetu_logger = logging.getLogger("HeTu.root")
    hetu_logger.setLevel(level)
    hetu_logger.propagate = True


def _connect_redis(url: str) -> Any:
    import redis.asyncio

    return redis.asyncio.Redis.from_url(
        url, socket_timeout=REDIS_TIMEOUT, socket_connect_timeout=REDIS_TIMEOUT
    )


def format_report(service: DnsService, selection: Selection, url: str) -> str:
    """--once 打印的报告"""
    lines = [
        _(
            "hetu dns --once：namespace {ns}，名字 {name}，端口 {port}，Redis {url}"
        ).format(
            ns=service.namespace,
            name=service.name.decode("ascii"),
            port=service.port,
            url=mask_url(url),
        ),
        _("本 namespace 的登记 {count} 条").format(count=selection.entries),
    ]
    if selection.kept_previous:
        lines.append(_("这些登记全都过期了（hetu dns 常驻时会接着答上一次的结果）"))
    lines.append(_("会答 {count} 个地址：").format(count=len(selection.addresses)))
    for node in selection.addresses:
        lines.append(
            _("  {addr}  {workers} 个 worker，{conns} 个连接").format(
                addr=node.addr, workers=node.workers, conns=node.conns
            )
        )
    if selection.excluded:
        lines.append(_("不答的地址："))
        for addr, reason in sorted(selection.excluded.items()):
            lines.append(f"  {addr}  {reason}")
    if selection.invalid:
        lines.append(_("解析不了的登记："))
        for field_name, reason in sorted(selection.invalid.items()):
            lines.append(f"  worker {field_name}  {reason}")
    others = {
        ns: n for ns, n in selection.namespaces.items() if ns != service.namespace
    }
    if others:
        lines.append(
            _("别的 namespace：{others}").format(
                others=_("，").join(
                    _("{ns}（{count} 条）").format(ns=ns, count=n)
                    for ns, n in sorted(others.items())
                )
            )
        )
    return "\n".join(lines)


async def _main(args: Any) -> int:
    import os

    url = args.redis or os.environ.get(REDIS_ENV)
    if not url:
        raise DnsCheckFailed(
            EXIT_USAGE,
            _(
                "❌ 要给 Redis 地址：环境变量 {env}（推荐，口令不会出现在进程列表里）或者 --redis，指向副本、"
                "用只读 ACL 用户"
            ).format(env=REDIS_ENV),
        )
    if not 0 < args.port < 65536:
        raise DnsCheckFailed(
            EXIT_USAGE,
            _("❌ --port 要是 1~65535，现在是 {port}").format(port=args.port),
        )
    try:
        name = normalize_name(args.name)
    except UnicodeEncodeError:
        name = b""
    if not name:
        raise DnsCheckFailed(
            EXIT_USAGE,
            _("❌ --name 要是一个 ASCII 域名，现在是 {name!r}").format(name=args.name),
        )
    host, port = parse_listen_arg(args.listen)
    # 服务模式一开始就接住 SIGTERM / SIGINT：容器里作为 PID 1 时，没装处理的 SIGTERM 会被忽略
    stop = asyncio.Event()
    if not args.once:
        _install_stop_signals(stop)
    check_network(url)
    try:
        client = _connect_redis(url)
    except ValueError as e:
        raise DnsCheckFailed(
            EXIT_USAGE,
            _(
                "❌ Redis 地址写得不对（{err}）：要写成 redis://用户:口令@主机:6379/0，指向副本"
            ).format(err=e),
        ) from e
    try:
        warnings = await check_redis(client, url)
        warnings += await check_anonymous_access(url)
        warnings += check_listen(host)
        service = DnsService(
            client, namespace=args.namespace, name=args.name, port=args.port
        )
        if args.once:
            # 告警只打在报告里，不再进 stderr 的日志，终端里不用看两遍
            selection = await service.refresh(wait_probes=True, keep_previous=False)
            assert selection is not None
            print(format_report(service, selection, url))
            for warning in warnings:
                print(warning)
            return 0
        for warning in warnings:
            logger.warning(warning)
        try:
            transport = await serve_udp(service, host, port)
        except OSError as e:
            raise DnsCheckFailed(
                EXIT_NOT_READY,
                _("❌ 监听 {listen} 失败（端口被占用？）：{err}").format(
                    listen=args.listen, err=e
                ),
            ) from e
        try:
            await service.refresh(wait_probes=True)
            logger.info(
                _(
                    "✅ [📣DNS] hetu dns 在 {listen} 上应答 {name}（namespace {ns}，端口 {port}），"
                    "Redis {url}"
                ).format(
                    listen=args.listen,
                    name=service.name.decode("ascii"),
                    ns=args.namespace,
                    port=args.port,
                    url=mask_url(url),
                )
            )
            await _serve_until_stopped(service, stop)
        finally:
            transport.close()
            service.prober.cancel()
        return 0
    finally:
        await client.aclose()


def _install_stop_signals(stop: asyncio.Event) -> None:
    """SIGINT / SIGTERM 到了就 set stop。Windows 的事件循环不支持，那里靠 KeyboardInterrupt"""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
            loop.add_signal_handler(sig, stop.set)


async def _serve_until_stopped(service: DnsService, stop: asyncio.Event) -> None:
    refresher = asyncio.create_task(service.run_refresh_loop(), name="hetu:dns-refresh")
    try:
        await stop.wait()
        logger.info(_("👋 [📣DNS] 收到停止信号，hetu dns 退出"))
    finally:
        refresher.cancel()
        await asyncio.gather(refresher, return_exceptions=True)


def main(args: Any) -> int:
    """
    `hetu dns` 的入口，返回退出码：0 正常退出；2 用法或部署错误（bridge 网络、原生集群、参数不对）；3 环境
    未就绪（Redis 连不上、被 ACL 拒绝、端口被占）。

    Entry point of `hetu dns`; returns the exit code.
    """
    _setup_logging(args.verbose)
    try:
        return asyncio.run(_main(args))
    except DnsCheckFailed as e:
        logger.error(str(e))
        return e.code
    except KeyboardInterrupt:
        # 常驻时 Ctrl+C 是正常停止；--once 被打断了，没跑完
        return 130 if args.once else 0
