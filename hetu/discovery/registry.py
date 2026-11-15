"""
节点发现的注册表（设计稿 docs/superpowers/specs/2026-11-15-node-discovery-design.md §4）。

一个 hash：key 是 `hetu:nodes`，field 是 worker id（十进制），value 是一条 JSON 登记（`NodeEntry`）。同一时刻
一个 worker id 只有一个持有者，field 不会撞；整个 hash 最多 1024 个 field，不用清理。

- 节点这边（`NodeRegistrar`）：每个 worker 开好服后每 5 秒写一次自己的登记，关服第一步改成 draining，释放
  Worker ID 租约之前删掉。只在发号围栏开着时写、删：过了安全期，这个 worker id 可能已经被别的进程接手了。
- 读的一方（`hetu nodes`、`hetu dns`）自己判断过期：Redis 7 没有 hash field 的过期。

@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024-2026, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

The node registry: one Redis hash with a JSON entry per worker id.
"""

import asyncio
import logging
import time
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

import msgspec

from ..common.snowflake_id import RENEW_INTERVAL
from ..i18n import _

if TYPE_CHECKING:
    from ..common.snowflake_id import WorkerKeeper

logger = logging.getLogger("HeTu.root")

REGISTRY_KEY = "hetu:nodes"
# 登记的格式版本。读的一方遇到不认识的版本时跳过这条并告警
ENTRY_VERSION = 1
# 节点多久写一次登记（秒），和续约同频
WRITE_INTERVAL = RENEW_INTERVAL
# 登记多久没变（hetu dns）、或者 ts 离本机时钟多远（hetu nodes），就算节点已经死了（秒）。节点每
# WRITE_INTERVAL 秒写一次，ts 每次都不一样
STALE_SEC = 15.0
# 关服时写 draining、删登记的时限（秒），写不进去不挡关服
WRITE_TIMEOUT = 1.0
# 写进 draining 之后照常服务多久再停止监听（秒）：等网关上的 hetu dns 重算（每秒一次）、Caddy 的缓存
# 过期（refresh 2s），加上副本延迟，最多约 3 秒（§4.3）
DEREGISTER_WAIT = 4.0

STATE_READY = "ready"
STATE_DRAINING = "draining"


class NodeEntry(msgspec.Struct, kw_only=True):
    """
    一个 worker 的登记（`hetu:nodes` 里一个 field 的值）。

    One worker's registry entry.
    """

    # 格式版本
    v: int = ENTRY_VERSION
    # 租约的持有者标识（RedisWorkerKeeper.identity），排查用
    node: str
    # NAMESPACE、INSTANCES
    ns: str
    inst: list[str]
    # 广播地址、端口（LISTEN 的端口）
    addr: str
    port: int
    # STATE_READY 或 STATE_DRAINING
    state: str
    # 本 worker 的在线连接数
    conns: int
    # 写入时的本机时钟（秒）
    ts: float
    # 本 worker 就绪的时刻（秒）
    up: float
    # HeTu 版本
    ver: str


class _Version(msgspec.Struct):
    v: int


class EntryError(ValueError):
    """一条登记解析不了（内容坏了），或者是不认识的格式版本"""


_encoder = msgspec.json.Encoder()
_version_decoder = msgspec.json.Decoder(_Version)
_entry_decoder = msgspec.json.Decoder(NodeEntry)


def encode_entry(entry: NodeEntry) -> bytes:
    return _encoder.encode(entry)


def decode_entry(raw: bytes | str) -> NodeEntry:
    """
    解析一条登记，内容坏了、或者是不认识的格式版本时抛 EntryError。

    Decode one entry; raises EntryError for garbage or an unknown format version.
    """
    try:
        version = _version_decoder.decode(raw).v
    except msgspec.DecodeError as e:
        raise EntryError(str(e)) from e
    if version != ENTRY_VERSION:
        raise EntryError(
            _("不认识的登记格式版本 {version}（这个版本的 HeTu 认识 {known}）").format(
                version=version, known=ENTRY_VERSION
            )
        )
    try:
        return _entry_decoder.decode(raw)
    except msgspec.DecodeError as e:
        raise EntryError(str(e)) from e


def decode_registry(
    raw: Mapping[Any, Any],
) -> tuple[dict[int, NodeEntry], dict[str, str]]:
    """
    HGETALL `hetu:nodes` 的结果 → ({worker id: 登记}, {解析不了的 field: 原因})。

    Decode a whole HGETALL reply: (entries by worker id, {bad field: reason}).
    """
    entries: dict[int, NodeEntry] = {}
    invalid: dict[str, str] = {}
    for field, value in raw.items():
        name = (
            field.decode("utf-8", "replace") if isinstance(field, bytes) else str(field)
        )
        try:
            worker_id = int(name)
        except ValueError:
            invalid[name] = _("field 不是 worker id")
            continue
        try:
            entries[worker_id] = decode_entry(value)
        except EntryError as e:
            invalid[name] = str(e)
    return entries, invalid


def is_fresh(entry: NodeEntry, now: float) -> bool:
    """只按本机时钟判断（hetu nodes 用；hetu dns 按登记有没有在变判断）：ts 离现在不超过 STALE_SEC"""
    return abs(now - entry.ts) <= STALE_SEC


def summarize(
    entries: Mapping[int, NodeEntry], invalid: Mapping[str, str], now: float
) -> dict[str, Any]:
    """
    `hetu nodes` 的输出：没过期的按 (namespace, 地址, 端口) 汇总，过期的、解析不了的单独列出，再列出注册表里
    出现过的 namespace。过期只按 ts 和本机时钟判断（一次性的命令没有历史）。

    Summary for `hetu nodes`: live entries grouped by address, plus stale and bad ones.
    """
    groups: dict[tuple[str, str, int], dict[str, Any]] = {}
    stale = []
    for worker_id, entry in sorted(entries.items()):
        age = round(now - entry.ts, 3)
        if not is_fresh(entry, now):
            stale.append(
                {
                    "worker_id": worker_id,
                    "ns": entry.ns,
                    "addr": entry.addr,
                    "port": entry.port,
                    "state": entry.state,
                    "node": entry.node,
                    "age": age,
                }
            )
            continue
        group = groups.setdefault(
            (entry.ns, entry.addr, entry.port),
            {
                "ns": entry.ns,
                "addr": entry.addr,
                "port": entry.port,
                "workers": 0,
                STATE_READY: 0,
                STATE_DRAINING: 0,
                "conns": 0,
                "max_age": age,
                "versions": set(),
                "worker_ids": [],
            },
        )
        group["workers"] += 1
        group[entry.state] = group.get(entry.state, 0) + 1
        group["conns"] += entry.conns
        group["max_age"] = max(group["max_age"], age)
        group["versions"].add(entry.ver)
        group["worker_ids"].append(worker_id)
    nodes = []
    for key in sorted(groups):
        group = groups[key]
        group["versions"] = sorted(group["versions"])
        nodes.append(group)
    return {
        "nodes": nodes,
        "stale": stale,
        "invalid": [
            {"field": field, "error": error} for field, error in sorted(invalid.items())
        ],
        "namespaces": sorted({entry.ns for entry in entries.values()}),
    }


def fence_open(keeper: WorkerKeeper) -> bool:
    """发号围栏开着：租约还是本进程的（确认过之后还在安全期内，也没有被关掉）"""
    deadline = keeper.lease_deadline
    return deadline is not None and time.monotonic() <= deadline


class NodeRegistrar:
    """
    一个 worker 在注册表里的登记（§4.3），挂在 `WorkerRuntime` 上：

    - `start()`：开好服后调，立刻写一次 ready，之后每 WRITE_INTERVAL 秒写一次（尽力而为，异常只告警）；
    - `mark_draining()`：关服第一步，限时写一次 draining，之后照样每 WRITE_INTERVAL 秒写；
    - `unregister()`：释放租约之前停掉循环、删掉登记。

    每次写、删之前看发号围栏（`fence_open`）：关了或者过了安全期就不写、不删，这时这个 worker id 可能已经
    被别的进程接手，field 是对方的。写、删都在一把锁里排队，先发的那条先落地，draining 不会被一条晚到的
    ready 盖掉。

    client 每次调用返回一个 redis.asyncio 客户端（指向 master）；keeper、worker_id 是本 worker 的租约；
    conns 返回当前的在线连接数。

    Keeps one worker's registry entry up to date, guarded by the lease fence.
    """

    def __init__(
        self,
        client: Callable[[], Any],
        keeper: WorkerKeeper,
        worker_id: int,
        *,
        node: str,
        namespace: str,
        instances: list[str],
        addr: str,
        port: int,
        version: str,
        conns: Callable[[], int],
    ) -> None:
        self._client = client
        self.keeper = keeper
        self.field = str(worker_id)
        self.node = node
        self.namespace = namespace
        self.instances = list(instances)
        self.addr = addr
        self.port = port
        self.version = version
        self._conns = conns
        self.state = STATE_READY
        self.up = time.time()
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._failing = False

    def entry(self) -> NodeEntry:
        return NodeEntry(
            node=self.node,
            ns=self.namespace,
            inst=self.instances,
            addr=self.addr,
            port=self.port,
            state=self.state,
            conns=self._conns(),
            ts=time.time(),
            up=self.up,
            ver=self.version,
        )

    async def write(self) -> bool:
        """按当前状态写一次登记。围栏关着时不写，返回 False"""
        async with self._lock:
            if not fence_open(self.keeper):
                return False
            await self._client().hset(
                REGISTRY_KEY, self.field, encode_entry(self.entry())
            )
            return True

    async def _run(self) -> None:
        while True:
            try:
                await self.write()
                if self._failing:
                    self._failing = False
                    logger.info(_("ℹ️ [📣Discovery] 写注册表恢复正常"))
            except Exception as e:  # noqa: BLE001 尽力而为，下个周期再试
                if not self._failing:
                    self._failing = True
                    logger.warning(
                        _(
                            "⚠️ [📣Discovery] 写注册表失败，{interval} 秒后重试：{err}"
                        ).format(
                            interval=WRITE_INTERVAL, err=f"{type(e).__name__}: {e}"
                        )
                    )
            await asyncio.sleep(WRITE_INTERVAL)

    def start(self) -> None:
        """开好服之后调：立刻写一次 ready，之后每 WRITE_INTERVAL 秒写一次"""
        assert self._task is None, "already started"
        self.up = time.time()
        self._task = asyncio.create_task(self._run(), name="hetu:discovery")
        logger.info(
            _(
                "📣 [📣Discovery] 节点发现：登记地址 {addr}:{port}（worker id {field}）"
            ).format(addr=self.addr, port=self.port, field=self.field)
        )

    async def mark_draining(self) -> bool:
        """
        关服第一步：把登记改成 draining，限时 WRITE_TIMEOUT 秒。写成功了返回 True（调用方照常服务
        DEREGISTER_WAIT 秒再停止监听）；没登记过、写不进去、围栏关着返回 False
        """
        if self._task is None:
            return False
        self.state = STATE_DRAINING
        try:
            async with asyncio.timeout(WRITE_TIMEOUT):
                return await self.write()
        except Exception as e:  # noqa: BLE001 写不进去不挡关服
            logger.warning(
                _(
                    "⚠️ [📣Discovery] 关服时把登记改成 draining 失败，网关要等它过期（{stale:g} 秒）"
                    "才不再连过来：{err}"
                ).format(stale=STALE_SEC, err=f"{type(e).__name__}: {e}")
            )
            return False

    async def stop(self) -> None:
        """停掉写登记的循环，可以重复调用"""
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def unregister(self) -> None:
        """释放租约之前调：停掉循环，围栏开着时删掉登记（限时 WRITE_TIMEOUT 秒），可以重复调用"""
        started = self._task is not None
        try:
            async with asyncio.timeout(WRITE_TIMEOUT):
                async with self._lock:
                    await self.stop()
                    if started and fence_open(self.keeper):
                        await self._client().hdel(REGISTRY_KEY, self.field)
        except Exception as e:  # noqa: BLE001 删不掉就等它过期
            await self.stop()
            logger.warning(
                _(
                    "⚠️ [📣Discovery] 关服时删登记失败，等它过期（{stale:g} 秒）：{err}"
                ).format(stale=STALE_SEC, err=f"{type(e).__name__}: {e}")
            )
