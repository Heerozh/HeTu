"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com

订阅驱动的 worker 行缓存。

每个进程（`Backend`）一份，只保留**本 worker 有客户端订阅着的行**：hub 对某行频道的 SUBSCRIBE
ack 后把它"激活"，之后该行的每次变更都会以带版本号的通知到达本进程（commit 主动 PUBLISH）。
缓存据此对每个激活行维护版本下限 floor（已知至少到达的版本），副本读回的行版本低于 floor 即判
滞后、不入缓存，改走 master 上的权威读；写事务另有 commit 的 VER 检查兜底。不设 TTL。

行进缓存有两条路：本进程 commit 的写穿，以及**收到第一条通知之后**（floor 变成整数，副本读
才校验得了）的副本读。刚激活、还没收到过通知的行（floor 未知）读回来照常返回给调用方，但不
入缓存——无从判断这份副本够不够新，缓存下来就没有失效路径了；也不为了填缓存去打 master：
项目的设计约束是能不读 master 就不读（CLAUDE.md），权威读只留给"副本确实滞后"的兜底。

floor 靠 `_version` 单调才挡得住滞后副本，而同 id 删除后重插版本从 1 重来：滞后副本上删除前的
旧行版本反而比新一代高，floor 挡不住。所以一个频道在本次激活周期内只要见过删除通知，之后就
只信权威读（副本读回的行不再入缓存），直到最后一个订阅者退订、下次激活重新开始。

事务读（`SessionRepository`）与订阅刷新（`SubscriptionBroker`）都经过 `CachedRowReader`，所以
客户端收到的行与服务端事务读到的永远是同一份。设计稿：
`docs/superpowers/specs/2026-09-22-row-cache-design.md`。

历史（原 `redis/batch.py`，已删）：
- 短 TTL 的 LRU 缓存：否决。客户端收到订阅更新时读到的还是老数据，而把失效和通知绑定在
  每连接一条 pubsub 的年代太不灵活；每 worker 一个 hub 之后这个绑定变得自然，就是本模块。
- 跨请求合批（前一个请求没完成就把后续请求 pipeline 成一批）：否决。以延迟换单节点吞吐，
  读写分离下副本并行更划算；事务内的合批（`range` / `get_many`）已经做了。
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Hashable, Iterable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, cast

import numpy as np

from .base import RowFormat

if TYPE_CHECKING:
    from .base import BackendClient
    from .table import TableReference


class _Floor(Enum):
    """floor 的两个哨兵值，其余情况 floor 是 int 版本号"""

    UNKNOWN = "unknown"  # 刚激活 / 节点恢复后重新激活：尚无任何版本信息
    DELETED = "deleted"  # 最近一次通知是删除


UNKNOWN = _Floor.UNKNOWN
DELETED = _Floor.DELETED

Floor = int | _Floor


@dataclass(frozen=True, slots=True)
class Lease:
    """读之前取的填充凭据：频道 + 取时的激活代次。失活再激活会推进代次，旧凭据作废"""

    channel: str
    epoch: int


@dataclass(slots=True)
class RowCacheStats:
    hits: int = 0
    misses: int = 0  # 行不在缓存里的读，含下面的 absent_hits
    # 已知该行不存在（最近一次通知是删除），直接返回 None、没打库的读
    absent_hits: int = 0
    authoritative_reads: int = 0  # 权威读的调用次数（批读算一次）
    size: int = 0


class RowCache:
    """
    进程级行缓存本体。键是行频道名（就是 Redis 的行 key），值是 `np.record`。

    - `activate` / `deactivate`：hub 在 SUBSCRIBE ack 后 / 最后一个本地订阅者退订时调用，
      同一频道可由多个 hub（多 servant）各自激活，最后一个失活才清掉行与 floor。
    - `notify`：hub 收到带版本号的行通知时调用；版本高于缓存行则逐出，floor 单调抬升。
      删除通知还把该频道标成"只信权威读"，直到本次激活结束。
    - `fill` / `put_committed`：读路径填充 / commit 写穿；都受 floor 约束。
    - 所有方法都是同步的纯 dict 操作，可以在 hub 的监听协程里直接调用。

    Process-wide cache of the rows this worker's clients subscribe to, keyed by row
    channel (the Redis row key). A row is only cacheable while some hub reports its
    SUBSCRIBE as acked (`activate`), so every later change arrives as a versioned
    notification (`notify`) that evicts it. Each channel keeps a version floor — the
    highest version this process knows about — and `fill` / `put_committed` refuse
    anything below it. Every method is a plain synchronous dict operation, safe to
    call from a hub's listener coroutine.
    """

    def __init__(self, max_rows: int = 200_000):
        self.max_rows = max_rows
        self.stats = RowCacheStats()
        self._rows: OrderedDict[str, np.record] = OrderedDict()
        self._active: dict[str, set[Hashable]] = {}
        self._epoch: dict[str, int] = {}
        self._floor: dict[str, Floor] = {}
        # 本次激活周期内收到过删除通知的频道：同 id 重插后版本从 1 重来，滞后副本上删除前的
        # 旧行版本反而更高、floor 挡不住，所以这些频道的副本读一律不信
        self._authoritative_only: set[str] = set()
        self._next_epoch = 0

    def __len__(self) -> int:
        return len(self._rows)

    # ------------------------------------------------------------ 激活状态

    def activate(self, channel: str, owner: Hashable) -> None:
        """hub 对该频道的订阅已 ack：允许缓存。首个 owner 激活时 floor 为未知"""
        owners = self._active.get(channel)
        if owners is None:
            self._active[channel] = {owner}
            self._next_epoch += 1
            self._epoch[channel] = self._next_epoch
            self._floor[channel] = UNKNOWN
        else:
            owners.add(owner)

    def deactivate(self, channel: str, owner: Hashable) -> None:
        """owner 不再订阅该频道；最后一个 owner 走了才清掉行、floor 与代次"""
        owners = self._active.get(channel)
        if owners is None:
            return
        owners.discard(owner)
        if owners:
            return
        del self._active[channel]
        del self._epoch[channel]
        del self._floor[channel]
        self._authoritative_only.discard(channel)
        self._drop(channel)

    def is_active(self, channel: str) -> bool:
        return channel in self._active

    def lease(self, channel: str) -> Lease | None:
        """读之前取：未激活返回 None（不缓存），否则带当前代次"""
        epoch = self._epoch.get(channel)
        if epoch is None:
            return None
        return Lease(channel, epoch)

    def floor(self, channel: str) -> Floor:
        return self._floor.get(channel, UNKNOWN)

    def is_absent(self, channel: str) -> bool:
        """
        已知该行不存在：最近一次通知是删除。删除通知和别的通知一样权威（激活期间不漏），
        所以读路径可以直接当 None 返回，不必打库——这是"负结果"的缓存。
        """
        return self._floor.get(channel) is DELETED

    def needs_authoritative(self, channel: str) -> bool:
        """
        这个频道的副本读一概不可信，只能走 master 的权威读：本次激活周期内见过删除通知
        （同 id 重插后版本从 1 重来，floor 挡不住滞后副本上删除前的旧行）。
        """
        return channel in self._authoritative_only

    def replica_floor(self, channel: str) -> int | None:
        """
        读路径用：副本读回的行不低于这个版本才收进缓存；返回 None 表示 floor 还未知
        （刚激活、还没收到过通知），此时读回的行无从判断够不够新，可以用但不能入缓存。
        """
        if channel in self._authoritative_only:
            return None
        floor = self._floor.get(channel, UNKNOWN)
        return floor if isinstance(floor, int) else None

    def clear(self) -> None:
        """全部失活并清空（hub 关闭 / 测试用）"""
        self._rows.clear()
        self._active.clear()
        self._epoch.clear()
        self._floor.clear()
        self._authoritative_only.clear()
        self.stats.size = 0

    # ---------------------------------------------------------------- 读写

    def get(self, channel: str) -> np.record | None:
        """命中返回副本（调用方可就地改），未缓存返回 None"""
        row = self._rows.get(channel)
        if row is None:
            self.stats.misses += 1
            return None
        self._rows.move_to_end(channel)
        self.stats.hits += 1
        return cast(np.record, row.copy())

    def fill(self, lease: Lease, row: np.record, *, authoritative: bool) -> bool:
        """
        读路径把读回的行放进缓存。
        - lease 的代次已过期（退订又重订）→ 拒；
        - floor 已知：行版本低于 floor 说明读到的是滞后副本（或读发生在更新的通知之前）→ 拒；
        - floor 未知 / 已删除、或本次激活周期内见过删除通知：只接受权威读回的行。
        通过则 floor 抬到该行版本。返回是否写入。
        """
        channel = lease.channel
        if self._epoch.get(channel) != lease.epoch:
            return False
        return self._store(channel, row, authoritative)

    def put_committed(self, lease: Lease, row: np.record) -> bool:
        """
        commit 成功后的写穿：本进程明知该行此刻在 master 上就是这个样子（版本已 +1）。
        `lease` 要在提交**之前**取：提交往返期间本频道失活过（最后一个订阅者退订、之后
        又有人订回来）的话，那段时间别的进程写的行本进程没收到通知，此刻写穿的就可能是
        过时的行，代次对不上直接丢弃，留给下次的权威读。已有更高版本的通知先到则不覆盖。
        """
        return self.fill(lease, row, authoritative=True)

    def _store(self, channel: str, row: np.record, authoritative: bool) -> bool:
        version = int(row["_version"])
        floor = self._floor[channel]
        if isinstance(floor, int):
            if version < floor:
                return False
            if not authoritative and channel in self._authoritative_only:
                return False
        elif not authoritative:
            return False
        self._rows[channel] = cast(np.record, row.copy())
        self._rows.move_to_end(channel)
        self._floor[channel] = version
        while len(self._rows) > self.max_rows:
            self._rows.popitem(last=False)
        self.stats.size = len(self._rows)
        return True

    def notify(self, channel: str, version: int) -> None:
        """
        收到该行的变更通知（或 commit RACE 回显的 master 当前版本）。
        `version` 为 0 表示删除：逐出、floor 置为已删除，并且本次激活周期内不再信副本读
        （重插后版本从 1 重来，floor 挡不住滞后副本上的旧行）；否则版本高于缓存行才逐出，
        floor 取 max（多 hub 重复送达、乱序到达都幂等）。未激活的频道忽略。
        """
        floor = self._floor.get(channel)
        if floor is None:
            return
        if version == 0:
            self._drop(channel)
            self._floor[channel] = DELETED
            self._authoritative_only.add(channel)
            return
        row = self._rows.get(channel)
        if row is not None and int(row["_version"]) < version:
            self._drop(channel)
        if isinstance(floor, int):
            self._floor[channel] = max(floor, version)
        else:
            self._floor[channel] = version

    def evict(self, channel: str) -> None:
        """只丢行、不动 floor（commit RACE 时用于本事务碰过的行）"""
        self._drop(channel)

    def _drop(self, channel: str) -> None:
        if self._rows.pop(channel, None) is not None:
            self.stats.size = len(self._rows)


class CachedRowReader:
    """
    事务与订阅共用的读路径：缓存命中直接返回；已知不存在的行直接给 None；其余读
    `fallback`——本次 miss 时读副本用的客户端（事务传 `session.master_or_servant`，订阅传
    `backend.servant`），读回的行够新才填进缓存。只有两种情况改走权威读：副本读回的版本低于
    floor（确实滞后），以及频道见过删除通知（同 id 重插后副本上可能还是删除前的旧行）。
    权威读固定走 `backend.master.get_authoritative`（master 上执行的 Lua HGETALL，代理模式下
    唯一一定被送到主节点的读）。返回的都是缓存外的独立 record，调用方可随意改。

    The single read path shared by transactions and subscription refreshes. A cache hit
    returns straight away; a row known to be deleted returns None without any round
    trip; everything else reads `fallback` — the client used for replica reads
    (`session.master_or_servant` for transactions, `backend.servant` for
    subscriptions) — and is cached only if it is new enough. Only two cases escalate to
    an authoritative read on the master: the replica came back below the version floor
    (it is lagging), and the channel has seen a delete notification (after a same-id
    reinsert the replica may still hold the pre-delete row). Returned records are
    copies outside the cache, so callers may modify them freely.
    """

    def __init__(self, backend: Any):
        self._backend = backend

    def _cache(self, ref: TableReference) -> RowCache | None:
        cache: RowCache | None = self._backend.row_cache
        if cache is None or ref.comp_cls.volatile_:
            return None  # 易失组件（direct_set 的对象）不缓存
        return cache

    async def get(
        self, ref: TableReference, row_id: int, fallback: BackendClient
    ) -> np.record | None:
        cache = self._cache(ref)
        if cache is None:
            return await fallback.get(ref, row_id, RowFormat.STRUCT)
        master: BackendClient = self._backend.master
        channel = master.row_channel(ref, row_id)
        row = cache.get(channel)
        if row is not None:
            return row
        lease = cache.lease(channel)
        if lease is None:
            return await fallback.get(ref, row_id, RowFormat.STRUCT)  # 没人订：不缓存
        if cache.is_absent(channel):
            cache.stats.absent_hits += 1
            return None  # 已知已删：删除通知本身就是权威的，0 往返
        if cache.needs_authoritative(channel):
            authoritative = True  # 见过删除通知：副本上可能是删除前的旧行，一概不信
        else:
            row = await fallback.get(ref, row_id, RowFormat.STRUCT)
            floor = cache.replica_floor(channel)
            # 副本滞后（版本低于已知下限，或该存在的行还读不到）→ 改走权威读；
            # floor 还未知（刚订上、没收到过通知）→ 读回的行照用，只是不入缓存（fill 会拒）
            authoritative = floor is not None and (
                row is None or int(row["_version"]) < floor
            )
        if authoritative:
            cache.stats.authoritative_reads += 1
            row = await master.get_authoritative(ref, row_id)
        if row is not None:
            cache.fill(lease, row, authoritative=authoritative)
        return row

    async def get_many(
        self, ref: TableReference, row_ids: Iterable[int], fallback: BackendClient
    ) -> list[np.record | None]:
        ids = list(row_ids)
        cache = self._cache(ref)
        if cache is None:
            rows = await fallback.get_many(ref, ids, RowFormat.STRUCT)
            return cast(list[np.record | None], rows)
        master: BackendClient = self._backend.master
        result: list[np.record | None] = [None] * len(ids)
        replica_idx: list[int] = []  # 未激活的 + floor 未知的 + floor 已知的：读副本
        auth_idx: list[int] = []  # 见过删除通知的：只能权威读
        leases: dict[int, Lease] = {}
        floors: dict[int, int] = {}
        for i, row_id in enumerate(ids):
            channel = master.row_channel(ref, row_id)
            row = cache.get(channel)
            if row is not None:
                result[i] = row
                continue
            lease = cache.lease(channel)
            if lease is None:
                replica_idx.append(i)
                continue
            if cache.is_absent(channel):
                cache.stats.absent_hits += 1
                continue  # 已知已删：结果保持 None，不进任何一组
            leases[i] = lease
            if cache.needs_authoritative(channel):
                auth_idx.append(i)
                continue
            floor = cache.replica_floor(channel)
            if floor is not None:
                floors[i] = floor  # 有下限才能判滞后；没有就是照用不入缓存
            replica_idx.append(i)

        stale_idx: list[int] = []

        async def read_replica() -> None:
            if not replica_idx:
                return
            rows = cast(
                list[np.record | None],
                await fallback.get_many(
                    ref, [ids[i] for i in replica_idx], RowFormat.STRUCT
                ),
            )
            for i, row in zip(replica_idx, rows):
                lease = leases.get(i)
                floor = floors.get(i)
                # 有版本下限才判得了滞后；没有（floor 未知）就照用，fill 会拒掉不入缓存
                if floor is not None and (row is None or int(row["_version"]) < floor):
                    stale_idx.append(i)
                    continue
                if lease is not None and row is not None:
                    cache.fill(lease, row, authoritative=False)
                result[i] = row

        async def read_authoritative(idx: list[int]) -> None:
            if not idx:
                return
            cache.stats.authoritative_reads += 1
            rows = await master.get_many_authoritative(ref, [ids[i] for i in idx])
            for i, row in zip(idx, rows):
                result[i] = row
                if row is not None:
                    cache.fill(leases[i], row, authoritative=True)

        await asyncio.gather(read_replica(), read_authoritative(auth_idx))
        await read_authoritative(stale_idx)
        return result
