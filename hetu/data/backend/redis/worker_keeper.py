import asyncio
import logging
from time import monotonic
from typing import TYPE_CHECKING, final, override

import redis.asyncio

from ....common.helper import lease_identity, lease_owner_id, lease_value
from ....common.helper import lease_owner_exited as _owner_exited
from ....common.snowflake_id import (
    FENCE_MARGIN_SEC,
    MAX_WORKER_ID,
    RENEW_INTERVAL,
    WORKER_ID_EXPIRE_SEC,
    WORKER_ID_KEY,
    WorkerKeeper,
)
from ....i18n import _

if TYPE_CHECKING:
    import redis

logger = logging.getLogger("HeTu.root")

__all__ = [
    "FENCE_MARGIN_SEC",
    "LUA_RELEASE_IF_MINE",
    "WORKER_ID_EXPIRE_SEC",
    "WORKER_ID_KEY",
    "RedisWorkerKeeper",
    "live_worker_ids",
    "live_worker_leases",
]


# 释放租约的 compare-and-delete：只删自己的那把。Redis 没有单条命令能做"值相符才删"
# （GETDEL 会先删掉才让你看见值，来不及判断），所以用一条单 key 的 Lua——单 key 脚本在
# Redis Cluster 下也安全。和 Redlock 的安全释放是同一个套路。
LUA_RELEASE_IF_MINE = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""

# 接手持有者标识和自己相同的租约（多半是自己重启前留下的）：值还是读到时的那个（ARGV[1]）、而且至少
# REUSE_STALE_SEC 秒没被续约（剩余 TTL 不超过 ARGV[3] 毫秒），才换成自己的值。判断和改写在同一条单 key
# 脚本里，中间不会被续约插进来。返回 1 接手了；0 值已经变了（过期了，或者被别人拿走了）；-1 还有人在续
LUA_TAKE_OVER_IF_STALE = """
if redis.call('get', KEYS[1]) ~= ARGV[1] then
    return 0
end
if redis.call('pttl', KEYS[1]) > tonumber(ARGV[3]) then
    return -1
end
redis.call('set', KEYS[1], ARGV[2], 'EX', ARGV[4])
return 1
"""

# 接手标识相同的租约之前，要确认它至少这么多秒没被续约：活着的持有者每 RENEW_INTERVAL 秒续一次（TTL
# 刷回满值），多留 2 秒给续约的抖动。非正常退出后马上重启的，开服最多因此多等这么久
REUSE_STALE_SEC = RENEW_INTERVAL + 2


def _decode(value: bytes | str | None) -> str | None:
    return value.decode("utf-8", "replace") if isinstance(value, bytes) else value


def live_worker_leases(io: redis.Redis | redis.RedisCluster) -> dict[int, str]:
    """
    还持有租约的 worker id → 持有者（node_id）：服务器在跑、hetu call / shell 在跑（持有者带
    `cli:` 前缀），或者异常退出后租约还没过期（最多 WORKER_ID_EXPIRE_SEC 秒）。一次 pipeline
    查完所有 id，cluster 下按 slot 分发。

    例外：Windows 上本机已经退出的进程留下的租约不算，见 `hetu.common.helper.lease_owner_exited`。
    """
    pipe = io.pipeline()
    for worker_id in range(MAX_WORKER_ID + 1):
        pipe.get(f"{WORKER_ID_KEY}:{worker_id}")
    return {
        worker_id: owner.decode("utf-8", "replace")
        if isinstance(owner, bytes)
        else str(owner)
        for worker_id, owner in enumerate(pipe.execute())
        if owner is not None and not _owner_exited(owner)
    }


def live_worker_ids(io: redis.Redis | redis.RedisCluster) -> list[int]:
    """还持有租约的 worker id，见 `live_worker_leases`"""
    return list(live_worker_leases(io))


@final
class RedisWorkerKeeper(WorkerKeeper):
    """
    基于 Redis 原生命令的 Worker ID 租约管理器，多机安全。生产环境（Redis 后端）走这条路。

    此类的目的是：
    1. 分配空余worker id，并让宕机的worker id会得到释放
    2. 让"租约被别人抢走"这件事能被可靠检测到（否则两个 worker 拿同一个 id 会静默产生
       重复雪花ID——雪花ID是所有Component的主键，后果是insert撞主键、以及按id去重的
       FutureCalls被静默吞掉）

    ## 三个操作都必须是 CAS（比较并交换），少一个都会漏

    「租约过期后被别的 worker 接手」是常态（宕机恢复就靠它），所以每个操作都得先确认
    "这把锁还是我的"：

    * **续约** 用 `GETEX key EX ttl`：一条原生命令，原子地"取回旧值 + 刷新过期时间"。
      拿回来的值不是自己的 node_id 就说明被抢了，抛 SystemExit 让上层重启 worker。
      不能用 `EXPIRE`——它只看 key 在不在、**不看 value**，key 被别人用 SET NX 抢走后
      它照样返回1，恰恰漏掉会产生重复ID的那种情况。
      副作用是"不是自己的"时会把对方的 TTL 也刷成满值，无害：对方本来每5秒就自己刷一次，
      而我们这侧马上关掉发号围栏、退出，不会反复刷。
    * **接手标识相同的租约**（多半是自己重启前留下的）用 LUA_TAKE_OVER_IF_STALE，见下面"复用"。
    * **释放** 用 LUA_RELEASE_IF_MINE。原来是无条件 `DEL`：如果自己的租约早已过期并被别人
      接手，关服时会删掉**别人的**租约，对方瞬间变无主，第三个 worker 又能抢走同一个 id。

    `SET key node_id NX EX ttl`（抢新id）本身就是原子的，不需要额外处理。

    比对用的值（node_id）是"持有者标识 + 本进程随机生成的部分"（`lease_value`）：标识就算撞了，
    换了一个进程也比对不上。

    ## 复用

    worker id 一共 1024 个。非正常退出（kill -9、OOM、崩溃）留下的租约要等 60 秒才过期，几百个
    worker 一起非正常重启时，新旧两份加起来就可能把池子用光，所以重启后要尽量接回自己原来的 id。
    "自己原来的"按持有者标识认（`lease_owner_id`：同一个容器重启、管理进程以同一序号重启 worker 时
    不变）。但标识相同、随机部分不同的租约，也可能属于一个标识撞了的、还活着的进程，所以接手之前
    要确认它至少 REUSE_STALE_SEC 秒没被续约（活着的每 5 秒续一次）；还在续的不碰，另拿一个空的。
    正常关服会释放租约，重启时没有这一步。

    "没被续约"只能说明它停住了，不能证明它死了：一个标识撞了的进程如果整个卡住超过 REUSE_STALE_SEC
    秒，正好赶上另一个同标识的进程开服，会被当成死的接手。它醒来后下一次续约就会发现、关围栏、退出，但在那之前
    发出的号可能和接手的进程重复。要完全排除，得等到它的发号围栏过了（45 秒）才接手，复用就失去了意义。
    正常情况下同一个标识的上一个进程一定已经退出了（容器重启、管理进程等旧 worker 退出才起新的），
    只有标识撞了（token 文件被打进镜像、几个容器共用 /var/tmp）才会遇到。

    开服时用一次 pipeline 读完所有 id 的租约，不续期。原来是逐个 `GETEX`，会把扫到的每个租约
    （包括已经死掉的）都刷回 60 秒：池子被死租约占满时，拿不到 id 的 worker 每秒重试、每次都把它们
    再续一遍，死租约就永远过不了期；而且每次开服是对 master 最多 1024 次的串行写。

    ## 配套的本地围栏

    上面的检测都是**事后**的：worker 冻结后醒来，到下一次 keep_alive 跑到之间还有最长一个
    续约周期，这期间它照样拿旧 worker_id 发号。所以每次确认"租约还是我的"时都会推进
    `lease_deadline`（基类属性），`SnowflakeID` 在超过它之后直接拒绝发号，把这个窗口关掉。
    见 `SnowflakeID._check_lease_fence`。标识相同时的接手不等租约过期，例外见上面"复用"。

    Redis-native worker id lease. Renew / take over / release are compare-and-swap against
    the full lease value (owner id plus a per-process random part), because lease takeover
    after expiry is a normal event and an unchecked operation silently yields duplicate
    snowflake ids. A lease left with the same owner id (a restart) is taken over only after
    nobody has renewed it for REUSE_STALE_SEC.
    """

    def __init__(
        self,
        pid: int,
        aio: redis.asyncio.Redis | redis.asyncio.RedisCluster,
        *,
        tool: bool = False,
    ):
        """
        初始化 RedisWorkerKeeper。

        tool: 工具进程模式（hetu call / shell）。node_id 带 `cli:` 前缀；分配时**不做**复用、
        从 MAX_WORKER_ID 往下 SET NX，见 `get_worker_id`。续约、释放、围栏不变。
        """
        super().__init__()
        self.aio = aio
        self.worker_id_key = WORKER_ID_KEY
        self.worker_id = -1
        self.tool = tool
        # 持有者标识：同一个位置重启前后不变，认"自己重启前留下的"租约靠它，格式见 lease_owner_id。
        # 租约里存的是它加上本进程随机生成的部分，续约、释放按整个值比对
        self.identity = lease_owner_id(pid, tool=tool)
        self.node_id = lease_value(self.identity)

    def _key(self, worker_id: int) -> str:
        return f"{self.worker_id_key}:{worker_id}"

    async def _getex_if_mine(self, worker_id: int) -> bool:
        """原子地"取回旧值+续期"，并判断这把锁是不是自己的。见类文档。

        确认是自己的就顺手推进发号围栏的安全期。安全期用**发起请求前**的 monotonic 时刻
        算——Redis 那边的 TTL 在我们发出命令的那一刻就开始走了，用返回后的时刻算会高估。
        """
        started_at = monotonic()
        value = await self.aio.getex(self._key(worker_id), ex=WORKER_ID_EXPIRE_SEC)
        if _decode(value) != self.node_id:
            return False
        self.lease_deadline = started_at + WORKER_ID_EXPIRE_SEC - FENCE_MARGIN_SEC
        return True

    async def _get_tool_worker_id(self) -> int:
        """
        工具进程分配：从 MAX_WORKER_ID 往下 SET NX，常见情况下一次就拿到。

        不做服务器那样的复用：工具进程每次都是新 pid，不会有自己重启前留下的租约。从上往下分配
        让工具进程远离服务器从 0 往上占用的 id。
        """
        if self.worker_id >= 0 and await self._getex_if_mine(self.worker_id):
            return self.worker_id
        for worker_id in range(MAX_WORKER_ID, -1, -1):
            started_at = monotonic()
            result = await self.aio.set(
                self._key(worker_id), self.node_id, nx=True, ex=WORKER_ID_EXPIRE_SEC
            )
            if result:
                self.lease_deadline = (
                    started_at + WORKER_ID_EXPIRE_SEC - FENCE_MARGIN_SEC
                )
                logger.info(
                    _(
                        "[❄️ID] 成功获取 Worker ID: {worker_id}, 进程码: {node_id}"
                    ).format(worker_id=worker_id, node_id=self.node_id)
                )
                self.worker_id = worker_id
                return worker_id
        raise KeyError(
            _(
                "无法获取可用的 Worker ID，所有 ID 均被占用。如果有宕机，请等待ID过期重试"
            )
        )

    async def _read_leases(self) -> list[str | None]:
        """一次 pipeline 读完所有 worker id 的租约值，不续期。cluster 下按 slot 分发"""
        pipe = self.aio.pipeline(transaction=False)
        for worker_id in range(MAX_WORKER_ID + 1):
            pipe.get(self._key(worker_id))
        return [_decode(value) for value in await pipe.execute()]

    async def _take_over_stale(self, candidates: dict[int, str]) -> int | None:
        """
        接手一把持有者标识和自己相同的租约（worker id → 读到的值），确认没人在续约了才换成自己的值
        （LUA_TAKE_OVER_IF_STALE）。最近续过的先等到它"该续却没续"再看，几把一起等，最多等
        REUSE_STALE_SEC 秒。接手了就武装围栏、返回 worker id；都还有人在续、或者已经不在了，返回 None
        """
        stale_pttl = (WORKER_ID_EXPIRE_SEC - REUSE_STALE_SEC) * 1000
        pipe = self.aio.pipeline(transaction=False)
        for worker_id in candidates:
            pipe.pttl(self._key(worker_id))
        wait = max((pttl - stale_pttl for pttl in await pipe.execute()), default=0)
        if wait > 0:
            logger.info(
                _(
                    "[❄️ID] 有持有者标识和本进程相同的 Worker ID 租约 {worker_ids}（多半是重启前"
                    "留下的），等 {wait} 秒确认没有进程在续约后接手"
                ).format(worker_ids=list(candidates), wait=f"{wait / 1000:.1f}")
            )
            await asyncio.sleep(wait / 1000 + 0.1)
        for worker_id, old_value in candidates.items():
            started_at = monotonic()
            result = await self.aio.eval(
                LUA_TAKE_OVER_IF_STALE,
                1,
                self._key(worker_id),
                old_value,
                self.node_id,
                stale_pttl,
                WORKER_ID_EXPIRE_SEC,
            )
            if result == 1:
                self.lease_deadline = (
                    started_at + WORKER_ID_EXPIRE_SEC - FENCE_MARGIN_SEC
                )
                logger.info(
                    _(
                        "[❄️ID] 接手持有者标识和本进程相同的 Worker ID: {worker_id}"
                        "（原来的值 {old}，现在 {node_id}）"
                    ).format(worker_id=worker_id, old=old_value, node_id=self.node_id)
                )
                return worker_id
            if result == -1:
                logger.warning(
                    _(
                        "[❄️ID] Worker ID {worker_id} 的持有者标识和本进程相同，但还有进程在续约，"
                        "不接手。可能有两个进程的持有者标识撞了（{owner}），比如节点 token 文件被"
                        "打进了镜像，或者几个容器共用了 /var/tmp"
                    ).format(worker_id=worker_id, owner=old_value)
                )
        return None

    async def _claim_free(self, worker_id: int) -> bool:
        """SET NX 抢一个空着的 worker id（本身就是原子的抢占），抢到就武装围栏"""
        started_at = monotonic()
        result = await self.aio.set(
            self._key(worker_id), self.node_id, nx=True, ex=WORKER_ID_EXPIRE_SEC
        )
        if not result:
            return False
        # 抢到的这一刻就武装围栏，别等到第一次续约（那要5秒后）
        self.lease_deadline = started_at + WORKER_ID_EXPIRE_SEC - FENCE_MARGIN_SEC
        logger.info(
            _("[❄️ID] 成功获取 Worker ID: {worker_id}, 进程码: {node_id}").format(
                worker_id=worker_id, node_id=self.node_id
            )
        )
        return True

    @override
    async def get_worker_id(self) -> int:
        """
        从 Redis 中获取一个可用的 Worker ID：本进程已经拿着的直接用；有持有者标识和自己相同的（多半是
        自己重启前留下的），确认没人在续约了就接手；否则在读到时空着的里面 SET NX 抢一个。都没有就抛
        KeyError（服务器由上层每秒重试）。复用的设计见类文档
        """
        if self.tool:
            return await self._get_tool_worker_id()
        leases = await self._read_leases()
        # 本进程已经拿着的（同一个进程又调了一次）
        if self.node_id in leases:
            worker_id = leases.index(self.node_id)
            if await self._getex_if_mine(worker_id):
                self.worker_id = worker_id
                return worker_id
        # 标识相同、随机部分不同：自己重启前留下的，或者一个标识撞了的进程的
        candidates = {
            worker_id: value
            for worker_id, value in enumerate(leases)
            if value is not None
            and value != self.node_id
            and lease_identity(value) == self.identity
        }
        if candidates:
            worker_id = await self._take_over_stale(candidates)
            if worker_id is not None:
                self.worker_id = worker_id
                return worker_id
        # 分配新的：只试读到时空着的（等的那几秒里刚过期的，抛 KeyError 后重试时会读到）
        for worker_id, value in enumerate(leases):
            if value is None and await self._claim_free(worker_id):
                self.worker_id = worker_id
                return worker_id

        raise KeyError(
            _(
                "无法获取可用的 Worker ID，所有 ID 均被占用。如果有宕机，请等待ID过期重试"
            )
        )

    @override
    async def release_worker_id(self):
        """
        释放当前占用的 Worker ID。只删自己那把（compare-and-delete，见类文档）。
        """
        if self.worker_id == -1:
            return
        # 决定释放就不再发号：释放之后别的进程马上就能拿走这个 id
        self.close_fence()
        deleted = await self.aio.eval(
            LUA_RELEASE_IF_MINE, 1, self._key(self.worker_id), self.node_id
        )
        if deleted:
            logger.info(
                _("[❄️ID] 释放 Worker ID: {worker_id}").format(worker_id=self.worker_id)
            )
        else:
            # 说明关服前租约就已经过期、并且被别的worker接手了。不能删，否则会把对方的
            # 租约删掉，让第三个worker抢到同一个id
            logger.warning(
                _("[❄️ID] Worker ID {worker_id} 的租约已不属于本进程，跳过释放").format(
                    worker_id=self.worker_id
                )
            )

    @override
    async def keep_alive(self):
        """
        续租 Worker ID 的有效期。用 `GETEX` 做 compare-and-expire，只有确认这把锁还是
        自己的才算续约成功；被抢走或已消失则马上关掉发号围栏、抛 SystemExit，由上层重启 worker。
        此方法需要每5秒调用1次。

        时间戳高水位不在这里写，已拆给 `SnowflakeTimestampKeeper`：租约要互斥、水位
        只要单调max，两者并发语义相反，捆一起没必要。
        """
        worker_id = self.worker_id
        if not await self._getex_if_mine(worker_id):
            # 上层收到 SystemExit 后是优雅关服，期间还会发号，围栏不关就会和接手的进程撞号
            self.close_fence()
            logger.error(
                _(
                    "[❄️ID] 续约 Worker ID {worker_id} 失败: "
                    "租约已不属于本进程（可能因本进程长时间卡住导致租约过期后被其他实例"
                    "接手），继续发号会产生重复雪花ID，将重启Worker..."
                ).format(worker_id=worker_id)
            )
            # 关闭Worker
            raise SystemExit(_("Worker ID 续约失败，重启Worker..."))
