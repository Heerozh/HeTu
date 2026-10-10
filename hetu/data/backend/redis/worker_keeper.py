import logging
from time import monotonic
from typing import TYPE_CHECKING, final, override

import redis.asyncio

from ....common.helper import get_machine_id
from ....common.helper import lease_owner_exited as _owner_exited
from ....common.snowflake_id import (
    FENCE_MARGIN_SEC,
    MAX_WORKER_ID,
    TOOL_NODE_PREFIX,
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
      而我们这侧立刻退出，不会反复刷。
    * **复用自己的id** 同样用 `GETEX`。原来是 `GET` 判断 node_id 后再 `EXPIRE`，两条命令
      非原子——中间 key 可能过期并被别人抢走，然后我们给对方续了期还以为拿到了自己的 id。
    * **释放** 用 LUA_RELEASE_IF_MINE。原来是无条件 `DEL`：如果自己的租约早已过期并被别人
      接手，关服时会删掉**别人的**租约，对方瞬间变无主，第三个 worker 又能抢走同一个 id。

    `SET key node_id NX EX ttl`（抢新id）本身就是原子的，不需要额外处理。

    ## 配套的本地围栏

    上面的检测都是**事后**的：worker 冻结后醒来，到下一次 keep_alive 跑到之间还有最长一个
    续约周期，这期间它照样拿旧 worker_id 发号。所以每次确认"租约还是我的"时都会推进
    `lease_deadline`（基类属性），`SnowflakeID` 在超过它之后直接拒绝发号，把这个窗口关掉。
    见 `SnowflakeID._check_lease_fence`。

    Redis-native worker id lease. All three operations (renew / reuse / release) are
    compare-and-swap against the owner token, because lease takeover after expiry is a
    normal event and an unchecked operation silently yields duplicate snowflake ids.
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

        tool: 工具进程模式（hetu call / shell）。node_id 带 `cli:` 前缀；分配时**不做**复用
        扫描、从 MAX_WORKER_ID 往下 SET NX，见 `get_worker_id`。续约、释放、围栏不变。
        """
        super().__init__()
        self.aio = aio
        self.worker_id_key = WORKER_ID_KEY
        self.worker_id = -1
        self.tool = tool
        # 机器码+pid组成的node_id。
        # 如果pid为固定值，则可以保证60秒内获取到的worker_id尽可能不变
        # 比如固定每个容器只启动一个worker，则pid是固定的1
        self.node_id = f"{get_machine_id()}:{pid}"
        if tool:
            self.node_id = TOOL_NODE_PREFIX + self.node_id

    def _key(self, worker_id: int) -> str:
        return f"{self.worker_id_key}:{worker_id}"

    async def _getex_if_mine(self, worker_id: int) -> bool:
        """原子地"取回旧值+续期"，并判断这把锁是不是自己的。见类文档。

        确认是自己的就顺手推进发号围栏的安全期。安全期用**发起请求前**的 monotonic 时刻
        算——Redis 那边的 TTL 在我们发出命令的那一刻就开始走了，用返回后的时刻算会高估。
        """
        started_at = monotonic()
        value = await self.aio.getex(self._key(worker_id), ex=WORKER_ID_EXPIRE_SEC)
        if value is None:
            return False
        if isinstance(value, bytes):
            value = value.decode("ascii", errors="replace")
        if value != self.node_id:
            return False
        self.lease_deadline = started_at + WORKER_ID_EXPIRE_SEC - FENCE_MARGIN_SEC
        return True

    async def _get_tool_worker_id(self) -> int:
        """
        工具进程分配：从 MAX_WORKER_ID 往下 SET NX，常见情况下一次就拿到。

        不做服务器那样的复用扫描：工具进程每次都是新 pid，扫描必然 1024 次全落空；而且扫描
        用的 GETEX 会把**所有**现存租约（包括已死 worker 的）TTL 刷回满值，工具进程调用得
        勤，死租约就永不过期，hetu upgrade 会一直以为有服务器在跑。从上往下分配让工具进程
        远离服务器从 0 往上占用的 id。
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

    @override
    async def get_worker_id(self) -> int:
        """
        从Redis中获取一个可用的 Worker ID。
        """
        if self.tool:
            return await self._get_tool_worker_id()
        # 查找之前是否已经分配过自己的worker id
        for worker_id in range(MAX_WORKER_ID + 1):
            # node_id相同说明是容器重启，直接复用。GETEX一条命令原子完成"读值+续期"，
            # 拆成 GET + EXPIRE 会有中间被抢走的窗口，见类文档
            if not await self._getex_if_mine(worker_id):
                continue
            logger.info(
                _(
                    "[❄️ID] 重新使用已分配的 Worker ID: {worker_id} "
                    "(通过相同进程码 {node_id} )"
                ).format(worker_id=worker_id, node_id=self.node_id)
            )
            self.worker_id = worker_id
            return worker_id

        # 尝试分配新的worker id
        for worker_id in range(MAX_WORKER_ID + 1):
            # SET NX 本身就是原子的抢占，不需要额外的CAS
            started_at = monotonic()
            result = await self.aio.set(
                self._key(worker_id), self.node_id, nx=True, ex=WORKER_ID_EXPIRE_SEC
            )
            if result:
                # 抢到的这一刻就武装围栏，别等到第一次续约（那要5秒后）
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

    @override
    async def release_worker_id(self):
        """
        释放当前占用的 Worker ID。只删自己那把（compare-and-delete，见类文档）。
        """
        if self.worker_id == -1:
            return
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
        自己的才算续约成功；被抢走或已消失则抛 SystemExit，由上层重启 worker。
        此方法需要每5秒调用1次。

        时间戳高水位不在这里写，已拆给 `SnowflakeTimestampKeeper`：租约要互斥、水位
        只要单调max，两者并发语义相反，捆一起没必要。
        """
        worker_id = self.worker_id
        if not await self._getex_if_mine(worker_id):
            logger.error(
                _(
                    "[❄️ID] 续约 Worker ID {worker_id} 失败: "
                    "租约已不属于本进程（可能因本进程长时间卡住导致租约过期后被其他实例"
                    "接手），继续发号会产生重复雪花ID，将重启Worker..."
                ).format(worker_id=worker_id)
            )
            # 关闭Worker
            raise SystemExit(_("Worker ID 续约失败，重启Worker..."))
