"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import contextlib
import logging
from asyncio.queues import Queue
from collections.abc import Callable, Iterable
from functools import partial

from redis.asyncio.client import PubSub, Redis
from redis.asyncio.cluster import ClusterNode, RedisCluster
from redis.asyncio.connection import ConnectionPool
from redis.cluster import LoadBalancingStrategy
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import SlotNotCoveredError

logger = logging.getLogger(__name__)

# 取消订阅等 ack 的上限；超时只记日志不报错（本地状态已经改完，多收几条消息会被上层忽略）
UNSUBSCRIBE_ACK_TIMEOUT = 5.0
# 节点失效后重新订阅的退避区间
RESUBSCRIBE_BACKOFF_MIN = 0.5
RESUBSCRIBE_BACKOFF_MAX = 5.0


class AsyncKeyspacePubSub:
    """
    由于redis-py对cluster的pubsub支持很差，自行实现一个async的集群pubsub。
    通过channel的slot，去对应的Node订阅频道。
    * 自动总集所有Node的消息
    * 支持拓扑更新，自动跟随cluster的更改，不过有几秒延迟。
    * 支持任何精确频道，但不支持pattern订阅
    * pubsub 走自建的独立连接（每节点一条），不占 client 的读写连接池
    """

    def __init__(
        self,
        client: Redis | RedisCluster,
        on_message: Callable[[dict], None] | None = None,
        on_reset: Callable[[], None] | None = None,
        on_restored: Callable[[], None] | None = None,
    ):
        """
        Parameters
        ----------
        client: redis.asyncio.Redis or redis.asyncio.cluster.RedisCluster
            redis.asyncio.Redis 或 redis.asyncio.cluster.RedisCluster 实例。
            只用来解析拓扑/取连接参数，pubsub 本身走本类自建的独立连接。
        on_message
            收到频道消息时在监听协程里直接同步调用的回调（每条消息一次，不能 await）。
            不传则消息进 `message_queue`，由 `get_message()` 取。
        on_reset
            某个节点的连接断了、即将重新订阅全部频道时调用：断到恢复之间的消息已经丢了，
            依赖通知的状态（如行缓存）要在这里作废。
        on_restored
            `resubscribe_all` 成功、所有频道重新订阅生效后调用。
        """
        self.main_client = client
        self.is_cluster = isinstance(client, RedisCluster)
        self.on_message = on_message
        self.on_reset = on_reset
        self.on_restored = on_restored

        # 存储每个节点的独立 Client 和 PubSub
        # Key: 节点标识 (f"host:port" 或 "standalone"), Value: {'client': Redis, 'pubsub': PubSub}
        self.node_resources: dict[str, dict] = {}
        # 已成功订阅（收到 ack 且之后没发过 UNSUBSCRIBE）的频道
        self._subscribed: set[str] = set()
        # 已发出 SUBSCRIBE / UNSUBSCRIBE、尚未收到 ack 的频道，每个频道一个 future，
        # ack 到了只唤醒等它的那几个调用方，不用广播
        self._pending_subscribe: dict[str, asyncio.Future[None]] = {}
        self._pending_unsubscribe: dict[str, asyncio.Future[None]] = {}
        # 每个频道当前订阅在哪个节点上：取消订阅时必须发回同一节点
        # （cluster模式下按ROUND_ROBIN选replica，两次解析可能得到不同节点）；
        # 发 UNSUBSCRIBE 时摘掉，监听协程据此判断收到的 subscribe ack 是不是已经过时
        self._channel_node: dict[str, str] = {}

        # 统一的消息队列（没有 on_message 回调时使用）
        self.message_queue: Queue[dict] = asyncio.Queue()

        # 每个节点一把锁：redis-py 的 PubSub 首次 connect 不是并发安全的（同时几个 subscribe
        # 会各自去池里拿连接），同一节点的 SUBSCRIBE/UNSUBSCRIBE 串行发
        self._node_locks: dict[str, asyncio.Lock] = {}

        # 节点失效后要重新订阅的频道：失效时把已订阅集合整个并进来（拓扑可能变了，全部重订），
        # 恢复流程跑着的时候又有节点失效会继续往里并；全部重订生效后清空
        self._resubscribe_targets: set[str] = set()

        # 运行状态
        self._tasks: set[asyncio.Task] = set()
        self._resubscribe_task: asyncio.Task | None = None
        self._closed = False

    def _node_lock(self, node_key: str) -> asyncio.Lock:
        lock = self._node_locks.get(node_key)
        if lock is None:
            lock = self._node_locks[node_key] = asyncio.Lock()
        return lock

    def _spawn_listener(self, node_key: str, pubsub: PubSub):
        """建立一个射后不管的task监听pubsub消息"""
        task = asyncio.create_task(self._node_listener(node_key, pubsub))
        task.add_done_callback(partial(self._on_node_listener_done, node_key))
        # 如果不保存task，task不会执行会被gc
        self._tasks.add(task)

    def _spawn(self, coro) -> asyncio.Task:
        """射后不管的 task：保存引用免得被 gc，close 时统一取消"""
        task = asyncio.create_task(coro)
        task.add_done_callback(self._tasks.discard)
        self._tasks.add(task)
        return task

    def standalone_connect(self):
        """
        获取standalone的独立连接和pubsub
        """
        assert isinstance(self.main_client, Redis)
        assert "standalone" not in self.node_resources

        logger.info("Setup standalone PubSub")

        # 与 cluster_connect 一样自建一条连接做 pubsub：pubsub 连接是常驻的，不能占用
        # main_client 那个有上限的读写连接池。照抄它的连接参数（含 ssl/unix socket 的
        # connection_class）另开一个只给 pubsub 用的小池
        main_pool = self.main_client.connection_pool
        pool = ConnectionPool(
            connection_class=main_pool.connection_class,
            max_connections=2,
            **main_pool.connection_kwargs,
        )
        r_client = Redis.from_pool(pool)
        pubsub = r_client.pubsub()
        self.node_resources["standalone"] = {
            "client": r_client,
            "pubsub": pubsub,
        }
        self._spawn_listener("standalone", pubsub)

    def cluster_connect(self, node: ClusterNode):
        """
        获取cluster node的独立连接和pubsub
        """
        node_key = node.name
        assert node_key not in self.node_resources
        logger.info(f"Creating standalone connection for node: {node_key}")

        # 为每个节点创建一个 Standalone 的 Redis Client。
        # ClusterNode.connection_kwargs 是给 Connection 的参数（host/port/ssl/auth 之外，
        # redis-py 8.1 起还塞了 himport_registry 这类内部对象），不能直接喂给 Redis(...)，
        # 会 TypeError；照 ClusterNode 自己建连接的方式，用它的 connection_class +
        # connection_kwargs 另开一个只给 pubsub 用的小池（与 standalone_connect 同款）
        pool = ConnectionPool(
            connection_class=node.connection_class,
            max_connections=2,
            **node.connection_kwargs,
        )
        r_client = Redis.from_pool(pool)
        pubsub = r_client.pubsub()

        self.node_resources[node_key] = {
            "client": r_client,
            "pubsub": pubsub,
        }
        self._spawn_listener(node_key, pubsub)

    async def _resolve_node(self, channel: str) -> str:
        """
        根据 Channel 中的 Key 计算 Slot，找到目标 Node，并确保已建立到该节点的连接。
        返回 node key。
        """
        target_node_key = "standalone"

        if self.is_cluster:
            assert isinstance(self.main_client, RedisCluster)
            # 客户端可能还没被任何命令初始化过（幂等，已初始化即返回）。不能跳过它直接
            # 初始化 nodes_manager：那样 default_node 有了而命令解析器还是空的，之后该客户端
            # 的第一条普通命令会在 _determine_slot 里报
            # "'AsyncCommandsParser' object has no attribute 'node'"
            await self.main_client.initialize()
            # 计算 Slot 和目标节点，如果找不到，说明node变更了，需要刷新拓扑
            slot = self.main_client.keyslot(channel)
            try:
                node = self.main_client.nodes_manager.get_node_from_slot(
                    slot,
                    load_balancing_strategy=LoadBalancingStrategy.ROUND_ROBIN_REPLICAS,
                )
            # 加KeyError是因为redis-py库的bug，没捕捉这个
            except KeyError, SlotNotCoveredError:
                # 如果slot不在覆盖范围内，强制刷新一次拓扑
                await asyncio.sleep(0.25)
                await self.main_client.nodes_manager.initialize()
                node = self.main_client.nodes_manager.get_node_from_slot(
                    slot,
                    load_balancing_strategy=LoadBalancingStrategy.ROUND_ROBIN_REPLICAS,
                )

            if not node:
                raise RuntimeError(f"Could not find node for channel: {channel}")

            target_node_key = node.name

            # 检查我们是否已经建立了到该节点的连接
            if target_node_key not in self.node_resources:
                self.cluster_connect(node)
        else:
            if target_node_key not in self.node_resources:
                self.standalone_connect()

        return target_node_key

    @staticmethod
    def _fail_pending(pending: dict[str, asyncio.Future[None]], exc: BaseException):
        for fut in pending.values():
            if not fut.done():
                fut.set_exception(exc)
                fut.exception()  # 没人等的话别在 gc 时报 "never retrieved"
        pending.clear()

    @staticmethod
    async def wait_acks(futures: list[asyncio.Future[None]]):
        """
        等一组 ack future 都完成，有失败的抛出其异常。
        future 是共享的（同一频道的所有等待方等同一个），所以不能用 gather：gather 所在
        的 task 被取消时会连带取消它等的 future，让别的等待方莫名其妙收到 CancelledError。
        asyncio.wait 只旁观不插手，本调用方被取消时 future 原样留给其他人。
        """
        if not futures:
            return
        done, _ = await asyncio.wait(futures)
        for fut in done:
            fut.result()

    async def subscribe(self, *channels: str):
        """
        精确订阅，可一次订阅多个频道，全部订阅成功（收到 ack）后返回。
        根据 Channel 中的 Key 计算 Slot，路由到指定 Node 的 PubSub；
        同一 Node 的频道合并成一条 SUBSCRIBE 命令，N 个频道只需 O(节点数) 次往返。
        Channel 格式预期: __keyspace@<db>__:<keyname> 或任何带 {hash tag} 的频道名
        """
        if not channels:
            return
        if self._closed:
            raise RedisConnectionError("pubsub closed")
        loop = asyncio.get_running_loop()

        # 已经在等 ack 的频道（别的调用方发的）不再重复发，等它的 future 即可；
        # 本次要发的频道在任何 await 之前就挂上 future，之后来搭车的人才能马上拿到
        futures: list[asyncio.Future[None]] = []
        own: list[str] = []
        for channel in channels:
            fut = self._pending_subscribe.get(channel)
            if fut is None:
                if channel in self._subscribed:
                    continue
                fut = self._pending_subscribe[channel] = loop.create_future()
                own.append(channel)
            futures.append(fut)

        # 发送放进独立 task 并 shield：future 是共享的，登记了就必须把 SUBSCRIBE 发出去
        # （或明确失败），不能因为本调用方中途被取消（连接断了）就半途而废，
        # 让搭车等 ack 的其他连接永远等不到
        if own:
            await asyncio.shield(self._spawn(self._send_subscribe(own)))

        # 等message返回了才能算订阅成功
        await self.wait_acks(futures)

    async def _send_subscribe(self, channels: list[str]):
        """按节点分组发 SUBSCRIBE；没发出去的频道作废，让等它们的人一起失败"""
        sent: set[str] = set()
        try:
            groups: dict[str, list[str]] = {}
            for channel in channels:
                node_key = await self._resolve_node(channel)
                groups.setdefault(node_key, []).append(channel)
                self._channel_node[channel] = node_key

            # 每个节点一条SUBSCRIBE命令
            for node_key, group in groups.items():
                ps = self.node_resources[node_key]["pubsub"]
                async with self._node_lock(node_key):
                    await ps.subscribe(*group)
                sent.update(group)
        except BaseException as e:
            # 共享的 future 里不能放 CancelledError（这里只会来自 close()），
            # 否则别的等待方会误以为是自己被取消了
            if isinstance(e, asyncio.CancelledError):
                e = RedisConnectionError("pubsub closed")
            for channel in channels:
                if channel in sent:
                    continue  # 已发出的照常等 ack
                fut = self._pending_subscribe.pop(channel, None)
                self._channel_node.pop(channel, None)
                if fut is not None and not fut.done():
                    fut.set_exception(e)
                    fut.exception()  # 没人等的话别在 gc 时报 "never retrieved"
            raise

    def is_subscribing(self, channel: str) -> bool:
        """频道已订阅成功，或 SUBSCRIBE 已发出正在等 ack"""
        return channel in self._subscribed or channel in self._pending_subscribe

    def is_subscribed(self, channel: str) -> bool:
        """频道已订阅成功（收到 ack、之后没退订、所在节点没失效）：此后它的消息不会漏"""
        return channel in self._subscribed

    def pending_acks(self, channels: Iterable[str]) -> list[asyncio.Future[None]]:
        """
        这些频道里已发出 SUBSCRIBE、尚未 ack 的 future（别的调用方发的），
        之后用 wait_acks 等它们。这是同步方法：调用方登记完自己后马上取，
        才不会漏掉中途发送失败的（失败的 future 会从待确认表里摘掉，之后就看不到了）。
        """
        return [
            fut
            for channel in channels
            if (fut := self._pending_subscribe.get(channel)) is not None
        ]

    async def unsubscribe(self, *channels: str):
        """
        取消订阅，可一次取消多个。发回各频道当初订阅的那个节点。
        等 Redis 回 ack 后才返回，保证返回后不会再收到这些频道的消息；
        ack 超过 UNSUBSCRIBE_ACK_TIMEOUT 没来只记日志，本地状态已经改完。
        """
        if not channels:
            return
        if self._closed:
            for channel in channels:
                self._subscribed.discard(channel)
                self._channel_node.pop(channel, None)
            return
        loop = asyncio.get_running_loop()

        groups: dict[str, list[str]] = {}
        futures: list[asyncio.Future[None]] = []
        for channel in channels:
            node_key = self._channel_node.pop(channel, "standalone")
            self._subscribed.discard(channel)
            self._resubscribe_targets.discard(channel)  # 恢复流程跑着也别把它订回来
            # SUBSCRIBE 还没 ack 就退订：让等它的人正常返回而不是报错。等的人就是刚撤了
            # 自己登记的那个连接（hub 只在没人订时才退订），它的 subscribe 没有失败，
            # 只是随后被自己的 unsub 覆盖了；报错会让 get_updates 把整个连接断掉
            pending = self._pending_subscribe.pop(channel, None)
            if pending is not None and not pending.done():
                pending.set_result(None)
            if node_key not in self.node_resources:
                continue
            groups.setdefault(node_key, []).append(channel)
            fut = self._pending_unsubscribe.get(channel)
            if fut is None or fut.done():
                fut = loop.create_future()
                self._pending_unsubscribe[channel] = fut
            futures.append(fut)

        for node_key, group in groups.items():
            async with self._node_lock(node_key):
                await self.node_resources[node_key]["pubsub"].unsubscribe(*group)

        if futures:
            try:
                # 超时只是本调用方不等了：wait_acks 不会取消 future，它们留在待确认表里
                # 等 ack 真的来（或节点失效时被 _fail_pending 统一失败），之后再退订
                # 同一频道的人复用它们也不会莫名收到 CancelledError
                async with asyncio.timeout(UNSUBSCRIBE_ACK_TIMEOUT):
                    await self.wait_acks(futures)
            except TimeoutError:
                logger.warning(
                    f"UNSUBSCRIBE ack timeout after {UNSUBSCRIBE_ACK_TIMEOUT}s, "
                    f"{len(futures)} channels"
                )

    async def resubscribe_all(self):
        """
        节点失效后重新订阅 `_resubscribe_targets` 里的频道（失效时的全部已订阅频道），
        失败就退避重试直到全部生效或 close。
        """
        targets = self._resubscribe_targets
        backoff = RESUBSCRIBE_BACKOFF_MIN
        while not self._closed:
            try:
                await self.subscribe(*targets)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 网络/拓扑错误都要重试，不能让恢复流程死掉
                logger.error(f"Resubscribe failed, retry in {backoff}s: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, RESUBSCRIBE_BACKOFF_MAX)
                continue
            # 等 ack 期间又有节点失效的话，它已把已订阅集合清掉、并回 targets：再来一轮，
            # 直到 targets 全部订阅生效才算恢复
            if targets <= self._subscribed:
                logger.info(f"Resubscribed {len(targets)} channels")
                targets.clear()
                self._callback(self.on_restored)
                return

    async def _node_listener(self, node_key: str, pubsub: PubSub):
        """
        单个节点的监听循环
        """
        while True:
            async for message in pubsub.listen():
                if message:
                    # 可以在这里注入任意信息到message
                    mtype = message["type"]
                    if mtype != "message":  # ignore_subscribe_messages
                        if mtype == "subscribe":
                            channel = message["channel"].decode()
                            # 只认"该频道当前就订在本节点上"的 ack：SUBSCRIBE 还没 ack
                            # 就被 unsubscribe() 的话，_channel_node 已经摘掉了它，
                            # 这条迟到的 ack 不能把它重新算作已订阅（UNSUBSCRIBE 紧随
                            # 其后，Redis 不会再推它的消息，之后的订阅者却会被"已订阅"
                            # 短路而永远收不到通知）
                            if self._channel_node.get(channel) != node_key:
                                continue
                            self._subscribed.add(channel)
                            fut = self._pending_subscribe.pop(channel, None)
                            if fut is not None and not fut.done():
                                fut.set_result(None)
                        elif mtype == "unsubscribe":
                            channel = message["channel"].decode()
                            fut = self._pending_unsubscribe.pop(channel, None)
                            if fut is not None and not fut.done():
                                fut.set_result(None)
                            # 期间没有重新订阅的话，兜底保证它不在已订阅集合里
                            if channel not in self._channel_node:
                                self._subscribed.discard(channel)
                        continue
                    if self.on_message is None:
                        await self.message_queue.put(message)
                        continue
                    try:
                        self.on_message(message)
                    except Exception:  # 一条消息处理失败不能拖死监听
                        logger.exception("on_message callback failed")

            # 走到这里只可能是node没订阅任何频道
            await asyncio.sleep(0.25)  # 等待订阅建立

    def _on_node_listener_done(self, node_key, task):
        # task关闭说明链接断开了，node可能失效，移除资源。一般发生在数据库扩容/容灾。
        self._tasks.discard(task)
        if self._closed:
            return
        try:
            # 获取结果，如果有异常会在这里重新抛出
            task.result()
            return  # 不应该走到这里
        except asyncio.CancelledError:
            # 正常取消
            return
        except Exception as e:
            logger.error(f"Listener error on node {node_key}: {e}")
            # 断线处理：丢弃并尽力关掉失效节点的自建连接，等 ack 的调用方全部失败
            res = self.node_resources.pop(node_key, None)
            if res is not None:
                dispose = asyncio.create_task(self._dispose_node(res))
                dispose.add_done_callback(self._tasks.discard)
                self._tasks.add(dispose)
            exc = RedisConnectionError(f"pubsub node {node_key} lost")
            self._fail_pending(self._pending_subscribe, exc)
            self._fail_pending(self._pending_unsubscribe, exc)
            # 该节点上已订阅生效的频道随连接一起没了。恢复流程重订全部频道（拓扑可能变了），
            # 所以把已订阅集合整个并进重订名单并清空：清空后 is_subscribed 对它们为假，行缓存
            # 不会把它们当有效订阅激活，subscribe() 也不会把它们当已订阅短路——恢复流程已在
            # 跑时（多个节点接连失效）第二个节点上刚订好的频道以前正是这样被永远漏掉的
            self._resubscribe_targets.update(self._subscribed)
            self._subscribed.clear()
            self._channel_node.clear()
            # 每次节点失效都通知一次：恢复期间新激活的频道也可能正在这个节点上
            self._callback(self.on_reset)
            # 灾难恢复逻辑（已有一个在退避重试中就不再起，它会把新并进来的频道一起订上）。
            # 如果不保存task，task不会执行会被gc
            if self._resubscribe_task is None or self._resubscribe_task.done():
                self._resubscribe_task = asyncio.create_task(self.resubscribe_all())

    @staticmethod
    def _callback(cb: Callable[[], None] | None) -> None:
        """调用 on_reset / on_restored；回调抛异常只记日志，不能拖死恢复流程"""
        if cb is None:
            return
        try:
            cb()
        except Exception:
            logger.exception("pubsub reset/restored callback failed")

    @staticmethod
    async def _dispose_node(res: dict):
        # 连接已坏，关不上也无所谓
        for key in ("pubsub", "client"):
            with contextlib.suppress(Exception):
                await res[key].aclose()

    async def get_message(self):
        """
        从内部队列获取消息，如果没有消息则堵塞等待。
        """
        return await self.message_queue.get()

    @property
    def subscribed(self):
        return self._subscribed.union(self._pending_subscribe)

    async def close(self):
        """
        清理资源
        """
        self._closed = True
        # 先停监听任务（否则关连接会触发 redis-py 的自动重连、甚至触发失效重订阅）
        tasks = [t for t in self._tasks if not t.done()]
        if self._resubscribe_task is not None and not self._resubscribe_task.done():
            tasks.append(self._resubscribe_task)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        exc = RedisConnectionError("pubsub closed")
        self._fail_pending(self._pending_subscribe, exc)
        self._fail_pending(self._pending_unsubscribe, exc)

        # 再关闭所有 PubSub 和自建的 Client
        for res in self.node_resources.values():
            await self._dispose_node(res)
        self.node_resources = {}

        logger.info("Resources closed.")
