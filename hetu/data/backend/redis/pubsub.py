"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from asyncio.queues import Queue


import asyncio
import logging
from functools import partial
from typing import TYPE_CHECKING

from redis.asyncio.client import PubSub, Redis
from redis.asyncio.cluster import ClusterNode, RedisCluster
from redis.cluster import LoadBalancingStrategy
from redis.exceptions import SlotNotCoveredError

if TYPE_CHECKING:
    import redis.asyncio


logger = logging.getLogger(__name__)


class AsyncKeyspacePubSub:
    """
    由于redis-py对cluster的pubsub支持很差，自行实现一个async的集群pubsub。
    通过channel的slot，去对应的Node订阅频道。
    * 自动总集所有Node的消息
    * 支持拓扑更新，自动跟随cluster的更改，不过有几秒延迟。
    * 支持任何精确频道，但不支持pattern订阅
    """

    def __init__(self, client: Redis | RedisCluster):
        """
        Parameters
        ----------
        client: redis.asyncio.Redis or redis.asyncio.cluster.RedisCluster
            redis.asyncio.Redis 或 redis.asyncio.cluster.RedisCluster 实例
        """
        self.main_client = client
        self.is_cluster = isinstance(client, RedisCluster)

        # 存储每个节点的独立 Client 和 PubSub
        # Key: 节点标识 (f"host:port" 或 "standalone"), Value: {'client': Redis, 'pubsub': PubSub}
        self.node_resources: dict[str, dict] = {}
        # 已成功订阅的频道
        self._subscribed: set[str] = set()
        self._pending_subscribe: set[str] = set()

        # 统一的消息队列
        self.message_queue: Queue[dict] = asyncio.Queue()

        # 运行状态
        self._tasks: set[asyncio.Task] = set()
        self._resubscribe_task = None

        # 订阅通知
        self._subscribe_notify = asyncio.Condition()

    def standalone_connect(self):
        """
        获取standalone的独立连接和pubsub
        """
        assert isinstance(self.main_client, Redis)
        assert "standalone" not in self.node_resources

        logger.info("Setup standalone PubSub")

        pubsub = self.main_client.pubsub()
        self.node_resources["standalone"] = {
            "client": self.main_client,
            "pubsub": pubsub,
        }

        # 建立一个射后不管的task监听pubsub消息
        task = asyncio.create_task(self._node_listener(pubsub))
        task.add_done_callback(partial(self._on_node_listener_done, "standalone"))
        # 如果不保存task，task不会执行会被gc
        self._tasks.add(task)

    def cluster_connect(self, node: ClusterNode):
        """
        获取cluster node的独立连接和pubsub
        """
        node_key = node.name
        assert node_key not in self.node_resources
        logger.info(f"Creating standalone connection for node: {node_key}")

        # 为每个节点创建一个 Standalone 的 Redis Client
        connection_kwargs = node.connection_kwargs.copy()
        # 如果是 Cluster 模式下的 redis-py，有些参数可能需要清理，比如 'path' 用于 unix socket
        if "path" in connection_kwargs:
            del connection_kwargs["path"]

        # 这会导致每个mq client拥有自己独立的连接pool，问题不大因为订阅就是每个用户一个连接
        r_client = Redis(**connection_kwargs)
        pubsub = r_client.pubsub()

        self.node_resources[node_key] = {
            "client": r_client,
            "pubsub": pubsub,
        }

        # 建立一个射后不管的task监听pubsub消息
        task = asyncio.create_task(self._node_listener(pubsub))
        task.add_done_callback(partial(self._on_node_listener_done, node_key))
        # 如果不保存task，task不会执行会被gc
        self._tasks.add(task)

    async def subscribe(self, channel: str):
        """
        精确订阅。根据 Channel 中的 Key 计算 Slot，路由到指定 Node 的 PubSub。
        Channel 格式预期: __keyspace@<db>__:<keyname>
        """
        target_node_key = "standalone"

        if self.is_cluster:
            assert isinstance(self.main_client, RedisCluster)
            # 计算 Slot 和目标节点，如果找不到，说明node变更了，需要刷新拓扑
            slot = self.main_client.keyslot(channel)
            try:
                node = self.main_client.nodes_manager.get_node_from_slot(
                    slot,
                    load_balancing_strategy=LoadBalancingStrategy.ROUND_ROBIN_REPLICAS,
                )
            # 加KeyError是因为redis-py库的bug，没捕捉这个
            except (KeyError, SlotNotCoveredError):
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

        # 3. 执行订阅
        ps = self.node_resources[target_node_key]["pubsub"]
        await ps.subscribe(channel)
        self._pending_subscribe.add(channel)

        # 等message返回了才能算订阅成功
        async with self._subscribe_notify:
            await self._subscribe_notify.wait_for(
                lambda: channel not in self._pending_subscribe
            )

    async def unsubscribe(self, channel: str):
        """
        取消订阅，逻辑同 subscribe
        """
        target_node_key = "standalone"
        if self.is_cluster:
            assert isinstance(self.main_client, RedisCluster)
            node = self.main_client.get_node_from_key(channel, replica=True)
            if node:
                target_node_key = f"{node.host}:{node.port}"

        if target_node_key in self.node_resources:
            await self.node_resources[target_node_key]["pubsub"].unsubscribe(channel)
        self._subscribed.discard(channel)

    async def resubscribe_all(self):
        """
        重新订阅所有频道，适用于拓扑变化后
        """
        current_subscriptions = list(self._subscribed)
        self._subscribed.clear()
        self._pending_subscribe.clear()
        for channel in current_subscriptions:
            await self.subscribe(channel)

    async def _node_listener(self, pubsub: PubSub):
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
                            self._subscribed.add(channel)
                            self._pending_subscribe.discard(channel)
                            async with self._subscribe_notify:
                                self._subscribe_notify.notify_all()
                        continue
                    await self.message_queue.put(message)

            # 走到这里只可能是node没订阅任何频道
            await asyncio.sleep(0.25)  # 等待订阅建立

    def _on_node_listener_done(self, node_key, task):
        # task关闭说明链接断开了，node可能失效，移除资源。一般发生在数据库扩容/容灾。
        self._tasks.discard(task)
        try:
            # 获取结果，如果有异常会在这里重新抛出
            task.result()
            return  # 不应该走到这里
        except asyncio.CancelledError:
            # 正常取消
            return
        except Exception as e:
            logger.error(f"Listener error on node {node_key}: {e}")
            # 断线处理
            if node_key in self.node_resources:
                del self.node_resources[node_key]
            # 灾难恢复逻辑。如果不保存task，task不会执行会被gc
            self._resubscribe_task = asyncio.create_task(self.resubscribe_all())

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
        # 先关闭所有 PubSub 和 Client
        for res in self.node_resources.values():
            await res["pubsub"].close()
            # 只有 Cluster 模式下创建了额外的 Client，需要关闭
            if self.is_cluster:
                await res["client"].aclose()

        # 取消所有监听任务
        for t in self._tasks:
            if not t.done():
                t.cancel()

        self.node_resources = {}

        logger.info("Resources closed.")
