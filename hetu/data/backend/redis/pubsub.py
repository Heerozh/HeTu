"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import asyncio
import logging
import redis
from redis.asyncio.cluster import RedisCluster
from redis.asyncio import Redis
from redis.exceptions import RedisError, ConnectionError
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import redis.asyncio


logger = logging.getLogger(__name__)


class AsyncKeyspacePubSub:
    """
    由于redis-py对cluster的pubsub支持很差，自行实现一个async的集群pubsub。
    通过Key的slot，去对应的Node订阅频道，因此不支持pattern订阅。
    * 自动总集所有Node的消息
    * 支持拓扑更新，自动跟随cluster的更改，不过有几秒延迟。
    """

    def __init__(
        self, client: Redis | RedisCluster, topology_refresh_interval: int = 30
    ):
        """
        Parameters
        ----------
        client: redis.asyncio.Redis or redis.asyncio.cluster.RedisCluster
            redis.asyncio.Redis 或 redis.asyncio.cluster.RedisCluster 实例
        topology_refresh_interval: int
            检查集群拓扑变化的间隔(秒)
        """
        self.main_client = client
        self.is_cluster = isinstance(client, RedisCluster)

        # 存储每个节点的独立 Client 和 PubSub
        # Key: 节点标识 (host:port 或 node_id), Value: {'client': Redis, 'pubsub': PubSub}
        self.node_resources: dict[str, dict] = {}

        # 统一的消息队列
        self.message_queue = asyncio.Queue()

        # 运行状态
        self._running = False
        self._tasks: list[asyncio.Task] = []

    async def connect(self):
        """
        初始化连接：
        如果是 Cluster，遍历所有 Primary 节点建立独立连接。
        如果是 Standalone，复用当前连接。
        """
        if self.is_cluster:
            # 获取所有 Primary 节点 (通常通知产生于 Master)
            # 注意：get_primaries() 返回的是 ClusterNode 对象
            nodes = self.main_client.get_primaries()

            # 获取原始连接参数 (password, encoding, decode_responses etc.)
            # 以此确保连接子节点时配置一致
            connection_kwargs = (
                self.main_client.connection_pool.connection_kwargs.copy()
            )

            for node in nodes:
                node_key = f"{node.host}:{node.port}"
                logger.info(f"Creating standalone connection for node: {node_key}")

                # 为每个节点创建一个 Standalone 的 Redis Client
                # 覆盖 host 和 port
                kwargs = connection_kwargs.copy()
                kwargs.update({"host": node.host, "port": node.port})

                # 如果是 Cluster 模式下的 redis-py，有些参数可能需要清理，比如 'path' 用于 unix socket
                if "path" in kwargs:
                    del kwargs["path"]

                r_client = Redis(**kwargs)
                pubsub = r_client.pubsub()

                self.node_resources[node_key] = {
                    "client": r_client,
                    "pubsub": pubsub,
                    "node_obj": node,  # 保存 ClusterNode 对象用于后续匹配
                }
        else:
            # Standalone 模式
            logger.info("Setup standalone PubSub")
            pubsub = self.main_client.pubsub()
            self.node_resources["standalone"] = {
                "client": self.main_client,
                "pubsub": pubsub,
            }

    async def subscribe(self, channel: str):
        """
        精确订阅。根据 Channel 中的 Key 计算 Slot，路由到指定 Node 的 PubSub。
        Channel 格式预期: __keyspace@<db>__:<keyname>
        """
        if not self.node_resources:
            raise RuntimeError("Please call connect() before subscribing.")

        target_node_key = "standalone"

        if self.is_cluster:
            # 1. 解析 Key
            # 格式通常是: __keyspace@0__:my_key
            # 我们需要提取 'my_key' 来计算 slot
            key_match = re.search(r"__key.*__:(.*)", channel)
            if not key_match:
                logger.warning(
                    f"Could not extract key from channel: {channel}, broadcasting to all (fallback)."
                )
                # 如果无法解析 key (比如直接订阅了非 keyspace 频道)，由于无法确定节点，
                # 这里的策略由你决定：报错 或者 对所有节点订阅。
                # 针对你的需求，这里假设必须是精确 Keyspace。
                return

            key = key_match.group(1)

            # 2. 计算 Slot 和目标节点
            # RedisCluster 内部有 slot 缓存
            node = self.main_client.get_node_from_key(key)
            if not node:
                logger.error(f"Could not find node for key: {key}")
                return

            target_node_key = f"{node.host}:{node.port}"

            # 检查我们是否已经建立了到该节点的连接 (处理扩容或重新分片可能需要刷新，这里简化处理)
            if target_node_key not in self.node_resources:
                # 尝试通过 cluster_nodes 重新刷新或容错，这里简单抛出
                logger.error(f"Node {target_node_key} not initialized in our pool.")
                return

        # 3. 执行订阅
        ps = self.node_resources[target_node_key]["pubsub"]
        await ps.subscribe(channel)
        logger.debug(f"Subscribed to {channel} on node {target_node_key}")

    async def unsubscribe(self, channel: str):
        """
        取消订阅，逻辑同 subscribe
        """
        target_node_key = "standalone"
        if self.is_cluster:
            key_match = re.search(r"__key.*__:(.*)", channel)
            if key_match:
                key = key_match.group(1)
                node = self.main_client.get_node_from_key(key)
                if node:
                    target_node_key = f"{node.host}:{node.port}"

        if target_node_key in self.node_resources:
            await self.node_resources[target_node_key]["pubsub"].unsubscribe(channel)

    async def internal_task(self):
        """
        外部调用的主任务。
        启动针对所有节点的监听循环，并堵塞在这里维持运行。
        """
        if not self.node_resources:
            await self.connect()

        self._running = True
        self._tasks = []

        # 为每个 Node 的 PubSub 启动一个后台 Reader
        for node_key, res in self.node_resources.items():
            ps = res["pubsub"]
            task = asyncio.create_task(self._node_listener(node_key, ps))
            self._tasks.append(task)

        logger.info(f"Started internal listener tasks for {len(self._tasks)} nodes.")

        try:
            # 聚合所有任务，只要有一个报错或全部结束才返回
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            logger.info("Internal task cancelled.")
        finally:
            await self.close()

    async def _node_listener(self, node_name: str, pubsub: PubSub):
        """
        单个节点的监听循环
        """
        try:
            # ignore_subscribe_messages=True 可以过滤掉订阅成功的 ack 消息，
            # 如果你需要确认订阅成功，可以设为 False，然后在 get_message 里处理
            async for message in pubsub.listen():
                if message:
                    # 可以在这里注入 node 信息，方便调试
                    # message['node'] = node_name
                    await self.message_queue.put(message)
        except Exception as e:
            logger.error(f"Listener error on node {node_name}: {e}")
            # 这里可以添加重连逻辑，或者直接让 task 结束触发整体异常
            raise e

    async def get_message(self):
        """
        从内部队列获取消息，如果没有消息则堵塞等待。
        """
        return await self.message_queue.get()

    async def close(self):
        """
        清理资源
        """
        self._running = False
        # 取消所有监听任务
        for t in self._tasks:
            if not t.done():
                t.cancel()

        # 关闭所有 PubSub 和 Client
        for res in self.node_resources.values():
            await res["pubsub"].close()
            # 只有 Cluster 模式下创建了额外的 Client，需要关闭
            if self.is_cluster:
                await res["client"].aclose()

        logger.info("Resources closed.")
