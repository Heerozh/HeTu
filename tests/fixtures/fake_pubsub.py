"""
不连 Redis 的 pubsub 测试替身：假的节点 pubsub 记录发出的 SUBSCRIBE/UNSUBSCRIBE，
ack 由测试手动投递，用来验证 AsyncKeyspacePubSub / PubSubHub / SubscriptionBroker
在各种协程交错下的行为。
"""

import asyncio
from collections.abc import AsyncIterator

from redis.asyncio import Redis

from hetu.data.backend.redis.mq import PubSubHub
from hetu.data.backend.redis.pubsub import AsyncKeyspacePubSub


class FakeNodePubSub:
    """redis-py PubSub 的替身：记录发出的命令，ack 由测试投递；subscribe 可被闸门卡住"""

    def __init__(self):
        self.commands: list[tuple[str, tuple[str, ...]]] = []
        self.inbox: asyncio.Queue[dict] = asyncio.Queue()
        self.gate = asyncio.Event()
        self.gate.set()
        self.entered = asyncio.Event()  # 有调用方进入了 subscribe（可能卡在闸门上）
        self.fail_next: Exception | None = None

    async def subscribe(self, *channels: str):
        self.entered.set()
        await self.gate.wait()
        if self.fail_next is not None:
            exc, self.fail_next = self.fail_next, None
            raise exc
        self.commands.append(("subscribe", channels))

    async def unsubscribe(self, *channels: str):
        self.commands.append(("unsubscribe", channels))

    async def listen(self) -> AsyncIterator[dict]:
        while True:
            yield await self.inbox.get()

    def ack(self, mtype: str, channel: str):
        self.inbox.put_nowait({"type": mtype, "channel": channel.encode(), "data": 1})

    async def aclose(self):
        pass

    def sent(self, mtype: str) -> list[str]:
        return [ch for cmd, chans in self.commands if cmd == mtype for ch in chans]


def attach_fake_node(pubsub: AsyncKeyspacePubSub) -> FakeNodePubSub:
    node = FakeNodePubSub()
    pubsub.node_resources["standalone"] = {"client": node, "pubsub": node}
    pubsub._spawn_listener("standalone", node)  # type: ignore[reportPrivateUsage]
    return node


def make_pubsub() -> tuple[AsyncKeyspacePubSub, FakeNodePubSub]:
    pubsub = AsyncKeyspacePubSub(Redis(host="127.0.0.1", port=1))  # 只占位，不会连
    return pubsub, attach_fake_node(pubsub)


def make_hub() -> tuple[PubSubHub, FakeNodePubSub]:
    hub = PubSubHub(Redis(host="127.0.0.1", port=1))
    return hub, attach_fake_node(hub._pubsub)  # type: ignore[reportPrivateUsage]


async def settle():
    """让已就绪的 task 都跑一步"""
    for _ in range(3):
        await asyncio.sleep(0)
