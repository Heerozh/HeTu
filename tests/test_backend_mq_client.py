"""
MQClient 本地队列（get_message / push_pulled_）：空闲时不轮询，来消息才醒；
消息按 1/UPDATE_FREQUENCY 合批。用假的 pubsub 节点，不连 Redis。
"""

import asyncio
import time

import pytest
from fixtures.fake_pubsub import make_hub

from hetu.data.backend.base import MQClient
from hetu.data.backend.redis.mq import RedisMQClient

real_sleep = asyncio.sleep  # 测试自己等待用它，不计入 get_message 的醒来次数


@pytest.fixture
def sleep_calls(monkeypatch):
    """记录 get_message 里 asyncio.sleep 的调用（睡多久）。asyncio 模块是共享的，
    补丁对所有调用方生效，所以测试自己要用 real_sleep"""
    calls: list[float] = []

    async def counting_sleep(delay, *args, **kwargs):
        calls.append(delay)
        return await real_sleep(delay, *args, **kwargs)

    monkeypatch.setattr(asyncio, "sleep", counting_sleep)
    return calls


async def test_get_message_idle_waits_without_polling(sleep_calls):
    """没有消息时 get_message 等信号，不能每 1/UPDATE_FREQUENCY 醒一次看队列"""
    hub, _node = make_hub()
    mq = RedisMQClient(hub)
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.35):  # 旧实现这段时间会醒 3 次
            await mq.get_message()
    assert sleep_calls == []
    await hub.close()


async def test_get_message_wakes_on_push_and_batches(sleep_calls):
    """来消息才醒；队头到 1/UPDATE_FREQUENCY 才取（合批），没到时间的留到下一次"""
    hub, _node = make_hub()
    mq = RedisMQClient(hub)
    interval = 1 / MQClient.UPDATE_FREQUENCY
    getter = asyncio.create_task(mq.get_message())
    await real_sleep(0.05)
    assert not getter.done() and sleep_calls == []

    t0 = time.monotonic()
    mq.push_pulled_("A", None)
    await real_sleep(interval * 0.3)
    mq.push_pulled_("B", ["7"])
    async with asyncio.timeout(1):
        first = await getter
    elapsed = time.monotonic() - t0
    assert first == {"A": None}, "B 还没到合批时间，不该一起取"
    assert interval * 0.9 <= elapsed < interval * 3
    assert len(sleep_calls) == 1, "精确睡到队头到期，不是每 interval 轮询"

    async with asyncio.timeout(1):
        second = await mq.get_message()
    assert second == {"B": {"7"}}
    assert len(sleep_calls) <= 2
    await hub.close()


async def test_get_message_loop_wakeups_scale_with_messages(sleep_calls):
    """连续取消息：醒来次数与消息数同量级，而不是与等待时长成正比"""
    hub, _node = make_hub()
    mq = RedisMQClient(hub)
    got: list[dict] = []

    async def consumer():
        while len(got) < 3:
            got.append(await mq.get_message())

    task = asyncio.create_task(consumer())
    for name in ("X", "Y", "Z"):
        await real_sleep(0.3)  # 旧实现每 0.3s 空闲要醒 3 次
        mq.push_pulled_(name, None)
    async with asyncio.timeout(2):
        await task
    assert [set(m) for m in got] == [{"X"}, {"Y"}, {"Z"}]
    assert len(sleep_calls) <= 3, f"空闲期间还在轮询：{sleep_calls}"
    await hub.close()
