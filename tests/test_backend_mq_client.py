"""
MQClient 本地队列（get_message / push_pulled_）：空闲时不轮询，来消息才醒；
消息按 1/UPDATE_FREQUENCY 合批。用假的 pubsub 节点，不连 Redis。
"""

import asyncio
import contextlib
import logging
import time

import pytest
from fixtures.fake_pubsub import make_hub, settle

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


# === 尾随重读：通知之后至少隔一个 interval 再读一次（副本复制延迟的预算） ===

INTERVAL = 1 / MQClient.UPDATE_FREQUENCY


async def _expect_nothing(mq: MQClient, wait: float = INTERVAL * 2.5):
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(wait):
            await mq.get_message()


async def test_merged_notification_gets_trailing_read():
    """队头的频道又来了一条通知（被合并）：这次弹出时离它不到一个 interval，读可能落在还没
    应用它的副本上，而它的通知已经被合并掉了——弹出后要再排一次，interval 后重读"""
    hub, _node = make_hub()
    mq = RedisMQClient(hub)
    mq.push_pulled_("A", None)
    await asyncio.sleep(INTERVAL * 0.5)
    mq.push_pulled_("A", None)  # 合并进队头
    async with asyncio.timeout(1):
        assert await mq.get_message() == {"A": None}
    popped = time.monotonic()
    async with asyncio.timeout(1):
        assert await mq.get_message() == {"A": None}, "合并进来的通知没有尾随重读"
    assert time.monotonic() - popped >= INTERVAL * 0.9
    await _expect_nothing(mq)  # 尾随那次之后没有新通知，就此结束
    await hub.close()


async def test_continuous_stream_does_not_starve_reads():
    """不间断的写入流：尾随重读不能做成"来一条就重置计时"的防抖，否则写入不停就永远不读。
    合并进来的通知只记时刻、不推后队头，写入期间照常每个 interval 读一次；写入停下后
    再多读一次，且那次读晚于最后一条通知至少一个 interval，之后就安静了"""
    hub, _node = make_hub()
    mq = RedisMQClient(hub)
    t0 = time.monotonic()
    pops: list[float] = []
    last_push = 0.0

    async def reader():
        while True:
            await mq.get_message()
            pops.append(time.monotonic() - t0)

    reading = asyncio.create_task(reader())
    while time.monotonic() - t0 < 1.0:  # 每 10ms 一条，持续 1 秒
        mq.push_pulled_("A", None)
        last_push = time.monotonic() - t0
        await asyncio.sleep(0.01)
    await asyncio.sleep(INTERVAL * 5)
    reading.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await reading

    during = [p for p in pops if p <= last_push]
    after = [p for p in pops if p > last_push]
    # 约每 interval 一次（理想 9 次），留足余量给全量跑时的 GC 停顿
    assert len(during) >= 4, f"写入期间读被饿住了：{pops}"
    assert after, "写入停下后没有尾随重读"
    assert after[-1] - last_push >= INTERVAL * 0.9, (
        f"最后一次读离最后一条通知太近：{pops}"
    )
    assert len(after) <= 2, f"写入停下后还在反复读：{pops}"
    await hub.close()


async def test_single_notification_has_no_trailing_read():
    """单条通知弹出时已经隔了一个 interval，不需要尾随重读"""
    hub, _node = make_hub()
    mq = RedisMQClient(hub)
    mq.push_pulled_("A", None)
    async with asyncio.timeout(1):
        assert await mq.get_message() == {"A": None}
    await _expect_nothing(mq)
    await hub.close()


async def test_merged_notification_old_enough_has_no_trailing_read():
    """合并进来的通知到弹出时已经超过一个 interval（取得晚）：这次读已经满足预算，不补排"""
    hub, _node = make_hub()
    mq = RedisMQClient(hub)
    mq.push_pulled_("A", None)
    mq.push_pulled_("A", None)
    await asyncio.sleep(INTERVAL * 2.5)
    async with asyncio.timeout(1):
        assert await mq.get_message() == {"A": None}
    await _expect_nothing(mq)
    await hub.close()


async def test_trailing_read_carries_only_late_payload():
    """表级频道的尾随重读只需要重读迟到那几条消息带来的 row_id"""
    hub, _node = make_hub()
    mq = RedisMQClient(hub)
    mq.push_pulled_("T", ["1"])
    await asyncio.sleep(INTERVAL * 0.5)
    mq.push_pulled_("T", [2, 3])
    async with asyncio.timeout(1):
        assert await mq.get_message() == {"T": {"1", "2", "3"}}
    async with asyncio.timeout(1):
        assert await mq.get_message() == {"T": {"2", "3"}}
    await hub.close()


async def test_merged_resync_is_left_to_trailing_read():
    """RESYNC 合并进已在队列里的表级频道：这次弹出离它不足一个 interval，整表重同步只留给
    尾随重读做一次（那时已过复制延迟预算），这次只重读原有的 row_id。两次都带 RESYNC
    就是两遍整表重读、全量重推"""
    hub, _node = make_hub()
    mq = RedisMQClient(hub)
    mq.push_pulled_("T", ["1"])
    await asyncio.sleep(INTERVAL * 0.5)
    mq.push_pulled_("T", [MQClient.RESYNC])  # 合并进队头
    async with asyncio.timeout(1):
        assert await mq.get_message() == {"T": {"1"}}
    async with asyncio.timeout(1):
        assert await mq.get_message() == {"T": {MQClient.RESYNC}}
    await _expect_nothing(mq)
    await hub.close()


async def test_merged_resync_old_enough_resyncs_in_this_read():
    """合并进来的 RESYNC 到弹出时已经超过一个 interval（取得晚）：这次读已经满足预算，
    就在这次整表重同步，不补排"""
    hub, _node = make_hub()
    mq = RedisMQClient(hub)
    mq.push_pulled_("T", ["1"])
    mq.push_pulled_("T", [MQClient.RESYNC])
    await asyncio.sleep(INTERVAL * 2.5)
    async with asyncio.timeout(1):
        assert await mq.get_message() == {"T": {"1", MQClient.RESYNC}}
    await _expect_nothing(mq)
    await hub.close()


async def test_request_reread():
    """request_reread：当作刚收到通知放进本地队列，interval 后弹出；不触发 watch 回调"""
    hub, node = make_hub()
    mq = RedisMQClient(hub)
    fired: list[str] = []
    watching = asyncio.create_task(mq.watch("W", lambda: fired.append("W")))
    await settle()
    node.ack("subscribe", "W")
    async with asyncio.timeout(1):
        await watching

    t0 = time.monotonic()
    mq.request_reread("A", "W")
    mq.request_reread("T", payload=["5"])
    async with asyncio.timeout(1):
        assert await mq.get_message() == {"A": None, "W": None, "T": {"5"}}
    assert time.monotonic() - t0 >= INTERVAL * 0.9
    assert fired == [], "补读不是真的通知，不能触发服务端内部的 watch 回调"
    await hub.close()


async def test_drop_after_clears_late_state(monkeypatch):
    """积压超过 DROP_AFTER 被丢弃的频道，合并记录也要一起清掉"""
    monkeypatch.setattr(MQClient, "DROP_AFTER", INTERVAL * 0.5)
    hub, _node = make_hub()
    mq = RedisMQClient(hub)
    mq.push_pulled_("A", None)
    mq.push_pulled_("A", ["1"])
    await asyncio.sleep(INTERVAL)
    assert mq.push_pulled_("B", None) == 1  # A 被当作积压丢弃
    assert "A" not in mq._late and "A" not in mq._late_payload
    await hub.close()


async def test_request_reread_warns_dropped_backlog(monkeypatch, caplog):
    """request_reread 入队时同样会清掉积压超过 DROP_AFTER 的旧通知：清掉了就得和收到通知时
    一样打积压警告，不能悄悄丢"""
    monkeypatch.setattr(MQClient, "DROP_AFTER", INTERVAL * 0.5)
    hub, _node = make_hub()
    mq = RedisMQClient(hub)
    mq.push_pulled_("A", None)
    await asyncio.sleep(INTERVAL)
    with caplog.at_level(logging.WARNING, logger="HeTu.root"):
        mq.request_reread("B")
    assert "A" not in mq.pulled_set  # A 被当作积压丢弃
    warns = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(warns) == 1, "积压的通知被丢弃了却没有警告"
    assert "💾Redis" in warns[0].getMessage()
    await hub.close()
