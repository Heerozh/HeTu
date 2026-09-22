"""
master 读预算：本项目的设计约束是"能不读 master 的就不读 master"，普通读走
`Backend.master_or_servant` 把负载摊到副本上。这里给几条典型路径钉死"这条路允许在 master
上读几次"，任何新代码让某条路多读一次 master 都会挂在这里。

手法：backend 配置里 `master_weight: 0`，加权随机永远选不中 master，于是 master 客户端上
还发生的读一定是代码显式指定的（`only_master` 事务、行缓存权威读、顶号核查……），计数确定、
不 flaky。哪些地方允许显式读 master 见 `test_arch_master_reads.py` 的清单。

预算里的 `master_authoritative` 是行缓存的权威读（master 上的 Lua HGETALL），只在两种情况
下发生：副本读回的版本低于已知下限（确实滞后），以及频道见过删除通知（同 id 重插后副本上
可能还是删除前的旧行）。下面几条路径都不属于这两种，所以预算全是 0。
"""

import copy
from contextlib import ExitStack

import pytest
from fixtures.backends import use_redis_family_backend_only
from fixtures.contexts import admin_ctx_
from fixtures.read_counts import count_reads

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend
from hetu.data.sub import SubscriptionBroker

SnowflakeID().init(1, 0)


@pytest.fixture
async def lb_backend(mod_backend_config, filled_item_ref):
    """master_weight=0 的独立 backend：加权随机永不选 master"""
    config = copy.deepcopy(mod_backend_config)
    config["master_weight"] = 0
    backend = Backend(config)
    backend.post_configure(components=[filled_item_ref.comp_cls])
    yield backend
    await backend.close()


async def _row_id(backend: Backend, comp, **query) -> int:
    async with backend.session("pytest", 1) as session:
        session.only_master = True
        row = await session.using(comp).get(**query)
        assert row is not None
        return int(row.id)


@use_redis_family_backend_only
async def test_plain_transaction_read_never_touches_master(lb_backend, filled_item_ref):
    """没人订阅的行：事务读完全由副本承担，master 一次不读（缓存也不参与）"""
    comp = filled_item_ref.comp_cls
    row_id = await _row_id(lb_backend, comp, time=120)
    with ExitStack() as stack:
        counts = count_reads(stack, lb_backend, include_range=True, by_role=True)
        async with lb_backend.session("pytest", 1) as session:
            repo = session.using(comp)
            assert await repo.get(id=row_id)
            assert len(await repo.range("owner", 10, limit=5)) == 5
        assert counts()["master"] == 0
        assert counts()["master_authoritative"] == 0
        assert counts()["servant"] > 0


@use_redis_family_backend_only
async def test_subscribe_and_read_budget(lb_backend, filled_item_ref):
    """
    订阅一行再读它：全程不该碰 master。

    行刚订上时本进程对它一无所知（floor 未知），这种读没法靠版本校验判断副本够不够新，
    但也没有任何理由去问 master——读副本、不入缓存即可，语义与没有缓存时完全一样。
    缓存靠两条路填：本进程 commit 的写穿、以及收到第一条通知（floor 变成整数）之后的
    副本读。权威读只留给"副本确实滞后"那一种情况。
    """
    comp = filled_item_ref.comp_cls
    row_id = await _row_id(lb_backend, comp, time=121)
    broker = SubscriptionBroker(lb_backend)
    broker2 = SubscriptionBroker(lb_backend)
    try:
        with ExitStack() as stack:
            counts = count_reads(stack, lb_backend, include_range=True, by_role=True)
            sub, _ = await broker.subscribe_get(
                filled_item_ref, admin_ctx_(), "id", row_id
            )
            assert sub
            got = counts()
            assert (got["master"], got["master_authoritative"]) == (0, 0), (
                f"订阅时的那次读该走副本：{got}"
            )

            async with lb_backend.session("pytest", 1) as session:
                assert await session.using(comp).get(id=row_id)
            got = counts()
            assert (got["master"], got["master_authoritative"]) == (0, 0), (
                f"刚订上、还没变更过的行，事务读也该走副本：{got}"
            )

            sub2, _ = await broker2.subscribe_get(
                filled_item_ref, admin_ctx_(), "id", row_id
            )
            assert sub2
            got = counts()
            assert (got["master"], got["master_authoritative"]) == (0, 0), (
                f"第二个订阅者同样不该碰 master：{got}"
            )
    finally:
        await broker.close()
        await broker2.close()


@use_redis_family_backend_only
async def test_push_refresh_budget(lb_backend, mod_backend_config, filled_item_ref):
    """别的进程改了订阅中的行：推送刷新走副本 + floor 校验，master 读 0 次"""
    comp = filled_item_ref.comp_cls
    row_id = await _row_id(lb_backend, comp, time=122)
    broker = SubscriptionBroker(lb_backend)
    other = Backend(copy.deepcopy(mod_backend_config))
    other.post_configure(components=[comp])
    try:
        sub, _ = await broker.subscribe_get(filled_item_ref, admin_ctx_(), "id", row_id)
        assert sub
        with ExitStack() as stack:
            counts = count_reads(stack, lb_backend, include_range=True, by_role=True)
            async with other.session("pytest", 1) as session:
                repo = session.using(comp)
                row = await repo.get(id=row_id)
                assert row is not None
                row.qty = 555
                await repo.update(row)
            await other.wait_for_synced()
            updates = await broker.get_updates(timeout=5)
            assert updates[sub][row_id]["qty"] == 555
            got = counts()
            assert got["master"] == 0 and got["master_authoritative"] == 0, (
                f"推送刷新不该读 master：{got}"
            )
    finally:
        await other.close()
        await broker.close()


@use_redis_family_backend_only
async def test_deleted_subscribed_row_read_budget(lb_backend, filled_item_ref):
    """订阅中的行被删除后，服务端逻辑还在反复读它：删除通知本身就是权威的，
    之后每次读都不该再打 master（也不该打副本）"""
    comp = filled_item_ref.comp_cls
    row_id = await _row_id(lb_backend, comp, time=123)
    broker = SubscriptionBroker(lb_backend)
    try:
        sub, _ = await broker.subscribe_get(filled_item_ref, admin_ctx_(), "id", row_id)
        assert sub
        async with lb_backend.session("pytest", 1) as session:
            repo = session.using(comp)
            assert await repo.get(id=row_id)
            repo.delete(row_id)

        with ExitStack() as stack:
            counts = count_reads(stack, lb_backend, include_range=True, by_role=True)
            for _ in range(3):
                async with lb_backend.session("pytest", 1) as session:
                    assert await session.using(comp).get(id=row_id) is None
            got = counts()
            assert got["master"] == 0 and got["master_authoritative"] == 0, (
                f"读已知不存在的行不该打 master：{got}"
            )
            assert got["plain"] == 0, f"也不该打副本：{got}"
    finally:
        await broker.close()
