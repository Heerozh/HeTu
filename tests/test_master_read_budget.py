"""
master 读预算：本项目的设计约束是"能不读 master 的就不读 master"，普通读走
`Backend.master_or_servant` 把负载摊到副本上。这里给几条典型路径钉死"这条路允许在 master
上读几次"，任何新代码让某条路多读一次 master 都会挂在这里。

手法：backend 配置里 `master_weight: 0`，加权随机永远选不中 master，于是 master 客户端上
还发生的读一定是代码显式指定的（`only_master` 事务、顶号核查……），计数确定、不 flaky。
哪些地方允许显式读 master 见 `test_arch_master_reads.py` 的清单。下面几条路径都不该有
这样的读，所以预算全是 0。
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
    """事务读完全由副本承担，master 一次不读"""
    comp = filled_item_ref.comp_cls
    row_id = await _row_id(lb_backend, comp, time=120)
    with ExitStack() as stack:
        counts = count_reads(stack, lb_backend, include_range=True)
        async with lb_backend.session("pytest", 1) as session:
            repo = session.using(comp)
            assert await repo.get(id=row_id)
            assert len(await repo.range("owner", 10, limit=5)) == 5
        assert counts()["master"] == 0
        assert counts()["servant"] > 0


@use_redis_family_backend_only
async def test_subscribe_and_read_budget(lb_backend, filled_item_ref):
    """订阅一行、事务里读它、第二个连接再订阅它：全程不该碰 master"""
    comp = filled_item_ref.comp_cls
    row_id = await _row_id(lb_backend, comp, time=121)
    broker = SubscriptionBroker(lb_backend)
    broker2 = SubscriptionBroker(lb_backend)
    try:
        with ExitStack() as stack:
            counts = count_reads(stack, lb_backend, include_range=True)
            sub, _ = await broker.subscribe_get(
                filled_item_ref, admin_ctx_(), "id", row_id
            )
            assert sub
            assert counts()["master"] == 0, f"订阅时的那次读该走副本：{counts()}"

            async with lb_backend.session("pytest", 1) as session:
                assert await session.using(comp).get(id=row_id)
            assert counts()["master"] == 0, f"事务读该走副本：{counts()}"

            sub2, _ = await broker2.subscribe_get(
                filled_item_ref, admin_ctx_(), "id", row_id
            )
            assert sub2
            assert counts()["master"] == 0, f"第二个订阅者同样不该碰 master：{counts()}"
    finally:
        await broker.close()
        await broker2.close()


@use_redis_family_backend_only
async def test_push_refresh_budget(lb_backend, mod_backend_config, filled_item_ref):
    """别的进程改了订阅中的行：推送刷新走副本，master 读 0 次"""
    comp = filled_item_ref.comp_cls
    row_id = await _row_id(lb_backend, comp, time=122)
    broker = SubscriptionBroker(lb_backend)
    other = Backend(copy.deepcopy(mod_backend_config))
    other.post_configure(components=[comp])
    try:
        sub, _ = await broker.subscribe_get(filled_item_ref, admin_ctx_(), "id", row_id)
        assert sub
        with ExitStack() as stack:
            counts = count_reads(stack, lb_backend, include_range=True)
            async with other.session("pytest", 1) as session:
                repo = session.using(comp)
                row = await repo.get(id=row_id)
                assert row is not None
                row.qty = 555
                await repo.update(row)
            await other.wait_for_synced()
            updates = await broker.get_updates(timeout=5)
            (pushed,) = updates[sub].values()
            assert pushed["qty"] == 555
            assert counts()["master"] == 0, f"推送刷新不该读 master：{counts()}"
    finally:
        await other.close()
        await broker.close()
