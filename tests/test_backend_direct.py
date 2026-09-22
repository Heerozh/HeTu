import asyncio

import pytest

from hetu.data.backend import Backend
from hetu.common.snowflake_id import SnowflakeID

SnowflakeID().init(1, 0)


async def test_table_direct_set(filled_rls_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()
    # 测试direct set
    async with backend.session(
        filled_rls_ref.instance_name, filled_rls_ref.cluster_id
    ) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        repo = session.using(filled_rls_ref.comp_cls)
        row = await repo.get(owner=10)
        assert row

    assert row.friend == 11

    # direct_set 不发通知（各后端一致）：它不动 _version，别的事务不会因它冲突，
    # 易失组件的行也不进 worker 缓存，所以没人需要这条通知
    mq = backend.get_mq_client()
    await mq.subscribe(backend.servant.row_channel(filled_rls_ref, row.id))
    try:
        await backend.master.direct_set(filled_rls_ref, row.id, friend="9")
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(mq.get_message(), 0.5)
    finally:
        await mq.close()

    async with backend.session(
        filled_rls_ref.instance_name, filled_rls_ref.cluster_id
    ) as session:
        session.only_master = True  # 强制master上读取，防止replica延迟导致测试不通过
        repo = session.using(filled_rls_ref.comp_cls)
        row = await repo.get(owner=10)
        assert row

    assert row.friend == 9

    # 测试写入不存在的行
    with pytest.raises(ValueError, match="aaa"):
        await backend.master.direct_set(filled_rls_ref, row.id, aaa="11")
