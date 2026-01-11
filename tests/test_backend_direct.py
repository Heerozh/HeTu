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
        repo = session.using(filled_rls_ref.comp_cls)
        row = await repo.get(owner=10)
        assert row

    assert row.friend == 11

    await backend.master.direct_set(filled_rls_ref, row.id, friend="9")

    async with backend.session(
        filled_rls_ref.instance_name, filled_rls_ref.cluster_id
    ) as session:
        repo = session.using(filled_rls_ref.comp_cls)
        row = await repo.get(owner=10)
        assert row

    assert row.friend == 9

    # 测试写入不存在的行
    with pytest.raises(ValueError, match="aaa"):
        await backend.master.direct_set(filled_rls_ref, row.id, aaa="11")
