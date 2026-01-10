from hetu.data.backend import Backend
from hetu.common.snowflake_id import SnowflakeID

SnowflakeID().init(1, 0)


async def test_table_direct_set(filled_item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()
    # 测试direct set
    async with backend.session(
        filled_item_ref.instance_name, filled_item_ref.cluster_id
    ) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        item = await item_repo.get(time=110)
        assert item

    assert item.qty == 999

    await backend.master.direct_set(filled_item_ref, item.id, qty="911")

    async with backend.session(
        filled_item_ref.instance_name, filled_item_ref.cluster_id
    ) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        item = await item_repo.get(time=110)
        assert item

    assert item.qty == 911
