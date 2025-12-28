import numpy as np
import pytest

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend

SnowflakeID().init(1, 0)


async def test_double_upsert(item_ref, mod_auto_backend):
    backend = mod_auto_backend()

    # 测试2此upsert，应该最后次为准
    async with backend.session("pytest", 1) as session:
        item_select = session.select(item_ref.comp_cls)

        async with item_select.upsert(name="itm1") as row:
            row.time = 32345
        async with item_select.upsert(name="itm1") as row:
            row.time = 32346

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        item_select = session.select(item_ref.comp_cls)
        async with item_select.upsert(name="itm1") as row:
            assert row.time == 32346


async def test_query_after_update(filled_item_ref, mod_auto_backend):
    backend = mod_auto_backend()

    # update
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        row = (await item_select.range("owner", 10))[0]
        old_name = row.name
        assert (await item_select.get(name=old_name)).name == old_name
        row.owner = 11
        row.name = "updated"
        await item_select.update(row)
        # 测试能否命中cache
        row = await item_select.get(id=row.id)
        assert row.name == "updated"

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        row = await item_select.get(id=row.id)  # 测试用numpy type进行select是否报错
        assert row.name == "updated"
        assert (await item_select.range("owner", row.owner, limit=30)).shape[0] == 1
        assert (await item_select.range("owner", 10, limit=30)).shape[0] == 24
        assert (await item_select.range("owner", 11)).shape[0] == 1
        assert (await item_select.range("owner", 11)).name == "updated"
        assert (await item_select.get(name="updated")).name == "updated"
        assert await item_select.get(name=old_name) is None
        assert len(await item_select.range("id", -np.inf, +np.inf, limit=999)) == 25


async def test_query_after_delete(filled_item_ref, mod_auto_backend):
    backend = mod_auto_backend()

    # delete
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        rows = await item_select.range(id=(-np.inf, +np.inf), limit=999)
        item_select.delete(rows.id[4])
        item_select.delete(rows.id[6])
        # 测试能否命中cache
        row = await item_select.get(id=rows.id[4])
        assert row is None

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        assert len(await item_select.range("id", -np.inf, +np.inf, limit=999)) == 23
        assert await item_select.get(name="Itm14") is None
        assert await item_select.get(name="Itm16") is None
        assert (await item_select.range("time", 114, 116)).shape[0] == 1


async def test_dup_update(filled_item_ref, mod_auto_backend):
    backend = mod_auto_backend()

    # 检测重复update工作正常
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        row = await item_select.get(name="Itm12")
        row.time = 32345
        await item_select.update(row)
        # 重复更新
        row.time = 32346
        await item_select.update(row)

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        row = await item_select.get(name="Itm12")
        assert row.time == 32346


async def test_dup_delect(filled_item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()

    # 检测重复删除报错
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        row = await item_select.get(name="Itm12")
        item_select.delete(row.id)  # type: ignore
        with pytest.raises(LookupError, match="not existing"):
            item_select.delete(row.id)  # type: ignore
        # 放弃此次删除
        session.discard()

    # 检测update后删除
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        row = await item_select.get(name="Itm12")
        assert row
        row.time = 32345
        await item_select.update(row)
        item_select.delete(row.id)

    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        row = await item_select.get(name="Itm12")
        assert row is None

    # 检测删除后update
    async with backend.session("pytest", 1) as session:
        item_select = session.select(filled_item_ref.comp_cls)
        row = await item_select.get(name="Itm13")
        assert row
        item_select.delete(row.id)
        row.time = 32345
        with pytest.raises(LookupError, match="row id"):
            await item_select.update(row)
        session.discard()
