import numpy as np
import pytest

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RowFormat

SnowflakeID().init(1, 0)


async def test_double_upsert(item_ref, mod_auto_backend):
    backend = mod_auto_backend()

    # 测试2此upsert，应该最后次为准
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(item_ref.comp_cls)

        async with item_repo.upsert(name="itm1") as row:
            row.time = 32345
        async with item_repo.upsert(name="itm1") as row:
            row.time = 32346

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        item_repo = session.using(item_ref.comp_cls)
        async with item_repo.upsert(name="itm1") as row:
            assert row.time == 32346


async def test_query_after_update(filled_item_ref, mod_auto_backend):
    backend = mod_auto_backend()

    # update
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        row = (await item_repo.range("owner", 10))[0]
        old_name = row.name
        assert (await item_repo.get(name=old_name)).name == old_name
        row.owner = 11
        row.name = "updated"
        await item_repo.update(row)
        # 测试能否命中cache
        row = await item_repo.get(id=row.id)
        assert row.name == "updated"

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        row = await item_repo.get(id=row.id)  # 测试用numpy type进行get是否报错
        assert row.name == "updated"
        assert (await item_repo.range("owner", row.owner, limit=30)).shape[0] == 1
        assert (await item_repo.range("owner", 10, limit=30)).shape[0] == 24
        assert (await item_repo.range("owner", 11)).shape[0] == 1
        assert (await item_repo.range("owner", 11)).name == "updated"
        assert (await item_repo.get(name="updated")).name == "updated"
        assert await item_repo.get(name=old_name) is None
        assert len(await item_repo.range("id", -np.inf, +np.inf, limit=999)) == 25


async def test_query_after_delete(filled_item_ref, mod_auto_backend):
    backend = mod_auto_backend()

    # delete
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        rows = await item_repo.range(id=(-np.inf, +np.inf), limit=999)
        item_repo.delete(rows.id[4])
        item_repo.delete(rows.id[6])
        # 测试能否命中cache
        row = await item_repo.get(id=rows.id[4])
        assert row is None

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        assert len(await item_repo.range("id", -np.inf, +np.inf, limit=999)) == 23
        assert await item_repo.get(name="Itm14") is None
        assert await item_repo.get(name="Itm16") is None
        assert (await item_repo.range("time", 114, 116)).shape[0] == 1


async def test_dup_update(filled_item_ref, mod_auto_backend):
    backend = mod_auto_backend()

    # 检测重复update工作正常
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        row = await item_repo.get(name="Itm12")
        row.time = 32345
        await item_repo.update(row)
        # 重复更新
        row.time = 32346
        await item_repo.update(row)

    await backend.wait_for_synced()

    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        row = await item_repo.get(name="Itm12")
        assert row.time == 32346


async def test_dup_delect(filled_item_ref, mod_auto_backend):
    backend: Backend = mod_auto_backend()

    # 检测重复删除报错
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        row = await item_repo.get(name="Itm12")
        item_repo.delete(row.id)  # type: ignore
        with pytest.raises(LookupError, match="not existing"):
            item_repo.delete(row.id)  # type: ignore
        # 放弃此次删除
        session.discard()

    # 检测update后删除
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        row = await item_repo.get(name="Itm12")
        assert row
        row.time = 32345
        await item_repo.update(row)
        item_repo.delete(row.id)

    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        row = await item_repo.get(name="Itm12")
        assert row is None

    # 检测删除后update
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(filled_item_ref.comp_cls)
        row = await item_repo.get(name="Itm13")
        assert row
        item_repo.delete(row.id)
        row.time = 32345
        with pytest.raises(LookupError, match="row id"):
            await item_repo.update(row)
        session.discard()


async def test_update_indexed_then_delete(filled_item_ref, mod_auto_backend):
    """同一事务先改索引列再删行：索引要按数据库里的原值清。按改后的值清的话（Redis 按值
    ZREM），原值上的索引项会留下来：这个 unique 值再也插不进去，区间读也会读到已删的行"""
    backend = mod_auto_backend()
    comp = filled_item_ref.comp_cls

    async with backend.session("pytest", 1) as session:
        item_repo = session.using(comp)
        row = await item_repo.get(name="Itm12")
        assert row is not None
        row_id = int(row.id)
        row.time, row.owner, row.name = 999_999, 11, "renamed"
        await item_repo.update(row)
        item_repo.delete(row_id)

    await backend.wait_for_synced()

    # 原值、改后的值上都不能留下这一行
    for index_name, value in [
        ("time", 112),
        ("time", 999_999),
        ("owner", 10),
        ("owner", 11),
        ("name", "Itm12"),
        ("name", "renamed"),
    ]:
        ids = await backend.master.range(
            filled_item_ref, index_name, value, value, 100, False, RowFormat.ID_LIST
        )
        assert row_id not in map(int, ids), f"{index_name}={value}"

    # 删掉的行原来的 unique 值能再用
    async with backend.session("pytest", 1) as session:
        row = comp.new_row()
        row.name, row.time = "Itm12", 112
        await session.using(comp).insert(row)


async def test_insert_then_delete(item_ref, mod_auto_backend):
    """同一事务 insert 了又删掉的行：数据库里从没有过，提交时不用管它。当成库里的行去删
    （按 _version=0 校验版本）的话，每次提交都判竞态，System 只能把重试次数耗光"""
    backend = mod_auto_backend()
    comp = item_ref.comp_cls

    async with backend.session("pytest", 1) as session:
        item_repo = session.using(comp)
        kept = comp.new_row()
        kept.name, kept.time = "kept", 1
        await item_repo.insert(kept)
        dropped = comp.new_row()
        dropped.name, dropped.time = "dropped", 2
        await item_repo.insert(dropped)
        item_repo.delete(dropped.id)
        # 事务内已经看不到它
        assert await item_repo.get(id=dropped.id) is None
        assert await item_repo.get(name="dropped") is None

    # 只有这一种写入、净效果为空的事务也照常结束
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(comp)
        alone = comp.new_row()
        alone.name, alone.time = "alone", 3
        await item_repo.insert(alone)
        item_repo.delete(alone.id)

    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(comp)
        assert await item_repo.get(name="kept") is not None
        assert await item_repo.get(id=dropped.id) is None
        assert await item_repo.get(name="dropped") is None
        assert await item_repo.get(name="alone") is None


async def test_reinsert_deleted_id_rejected(filled_item_ref, mod_auto_backend):
    """删掉库里的行后，同一事务不能再用这个 id insert（upsert 锚定这个 id 新建也一样），
    要改这行请直接 update。放行的话缓存里会有两行同 id，事务内 get(id) 读到已删的旧行，
    提交时报 UniqueViolation。被拒不影响之前的删除"""
    backend = mod_auto_backend()
    comp = filled_item_ref.comp_cls

    async with backend.session("pytest", 1) as session:
        item_repo = session.using(comp)
        row = await item_repo.get(name="Itm12")
        assert row is not None
        row_id = int(row.id)
        item_repo.delete(row_id)
        again = comp.new_row(id_=row_id)
        again.name, again.time = "again", 999_999
        with pytest.raises(ValueError, match="已在本事务中删除"):
            await item_repo.insert(again)
        with pytest.raises(ValueError, match="已在本事务中删除"):
            async with item_repo.upsert(id=row_id) as upserted:
                upserted.name, upserted.time = "again", 999_999
        assert await item_repo.get(id=row_id) is None

    await backend.wait_for_synced()
    async with backend.session("pytest", 1) as session:
        item_repo = session.using(comp)
        assert await item_repo.get(id=row_id) is None
        assert await item_repo.get(name="Itm12") is None
        assert await item_repo.get(name="again") is None
