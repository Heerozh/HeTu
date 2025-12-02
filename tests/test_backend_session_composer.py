import numpy as np
import pytest


@pytest.mark.xfail(reason="todo: 等idmap")
async def test_double_upsert(item_table):
    backend = item_table.backend

    # 测试2此upsert，应该最后次为准
    async with backend.transaction(1) as session:
        tbl = item_table.attach(session)

        async with tbl.update_or_insert("itm1", "name") as row:
            row.time = 32345
        async with tbl.update_or_insert("itm1", "name") as row:
            row.time = 32346

    async with backend.transaction(1) as session:
        tbl = item_table.attach(session)
        async with tbl.update_or_insert("itm1", "name") as row:
            assert row.time == 32346


async def test_query_after_update(filled_item_table):
    backend = filled_item_table.backend

    # update
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = (await tbl.query("owner", 10))[0]
        old_name = row.name
        assert (await tbl.select(old_name, where="name")).name == old_name
        row.owner = 11
        row.name = "updated"
        await tbl.update(row.id, row)
        # 测试能否命中cache
        row = await tbl.select(row.id)
        assert row.name == "updated"

    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(row.id)  # 测试用numpy type进行select是否报错
        assert row.name == "updated"
        assert (await tbl.query("owner", row.owner, limit=30)).shape[0] == 1
        assert (await tbl.query("owner", 10, limit=30)).shape[0] == 24
        assert (await tbl.query("owner", 11)).shape[0] == 1
        assert (await tbl.query("owner", 11)).name == "updated"
        assert (await tbl.select("updated", where="name")).name == "updated"
        assert await tbl.select(old_name, where="name") is None
        assert len(await tbl.query("id", -np.inf, +np.inf, limit=999)) == 25


async def test_query_after_delete(filled_item_table):
    backend = filled_item_table.backend

    # delete
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        await tbl.delete(5)
        await tbl.delete(7)
        # 测试能否命中cache
        row = await tbl.select(5)
        assert row is None

    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        assert len(await tbl.query("id", -np.inf, +np.inf, limit=999)) == 23
        assert await tbl.select("Itm14", where="name") is None
        assert await tbl.select("Itm16", where="name") is None
        assert (await tbl.query("time", 114, 116)).shape[0] == 1


async def test_dup_update(mod_item_model, filled_item_table):
    backend = filled_item_table.backend
    # todo 改成完整的UnitOfWork模式, 使用 Identity Map。不再使用cache
    #      所有数据操作首先经过Identity Map，并每行标记是否为脏（update, insert, delete）状态
    #      select时如果在idmap，则返回，不然从数据库读取，并放入干净缓存
    #      query时，维护一个query range cache保存查询的row.id序列，如果range不变则从缓存
    #              直接np二次查询所有idmap符合的row
    #              如果缓存变了，则重新query数据库，新行加入缓存，旧行不变，然后在idmap筛选所有row
    #      insert时，放入idmap，并标记为脏，且id为None。该有的Unique检查也要在idmap和数据库中同时进行
    #      update时，修改idmap值，并标记为脏
    #      delete时，标记idmap为删除状态
    #      upsert时，先要从idmap筛选where值(range(where=(value, value))，如果不存在才去数据库取
    #      commit时，按照insert, update, delete标记修改数据库

    # 检测重复update报错 todo 改成可以顺利执行
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(2)
        row.time = 32345
        await tbl.update(2, row)
        with pytest.raises(KeyError, match="重复更新"):
            await tbl.update(2, row)
        await session.end_transaction(discard=True)

    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(2)
        row.time = 32345
        await tbl.update(2, row)
        row.time = 42345
        with pytest.raises(KeyError, match="重复更新"):
            await tbl.update(2, row)
        await session.end_transaction(discard=True)


async def test_dup_delect(mod_item_model, filled_item_table):
    backend = filled_item_table.backend

    # 检测重复删除报错  todo 改成可以顺利执行
    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        await tbl.delete(1)
        with pytest.raises(KeyError, match="重复删除"):
            await tbl.delete(1)
        await session.end_transaction(discard=True)

    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(2)
        row.time = 32345
        await tbl.delete(2)
        with pytest.raises(KeyError, match="再次更新"):
            await tbl.update(2, row)
        await session.end_transaction(discard=True)

    async with backend.transaction(1) as session:
        tbl = filled_item_table.attach(session)
        row = await tbl.select(2)
        row.time = 32345
        await tbl.update(2, row)
        with pytest.raises(KeyError, match='再次删除'):
            await tbl.delete(2)
        await session.end_transaction(discard=True)
