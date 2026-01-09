from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend

SnowflakeID().init(1, 0)


async def test_double_cluster(item_ref, mod_auto_backend):
    import asyncio
    from hetu.data.backend import RaceCondition

    # 测试在2个cluster_id环境下的同时写入和读取，只测试结果是否正常，具体的各个node数据是否正确不做测试
    backend: Backend = mod_auto_backend()

    async def upsert_owner(cluster_id, sleep):
        async with backend.session("pytest", cluster_id) as _session:
            _item_repo = _session.using(item_ref.comp_cls)
            async with _item_repo.upsert(name="test") as _row:
                _row.owner = _row.owner + 1  # type: ignore
            await asyncio.sleep(sleep)

    # 测试update_owner在不同的cluster id下不冲突
    task1 = asyncio.create_task(upsert_owner(1, 0.01))
    task2 = asyncio.create_task(upsert_owner(2, 0.2))
    await asyncio.gather(task1, task2)

    # 读取结果
    async with backend.session("pytest", 1) as session:
        session.only_master = True  # 强制读主节点，cluster环境目前没有replica
        item_repo = session.using(item_ref.comp_cls)
        row = await item_repo.get(name="test")
        assert row
        assert row.owner == 1
    async with backend.session("pytest", 2) as session:
        session.only_master = True  # 强制读主节点，cluster环境目前没有replica
        item_repo = session.using(item_ref.comp_cls)
        row = await item_repo.get(name="test")
        assert row
        assert row.owner == 1
