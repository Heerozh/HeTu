import numpy as np


async def test_table_direct_get_set(filled_item_table):
    # 测试direct get/set
    np.testing.assert_array_equal((await filled_item_table.direct_get(1)).qty, 999)
    await filled_item_table.direct_set(1, qty=911)
    np.testing.assert_array_equal((await filled_item_table.direct_get(1)).qty, 911)
    # 测试direct set index and direct query index
    await filled_item_table.direct_set(1, owner=911)
    np.testing.assert_array_equal((await filled_item_table.direct_get(1)).owner, 911)
    np.testing.assert_array_equal(
        (await filled_item_table.direct_query('owner', 911)).owner, 911)


async def test_table_direct_insert_delete(filled_item_table):
    # 测试direct insert and direct delete
    row_ids = await filled_item_table.direct_insert(owner=912, time=912)
    np.testing.assert_array_equal(
        (await filled_item_table.direct_get(row_ids[0])).owner, 912)
    np.testing.assert_array_equal(
        (await filled_item_table.direct_query('owner', 912)).owner, 912)
    await filled_item_table.direct_delete(row_ids[0])


async def test_table_direct_query(filled_item_table):
    # test direct query
    np.testing.assert_array_equal(
        (await filled_item_table.direct_query('name', 'Itm11', 'Itm12')).time,
        [111, 112])
