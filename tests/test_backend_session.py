from hetu.data.backend import ComponentTable


async def test_session_exception(mod_auto_backend, mod_item_component,
                                 mod_item_table: ComponentTable):
    comp_tbl_class, get_or_create_backend = mod_auto_backend

    backend = get_or_create_backend()
    try:
        async with backend.transaction(1) as session:
            tbl = mod_item_table.attach(session)
            row = mod_item_component.new_row()
            row.owner = 123
            await tbl.insert(row)

            raise Exception("测试异常回滚")

            row = defined_item_component.new_row()
            row.owner = 321
            await tbl.insert(row)
    except Exception as e:
        pass

    # 验证数据没有被提交
    row = await mod_item_table.direct_get(0)
    assert row is None

    row = await mod_item_table.direct_get(1)
    assert row is None
