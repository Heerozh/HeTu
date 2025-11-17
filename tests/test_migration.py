


#
#
#     @parameterized(implements)
#     async def test_migration(self, table_cls: type[ComponentTable],
#                              backend_cls: type[Backend], config):
#         # 测试迁移，先用原定义写入数据
#         backend = backend_cls(config)
#         item_data = table_cls(Item, 'test', 1, backend)
#         item_data.create_or_migrate()
#         item_data.flush(force=True)
#
#         async with backend.transaction(1) as session:
#             tbl = item_data.attach(session)
#             for i in range(25):
#                 row = Item.new_row()
#                 row.id = 0
#                 row.name = f'Itm{i + 10}aaaaaaaaaa'
#                 row.owner = 10
#                 row.time = i + 110
#                 row.qty = 999
#                 await tbl.insert(row)
#
#         # 重新定义新的属性
#         ComponentDefines().clear_()
#
#         @define_component(namespace="ssw")
#         class ItemNew(BaseComponent):
#             owner: np.int64 = Property(0, unique=False, index=True)
#             model: np.int32 = Property(0, unique=False, index=True)
#             qty_new: np.int16 = Property(111, unique=False, index=False)
#             level: np.int8 = Property(1, unique=False, index=False)
#             time: np.int64 = Property(0, unique=True, index=True)
#             name: 'U6' = Property("", unique=True, index=False)
#             used: bool = Property(False, unique=False, index=True)
#
#         # 从ItemNew改名回Item，以便迁移同名的
#         import json
#         define = json.loads(ItemNew.json_)
#         define['component_name'] = 'Item'
#         renamed_new_item_cls = BaseComponent.load_json(json.dumps(define))
#
#         # 测试迁移
#         item_data = table_cls(renamed_new_item_cls, 'test', 2, backend)
#         item_data.create_or_migrate()
#         # 检测跨cluster报错
#         with self.assertRaisesRegex(AssertionError, "cluster"):
#             async with backend.transaction(1) as session:
#                 item_data.attach(session)
#
#         async with backend.transaction(2) as session:
#             tbl = item_data.attach(session)
#             assert (await tbl.select(111, where='time')).name == 'Itm11a'
#             assert (await tbl.select(111, where='time')).qty_new == 111
#
#             assert (await tbl.select('Itm30a', where='name')).name == 'Itm30a'
#             assert (await tbl.select(130, where='time')).qty_new == 111
#
#         await backend.close()