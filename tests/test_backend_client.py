#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """
import asyncio
import msgpack
import numpy as np

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RedisBackendClient, TableReference
from hetu.data.backend.idmap import IdentityMap

SnowflakeID().init(1, 0)


async def test_redis_serialize_sortable():
    """测试sortable字段的序列化和反序列化"""
    client = RedisBackendClient.__new__(RedisBackendClient)

    int64_max = 2**63 - 1
    int64_min = -(2**63)
    uint64_max = 2**64 - 1
    uint64_min = 1
    double_int_max = 2**53 - 1 + 0.123
    double_int_min = -(2**53 - 1) + 0.123

    b1 = client.to_sortable_bytes(np.int64(int64_max))
    b2 = client.to_sortable_bytes(np.int64(int64_min))
    assert b1 > b2
    b1 = client.to_sortable_bytes(np.int8(127))
    b2 = client.to_sortable_bytes(np.int8(-1))
    assert b1 > b2
    # test int16 vs int8
    b1 = client.to_sortable_bytes(np.int16(128))
    b2 = client.to_sortable_bytes(np.int8(-1))
    assert b1 > b2

    b1 = client.to_sortable_bytes(np.uint64(uint64_max))
    b2 = client.to_sortable_bytes(np.uint64(uint64_min))
    assert b1 > b2

    b1 = client.to_sortable_bytes(np.float64(double_int_max))
    b2 = client.to_sortable_bytes(np.float64(double_int_min))
    assert b1 > b2
    # 123.456 = 405edd2f1a9fbe77, -0.033468749999999936 = bfa122d0e5604180
    # 405edd2f1a9fbe77 ^ 1 << 63 = c05edd2f1a9fbe77
    # bfa122d0e5604180 ^ 0xFFFFFFFFFFFFFFFF = 405edd2f1a9fbe7f
    b1 = client.to_sortable_bytes(np.float64(123.456))
    b2 = client.to_sortable_bytes(np.float64(-0.033468749999999936))
    assert b1 > b2


async def test_redis_commit_payload(mod_item_model, mod_rls_test_model):
    item_ref = TableReference(mod_item_model, "pytest", 1)
    rls_ref = TableReference(mod_rls_test_model, "pytest", 1)
    client = RedisBackendClient.__new__(RedisBackendClient)
    client.is_servant = False

    # 建立测试数据
    idmap = IdentityMap()
    checks = []
    pushes = []

    # insert
    # 插入 item row1
    row = item_ref.comp_cls.new_row()
    row.owner = 10
    row.time = row.owner
    row.name = f"{row.owner}"
    row.model = 123.31
    idmap.add_insert(item_ref, row)
    # 插入的payload应该是这些check和push
    b_rowid = client.to_sortable_bytes(row.id)
    checks.append(["NX", "pytest:Item:{CLU1}:id:" + f"{row.id}"])
    checks.append(
        ["UNIQ", "pytest:Item:{CLU1}:index:id"]
        + [b"[" + b_rowid + b":", b"[" + b_rowid + b";"]
    )
    checks.append(["UNIQ", "pytest:Item:{CLU1}:index:name", b"[10:", b"[10;"])
    checks.append(
        ["UNIQ", "pytest:Item:{CLU1}:index:time"]
        + [b"[\x80\x00\x00\x00\x00\x00\x00\n:", b"[\x80\x00\x00\x00\x00\x00\x00\n;"]
    )
    pushes.append(
        ["HSET", "pytest:Item:{CLU1}:id:" + f"{row.id}", "_version", "1"]
        + [
            x
            for k, v in zip(row.dtype.names, map(str, row.item()))  # type: ignore
            if k != "_version"
            for x in (k, v)
        ]
    )
    # insert的索引部分
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:id"]
        + ["0", b_rowid + b":" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:model"]
        + ["0", b"\xc0^\xd3\xd7\x00\x00\x00\x00:" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:name"] + ["0", b"10:" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:owner"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\n:" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:time"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\n:" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:used"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\x00:" + str(row.id).encode()]
    )

    # 插入 item row2
    row = item_ref.comp_cls.new_row()
    row.owner = 11
    row.time = row.owner
    row.model = -123.31
    row.name = f"{row.owner}"
    row.used = True
    idmap.add_insert(item_ref, row)
    # 插入的payload应该是这些check和push
    b_rowid = client.to_sortable_bytes(row.id)
    checks.append(["NX", "pytest:Item:{CLU1}:id:" + f"{row.id}"])
    checks.append(
        ["UNIQ", "pytest:Item:{CLU1}:index:id"]
        + [b"[" + b_rowid + b":", b"[" + b_rowid + b";"]
    )
    checks.append(["UNIQ", "pytest:Item:{CLU1}:index:name", b"[11:", b"[11;"])
    checks.append(
        ["UNIQ", "pytest:Item:{CLU1}:index:time"]
        + [b"[\x80\x00\x00\x00\x00\x00\x00\x0b:", b"[\x80\x00\x00\x00\x00\x00\x00\x0b;"]
    )
    pushes.append(
        ["HSET", "pytest:Item:{CLU1}:id:" + f"{row.id}", "_version", "1"]
        + [
            x
            for k, v in zip(row.dtype.names, map(str, row.item()))  # type: ignore
            if k != "_version"
            for x in (k, v)
        ]
    )
    # insert的索引部分
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:id"]
        + ["0", b_rowid + b":" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:model"]
        + ["0", b"?\xa1,(\xff\xff\xff\xff:" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:name"] + ["0", b"11:" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:owner"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\x0b:" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:time"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\x0b:" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:used"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\x01:" + str(row.id).encode()]
    )

    # 插入 rls row1
    row = rls_ref.comp_cls.new_row()
    row.owner = 11
    idmap.add_insert(rls_ref, row)
    # 插入的payload应该是这些check和push
    b_rowid = client.to_sortable_bytes(row.id)
    checks.append(["NX", "pytest:RLSTest:{CLU1}:id:" + f"{row.id}"])
    checks.append(
        ["UNIQ", "pytest:RLSTest:{CLU1}:index:id"]
        + [b"[" + b_rowid + b":", b"[" + b_rowid + b";"]
    )
    pushes.append(
        ["HSET", "pytest:RLSTest:{CLU1}:id:" + f"{row.id}", "_version", "1"]
        + [
            x
            for k, v in zip(row.dtype.names, map(str, row.item()))  # type: ignore
            if k != "_version"
            for x in (k, v)
        ]
    )
    # insert的索引部分
    pushes.append(
        ["ZADD", "pytest:RLSTest:{CLU1}:index:id"]
        + ["0", b_rowid + b":" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:RLSTest:{CLU1}:index:owner"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\x0b:" + str(row.id).encode()]
    )

    # 插入 rls row2
    row = rls_ref.comp_cls.new_row()
    row.owner = 12
    idmap.add_insert(rls_ref, row)
    # 插入的payload应该是这些check和push
    b_rowid = client.to_sortable_bytes(row.id)
    checks.append(["NX", "pytest:RLSTest:{CLU1}:id:" + f"{row.id}"])
    checks.append(
        ["UNIQ", "pytest:RLSTest:{CLU1}:index:id"]
        + [b"[" + b_rowid + b":", b"[" + b_rowid + b";"]
    )
    pushes.append(
        ["HSET", "pytest:RLSTest:{CLU1}:id:" + f"{row.id}", "_version", "1"]
        + [
            x
            for k, v in zip(row.dtype.names, map(str, row.item()))  # type: ignore
            if k != "_version"
            for x in (k, v)
        ]
    )
    # insert的索引部分
    pushes.append(
        ["ZADD", "pytest:RLSTest:{CLU1}:index:id"]
        + ["0", b_rowid + b":" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:RLSTest:{CLU1}:index:owner"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\x0c:" + str(row.id).encode()]
    )

    # update 1, change time
    row = item_ref.comp_cls.new_row()
    row.owner = 20
    row.time = row.owner
    row.name = f"{row.owner}"
    row._version = 16
    idmap.add_clean(item_ref, row)
    row.time = 23
    idmap.update(item_ref, row)
    # 更新的payload应该是这些check和push
    checks.append(["VER", "pytest:Item:{CLU1}:id:" + f"{row.id}", "16"])
    checks.append(
        ["UNIQ", "pytest:Item:{CLU1}:index:time"]
        + [b"[\x80\x00\x00\x00\x00\x00\x00\x17:", b"[\x80\x00\x00\x00\x00\x00\x00\x17;"]
    )
    pushes.append(
        ["HSET", "pytest:Item:{CLU1}:id:" + f"{row.id}", "_version", "17"]
        + ["time", "23"]
    )
    # update的index变更
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:time"]
        + [b"\x80\x00\x00\x00\x00\x00\x00\x14:" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:time"]
        + ["0", b"\x80\x00\x00\x00\x00\x00\x00\x17:" + str(row.id).encode()]
    )

    # update 2, change name
    row = item_ref.comp_cls.new_row()
    row.owner = 21
    row.time = row.owner
    row.name = f"{row.owner}"
    row._version = 233
    idmap.add_clean(item_ref, row)
    row.name = "23"
    idmap.update(item_ref, row)
    # 更新的payload应该是这些check和push
    checks.append(["VER", "pytest:Item:{CLU1}:id:" + f"{row.id}", "233"])
    checks.append(["UNIQ", "pytest:Item:{CLU1}:index:name", b"[23:", b"[23;"])
    pushes.append(
        ["HSET", "pytest:Item:{CLU1}:id:" + f"{row.id}", "_version", "234"]
        + ["name", "23"]
    )
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:name"] + [b"21:" + str(row.id).encode()]
    )
    pushes.append(
        ["ZADD", "pytest:Item:{CLU1}:index:name"] + ["0", b"23:" + str(row.id).encode()]
    )

    # delete
    row = item_ref.comp_cls.new_row()
    row.owner = 22
    row.time = row.owner
    row.name = f"{row.owner}"
    row._version = 9
    idmap.add_clean(item_ref, row)
    idmap.mark_deleted(item_ref, row.id)
    # 删除的payload应该是这些check和push
    b_rowid = client.to_sortable_bytes(row.id)
    checks.append(["VER", "pytest:Item:{CLU1}:id:" + f"{row.id}", "9"])
    pushes.append(["DEL", "pytest:Item:{CLU1}:id:" + f"{row.id}"])
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:id"]
        + [b_rowid + b":" + str(row.id).encode()]
    )
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:model"]
        + [b"\x80\x00\x00\x00\x00\x00\x00\x00:" + str(row.id).encode()]
    )
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:name"] + [b"22:" + str(row.id).encode()]
    )
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:owner"]
        + [b"\x80\x00\x00\x00\x00\x00\x00\x16:" + str(row.id).encode()]
    )
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:time"]
        + [b"\x80\x00\x00\x00\x00\x00\x00\x16:" + str(row.id).encode()]
    )
    pushes.append(
        ["ZREM", "pytest:Item:{CLU1}:index:used"]
        + [b"\x80\x00\x00\x00\x00\x00\x00\x00:" + str(row.id).encode()]
    )

    # commit
    json = []

    async def mock_lua_commit(keys, payload_json):
        # 反序列化payload_json
        assert keys[0] == "pytest:Item:{CLU1}:id:1"
        # 比较idmap和idmap_deser是否相等
        json_str = payload_json[0]
        nonlocal json
        json = msgpack.unpackb(json_str, raw=True)
        return b"committed"

    client.lua_commit = mock_lua_commit
    await client.commit(idmap)

    # test
    checks = [  # 先全转换为bytes, 因为msgpack解包后str会变bytes
        [arg.encode() if type(arg) is str else arg for arg in args] for args in checks
    ]
    pushes = [
        [arg.encode() if type(arg) is str else arg for arg in args] for args in pushes
    ]
    for check in json[0]:
        assert check in checks
    for push in json[1]:
        assert push in pushes
    for check in checks:
        assert check in json[0]
    for push in pushes:
        assert push in json[1]


async def test_insert(item_ref, rls_ref, mod_auto_backend):
    """测试client的commit(insert)/get"""
    # 启动backend
    backend: Backend = mod_auto_backend()
    client = backend.master

    # 测试client的commit(insert)数据，以及get
    idmap = IdentityMap()

    row1 = item_ref.comp_cls.new_row()
    row1.owner = 10
    idmap.add_insert(item_ref, row1)

    row2 = rls_ref.comp_cls.new_row()
    row2.owner = 11
    idmap.add_insert(rls_ref, row2)
    await client.commit(idmap)

    # 测试insert的是否有效
    row_get = await client.get(item_ref, row1.id)
    row1._version += 1
    assert row_get == row1

    row_get = await client.get(rls_ref, row2.id)
    row2._version += 1
    assert row_get == row2


async def test_update_delete(item_ref, rls_ref, mod_auto_backend):
    """测试client的commit(update/delete) get/range"""
    # 启动backend
    backend: Backend = mod_auto_backend()
    client = backend.master

    from hetu.data.backend.idmap import IdentityMap

    # 添加多条数据
    idmap = IdentityMap()
    for i in range(10):
        row1 = item_ref.comp_cls.new_row()
        row1.owner = i
        row1.time = i + 10
        row1.name = f"Item{i + 100}"
        idmap.add_insert(item_ref, row1)
        row2 = rls_ref.comp_cls.new_row()
        row2.owner = i
        idmap.add_insert(rls_ref, row2)

    # update insert的内容
    rows_cache, _, _ = idmap._cache(item_ref)
    row = rows_cache[5]
    row.name = "mid"
    idmap.update(item_ref, row)

    await client.commit(idmap)

    # 开始测试新的事务
    idmap = IdentityMap()

    # 测试range查询
    rows1 = await client.range(item_ref, "time", 13, 16)
    np.testing.assert_array_equal(
        rows1.name,
        [f"Item{3 + 100}", f"Item{4 + 100}", "mid", f"Item{6 + 100}"],
    )

    rows2 = await client.range(rls_ref, "owner", 9, 15)
    np.testing.assert_array_equal(rows2.owner, [9])

    idmap.add_clean(item_ref, rows1)
    idmap.add_clean(rls_ref, rows2)
    # 测试update查询到的数据
    row1 = rows1[rows1.time == 13][0]
    row1.name = "updated"
    idmap.update(item_ref, row1)

    row2 = rows2[0]
    row2.owner = 11
    idmap.update(rls_ref, row2)
    await client.commit(idmap)

    # 测试update后再次查询是否更新了
    rows1 = await client.range(item_ref, "time", 13, 16)
    np.testing.assert_array_equal(
        rows1.name,
        ["updated", f"Item{4 + 100}", "mid", f"Item{6 + 100}"],
    )

    rows2 = await client.range(rls_ref, "owner", 9, 15)
    np.testing.assert_array_equal(rows2.owner, [11])

    # 测试删除
    idmap = IdentityMap()
    idmap.add_clean(item_ref, rows1)
    idmap.add_clean(rls_ref, rows2)

    idmap.mark_deleted(item_ref, rows1[rows1.time == 13]["id"][0])
    idmap.mark_deleted(rls_ref, rows2.id[0])
    await client.commit(idmap)

    # 测试删除后再次查询
    rows1 = await client.range(item_ref, "time", 13, 16)
    np.testing.assert_array_equal(
        rows1.name,
        [f"Item{4 + 100}", "mid", f"Item{6 + 100}"],
    )

    rows2 = await client.range(rls_ref, "owner", 9, 15)
    np.testing.assert_array_equal(rows2.owner, [])


async def test_mq_client(filled_item_ref, mod_auto_backend):
    """测试mq client的订阅是否有效。这里只做基本的测试，更复杂的在综合测试中"""
    backend: Backend = mod_auto_backend()
    servant = backend.servant
    mq = backend.get_mq_client()

    # 获取测试行
    rows = await servant.range(filled_item_ref, "time", 110)
    row = rows[0]
    assert row

    # 测试订阅
    channel_name = servant.row_channel(filled_item_ref, row.id)
    await mq.subscribe(channel_name)

    # 写入数据库查看通知是否生效
    idmap = IdentityMap()
    idmap.add_clean(filled_item_ref, row)
    row.qty = 9999
    idmap.update(filled_item_ref, row)
    await backend.master.commit(idmap)

    # 拉取消息(堵塞直到收到消息)
    try:
        async with asyncio.timeout(0.1):
            # 多拉几次去掉服务器刚启动多余的消息
            await mq.pull()
            await mq.pull()
            await mq.pull()
    except TimeoutError:
        pass

    async with asyncio.timeout(0.1):
        messages = await mq.get_message()

    assert channel_name.encode() in messages
