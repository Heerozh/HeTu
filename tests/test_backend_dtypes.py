"""
非常规字段类型在各后端的往返与索引查询：bool（定义时转成 int8）、无符号整型、bytes。
"""

import numpy as np
import pytest
from fixtures.backends import SQL_BACKENDS, use_redis_family_backend_only
from fixtures.testdata import create_ref

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RowFormat, TableReference

SnowflakeID().init(1, 0)

U32_MAX = 2**32 - 1
I64_MAX = 2**63 - 1


def def_blob():
    from hetu.data import BaseComponent, Permission, define_component, property_field

    global Blob

    @define_component(namespace="pytest", permission=Permission.USER, force=True)
    class Blob(BaseComponent):
        flag: bool = property_field(False, index=True)
        small: np.uint32 = property_field(0, index=True)
        big: np.uint64 = property_field(0, index=True)
        tag: "S16" = property_field(b"", unique=True)  # type: ignore  # noqa

    return Blob


@pytest.fixture
async def blob_ref(new_component_env, mod_auto_backend) -> TableReference:
    """定义 Blob 组件并建空表"""
    return create_ref(def_blob(), mod_auto_backend())


async def _insert(backend: Backend, comp, **values) -> list[int]:
    """values 的每个字段是等长列表，按位组成一行插入；返回各行 id"""
    n = len(next(iter(values.values())))
    ids = []
    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        for i in range(n):
            row = comp.new_row()
            for field, vals in values.items():
                row[field] = vals[i]
            await repo.insert(row)
            ids.append(int(row.id))
    await backend.wait_for_synced()
    return ids


async def test_bool_field_is_int8(blob_ref, mod_auto_backend):
    """bool 字段定义时被转成 int8（部分后端不支持 bool 列）：存 True 读回 1，
    按它的索引点查 / 更新后索引跟着变"""
    backend: Backend = mod_auto_backend()
    comp = blob_ref.comp_cls
    assert comp.dtype_map_["flag"] == np.int8

    on, off = await _insert(backend, comp, flag=[True, False], tag=[b"on", b"off"])
    servant = backend.servant
    assert (await servant.get(blob_ref, on)).flag == 1
    assert (await servant.get(blob_ref, off, RowFormat.TYPED_DICT))["flag"] == 0

    rows = await servant.range(blob_ref, "flag", 1)
    assert [int(r.id) for r in rows] == [on]

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(id=off)
        assert row is not None
        row.flag = True
        await repo.update(row)
    await backend.wait_for_synced()
    rows = await servant.range(blob_ref, "flag", 1, limit=10)
    assert sorted(int(r.id) for r in rows) == sorted([on, off])
    assert len(await servant.range(blob_ref, "flag", 0)) == 0


async def test_unsigned_roundtrip(blob_ref, mod_auto_backend):
    """无符号整型最大值往返不溢出、不变号，三种 RowFormat 类型都对"""
    backend: Backend = mod_auto_backend()
    comp = blob_ref.comp_cls
    (row_id,) = await _insert(backend, comp, small=[U32_MAX], big=[I64_MAX])
    servant = backend.servant

    row = await servant.get(blob_ref, row_id)
    assert type(row.small) is np.uint32 and row.small == U32_MAX
    assert type(row.big) is np.uint64 and row.big == I64_MAX

    typed = await servant.get(blob_ref, row_id, RowFormat.TYPED_DICT)
    assert typed["small"] == U32_MAX and type(typed["small"]) is int
    assert typed["big"] == I64_MAX and type(typed["big"]) is int

    raw = await servant.get(blob_ref, row_id, RowFormat.RAW)
    assert str(raw["small"]) == str(U32_MAX)
    assert str(raw["big"]) == str(I64_MAX)


async def test_unsigned_index_range(blob_ref, mod_auto_backend):
    """无符号索引按数值排序：跨 2**31 的值不能按有符号排到前面去；desc、limit、更新后索引跟着变"""
    backend: Backend = mod_auto_backend()
    comp = blob_ref.comp_cls
    smalls = [U32_MAX, 0, 2**31, 2**31 - 1, U32_MAX - 1, 1]
    ids = await _insert(
        backend, comp, small=smalls, tag=[f"t{i}".encode() for i in range(6)]
    )
    by_small = dict(zip(smalls, ids))
    servant = backend.servant

    rows = await servant.range(blob_ref, "small", 2**31 - 1, U32_MAX, limit=10)
    assert list(rows.small) == [2**31 - 1, 2**31, U32_MAX - 1, U32_MAX]
    rows = await servant.range(blob_ref, "small", 0, U32_MAX, limit=2, desc=True)
    assert list(rows.small) == [U32_MAX, U32_MAX - 1]
    rows = await servant.range(blob_ref, "small", 2**31)
    assert [int(r.id) for r in rows] == [by_small[2**31]]
    rows = await servant.range(blob_ref, "big", 0, I64_MAX, limit=10)
    assert len(rows) == 6

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(id=by_small[0])
        assert row is not None
        row.small = 2**31 + 5
        await repo.update(row)
    await backend.wait_for_synced()
    rows = await servant.range(blob_ref, "small", 2**31, 2**31 + 10, limit=10)
    assert list(rows.small) == [2**31, 2**31 + 5]
    assert len(await servant.range(blob_ref, "small", 0)) == 0


@use_redis_family_backend_only
async def test_uint64_above_int64_max(blob_ref, mod_auto_backend):
    """uint64 超过 int64 上限的值原样往返，按无符号数值排序"""
    backend: Backend = mod_auto_backend()
    comp = blob_ref.comp_cls
    bigs = [2**64 - 1, I64_MAX, 2**63 + 5]
    ids = await _insert(backend, comp, big=bigs, tag=[b"x", b"y", b"z"])
    servant = backend.servant

    assert (await servant.get(blob_ref, ids[0])).big == 2**64 - 1
    rows = await servant.range(blob_ref, "big", I64_MAX, 2**64 - 1, limit=10)
    assert list(rows.big) == [I64_MAX, 2**63 + 5, 2**64 - 1]


@pytest.mark.parametrize("backend_name", SQL_BACKENDS, indirect=True)
async def test_sql_rejects_uint64_above_bigint(blob_ref, mod_auto_backend):
    """SQL 后端的无符号整型存在 BIGINT 列里：超过 2**63-1 的 uint64 写入时明确拒绝
    （报错带组件名、字段名，整个事务什么都不写），而不是各驱动各自的溢出错误。
    查询边界超出这个范围时按语义收回：上界超了等于到头，下界超了什么都查不到"""
    backend: Backend = mod_auto_backend()
    comp = blob_ref.comp_cls
    servant = backend.servant

    with pytest.raises(ValueError, match=r"Blob\.big"):
        await _insert(backend, comp, big=[I64_MAX, 2**63 + 5], tag=[b"x", b"y"])
    assert len(await servant.range(blob_ref, "big", 0, float("inf"), limit=10)) == 0

    ids = await _insert(backend, comp, big=[5, I64_MAX], tag=[b"x", b"y"])
    # 开放上界：inf 会被钳到 uint64 的最大值，同样要收回来
    rows = await servant.range(blob_ref, "big", 0, float("inf"), limit=10)
    assert [int(r.id) for r in rows] == ids
    rows = await servant.range(blob_ref, "big", 1, 2**64 - 1, limit=10, desc=True)
    assert list(rows.big) == [I64_MAX, 5]
    assert len(await servant.range(blob_ref, "big", 2**63 + 5, float("inf"))) == 0
    assert len(await servant.range(blob_ref, "big", 2**64 - 1)) == 0


BIN = b"\xff\x80\x00\xfe"  # 不是合法 UTF-8，中间还有 \x00


async def test_bytes_roundtrip(blob_ref, mod_auto_backend):
    """bytes 字段原样往返：中间的 \\x00、不是合法 UTF-8 的字节都不丢，
    STRUCT / TYPED_DICT / get_many 都一样"""
    backend: Backend = mod_auto_backend()
    comp = blob_ref.comp_cls
    tags = [b"a\x00b", BIN, "河图".encode()]
    ids = await _insert(backend, comp, tag=tags)
    servant = backend.servant

    for row_id, tag in zip(ids, tags):
        assert (await servant.get(blob_ref, row_id)).tag == tag
        typed = await servant.get(blob_ref, row_id, RowFormat.TYPED_DICT)
        assert typed["tag"] == tag
    rows = await servant.get_many(blob_ref, ids, RowFormat.TYPED_DICT)
    assert [row["tag"] for row in rows] == tags


async def test_bytes_index_follows_update_and_delete(blob_ref, mod_auto_backend):
    """bytes 字段改值、删行后索引跟着变：旧索引项要按真实字节撤掉，删掉后同值还能再插"""
    backend: Backend = mod_auto_backend()
    comp = blob_ref.comp_cls
    (row_id,) = await _insert(backend, comp, tag=[BIN])
    servant = backend.servant
    new_tag = b"\x01new\xff"

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        row = await repo.get(id=row_id)
        assert row is not None
        row.tag = new_tag
        await repo.update(row)
    await backend.wait_for_synced()
    assert len(await servant.range(blob_ref, "tag", BIN)) == 0
    rows = await servant.range(blob_ref, "tag", new_tag)
    assert [int(r.id) for r in rows] == [row_id]

    async with backend.session("pytest", 1) as session:
        repo = session.using(comp)
        assert await repo.get(id=row_id) is not None
        repo.delete(row_id)
    await backend.wait_for_synced()
    assert len(await servant.range(blob_ref, "tag", new_tag)) == 0
    # unique 索引里的旧项撤干净了：同值可以再插
    (again,) = await _insert(backend, comp, tag=[new_tag])
    assert (await servant.get(blob_ref, again)).tag == new_tag


async def test_bytes_index_range(blob_ref, mod_auto_backend):
    """bytes 索引按字节序：含 \\x00 的值夹在前缀和更大的值之间；点查只中那一行"""
    backend: Backend = mod_auto_backend()
    comp = blob_ref.comp_cls
    tags = [b"b", b"a\x00b", b"ab", b"a"]
    ids = await _insert(backend, comp, tag=tags)
    servant = backend.servant

    rows = await servant.range(blob_ref, "tag", b"a\x00b")
    assert [int(r.id) for r in rows] == [ids[1]]
    rows = await servant.range(blob_ref, "tag", b"a", b"ab", limit=10)
    assert list(rows.tag) == [b"a", b"a\x00b", b"ab"]
    rows = await servant.range(blob_ref, "tag", b"a", b"b", limit=2, desc=True)
    assert list(rows.tag) == [b"b", b"ab"]


@use_redis_family_backend_only
async def test_rebuild_index_matches_commit(blob_ref, mod_auto_backend):
    """重建索引（hetu upgrade 默认每次都做）按行数据重算的 member 要与 commit 写的逐字节
    一致：bytes 用真实字节，不是合法 UTF-8、非 ASCII 的值都不能让重建失败"""
    from hetu.data.backend.redis import RedisBackendClient

    backend: Backend = mod_auto_backend()
    comp = blob_ref.comp_cls
    ids = await _insert(
        backend,
        comp,
        flag=[True, False, True],
        small=[U32_MAX, 0, 2**31],
        big=[2**64 - 1, I64_MAX, 5],
        tag=[b"a\x00b", BIN, "河图".encode()],
    )
    io = backend.master.io
    idx_keys = [RedisBackendClient.index_key(blob_ref, name) for name in comp.indexes_]
    before = [io.zrange(key, 0, -1) for key in idx_keys]

    backend.get_table_maintenance().rebuild_index(blob_ref)
    assert [io.zrange(key, 0, -1) for key in idx_keys] == before
    await backend.wait_for_synced()
    rows = await backend.servant.range(blob_ref, "tag", BIN)
    assert [int(r.id) for r in rows] == [ids[1]]
