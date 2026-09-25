"""
索引查询的比较语义在各后端一致。期望的行为以 Redis 后端为准：索引值编码成可排序的字节
（`to_sortable_bytes`）后按字节比较，同一个值内按 id 的十进制字符串排序。

SQL 后端用数据库原生的列类型，比较规则交给了各数据库（collation、浮点类型提升……），已知
的偏差用 strict xfail 标出（`xfail_on_backends`），修好后 XPASS 会报错，提醒去掉标记。
越界边界、两端开区间这两条是 Redis 后端的缺陷，按期望的行为写，标的是 Redis。
"""

from fixtures.backends import SQL_BACKENDS, xfail_on_backends

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RaceCondition, RowFormat, UniqueViolation

SnowflakeID().init(1, 0)

REDIS_FAMILY = ("redis", "redis_cluster", "valkey")


def _item(comp, *, time: int, name: str, owner: int = 0, **fields):
    """造一行 Item。time / name 是 unique 列，每行要给不同的值（name 最多 8 个字符）"""
    row = comp.new_row(id_=fields.pop("id_", None))
    row.owner, row.time, row.name = owner, time, name
    for key, value in fields.items():
        row[key] = value
    return row


async def _insert_rows(backend: Backend, comp, *rows) -> None:
    """一个事务插入这些行"""
    async with backend.session("pytest", 1) as session:
        for row in rows:
            await session.using(comp).insert(row)
    await backend.wait_for_synced()


async def _ids(backend: Backend, ref, index_name: str, *args, **kwargs) -> list[int]:
    return await backend.master.range(
        ref, index_name, *args, row_format=RowFormat.ID_LIST, **kwargs
    )


async def _names(backend: Backend, ref, *args, **kwargs) -> list[str]:
    rows = await backend.master.range(ref, "name", *args, **kwargs)
    return [str(name) for name in rows.name]


async def test_float32_point_query(item_ref, mod_auto_backend, backend_name, request):
    """
    float32 列按 0.1 点查、以 0.1 为闭区间上界：边界先按列的 dtype 取整（float32(0.1)），
    才对得上存进去的值。SQLite / MariaDB 拿 double 的 0.1 去比 float32 的 0.1
    （0.10000000149…），查不到。
    """
    xfail_on_backends(
        request,
        backend_name,
        ("sqlite", "mariadb"),
        raises=AssertionError,
        reason="float32 索引的查询边界没有先转成 float32",
    )
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    row = _item(comp, time=1, name="f", model=0.1)
    await _insert_rows(backend, comp, row)

    assert await _ids(backend, item_ref, "model", 0.1) == [int(row.id)]
    assert await _ids(backend, item_ref, "model", 0.05, 0.1) == [int(row.id)]
    async with backend.session("pytest", 1) as session:
        got = await session.using(comp).get(model=0.1)
        assert got is not None and got.id == row.id


async def test_str_index_case_sensitive(
    item_ref, mod_auto_backend, backend_name, request
):
    """
    字符串按字节比较，只差大小写是两个值：unique 不冲突，点查只命中自己。MariaDB 默认的
    collation 不区分大小写，第二行插入报 UniqueViolation。
    """
    xfail_on_backends(
        request,
        backend_name,
        ("mariadb",),
        raises=UniqueViolation,
        reason="MariaDB 默认 collation 不区分大小写",
    )
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, _item(comp, time=1, name="Abc"))
    await _insert_rows(backend, comp, _item(comp, time=2, name="abc"))

    assert await _names(backend, item_ref, "abc", limit=-1) == ["abc"]
    assert await _names(backend, item_ref, "Abc", limit=-1) == ["Abc"]


async def test_str_case_variants_in_one_transaction(
    item_ref, mod_auto_backend, backend_name, request
):
    """
    同一个事务插入只差大小写的两个 unique 值：两个都是新值，提交成功。MariaDB 的 collation
    认为它们重复，UNIQUE 约束报的 IntegrityError 被当成竞态抛 RaceCondition，每次重试结果都
    一样，System 会一直重试到上限。
    """
    xfail_on_backends(
        request,
        backend_name,
        ("mariadb",),
        raises=RaceCondition,
        reason="MariaDB 的 collation 冲突被当成竞态，重试不会自愈",
    )
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(
        backend, comp, _item(comp, time=1, name="Q"), _item(comp, time=2, name="q")
    )

    assert await _names(backend, item_ref, "Q", limit=-1) == ["Q"]
    assert await _names(backend, item_ref, "q", limit=-1) == ["q"]


async def test_str_index_trailing_space(
    item_ref, mod_auto_backend, backend_name, request
):
    """
    尾随空格是值的一部分。MariaDB 默认 collation 是 PAD SPACE，比较时忽略尾随空格，
    第二行插入报 UniqueViolation。
    """
    xfail_on_backends(
        request,
        backend_name,
        ("mariadb",),
        raises=UniqueViolation,
        reason="MariaDB 默认 collation 忽略尾随空格（PAD SPACE）",
    )
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, _item(comp, time=1, name="x "))
    await _insert_rows(backend, comp, _item(comp, time=2, name="x"))

    assert await _names(backend, item_ref, "x", limit=-1) == ["x"]
    assert await _names(backend, item_ref, "x ", limit=-1) == ["x "]


async def test_str_index_byte_order(item_ref, mod_auto_backend, backend_name, request):
    """
    字符串区间按 UTF-8 字节序：大写在小写前，"_" 在两者之间，非 ASCII 在最后。PG 默认
    collation 按 locale 排序，MariaDB 的 collation 不区分大小写，区间和顺序都不一样。
    """
    xfail_on_backends(
        request,
        backend_name,
        ("postgres", "mariadb"),
        raises=AssertionError,
        reason="PG / MariaDB 的字符串不按字节序比较",
    )
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    # 故意不放只差大小写的值，免得 MariaDB 先撞上 unique
    names = ["c", "B", "_", "a", "Z", "é"]
    await _insert_rows(
        backend, comp, *[_item(comp, time=i, name=n) for i, n in enumerate(names)]
    )
    by_bytes = sorted(names, key=lambda n: n.encode())
    assert by_bytes == ["B", "Z", "_", "a", "c", "é"]

    assert await _names(backend, item_ref, "B", "é", limit=-1) == by_bytes
    # desc 时边界照样是 (下界, 上界)
    desc = await _names(backend, item_ref, "B", "é", limit=-1, desc=True)
    assert desc == by_bytes[::-1]
    assert await _names(backend, item_ref, "Z", "a", limit=-1) == ["Z", "_", "a"]


async def test_same_value_rows_ordered_by_id_string(
    item_ref, mod_auto_backend, backend_name, request
):
    """
    同一个索引值内的行按 id 的十进制字符串排序（Redis 索引的 member 是 值\\x00id）。SQL
    后端按数值排。雪花 id 位数相同时两者一致，只有位数不同的显式 id 才看得出来，低优先级。
    """
    xfail_on_backends(
        request,
        backend_name,
        SQL_BACKENDS,
        raises=AssertionError,
        reason="SQL 后端同一值内按 id 数值排序（低优先级）",
    )
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(
        backend,
        comp,
        *[_item(comp, time=i, name=f"n{i}", owner=1, id_=i) for i in (9, 10, 100)],
    )

    assert await _ids(backend, item_ref, "owner", 1, limit=-1) == [10, 100, 9]
    desc = await _ids(backend, item_ref, "owner", 1, limit=-1, desc=True)
    assert desc == [9, 100, 10]


async def test_int_index_bounds_beyond_dtype_range(
    item_ref, mod_auto_backend, backend_name, request
):
    """
    整数列的区间边界超出 dtype 的范围时当作"到头"（和 ±inf 边界一样），返回范围内的行。
    Redis 后端把边界转成 dtype 时 numpy 直接抛 OverflowError。
    """
    xfail_on_backends(
        request,
        backend_name,
        REDIS_FAMILY,
        raises=OverflowError,
        reason="Redis 后端把越界的边界转成 dtype 时溢出",
    )
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    off = _item(comp, time=1, name="off", used=False)
    on = _item(comp, time=2, name="on", used=True)
    await _insert_rows(backend, comp, off, on)

    # used 是 bool 字段，定义时转成了 int8
    ids = await _ids(backend, item_ref, "used", -1000, 1000, limit=-1)
    assert sorted(ids) == sorted([int(off.id), int(on.id)])
    assert await _ids(backend, item_ref, "used", 1, 1000, limit=-1) == [int(on.id)]


async def test_open_bounds_on_same_value_is_empty(
    item_ref, mod_auto_backend, backend_name, request
):
    """
    两端都是开区间、值相同：区间为空，返回空结果。Redis 后端比较编码后的边界时，开区间
    的后缀让右边界比左边界小，误报"left必须大于等于right"的 ValueError。
    """
    xfail_on_backends(
        request,
        backend_name,
        REDIS_FAMILY,
        raises=ValueError,
        reason="Redis 后端把两端开区间的同值误判成左右边界颠倒",
    )
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, _item(comp, time=1, name="x"))

    assert await _names(backend, item_ref, "(x", "(x", limit=-1) == []
    assert await _names(backend, item_ref, "(x", "[x", limit=-1) == []
    assert await _names(backend, item_ref, "[x", "[x", limit=-1) == ["x"]
