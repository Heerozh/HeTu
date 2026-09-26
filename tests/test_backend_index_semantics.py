"""
索引查询的比较语义在各后端一致。期望的行为以 Redis 后端为准：索引值编码成可排序的字节
（`to_sortable_bytes`）后按字节比较，同一个值内按 id 的十进制字符串排序。

SQL 后端用数据库原生的列类型，比较规则交给了各数据库（collation、浮点类型提升……），已知
的偏差用 strict xfail 标出（`xfail_on_backends`），修好后 XPASS 会报错，提醒去掉标记。
整数列的区间按数学含义处理（`normalize_int_bounds_`）：越界的边界到头、小数边界向区间内取整。
"""

import pytest
from fixtures.backends import xfail_on_backends
from fixtures.testdata import create_ref
from sqlalchemy import exc as sa_exc

from hetu.common.snowflake_id import SnowflakeID
from hetu.data.backend import Backend, RaceCondition, RowFormat, UniqueViolation
from hetu.data.backend.base import normalize_int_bounds_

SnowflakeID().init(1, 0)


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


def def_nums():
    import numpy as np

    from hetu.data import BaseComponent, Permission, define_component, property_field

    global Nums

    @define_component(namespace="pytest", permission=Permission.USER, force=True)
    class Nums(BaseComponent):
        i8: np.int8 = property_field(0, index=True)
        i16: np.int16 = property_field(0, index=True)
        u32: np.uint32 = property_field(0, index=True)
        i64: np.int64 = property_field(0, index=True)

    return Nums


# 标签: (i8, i16, u32, i64)。A 是各列的最大值，E 是有符号列的最小值（u32 的最小值在 C）
NUM_ROWS = {
    "A": (127, 32767, 2**32 - 1, 2**63 - 1),
    "B": (1, 1, 5, 1),
    "C": (0, 0, 0, 0),
    "D": (-1, -1, 7, -1),
    "E": (-128, -32768, 10, -(2**63)),
}


@pytest.fixture
async def nums(new_component_env, mod_auto_backend):
    """建 Nums 表、插入 NUM_ROWS，返回 (表引用, {行 id: 标签})"""
    backend: Backend = mod_auto_backend()
    ref = create_ref(def_nums(), backend)
    comp = ref.comp_cls
    tags: dict[int, str] = {}
    async with backend.session("pytest", 1) as session:
        for tag, (i8, i16, u32, i64) in NUM_ROWS.items():
            row = comp.new_row()
            row.i8, row.i16, row.u32, row.i64 = i8, i16, u32, i64
            await session.using(comp).insert(row)
            tags[int(row.id)] = tag
    await backend.wait_for_synced()
    return ref, tags


async def _which(backend: Backend, nums, field: str, *args, **kwargs) -> str:
    """按索引区间查 Nums，返回命中行的标签，按返回的顺序拼起来"""
    ref, tags = nums
    ids = await _ids(backend, ref, field, *args, limit=-1, **kwargs)
    return "".join(tags[i] for i in ids)


async def test_float32_point_query(item_ref, mod_auto_backend, backend_name, request):
    """
    float32 列按 0.1 点查、以 0.1 为闭区间上界：边界先按列的 dtype 取整（float32(0.1)），
    才对得上存进去的值。SQLite / MariaDB 拿 double 的 0.1 去比 float32 的 0.1
    （0.10000000149…），查不到。
    """
    xfail_on_backends(
        request,
        backend_name,
        ("mariadb",),
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
        ("postgres", "mariadb"),
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


async def test_int_index_bounds_beyond_dtype_range(nums, mod_auto_backend):
    """
    整数列的区间边界超出 dtype 的范围时当作"到头"（和 ±inf 边界一样）：上界超过最大值就是
    到最大值，下界低于最小值就是从最小值起；整个区间都在范围外就是空。边界常常是算出来的：
    满级（int8 的 127）玩家查"上下 5 级"、用大数表示"不设上限"、无符号列减出负数。
    """
    backend: Backend = mod_auto_backend()

    assert await _which(backend, nums, "i8", 127 - 5, 127 + 5) == "A"
    assert await _which(backend, nums, "i8", -300, 300) == "EDCBA"
    assert await _which(backend, nums, "i8", 0, 1000, desc=True) == "ABC"
    assert await _which(backend, nums, "i8", 200, 300) == ""
    assert await _which(backend, nums, "i8", -300, -200) == ""
    assert await _which(backend, nums, "u32", -1, 6) == "CB"
    assert await _which(backend, nums, "u32", 6, 2**40) == "DEA"


async def test_int_index_bounds_beyond_sql_column_range(
    nums, mod_auto_backend, backend_name, request
):
    """
    边界连 SQL 列类型也装不下时，一样当作"到头"。int8 / int16 列在 SQL 里建成 SMALLINT，
    asyncpg 按列类型给参数定型，超过 int16 的参数直接报错；2**64 超过 int64，PG 和 SQLite
    的驱动也都报错。
    """
    xfail_on_backends(
        request,
        backend_name,
        ("postgres",),
        raises=sa_exc.DBAPIError,
        reason="PG 的查询参数超出列类型的范围",
    )
    backend: Backend = mod_auto_backend()

    assert await _which(backend, nums, "i8", 0, 100000) == "CBA"
    assert await _which(backend, nums, "i16", -100000, 0) == "EDC"
    assert await _which(backend, nums, "i64", 0, 2**64) == "CBA"


async def test_int_index_fractional_bounds(
    nums, mod_auto_backend, backend_name, request
):
    """
    整数列的小数边界按区间的含义取整：下界向上、上界向下（x >= 0.5 即 x >= 1），区间里没有
    整数就是空。常见于按比例算出来的边界，比如匹配"等级的 0.8 ~ 1.2 倍"。SQL 后端把边界
    向 0 截断，(0.5, 1) 连 0 也查了出来。
    """
    xfail_on_backends(
        request,
        backend_name,
        ("postgres", "mariadb"),
        raises=AssertionError,
        reason="SQL 后端把整数列的小数边界向 0 截断",
    )
    backend: Backend = mod_auto_backend()

    assert await _which(backend, nums, "i8", 0.5, 1) == "B"
    assert await _which(backend, nums, "i8", 0.5, 127) == "BA"
    assert await _which(backend, nums, "i8", 0.5, 127, desc=True) == "AB"
    assert await _which(backend, nums, "i8", 1.5, 127) == "A"
    assert await _which(backend, nums, "i8", -1.5, -0.5) == "D"
    assert await _which(backend, nums, "i8", 1.2, 1.8) == ""


async def test_int_index_infinite_bounds(nums, mod_auto_backend, backend_name, request):
    """
    两端都是 +inf（或都是 -inf）的区间里没有整数，是空的。SQL 后端把 ±inf 钳成 dtype 的
    极值（闭区间），(inf, inf) 查出了值正好是最大值的行。
    """
    xfail_on_backends(
        request,
        backend_name,
        ("postgres", "mariadb"),
        raises=AssertionError,
        reason="SQL 后端把 ±inf 钳成 dtype 的极值（闭区间）",
    )
    backend: Backend = mod_auto_backend()
    inf = float("inf")

    assert await _which(backend, nums, "i8", -inf, inf) == "EDCBA"
    assert await _which(backend, nums, "i8", inf, inf) == ""
    assert await _which(backend, nums, "i8", -inf, -inf) == ""


async def test_inverted_bounds_raise(item_ref, mod_auto_backend):
    """
    下界大于上界多半是参数传反了，报 ValueError。desc 时区间照样按 (下界, 上界) 给出，
    后端自己倒过来扫。
    """
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(
        backend, comp, _item(comp, time=1, name="a"), _item(comp, time=2, name="b")
    )

    assert await _names(backend, item_ref, "a", "b", limit=-1, desc=True) == ["b", "a"]
    with pytest.raises(ValueError):
        await _names(backend, item_ref, "b", "a", limit=-1)
    with pytest.raises(ValueError):
        await _names(backend, item_ref, "b", "a", limit=-1, desc=True)
    with pytest.raises(ValueError):
        await _ids(backend, item_ref, "time", 2, 1)


async def test_open_bounds_on_same_value_is_empty(item_ref, mod_auto_backend):
    """
    两端都是开区间、值相同：区间为空，返回空结果，不是传反了。边界常常是算出来的，比如按
    时间取"上次处理之后、这一刻之前"的行 `(last, now)`，同一个 tick 里 last == now。
    """
    backend: Backend = mod_auto_backend()
    comp = item_ref.comp_cls
    await _insert_rows(backend, comp, _item(comp, time=1, name="x"))

    assert await _names(backend, item_ref, "(x", "(x", limit=-1) == []
    assert await _names(backend, item_ref, "(x", "[x", limit=-1) == []
    assert await _names(backend, item_ref, "[x", "[x", limit=-1) == ["x"]


def test_normalize_int_bounds():
    """整数区间按数学含义收成 dtype 范围内的闭区间，里面没有整数时为 None"""
    import numpy as np

    i8 = np.dtype(np.int8)
    inf = float("inf")

    def norm(lower, lower_inclusive, upper, upper_inclusive, dtype=i8):
        return normalize_int_bounds_(
            dtype, lower, lower_inclusive, upper, upper_inclusive
        )

    assert norm(5, True, 10, True) == (5, 10)
    assert norm(5, False, 10, False) == (6, 9)  # 开区间收进一格
    assert norm(0.5, True, 1.5, True) == (1, 1)  # 小数向区间内取整
    assert norm(0.5, False, 1.5, False) == (1, 1)
    assert norm(-1.5, True, -0.5, True) == (-1, -1)
    assert norm(5.0, False, 7.0, True) == (6, 7)  # 整数值的 float 同整数
    assert norm(1.2, True, 1.8, True) is None  # 区间里没有整数
    assert norm(5, False, 5, False) is None
    assert norm(5, True, 5, False) is None
    assert norm(-1000, True, 1000, True) == (-128, 127)  # 越界钳到极值
    assert norm(200, True, 300, True) is None  # 整个区间在范围外
    assert norm(-300, True, -200, True) is None
    assert norm(127, False, 1000, True) is None  # x > 127
    assert norm(-1000, True, -128, False) is None  # x < -128
    assert norm(-inf, True, inf, True) == (-128, 127)
    assert norm(inf, True, inf, True) is None
    assert norm(-inf, True, -inf, True) is None
    assert norm(-1, True, 2**40, True, np.dtype(np.uint32)) == (0, 2**32 - 1)
    assert norm(0, True, 2**64, True, np.dtype(np.int64)) == (0, 2**63 - 1)
    with pytest.raises(ValueError):
        norm(float("nan"), True, 1, True)
