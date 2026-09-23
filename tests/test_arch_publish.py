"""
设计约束守护：**commit 里的 PUBLISH 只发"有人声明要"的通知，而且每条都最小。**

PUBLISH 很贵，而且贵在最不该贵的地方（数据见 benchmark/redis_publish_cost_result.md）：
- master 上每条约 0.9~1.3 万条指令（redis.call 调度 + 强制写进复制流 + payload 搬运），
  单行 commit 本身才 6.7~8.5 万条，多一条就是 +14%~19%；master 正是整个架构的瓶颈；
- 它进复制流，每个副本都要再执行一遍，副本越多越贵；SPUBLISH 在非 cluster 下一样贵。

所以 commit 只允许发两种通知（docs/superpowers/specs/2026-09-24-publish-opt-in-design.md）：
- 表频道：只给声明了 `table_sub` 的组件，一个事务一张表一条，消息是 msgpack 的 row_id 列表；
- 值频道：只给声明了 `point_sub` 的索引，只发"进入"（insert、字段改成该值），一个事务每个
  (索引, 值) 一条，消息为空串（订阅者收到就重跑 range，不看内容）。

本文件用源码扫描守住调用点，用运行时计数守住"没声明就不发、声明了也只发这么多"、
用订阅抓包守住"消息里不夹带东西"。

新功能想要通知而这里挂了，先别改清单 / 计数，先回答：
1. 已有的 keyspace 通知（行频道、整索引频道）够不够？它由副本应用写入时自己产生，不占 master；
2. 能不能复用已经在发的消息，而不是新增一条？
3. 真要新增，先用 `benchmark/redis_commit_cost.py replay` 加一个 payload 变体量出成本，
   再改这里的 ALLOWED 并写清理由，让下一个人能复核。
"""

import re
from pathlib import Path

import msgpack
import numpy as np
import pytest
from fixtures.backends import use_redis_family_backend_only
from fixtures.testdata import create_ref

from hetu.common.snowflake_id import SnowflakeID

SnowflakeID().init(1, 0)

HETU_ROOT = Path(__file__).resolve().parent.parent / "hetu"

LUA_PUBLISH = re.compile(r"\bS?PUBLISH\b", re.IGNORECASE)
PY_PUBLISH = re.compile(
    r"\.\s*s?publish\s*\(|execute_command\(\s*[\"']S?PUBLISH", re.IGNORECASE
)

# {(相对路径, 规范化后的整行代码): 理由}。key 不含行号，挪动代码不会误报
ALLOWED_LUA: dict[tuple[str, str], str] = {
    (
        "data/backend/redis/commit_v2.lua",
        'redis_call("PUBLISH", pub[1], pub[2])',
    ): "表频道：只给声明了 table_sub 的组件，一个事务一张表一条，消息是 row_id 列表",
    (
        "data/backend/redis/commit_v2.lua",
        'redis_call("PUBLISH", ch, "")',
    ): "值频道：只给声明了 point_sub 的索引、只发新值，一个事务每个 (索引, 值) 一条，消息为空",
}


def _lua_hits() -> list[tuple[str, str, int]]:
    found = []
    for path in sorted(HETU_ROOT.rglob("*.lua")):
        rel = path.relative_to(HETU_ROOT).as_posix()
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            code = line.split("--", 1)[0]
            if LUA_PUBLISH.search(code):
                found.append((rel, " ".join(code.split()), lineno))
    return found


def test_lua_publish_call_sites_are_allowlisted():
    """Lua 里的 PUBLISH / SPUBLISH 调用点只能是清单里那两处"""
    unlisted = [
        (rel, code, lineno)
        for rel, code, lineno in _lua_hits()
        if (rel, code) not in ALLOWED_LUA
    ]
    assert not unlisted, (
        "发现未登记的 PUBLISH 调用点（详见本文件顶部说明）：\n"
        + "\n".join(f"  {rel}:{lineno}  {code}" for rel, code, lineno in unlisted)
    )


def test_lua_allowlist_has_no_stale_entries():
    """清单里的调用点都还在：代码改掉之后要顺手删掉清单，别让它越滚越大"""
    live = {(rel, code) for rel, code, _ in _lua_hits()}
    stale = sorted(set(ALLOWED_LUA) - live)
    assert not stale, f"ALLOWED_LUA 里这些调用点已经不在了，请删除或更新：{stale}"


def test_python_does_not_publish_directly():
    """Python 代码不直接发 PUBLISH：通知只能由 commit 的 Lua 按声明发"""
    hits = []
    for path in sorted(HETU_ROOT.rglob("*.py")):
        rel = path.relative_to(HETU_ROOT).as_posix()
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            code = line.split("#", 1)[0]
            if PY_PUBLISH.search(code):
                hits.append(f"  {rel}:{lineno}  {line.strip()}")
    assert not hits, (
        "Python 里直接调用了 publish（详见本文件顶部说明）：\n" + "\n".join(hits)
    )


# ============================ 运行时 ============================


@pytest.fixture(scope="module")
async def mod_publish_refs(mod_new_component_env, mod_auto_backend):
    """一个什么都没声明的组件、一个声明了 table_sub 与 owner 的 point_sub 的组件"""
    from hetu.data import BaseComponent, define_component, property_field

    @define_component(namespace="pytest", force=True)
    class PubPlain(BaseComponent):
        owner: np.int64 = property_field(0, index=True)
        tag: np.int32 = property_field(0)

    @define_component(namespace="pytest", force=True, table_sub=True)
    class PubDecl(BaseComponent):
        owner: np.int64 = property_field(0, point_sub=True)
        zone: np.int32 = property_field(0, index=True)
        tag: np.int32 = property_field(0)

    backend = mod_auto_backend()
    return create_ref(PubPlain, backend), create_ref(PubDecl, backend)


def _master_nodes(backend):
    """master 上执行 commit 脚本的节点（原生集群取所有节点：cluster 模式下 PUBLISH 走
    cluster bus 投递，不在别的节点上重新执行命令，所以不会重复计数）"""
    from redis.cluster import RedisCluster

    io = backend.master.io
    if isinstance(io, RedisCluster):
        return [io.get_redis_connection(node) for node in io.get_nodes()]
    return [io]


def _publish_calls(backend) -> int:
    """master 上执行过的 PUBLISH + SPUBLISH 次数（含 Lua 里 redis.call 的）"""
    total = 0
    for node in _master_nodes(backend):
        stats = node.info("commandstats")
        for cmd in ("cmdstat_publish", "cmdstat_spublish"):
            total += stats.get(cmd, {}).get("calls", 0)
    return total


@use_redis_family_backend_only
async def test_commit_publish_budget(mod_auto_backend, mod_publish_refs):
    """
    没声明的组件 / 索引一条不发；table_sub 一个事务一张表一条；point_sub 只发"进入"，
    一个事务每个 (索引, 值) 一条
    """
    backend = mod_auto_backend()
    plain_ref, decl_ref = mod_publish_refs
    Plain, Decl = plain_ref.comp_cls, decl_ref.comp_cls
    ids: dict[str, int] = {}

    async def published(write) -> int:
        before = _publish_calls(backend)
        async with backend.session("pytest", 1) as session:
            await write(session)
        return _publish_calls(backend) - before

    async def plain_insert(s):
        row = Plain.new_row()
        row.owner = 1
        await s.using(Plain).insert(row)
        ids["plain"] = int(row.id)

    async def plain_move(s):  # 改索引字段
        row = await s.using(Plain).get(id=ids["plain"])
        row.owner = 2
        await s.using(Plain).update(row)

    async def plain_tag(s):  # 改普通字段
        row = await s.using(Plain).get(id=ids["plain"])
        row.tag = 5
        await s.using(Plain).update(row)

    async def plain_delete(s):
        repo = s.using(Plain)
        assert await repo.get(id=ids["plain"]) is not None
        repo.delete(ids["plain"])

    for write in (plain_insert, plain_move, plain_tag, plain_delete):
        assert await published(write) == 0, (
            f"{write.__name__}：没声明的组件不该发 PUBLISH"
        )

    async def decl_insert(s):
        row = Decl.new_row()
        row.owner = 1
        row.zone = 1
        await s.using(Decl).insert(row)
        ids["decl"] = int(row.id)

    async def decl_insert_same_owner(s):  # 3 行同一个 owner
        for _ in range(3):
            row = Decl.new_row()
            row.owner = 7
            await s.using(Decl).insert(row)

    async def decl_tag(s):
        row = await s.using(Decl).get(id=ids["decl"])
        row.tag = 5
        await s.using(Decl).update(row)

    async def decl_zone(s):  # 改没声明 point_sub 的索引字段
        row = await s.using(Decl).get(id=ids["decl"])
        row.zone = 2
        await s.using(Decl).update(row)

    async def decl_move(s):  # owner 1 → 3：离开 1 不发，进入 3 发
        row = await s.using(Decl).get(id=ids["decl"])
        row.owner = 3
        await s.using(Decl).update(row)

    async def decl_delete(s):
        repo = s.using(Decl)
        assert await repo.get(id=ids["decl"]) is not None
        repo.delete(ids["decl"])

    expected = {
        decl_insert: 2,  # 表 + owner=1
        decl_insert_same_owner: 2,  # 表 + owner=7（三行合并成一条）
        decl_tag: 1,  # 表
        decl_zone: 1,  # 表
        decl_move: 2,  # 表 + owner=3
        decl_delete: 1,  # 表
    }
    for write, count in expected.items():
        assert await published(write) == count, write.__name__


@use_redis_family_backend_only
async def test_commit_publish_messages_are_minimal(mod_auto_backend, mod_publish_refs):
    """值频道的消息是空串；表频道的消息恰好是 msgpack 的 row_id 字符串列表，不夹带别的"""
    from redis.cluster import RedisCluster

    backend = mod_auto_backend()
    io = backend.master.io
    if isinstance(io, RedisCluster):
        pytest.skip("原生集群的 pubsub 要按节点订阅；调用点与条数已由其它用例覆盖")
    _, decl_ref = mod_publish_refs
    Decl = decl_ref.comp_cls
    servant = backend.servant
    table_chan = servant.table_channel(decl_ref)
    value_chan = servant.index_value_channel(decl_ref, "owner", 42)

    ps = io.pubsub()
    try:
        ps.subscribe(table_chan, value_chan)
        # 等两条 subscribe 回执：订阅生效后才提交，否则消息可能在订阅之前就发完了
        acks = 0
        for _ in range(100):
            msg = ps.get_message(timeout=0.05)
            if msg and msg["type"] == "subscribe":
                acks += 1
                if acks == 2:
                    break
        assert acks == 2, "订阅没有生效"

        new_ids = []
        async with backend.session("pytest", 1) as session:
            for _ in range(2):
                row = Decl.new_row()
                row.owner = 42
                await session.using(Decl).insert(row)
                new_ids.append(str(int(row.id)))

        got: dict[str, bytes] = {}
        for _ in range(50):
            msg = ps.get_message(timeout=0.1)
            if msg and msg["type"] == "message":
                got[msg["channel"].decode()] = msg["data"]
            if len(got) == 2:
                break
    finally:
        ps.close()

    assert got.get(value_chan) == b"", (
        f"值频道的消息必须是空串：{got.get(value_chan)!r}"
    )
    ids = msgpack.unpackb(got[table_chan])
    assert isinstance(ids, list) and all(isinstance(i, str) for i in ids), ids
    assert sorted(ids) == sorted(new_ids)
