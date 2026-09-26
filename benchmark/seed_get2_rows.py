"""Seed both tables and all indexes for benchmark_get2_update2.

完整填充两张表和索引，让随机 upsert 测量稳定的数据更新，而不是逐渐增长的数据集。
Only use against an isolated benchmark Redis after the server starts.
"""

import itertools
import string
import sys
from pathlib import Path


def seed_get2_rows(root: Path, redis_url: str) -> dict[str, int]:
    """Populate the full key space with valid rows and lexicographic indexes.

    使用服务端实际簇号及索引编码，填充 30,000 个整数和全部 36³ 个字符串。
    """
    import redis
    from redis.typing import EncodableT, FieldT

    sys.path.insert(0, str(root))
    from benchmark.server.app import IntTable, StrTable
    from benchmark.ya_hetu_rpc import BENCH_ID_RANGE
    from hetu.data.backend.base import to_sortable_bytes
    from hetu.system.definer import SystemClusters

    clusters = SystemClusters()
    clusters.build_clusters("bench")
    names = (
        "".join(chars)
        for chars in itertools.product(string.ascii_uppercase + string.digits, repeat=3)
    )
    seeded = {}
    with redis.Redis.from_url(redis_url) as db:
        for comp, values in (
            (
                IntTable,
                ({"number": i, "name": "Test"} for i in range(1, BENCH_ID_RANGE + 1)),
            ),
            (StrTable, ({"name": name, "number": 0} for name in names)),
        ):
            cluster_id = clusters.get_component_cluster_id("bench", comp)
            assert cluster_id is not None
            prefix = f"bench:{comp.name_}:{{CLU{cluster_id}}}"
            with db.pipeline(transaction=False) as pipe:
                count = 0
                for count, fields in enumerate(values, 1):
                    row: dict[FieldT, EncodableT] = {
                        "_version": 1,
                        "id": count,
                        **fields,
                    }
                    pipe.hset(f"{prefix}:id:{count}", mapping=row)
                    for field in comp.indexes_:
                        member = to_sortable_bytes(
                            comp.dtype_map_[field].type(row[field])
                        )
                        member += b"\x00" + str(count).encode("ascii")
                        pipe.zadd(f"{prefix}:index:{field}", {member: 0})
                    if count % 1000 == 0:
                        pipe.execute()
                pipe.execute()
            for field in comp.indexes_:
                assert db.zcard(f"{prefix}:index:{field}") == count
            assert db.hgetall(f"{prefix}:id:1")
            assert db.hgetall(f"{prefix}:id:{count}")
            seeded[prefix] = count
    return seeded


def verify_get2_rows(redis_url: str, seeded: dict[str, int]) -> dict:
    """Check that the run updated the seeded tables without growing them.

    压测后核对行数与索引基数，并抽样确认版本增加；此检查不计入计时窗口。
    """
    import redis

    result = {}
    with redis.Redis.from_url(redis_url) as db:
        for prefix, expected in seeded.items():
            count = sum(1 for _ in db.scan_iter(match=f"{prefix}:id:*", count=1000))
            assert count == expected, (prefix, count, expected)
            field = "number" if ":IntTable:" in prefix else "name"
            for index in ("id", field):
                assert db.zcard(f"{prefix}:index:{index}") == expected
            with db.pipeline(transaction=False) as pipe:
                for row_id in range(1, expected + 1, max(1, expected // 64)):
                    pipe.hget(f"{prefix}:id:{row_id}", "_version")
                versions = [int(v) for v in pipe.execute()]
            assert max(versions) > 1, (prefix, "no sampled updates")
            result[prefix] = {"rows": count, "sample_max_version": max(versions)}
    return result
