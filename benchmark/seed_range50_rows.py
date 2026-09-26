"""Seed and verify 30,000 IntTable rows for the range50_update2 RPC.

仅对隔离压测 Redis 使用。表和索引在每轮运行前重新填充；计时结束后全量核对。
"""

import argparse
import sys
from pathlib import Path


def seed_range50_rows(root: Path, redis_url: str) -> dict[str, int]:
    """Populate all row hashes and indexes in the server's transaction cluster.

    填充 1..30,000 的 number 及对应索引；从服务器定义取得实际簇号。
    """
    import redis
    from redis.typing import EncodableT, FieldT

    sys.path.insert(0, str(root))
    from benchmark.server.app import IntTable
    from benchmark.ya_hetu_rpc import BENCH_ID_RANGE
    from hetu.data.backend.base import to_sortable_bytes
    from hetu.system.definer import SystemClusters

    clusters = SystemClusters()
    clusters.build_clusters("bench")
    cluster_id = clusters.get_component_cluster_id("bench", IntTable)
    assert cluster_id is not None and "number" in IntTable.indexes_
    prefix = f"bench:IntTable:{{CLU{cluster_id}}}"
    with redis.Redis.from_url(redis_url) as db, db.pipeline(transaction=False) as pipe:
        keys = list(db.scan_iter(match=f"{prefix}:*"))
        for start in range(0, len(keys), 1000):
            db.delete(*keys[start : start + 1000])
        for row_id in range(1, BENCH_ID_RANGE + 1):
            row: dict[FieldT, EncodableT] = {
                "id": row_id,
                "_version": 1,
                "number": row_id,
                "name": f"row{row_id}",
            }
            pipe.hset(f"{prefix}:id:{row_id}", mapping=row)
            for field in IntTable.indexes_:
                member = to_sortable_bytes(IntTable.dtype_map_[field].type(row[field]))
                pipe.zadd(
                    f"{prefix}:index:{field}",
                    {member + b"\x00" + str(row_id).encode("ascii"): 0},
                )
            if row_id % 1000 == 0:
                pipe.execute()
        pipe.execute()
        for field in IntTable.indexes_:
            assert db.zcard(f"{prefix}:index:{field}") == BENCH_ID_RANGE
    return {prefix: BENCH_ID_RANGE}


def verify_range50_rows(redis_url: str, seeded: dict[str, int]) -> dict:
    """Verify every row, index member and successful update's version increment.

    核对主键和 number 索引、30,000 行、每行版本递增；runner 另核对增量等于成功请求数×2。
    """
    import redis

    from benchmark.server.app import IntTable
    from hetu.data.backend.base import to_sortable_bytes

    result = {}
    with redis.Redis.from_url(redis_url) as db:
        for prefix, count in seeded.items():
            assert sum(1 for _ in db.scan_iter(match=f"{prefix}:id:*")) == count
            indexes = {field: set() for field in IntTable.indexes_}
            version_increments = 0
            for start in range(1, count + 1, 1000):
                with db.pipeline(transaction=False) as pipe:
                    for row_id in range(start, min(start + 1000, count + 1)):
                        pipe.hgetall(f"{prefix}:id:{row_id}")
                    rows = pipe.execute()
                for offset, row in enumerate(rows, start):
                    row_id = int(row[b"id"])
                    assert row_id == offset and int(row[b"number"]) == row_id
                    version_increments += int(row[b"_version"]) - 1
                    for field, members in indexes.items():
                        value = IntTable.dtype_map_[field].type(
                            int(row[field.encode()])
                        )
                        members.add(to_sortable_bytes(value) + b"\x00" + row[b"id"])
            assert version_increments > 0 and version_increments % 2 == 0
            for field, expected in indexes.items():
                actual = set(db.zrange(f"{prefix}:index:{field}", 0, -1))
                assert actual == expected, field
            result[prefix] = {
                "rows": count,
                "version_increments": version_increments,
                "all_indexes_verified": True,
            }
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redis", required=True)
    parser.add_argument(
        "--server-root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    args = parser.parse_args()
    seed_range50_rows(args.server_root.resolve(), args.redis)
