"""Seed the IntTable rows that benchmark_get reads.

benchmark_get 调 just_get 按 id 读 IntTable 的 1..BENCH_ID_RANGE 行，读空直接报错。
服务端启动后运行一次，把这些行写进服务端实际使用的簇；rpc_throughput.py 的 get
负载会自动调用。Run with --help.
"""

import argparse
import sys
from pathlib import Path


def seed_get_rows(root: Path, redis_url: str) -> None:
    """Write IntTable rows 1..BENCH_ID_RANGE into the cluster the server uses.

    簇号按 root 下的服务端代码计算。只写主键读取需要的行 hash，不建 number 的索引。
    """
    import redis

    sys.path.insert(0, str(root))
    from benchmark.server.app import IntTable
    from benchmark.ya_hetu_rpc import BENCH_ID_RANGE
    from hetu.system.definer import SystemClusters

    clusters = SystemClusters()
    clusters.build_clusters("bench")
    cluster_id = clusters.get_component_cluster_id("bench", IntTable)
    assert cluster_id is not None
    key_prefix = f"bench:IntTable:{{CLU{cluster_id}}}:id:"
    db = redis.Redis.from_url(redis_url)
    with db.pipeline(transaction=False) as pipe:
        for row_id in range(1, BENCH_ID_RANGE + 1):
            pipe.hset(
                key_prefix + str(row_id),
                mapping={"_version": 1, "id": row_id, "name": "Test", "number": row_id},
            )
            if row_id % 1000 == 0:
                pipe.execute()
        pipe.execute()
    assert db.hgetall(key_prefix + "1")
    assert db.hgetall(key_prefix + str(BENCH_ID_RANGE))
    db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redis", required=True, help="the server's Redis master")
    parser.add_argument(
        "--server-root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    args = parser.parse_args()
    seed_get_rows(args.server_root.resolve(), args.redis)


if __name__ == "__main__":
    main()
