# HeTu RPC Benchmark Results

结论：使用batch后200%提升

## 运行命令

```
uv run ya ya_hetu_rpc.py -n 200 -t 5
```

Found 4 benchmark(s): benchmark_get, benchmark_get2_update2, benchmark_get_then_update,
benchmark_hello_world
Running with 64 workers, 3 tasks per worker, for 5.0 minute(s)

Running benchmark: benchmark_ge
Using multiprocessing with 64 workers

## Windows 开发机 9950x3D 32核 + redis:8.0 Windows Docker 默认设置

|                           | CPS        |
|:--------------------------|:-----------|
| benchmark_get             | 63,378.76  |
| benchmark_get2_update2    | 11,769.40  |
| benchmark_get_then_update | 16,587.53  |
| benchmark_hello_world     | 140,217.49 |

Function Execution Time Statistics:

|                           |  Mean |   k50 |   k90 |   k99 | Count     |  Min |    Max | Median |
|:--------------------------|------:|------:|------:|------:|:----------|-----:|-------:|-------:|
| benchmark_get             |  9.03 |  8.35 | 12.42 | 25.38 | 3,825,994 | 0.67 | 533.36 |   8.35 |
| benchmark_get2_update2    | 48.94 | 47.21 | 57.49 | 84.12 | 706,339   | 4.04 | 534.17 |  47.21 |
| benchmark_get_then_update | 34.51 | 33.48 | 42.31 |  54.8 | 1,001,488 | 2.46 |  466.7 |  33.48 |
| benchmark_hello_world     |  4.09 |  1.77 |  3.24 | 76.75 | 8,448,389 | 0.14 | 1270.4 |   1.77 |

Return Value Distribution Statistics:

|   | benchmark                 | return_value |   count | percentage |
|--:|:--------------------------|:-------------|--------:|-----------:|
| 0 | benchmark_get             | 0            | 3825994 |        100 |
| 1 | benchmark_get2_update2    | 0            |  699697 |      99.06 |
| 2 | benchmark_get2_update2    | 1            |    6572 |       0.93 |
| 3 | benchmark_get2_update2    | 2            |      69 |       0.01 |
| 4 | benchmark_get2_update2    | 3            |       1 |          0 |
| 5 | benchmark_get_then_update | 0            |  996974 |      99.55 |
| 6 | benchmark_get_then_update | 1            |    4493 |       0.45 |
| 7 | benchmark_get_then_update | 2            |      21 |          0 |
| 8 | benchmark_hello_world     | 世界收到         | 8448389 |        100 |

================================================================================

## debian13 ecs.c8a.16xlarge 64核 + 本地 Redis-8.4.0 默认设置➕io-thread=8

## debian13 ecs.c8a.16xlarge 64核 + 阿里云最低配Redis: redis.shard.small.2.ce 7.0.2.6 云原生 单节点 读写分离未开启 默认设置

## debian13 ecs.c8a.16xlarge 64核 + 阿里云Tair读写分离：tair.rdb.with.proxy.1g Tair 内存型 7.0(25.11.0.0) 云原生 3读写分离 默认设置

此项压测机ecs.c8a.16xlarge 64核 CPU 36%，master CPU 33%，2个只读各9%
Running with 128 workers, 1 tasks per worker, for 5 minute(s)

Calls Per Minute (CPM) Statistics:

| benchmark              | 00:01:00 | 00:02:00  | 00:03:00  | 00:04:00  | 00:05:00  | 00:06:00  |
|:-----------------------|:---------|:----------|:----------|:----------|:----------|:----------|
| benchmark_redis_upsert | 790,878  | 2,415,764 | 2,415,706 | 2,415,117 | 2,414,766 | 1,146,030 |

Average CPS (Calls Per Second) per Function:

|                        | CPS       |
|:-----------------------|:----------|
| benchmark_redis_upsert | 40,261.86 |

Function Execution Time Statistics:

|                        | Mean |  k50 |  k90 |  k99 | Count      | Min |    Max | Median |
|:-----------------------|-----:|-----:|-----:|-----:|:-----------|----:|-------:|-------:|
| benchmark_redis_upsert | 3.31 | 2.86 | 4.81 | 7.92 | 11,598,261 | 0.7 | 519.42 |   2.86 |

Return Value Distribution Statistics:

|    | benchmark              | return_value |    count | percentage |
|---:|:-----------------------|-------------:|---------:|-----------:|
|  0 | benchmark_redis_upsert |            1 | 11323332 |      97.63 |
| 55 | benchmark_redis_upsert |            2 |   143549 |       1.24 |
| 71 | benchmark_redis_upsert |            3 |    49521 |       0.43 |
| 82 | benchmark_redis_upsert |            4 |    25700 |       0.22 |
| 93 | benchmark_redis_upsert |            5 |    15106 |       0.13 |
