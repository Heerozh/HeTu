# HeTu RPC Benchmark Results

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
| benchmark_get             | 29,434.73  |
| benchmark_get2_update2    | 6,204.35   |
| benchmark_get_then_update | 9,387.89   |
| benchmark_hello_world     | 130,359.08 |

Function Execution Time Statistics:

|                           |  Mean |   k50 |   k90 |   k99 | Count     |  Min |    Max | Median |
|:--------------------------|------:|------:|------:|------:|:----------|-----:|-------:|-------:|
| benchmark_get             |  6.53 |  5.44 | 11.02 | 24.62 | 1,764,621 | 0.61 | 174.83 |   5.44 |
| benchmark_get2_update2    | 30.82 | 29.21 | 41.63 | 60.86 | 373,829   | 2.84 | 400.88 |  29.21 |
| benchmark_get_then_update | 20.35 | 19.16 | 29.37 | 43.03 | 566,188   |  1.9 | 271.78 |  19.16 |
| benchmark_hello_world     |  1.46 |  0.63 |  0.96 | 11.43 | 7,878,632 |  0.1 |  735.2 |   0.63 |

Return Value Distribution Statistics:

|   | benchmark                 | return_value |   count | percentage |
|--:|:--------------------------|:-------------|--------:|-----------:|
| 0 | benchmark_get             | 0            | 1764621 |        100 |
| 1 | benchmark_get2_update2    | 0            |  372430 |      99.63 |
| 2 | benchmark_get2_update2    | 1            |    1394 |       0.37 |
| 3 | benchmark_get2_update2    | 2            |       5 |          0 |
| 4 | benchmark_get_then_update | 0            |  565041 |       99.8 |
| 5 | benchmark_get_then_update | 1            |    1146 |        0.2 |
| 6 | benchmark_get_then_update | 2            |       1 |          0 |
| 7 | benchmark_hello_world     | 世界收到         | 7878632 |        100 |

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
