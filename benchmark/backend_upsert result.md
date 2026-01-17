# HeTu Backend Upsert Benchmark Results

## 运行命令

```
export REDIS_HOST=127.0.0.1
export REDIS_PASSWORD=
uv run ya ./benchmark/ya_backend_upsert.py -n 1200 -t 1.1
```

Found 1 benchmark(s): benchmark_redis_upsert
Running with 64 workers, 9 tasks per worker, for 5 minute(s)

## 本机 9950x3D redis:8.0 Windows Docker 默认设置

Calls Per Minute (CPM) Statistics:

| benchmark              | 00:01:00 | 00:02:00  | 00:03:00  | 00:04:00  | 00:05:00  | 00:06:00 |
|:-----------------------|:---------|:----------|:----------|:----------|:----------|:---------|
| benchmark_redis_upsert | 998,232  | 1,103,828 | 1,087,547 | 1,117,409 | 1,125,254 | 91,951   |

Average CPS (Calls Per Second) per Function:

|                        | CPS       |
|:-----------------------|:----------|
| benchmark_redis_upsert | 18,607.72 |

Function Execution Time Statistics:

|                        |  Mean |   k50 |   k90 |   k99 | Count     |  Min |    Max | Median |
|:-----------------------|------:|------:|------:|------:|:----------|-----:|-------:|-------:|
| benchmark_redis_upsert | 31.28 | 30.03 | 39.99 | 66.55 | 5,524,221 | 2.33 | 374.02 |  30.03 |

Return Value Distribution Statistics:

|   | benchmark              | return_value |   count | percentage |
|--:|:-----------------------|-------------:|--------:|-----------:|
| 0 | benchmark_redis_upsert |            1 | 5400129 |      97.75 |
| 2 | benchmark_redis_upsert |            2 |  104161 |       1.89 |
| 3 | benchmark_redis_upsert |            3 |   16532 |        0.3 |
| 4 | benchmark_redis_upsert |            4 |    2777 |       0.05 |
| 5 | benchmark_redis_upsert |            5 |     498 |       0.01 |

================================================================================

## 本机 9950x3D Redis-8.4.0-Windows-x64-msys2 默认配置➕io-thread=8

Calls Per Minute (CPM) Statistics:

| benchmark              | 00:01:00 | 00:02:00 | 00:03:00 | 00:04:00 | 00:05:00 | 00:06:00 |
|:-----------------------|:---------|:---------|:---------|:---------|:---------|:---------|
| benchmark_redis_upsert | 175,081  | 642,880  | 634,991  | 617,503  | 623,661  | 396,987  |

Average CPS (Calls Per Second) per Function:

|                        | CPS       |
|:-----------------------|:----------|
| benchmark_redis_upsert | 10,442.72 |

Function Execution Time Statistics:

|                        |  Mean |   k50 |   k90 |   k99 | Count     |  Min |    Max | Median |
|:-----------------------|------:|------:|------:|------:|:----------|-----:|-------:|-------:|
| benchmark_redis_upsert | 18.63 | 18.35 | 20.23 | 33.34 | 3,091,103 | 1.23 | 1098.2 |  18.35 |

Return Value Distribution Statistics:

|   | benchmark              | return_value |   count | percentage |
|--:|:-----------------------|-------------:|--------:|-----------:|
| 0 | benchmark_redis_upsert |            1 | 3091103 |        100 |

## 本机 9950x3D valkey:9.0.1 Windows Docker 默认设置

Calls Per Minute (CPM) Statistics:

| benchmark              | 00:01:00 | 00:02:00 | 00:03:00 | 00:04:00 | 00:05:00 | 00:06:00 |
|:-----------------------|:---------|:---------|:---------|:---------|:---------|:---------|
| benchmark_redis_upsert | 53,866   | 764,413  | 765,401  | 767,110  | 797,344  | 681,445  |

Average CPS (Calls Per Second) per Function:

|                        | CPS       |
|:-----------------------|:----------|
| benchmark_redis_upsert | 12,997.61 |

Function Execution Time Statistics:

|                        |  Mean |   k50 |   k90 |   k99 | Count     |  Min |     Max | Median |
|:-----------------------|------:|------:|------:|------:|:----------|-----:|--------:|-------:|
| benchmark_redis_upsert | 15.04 | 14.13 | 20.25 | 31.89 | 3,829,579 | 1.18 | 1301.95 |  14.13 |

Return Value Distribution Statistics:

|    | benchmark              | return_value |   count | percentage |
|---:|:-----------------------|-------------:|--------:|-----------:|
|  0 | benchmark_redis_upsert |            1 | 3768222 |       98.4 |
| 11 | benchmark_redis_upsert |            2 |   37455 |       0.98 |
| 22 | benchmark_redis_upsert |            3 |   10476 |       0.27 |
| 32 | benchmark_redis_upsert |            4 |    4941 |       0.13 |
| 37 | benchmark_redis_upsert |            5 |    2766 |       0.07 |

## debian13 ecs.c9ae.16xlarge 64核 + 本地 Redis-8.4.0 默认设置

Average CPS (Calls Per Second) per Function:

|                        | CPS        |
|:-----------------------|:-----------|
| benchmark_ping         | 240,301.77 |
| benchmark_redis_upsert | 30,705.70  |

Function Execution Time Statistics:

|                        |  Mean |   k50 |   k90 |   k99 | Count      |  Min |    Max | Median |
|:-----------------------|------:|------:|------:|------:|:-----------|-----:|-------:|-------:|
| benchmark_ping         |  4.79 |  4.74 |     5 |  8.85 | 15,864,114 | 0.09 |  74.96 |   4.74 |
| benchmark_redis_upsert | 36.72 | 36.47 | 38.86 | 73.98 | 2,070,622  | 1.21 | 334.39 |  36.47 |

Return Value Distribution Statistics:

|   | benchmark              | return_value |    count | percentage |
|--:|:-----------------------|-------------:|---------:|-----------:|
| 0 | benchmark_ping         |          nan | 15864114 |        100 |
| 1 | benchmark_redis_upsert |            1 |  2044339 |      98.73 |
| 2 | benchmark_redis_upsert |            2 |    25959 |       1.25 |
| 3 | benchmark_redis_upsert |            3 |      321 |       0.02 |
| 4 | benchmark_redis_upsert |            4 |        3 |          0 |

## debian13 ecs.c9ae.16xlarge 64核 + 阿里云Arm倚天Redis: redis.shard.small.y.ee 7.0(26.1.0.0) 云原生 双节点高可用 默认设置

Average CPS (Calls Per Second) per Function:

|                        | CPS        |
|:-----------------------|:-----------|
| benchmark_ping         | 220,716.70 |
| benchmark_redis_upsert | 17,614.16  |

Function Execution Time Statistics:

|                        | Mean |   k50 |   k90 |    k99 | Count      |  Min |    Max | Median |
|:-----------------------|-----:|------:|------:|-------:|:-----------|-----:|-------:|-------:|
| benchmark_ping         | 5.24 |  5.18 |  5.45 |   5.85 | 14,522,070 | 0.15 |  132.8 |   5.18 |
| benchmark_redis_upsert | 66.5 | 63.91 | 75.66 | 126.57 | 1,143,577  | 1.62 | 674.09 |  63.91 |

Return Value Distribution Statistics:

|   | benchmark              | return_value |    count | percentage |
|--:|:-----------------------|-------------:|---------:|-----------:|
| 0 | benchmark_ping         |          nan | 14522070 |        100 |
| 1 | benchmark_redis_upsert |            1 |  1129100 |      98.73 |
| 2 | benchmark_redis_upsert |            2 |    14276 |       1.25 |
| 3 | benchmark_redis_upsert |            3 |      200 |       0.02 |
| 4 | benchmark_redis_upsert |            4 |        1 |          0 |

## debian13 ecs.c9ae.16xlarge 64核 + 阿里云最低配Redis: redis.shard.small.2.ce 7.0.2.6 云原生 单节点 默认设置

Average CPS (Calls Per Second) per Function:

|                        | CPS        |
|:-----------------------|:-----------|
| benchmark_ping         | 211,169.26 |
| benchmark_redis_upsert | 36,752.57  |

Function Execution Time Statistics:

|                        |  Mean |   k50 |  k90 |   k99 | Count      |  Min |    Max | Median |
|:-----------------------|------:|------:|-----:|------:|:-----------|-----:|-------:|-------:|
| benchmark_ping         |  5.45 |  5.25 | 5.38 | 10.58 | 13,952,760 | 0.13 |  55.49 |   5.25 |
| benchmark_redis_upsert | 31.32 | 28.23 | 38.4 | 56.41 | 2,428,027  | 1.51 | 236.81 |  28.23 |

Return Value Distribution Statistics:

|   | benchmark              | return_value |    count | percentage |
|--:|:-----------------------|-------------:|---------:|-----------:|
| 0 | benchmark_ping         |          nan | 13952760 |        100 |
| 1 | benchmark_redis_upsert |            1 |  2395379 |      98.66 |
| 2 | benchmark_redis_upsert |            2 |    32242 |       1.33 |
| 3 | benchmark_redis_upsert |            3 |      400 |       0.02 |
| 4 | benchmark_redis_upsert |            4 |        6 |          0 |

## debian13 ecs.c9ae.16xlarge 64核 + 阿里云最低配Redis读写分离: redis.shard.with.proxy.small.ce 7.0.2.6 云原生 4节点 读写分离代理 默认设置

Average CPS (Calls Per Second) per Function:

|                        | CPS          |
|:-----------------------|:-------------|
| benchmark_ping         | 2,105,264.67 |
| benchmark_redis_upsert | 99,913.67    |

Function Execution Time Statistics:

|                        |  Mean |  k50 |  k90 |  k99 | Count      |  Min |   Max | Median |
|:-----------------------|------:|-----:|-----:|-----:|:-----------|-----:|------:|-------:|
| benchmark_ping         |  0.54 | 0.27 | 0.34 | 5.09 | 25,367,085 | 0.11 |  31.7 |   0.27 |
| benchmark_redis_upsert | 11.43 | 9.39 | 17.6 | 26.3 | 1,210,091  | 1.14 | 62.07 |   9.39 |

Return Value Distribution Statistics:

|   | benchmark              | return_value |    count | percentage |
|--:|:-----------------------|-------------:|---------:|-----------:|
| 0 | benchmark_ping         |          nan | 25367085 |        100 |
| 1 | benchmark_redis_upsert |            1 |  1185387 |      97.96 |
| 2 | benchmark_redis_upsert |            2 |    24136 |       1.99 |
| 3 | benchmark_redis_upsert |            3 |      560 |       0.05 |
| 4 | benchmark_redis_upsert |            4 |        8 |          0 |

## debian13 ecs.c9ae.16xlarge 64核 + 阿里云 tair.rdb.with.proxy.1g Tair 内存型 7.0(25.11.0.0) 云原生 3读写分离 默认设置

Average CPS (Calls Per Second) per Function:

|                        | CPS          |
|:-----------------------|:-------------|
| benchmark_ping         | 2,107,972.95 |
| benchmark_redis_upsert | 87,688.87    |

Function Execution Time Statistics:

|                        |  Mean |   k50 |   k90 |   k99 | Count       |  Min |    Max | Median |
|:-----------------------|------:|------:|------:|------:|:------------|-----:|-------:|-------:|
| benchmark_ping         |  0.55 |  0.27 |  0.35 |   5.3 | 139,188,783 | 0.11 | 213.52 |   0.27 |
| benchmark_redis_upsert | 13.15 | 11.34 | 20.95 | 30.91 | 5,783,948   | 2.39 |   93.1 |  11.34 |

Return Value Distribution Statistics:

|   | benchmark              | return_value |     count | percentage |
|--:|:-----------------------|-------------:|----------:|-----------:|
| 0 | benchmark_ping         |          nan | 139188783 |        100 |
| 1 | benchmark_redis_upsert |            1 |   5667252 |      97.98 |
| 2 | benchmark_redis_upsert |            2 |    114388 |       1.98 |
| 3 | benchmark_redis_upsert |            3 |      2259 |       0.04 |
| 4 | benchmark_redis_upsert |            4 |        48 |          0 |
| 5 | benchmark_redis_upsert |            5 |         1 |          0 |

