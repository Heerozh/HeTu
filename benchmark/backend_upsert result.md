# HeTu Backend Upsert Benchmark Results

结论：新结构读取部分可以降低30%左右的负载，但是lua部分性能开销还是大，效率反而下降了

## 运行命令

```
export REDIS_HOST=127.0.0.1
export REDIS_PASSWORD=
uv run ya ./benchmark/ya_backend_upsert.py
```

Found 1 benchmark(s): benchmark_redis_upsert
Running with 32 workers, 6 tasks per worker, for 5 minute(s)

## 本机 9950x3D redis:latest Windows Docker 默认设置

Calls Per Minute (CPM) Statistics:

| benchmark              | 00:01:00 | 00:02:00 | 00:03:00 | 00:04:00 | 00:05:00 | 00:06:00 |
|:-----------------------|:---------|:---------|:---------|:---------|:---------|:---------|
| benchmark_redis_upsert | 472,651  | 701,571  | 722,767  | 725,927  | 735,374  | 189,512  |

Average CPS (Calls Per Second) per Function:

|                        | CPS       |
|:-----------------------|:----------|
| benchmark_redis_upsert | 11,997.34 |

Function Execution Time Statistics:

|                        |  Mean |   k50 |   k90 |   k99 | Count     |  Min |     Max | Median |
|:-----------------------|------:|------:|------:|------:|:----------|-----:|--------:|-------:|
| benchmark_redis_upsert | 16.23 | 15.22 | 22.33 | 34.64 | 3,547,802 | 1.19 | 1331.16 |  15.22 |

Return Value Distribution Statistics:

|   | benchmark              | return_value |   count | percentage |
|--:|:-----------------------|-------------:|--------:|-----------:|
| 0 | benchmark_redis_upsert |            1 | 3547802 |        100 |

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

## 服务器本地 ecs.c8a.16xlarge debian13 Redis-8.4.0 默认设置➕io-thread=8

CPU 50%

Running with 128 workers, 1 tasks per worker, for 5 minute(s)

Calls Per Minute (CPM) Statistics:

| benchmark              | 00:01:00   | 00:02:00   | 00:03:00   | 00:04:00   | 00:05:00   | 00:06:00   |
|:-----------------------|:-----------|:-----------|:-----------|:-----------|:-----------|:-----------|
| benchmark_redis_upsert | 1,334,646  | 2,618,999  | 2,624,694  | 2,634,956  | 2,614,081  | 849,144    |

Average CPS (Calls Per Second) per Function:

|                        | CPS       |
|:-----------------------|:----------|
| benchmark_redis_upsert | 43,730.67 |

Function Execution Time Statistics:

|                        |   Mean |   k50 |   k90 |   k99 | Count      |   Min |    Max |   Median |
|:-----------------------|-------:|------:|------:|------:|:-----------|------:|-------:|---------:|
| benchmark_redis_upsert |   3.03 |   2.9 |  4.02 |  6.62 | 12,676,520 |  0.42 | 362.83 |      2.9 |

Return Value Distribution Statistics:

|    | benchmark              |   return_value |    count |   percentage |
|---:|:-----------------------|---------------:|---------:|-------------:|
|  0 | benchmark_redis_upsert |              1 | 12401941 |        97.83 |
| 54 | benchmark_redis_upsert |              2 |   143939 |         1.14 |
| 65 | benchmark_redis_upsert |              3 |    50865 |         0.4  |
| 76 | benchmark_redis_upsert |              4 |    26090 |         0.21 |
| 87 | benchmark_redis_upsert |              5 |    14874 |         0.12 |


## 阿里云 redis.shard.small.2.ce 7.0.2.6 云原生 单节点 读写分离未开启 默认设置

Calls Per Minute (CPM) Statistics:

| benchmark              | 00:01:00   | 00:02:00   | 00:03:00   | 00:04:00   | 00:05:00   | 00:06:00   |
|:-----------------------|:-----------|:-----------|:-----------|:-----------|:-----------|:-----------|
| benchmark_redis_upsert | 640,610    | 1,580,831  | 1,562,021  | 1,562,169  | 1,582,375  | 846,201    |

Average CPS (Calls Per Second) per Function:

|                        | CPS       |
|:-----------------------|:----------|
| benchmark_redis_upsert | 26,185.09 |

Function Execution Time Statistics:

|                        |   Mean |   k50 |   k90 |   k99 | Count     |   Min |    Max |   Median |
|:-----------------------|-------:|------:|------:|------:|:----------|------:|-------:|---------:|
| benchmark_redis_upsert |   7.41 |   7.2 |  9.64 | 12.72 | 7,774,207 |  0.75 | 357.66 |      7.2 |

Return Value Distribution Statistics:

|    | benchmark              |   return_value |   count |   percentage |
|---:|:-----------------------|---------------:|--------:|-------------:|
|  0 | benchmark_redis_upsert |              1 | 7702541 |        99.08 |
| 11 | benchmark_redis_upsert |              2 |   46251 |         0.59 |
| 22 | benchmark_redis_upsert |              3 |   10671 |         0.14 |
| 33 | benchmark_redis_upsert |              4 |    5119 |         0.07 |
| 42 | benchmark_redis_upsert |              5 |    2961 |         0.04 |

## 阿里云 tair.rdb.with.proxy.1g Tair 内存型 7.0(25.11.0.0) 云原生 3读写分离 默认设置

此项压测机ecs.c8a.4xlarge 32核 CPU跑满了，master CPU 28%，2个只读各6%, 未测出最佳性能

Calls Per Minute (CPM) Statistics:

| benchmark              | 00:01:00   | 00:02:00   | 00:03:00   | 00:04:00   | 00:05:00   | 00:06:00   |
|:-----------------------|:-----------|:-----------|:-----------|:-----------|:-----------|:-----------|
| benchmark_redis_upsert | 760,650    | 1,735,729  | 1,735,225  | 1,735,164  | 1,736,298  | 886,241    |

Average CPS (Calls Per Second) per Function:

|                        | CPS       |
|:-----------------------|:----------|
| benchmark_redis_upsert | 28,926.55 |

Function Execution Time Statistics:

|                        |   Mean |   k50 |   k90 |   k99 | Count     |   Min |    Max |   Median |
|:-----------------------|-------:|------:|------:|------:|:----------|------:|-------:|---------:|
| benchmark_redis_upsert |    6.7 |  7.14 | 11.41 | 16.05 | 8,589,307 |  1.07 | 321.18 |     7.14 |

Return Value Distribution Statistics:

|    | benchmark              |   return_value |   count |   percentage |
|---:|:-----------------------|---------------:|--------:|-------------:|
|  0 | benchmark_redis_upsert |              1 | 8509092 |        99.07 |
| 11 | benchmark_redis_upsert |              2 |   52618 |         0.61 |
| 22 | benchmark_redis_upsert |              3 |   11791 |         0.14 |
| 33 | benchmark_redis_upsert |              4 |    5611 |         0.07 |
| 42 | benchmark_redis_upsert |              5 |    3155 |         0.04 |

此项压测机ecs.c8a.16xlarge 64核 CPU 36%，master CPU 33%，2个只读各9%
Running with 128 workers, 1 tasks per worker, for 5 minute(s)

Calls Per Minute (CPM) Statistics:

| benchmark              | 00:01:00   | 00:02:00   | 00:03:00   | 00:04:00   | 00:05:00   | 00:06:00   |
|:-----------------------|:-----------|:-----------|:-----------|:-----------|:-----------|:-----------|
| benchmark_redis_upsert | 790,878    | 2,415,764  | 2,415,706  | 2,415,117  | 2,414,766  | 1,146,030  |

Average CPS (Calls Per Second) per Function:

|                        | CPS       |
|:-----------------------|:----------|
| benchmark_redis_upsert | 40,261.86 |

Function Execution Time Statistics:

|                        |   Mean |   k50 |   k90 |   k99 | Count      |   Min |    Max |   Median |
|:-----------------------|-------:|------:|------:|------:|:-----------|------:|-------:|---------:|
| benchmark_redis_upsert |   3.31 |  2.86 |  4.81 |  7.92 | 11,598,261 |   0.7 | 519.42 |     2.86 |

Return Value Distribution Statistics:

|    | benchmark              |   return_value |    count |   percentage |
|---:|:-----------------------|---------------:|---------:|-------------:|
|  0 | benchmark_redis_upsert |              1 | 11323332 |        97.63 |
| 55 | benchmark_redis_upsert |              2 |   143549 |         1.24 |
| 71 | benchmark_redis_upsert |              3 |    49521 |         0.43 |
| 82 | benchmark_redis_upsert |              4 |    25700 |         0.22 |
| 93 | benchmark_redis_upsert |              5 |    15106 |         0.13 |
