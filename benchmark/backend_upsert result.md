# HeTu Backend Upsert Benchmark Results

## 运行命令

```
export REDIS_HOST=127.0.0.1
export REDIS_PASSWORD=
uv run ya ./benchmark/ya_backend_upsert.py -n 600
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

## 服务器本地 ecs.c8a.16xlarge debian13 Redis-8.4.0 默认设置➕io-thread=8

CPU 50%

Running with 128 workers, 1 tasks per worker, for 5 minute(s)

Calls Per Minute (CPM) Statistics:

| benchmark              | 00:01:00  | 00:02:00  | 00:03:00  | 00:04:00  | 00:05:00  | 00:06:00 |
|:-----------------------|:----------|:----------|:----------|:----------|:----------|:---------|
| benchmark_redis_upsert | 1,334,646 | 2,618,999 | 2,624,694 | 2,634,956 | 2,614,081 | 849,144  |

Average CPS (Calls Per Second) per Function:

|                        | CPS       |
|:-----------------------|:----------|
| benchmark_redis_upsert | 43,730.67 |

Function Execution Time Statistics:

|                        | Mean | k50 |  k90 |  k99 | Count      |  Min |    Max | Median |
|:-----------------------|-----:|----:|-----:|-----:|:-----------|-----:|-------:|-------:|
| benchmark_redis_upsert | 3.03 | 2.9 | 4.02 | 6.62 | 12,676,520 | 0.42 | 362.83 |    2.9 |

Return Value Distribution Statistics:

|    | benchmark              | return_value |    count | percentage |
|---:|:-----------------------|-------------:|---------:|-----------:|
|  0 | benchmark_redis_upsert |            1 | 12401941 |      97.83 |
| 54 | benchmark_redis_upsert |            2 |   143939 |       1.14 |
| 65 | benchmark_redis_upsert |            3 |    50865 |        0.4 |
| 76 | benchmark_redis_upsert |            4 |    26090 |       0.21 |
| 87 | benchmark_redis_upsert |            5 |    14874 |       0.12 |

## 阿里云 redis.shard.small.2.ce 7.0.2.6 云原生 单节点 读写分离未开启 默认设置

Calls Per Minute (CPM) Statistics:

| benchmark              | 00:01:00 | 00:02:00  | 00:03:00  | 00:04:00  | 00:05:00  | 00:06:00 |
|:-----------------------|:---------|:----------|:----------|:----------|:----------|:---------|
| benchmark_redis_upsert | 640,610  | 1,580,831 | 1,562,021 | 1,562,169 | 1,582,375 | 846,201  |

Average CPS (Calls Per Second) per Function:

|                        | CPS       |
|:-----------------------|:----------|
| benchmark_redis_upsert | 26,185.09 |

Function Execution Time Statistics:

|                        | Mean | k50 |  k90 |   k99 | Count     |  Min |    Max | Median |
|:-----------------------|-----:|----:|-----:|------:|:----------|-----:|-------:|-------:|
| benchmark_redis_upsert | 7.41 | 7.2 | 9.64 | 12.72 | 7,774,207 | 0.75 | 357.66 |    7.2 |

Return Value Distribution Statistics:

|    | benchmark              | return_value |   count | percentage |
|---:|:-----------------------|-------------:|--------:|-----------:|
|  0 | benchmark_redis_upsert |            1 | 7702541 |      99.08 |
| 11 | benchmark_redis_upsert |            2 |   46251 |       0.59 |
| 22 | benchmark_redis_upsert |            3 |   10671 |       0.14 |
| 33 | benchmark_redis_upsert |            4 |    5119 |       0.07 |
| 42 | benchmark_redis_upsert |            5 |    2961 |       0.04 |

## 阿里云 tair.rdb.with.proxy.1g Tair 内存型 7.0(25.11.0.0) 云原生 3读写分离 默认设置

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
