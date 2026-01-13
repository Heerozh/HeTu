# Redis Upsert Benchmark Results

结论：2 种方式性能差异不大，Lua 方式灵活性更高，支持更多种类的 Redis 和架构。

主要损耗在网络 IO 上，得想办法减少 Lua 外 GET 的次数，比如增加缓存层，而不去 redis 读。

## 运行命令

```
export REDIS_HOST=r-uf6v7vch86ipmsqmhq.redis.rds.aliyuncs.com
export REDIS_PASSWORD=...
uv run ya ./benchmark/hypothesis/ya_redis_raw_upsert.py
```

Found 2 benchmark(s): benchmark_lua_version, benchmark_watch_multi
Running with 64 workers, 3 tasks per worker, for 5.0 minute(s)

## 本机 9950x3D redis:8.0 Windows Docker 默认设置

Calls Per Minute (CPM) Statistics:

| benchmark             | 00:01:00 | 00:02:00  | 00:03:00  | 00:04:00  | 00:05:00  | 00:06:00  |
|:----------------------|:---------|:----------|:----------|:----------|:----------|:----------|
| benchmark_lua_version | 296,369  | 891,812   | 896,794   | 905,927   | 905,259   | 621,343   |
| benchmark_ping        | 523,372  | 2,704,156 | 2,704,052 | 2,701,173 | 2,697,884 | 2,170,856 |
| benchmark_watch_multi | 381,917  | 538,190   | 539,431   | 538,071   | 539,673   | 160,714   |

Average CPS (Calls Per Second) per Function:

|                       | CPS       |
|:----------------------|:----------|
| benchmark_lua_version | 15,018.68 |
| benchmark_ping        | 45,000.64 |
| benchmark_watch_multi | 8,984.48  |

Function Execution Time Statistics:

|                       |  Mean |   k50 |   k90 |   k99 | Count      |  Min |     Max | Median |
|:----------------------|------:|------:|------:|------:|:-----------|-----:|--------:|-------:|
| benchmark_lua_version | 12.75 | 12.04 | 16.88 | 25.27 | 4,517,504  | 0.79 |  112.27 |  12.04 |
| benchmark_ping        |  4.27 |  3.89 |  5.22 |  12.7 | 13,501,493 | 0.33 | 7675.79 |   3.89 |
| benchmark_watch_multi | 21.35 | 20.05 | 26.94 | 37.23 | 2,697,996  | 1.88 | 7962.45 |  20.05 |

Return Value Distribution Statistics:

|   | benchmark             | return_value |    count | percentage |
|--:|:----------------------|-------------:|---------:|-----------:|
| 0 | benchmark_lua_version |            0 |  4507935 |      99.79 |
| 1 | benchmark_lua_version |            1 |     9543 |       0.21 |
| 2 | benchmark_lua_version |            2 |       26 |          0 |
| 3 | benchmark_ping        |          nan | 13501493 |        100 |
| 4 | benchmark_watch_multi |            0 |  2691199 |      99.75 |
| 5 | benchmark_watch_multi |            1 |     6765 |       0.25 |
| 6 | benchmark_watch_multi |            2 |       32 |          0 |

## 本机 9950x3D Redis-8.4.0-Windows-x64-msys2 默认配置➕io-thread=8

和 memurai 性能一致。

Calls Per Minute (CPM) Statistics:

| benchmark             | 00:01:00  | 00:02:00  | 00:03:00  | 00:04:00  | 00:05:00  | 00:06:00  |
|:----------------------|:----------|:----------|:----------|:----------|:----------|:----------|
| benchmark_lua_version | 723,938   | 1,541,484 | 1,513,727 | 1,501,430 | 1,558,996 | 803,732   |
| benchmark_ping        | 1,231,055 | 4,298,448 | 4,274,507 | 4,296,738 | 4,222,432 | 3,074,693 |
| benchmark_watch_multi | 172,325   | 1,018,775 | 1,021,442 | 1,028,132 | 1,025,486 | 841,561   |

Average CPS (Calls Per Second) per Function:

|                       | CPS       |
|:----------------------|:----------|
| benchmark_lua_version | 25,492.46 |
| benchmark_ping        | 71,230.48 |
| benchmark_watch_multi | 17,058.18 |

Function Execution Time Statistics:

|                       |  Mean |   k50 |  k90 |   k99 | Count      |  Min |    Max | Median |
|:----------------------|------:|------:|-----:|------:|:-----------|-----:|-------:|-------:|
| benchmark_lua_version |  7.54 |  7.48 | 8.29 | 10.25 | 7,643,307  | 1.36 |  55.24 |   7.48 |
| benchmark_ping        |  2.69 |   2.9 |  3.4 |  3.94 | 21,397,873 | 0.25 | 111.46 |    2.9 |
| benchmark_watch_multi | 11.28 | 11.11 | 14.4 | 17.04 | 5,107,721  | 1.14 |  96.96 |  11.11 |

Return Value Distribution Statistics:

|   | benchmark             | return_value |    count | percentage |
|--:|:----------------------|-------------:|---------:|-----------:|
| 0 | benchmark_lua_version |            0 |  7627138 |      99.79 |
| 1 | benchmark_lua_version |            1 |    16117 |       0.21 |
| 2 | benchmark_lua_version |            2 |       52 |          0 |
| 3 | benchmark_ping        |          nan | 21397873 |        100 |
| 4 | benchmark_watch_multi |            0 |  5094457 |      99.74 |
| 5 | benchmark_watch_multi |            1 |    13203 |       0.26 |
| 6 | benchmark_watch_multi |            2 |       60 |          0 |
| 7 | benchmark_watch_multi |            3 |        1 |          0 |

## 服务器本地 ecs.c8a.4xlarge debian13 Redis-8.4.0 默认设置

Calls Per Minute (CPM) Statistics:

| benchmark             | 00:01:00  | 00:02:00  | 00:03:00  | 00:04:00  | 00:05:00  | 00:06:00 |
|:----------------------|:----------|:----------|:----------|:----------|:----------|:---------|
| benchmark_lua_version | 1,453,482 | 1,632,153 | 1,624,670 | 1,622,845 | 1,624,070 | 183,986  |
| benchmark_watch_multi | 1,083,064 | 1,329,259 | 1,328,652 | 1,328,851 | 1,336,031 | 247,944  |

Average CPS (Calls Per Second) per Function:

|                       | CPS       |
|:----------------------|:----------|
| benchmark_lua_version | 27,112.19 |
| benchmark_watch_multi | 22,141.17 |

Function Execution Time Statistics:

|                       | Mean |  k50 |  k90 |   k99 | Count     |  Min |   Max | Median |
|:----------------------|-----:|-----:|-----:|------:|:----------|-----:|------:|-------:|
| benchmark_lua_version | 7.07 | 6.76 |  7.9 | 10.21 | 8,141,206 | 0.48 | 26.78 |   6.76 |
| benchmark_watch_multi | 8.66 | 8.39 | 9.07 | 13.53 | 6,653,801 | 0.68 | 32.85 |   8.39 |

Return Value Distribution Statistics:

|   | benchmark             | return_value |   count | percentage |
|--:|:----------------------|-------------:|--------:|-----------:|
| 0 | benchmark_lua_version |            0 | 8123538 |      99.78 |
| 1 | benchmark_lua_version |            1 |   17623 |       0.22 |
| 2 | benchmark_lua_version |            2 |      45 |          0 |
| 3 | benchmark_watch_multi |            0 | 6636655 |      99.74 |
| 4 | benchmark_watch_multi |            1 |   17092 |       0.26 |
| 5 | benchmark_watch_multi |            2 |      54 |          0 |

## 阿里云 redis.shard.small.2.ce 7.0.2.6 云原生 单节点 读写分离未开启 默认设置

Calls Per Minute (CPM) Statistics:

| benchmark             | 00:01:00 | 00:02:00  | 00:03:00  | 00:04:00  | 00:05:00  | 00:06:00  |
|:----------------------|:---------|:----------|:----------|:----------|:----------|:----------|
| benchmark_lua_version | 429,795  | 1,364,324 | 1,375,803 | 1,372,913 | 1,372,445 | 940,366   |
| benchmark_watch_multi | 465,692  | 1,820,799 | 1,814,199 | 1,816,239 | 1,832,204 | 1,355,230 |

Average CPS (Calls Per Second) per Function:

|                       | CPS       |
|:----------------------|:----------|
| benchmark_lua_version | 22,869.12 |
| benchmark_watch_multi | 30,362.09 |

Function Execution Time Statistics:

|                       | Mean |  k50 |   k90 |   k99 | Count     |  Min |   Max | Median |
|:----------------------|-----:|-----:|------:|------:|:----------|-----:|------:|-------:|
| benchmark_lua_version |  8.4 | 8.36 |  8.88 |  10.6 | 6,855,646 | 1.46 | 29.42 |   8.36 |
| benchmark_watch_multi | 6.33 | 6.46 | 10.24 | 15.07 | 9,104,363 | 0.95 | 46.74 |   6.46 |

Return Value Distribution Statistics:

|   | benchmark             | return_value |   count | percentage |
|--:|:----------------------|-------------:|--------:|-----------:|
| 0 | benchmark_lua_version |            0 | 6840913 |      99.79 |
| 1 | benchmark_lua_version |            1 |   14680 |       0.21 |
| 2 | benchmark_lua_version |            2 |      53 |          0 |
| 3 | benchmark_watch_multi |            0 | 9081730 |      99.75 |
| 4 | benchmark_watch_multi |            1 |   22555 |       0.25 |
| 5 | benchmark_watch_multi |            2 |      78 |          0 |

## 阿里云 redis.shard.3xlarge.ce 7.0.2.6 云原生 单节点 读写分离未开启 默认设置

Calls Per Minute (CPM) Statistics:

| benchmark             | 00:01:00 | 00:02:00  | 00:03:00  | 00:04:00  | 00:05:00  | 00:06:00 |
|:----------------------|:---------|:----------|:----------|:----------|:----------|:---------|
| benchmark_lua_version | 833,812  | 1,444,162 | 1,442,625 | 1,442,746 | 1,455,626 | 621,759  |
| benchmark_watch_multi | 931,326  | 1,841,098 | 1,838,810 | 1,837,894 | 1,838,097 | 907,612  |

Average CPS (Calls Per Second) per Function:

|                       | CPS       |
|:----------------------|:----------|
| benchmark_lua_version | 24,097.11 |
| benchmark_watch_multi | 30,649.54 |

Function Execution Time Statistics:

|                       | Mean |  k50 |   k90 |  k99 | Count     |  Min |    Max | Median |
|:----------------------|-----:|-----:|------:|-----:|:----------|-----:|-------:|-------:|
| benchmark_lua_version | 7.95 | 7.93 |  8.43 | 9.84 | 7,240,730 | 1.19 |  35.86 |   7.93 |
| benchmark_watch_multi | 6.26 | 6.97 | 10.89 |   15 | 9,194,837 | 1.13 | 229.94 |   6.97 |

Return Value Distribution Statistics:

|   | benchmark             | return_value |   count | percentage |
|--:|:----------------------|-------------:|--------:|-----------:|
| 0 | benchmark_lua_version |            0 | 7225260 |      99.79 |
| 1 | benchmark_lua_version |            1 |   15407 |       0.21 |
| 2 | benchmark_lua_version |            2 |      63 |          0 |
| 3 | benchmark_watch_multi |            0 | 9172849 |      99.76 |
| 4 | benchmark_watch_multi |            1 |   21942 |       0.24 |
| 5 | benchmark_watch_multi |            2 |      45 |          0 |
| 6 | benchmark_watch_multi |            3 |       1 |          0 |

## 阿里云 redis.shard.small.y.ee 倚天 7.0(25.11.0.0) 云原生 单节点 高可用 默认设置

- 注： redis cpu 未跑满

Calls Per Minute (CPM) Statistics:

| benchmark             | 00:01:00 | 00:02:00  | 00:03:00  | 00:04:00  | 00:05:00  | 00:06:00 |
|:----------------------|:---------|:----------|:----------|:----------|:----------|:---------|
| benchmark_lua_version | 766,901  | 1,080,846 | 1,085,838 | 1,074,854 | 1,092,980 | 320,386  |
| benchmark_watch_multi | 700,482  | 1,064,379 | 1,066,189 | 1,063,714 | 1,065,482 | 363,905  |

Average CPS (Calls Per Second) per Function:

|                       | CPS       |
|:----------------------|:----------|
| benchmark_lua_version | 18,049.29 |
| benchmark_watch_multi | 17,749.01 |

Function Execution Time Statistics:

|                       |  Mean |   k50 |   k90 |   k99 | Count     |  Min |    Max | Median |
|:----------------------|------:|------:|------:|------:|:----------|-----:|-------:|-------:|
| benchmark_lua_version | 10.62 | 10.55 | 12.35 | 14.94 | 5,421,805 | 3.29 | 233.58 |  10.55 |
| benchmark_watch_multi | 10.82 | 10.57 | 12.98 | 14.17 | 5,324,151 |  7.5 | 243.59 |  10.57 |

Return Value Distribution Statistics:

|   | benchmark             | return_value |   count | percentage |
|--:|:----------------------|-------------:|--------:|-----------:|
| 0 | benchmark_lua_version |            0 | 5410265 |      99.79 |
| 1 | benchmark_lua_version |            1 |   11506 |       0.21 |
| 2 | benchmark_lua_version |            2 |      34 |          0 |
| 3 | benchmark_watch_multi |            0 | 5310607 |      99.75 |
| 4 | benchmark_watch_multi |            1 |   13487 |       0.25 |
| 5 | benchmark_watch_multi |            2 |      57 |          0 |

## 阿里云 tair.scm.standard.1m.4d Tair 内存持久型 6.0(1.2.7.2) 云原生 单节点 默认设置

Tair 的 watch 支持有问题，只有 lua 模式有实际意义。测试数据仅供参考

- 注： Tair cpu 无法跑满，可能和 tair 自身实现有关

Calls Per Minute (CPM) Statistics:

| benchmark             | 00:01:00 | 00:02:00  | 00:03:00  | 00:04:00  | 00:05:00  | 00:06:00 |
|:----------------------|:---------|:----------|:----------|:----------|:----------|:---------|
| benchmark_lua_version | 640,987  | 1,304,144 | 1,288,367 | 1,306,130 | 1,310,749 | 675,871  |
| benchmark_watch_multi | 367,084  | 858,328   | 858,261   | 858,957   | 859,059   | 491,412  |

Average CPS (Calls Per Second) per Function:

|                       | CPS       |
|:----------------------|:----------|
| benchmark_lua_version | 21,706.46 |
| benchmark_watch_multi | 14,310.06 |

Function Execution Time Statistics:

|                       |  Mean |  k50 |   k90 |   k99 | Count     |   Min |   Max | Median |
|:----------------------|------:|-----:|------:|------:|:----------|------:|------:|-------:|
| benchmark_lua_version |  8.83 | 8.83 |  9.54 | 10.57 | 6,526,248 |  4.71 | 32.02 |   8.83 |
| benchmark_watch_multi | 13.42 | 13.4 | 14.44 |    15 | 4,293,101 | 11.02 | 69.88 |   13.4 |

Return Value Distribution Statistics:

|   | benchmark             | return_value |   count | percentage |
|--:|:----------------------|-------------:|--------:|-----------:|
| 0 | benchmark_lua_version |            0 | 6512751 |      99.79 |
| 1 | benchmark_lua_version |            1 |   13444 |       0.21 |
| 2 | benchmark_lua_version |            2 |      53 |          0 |
| 3 | benchmark_watch_multi |            0 | 4282266 |      99.75 |
| 4 | benchmark_watch_multi |            1 |   10801 |       0.25 |
| 5 | benchmark_watch_multi |            2 |      34 |          0 |

## 阿里云 tair.rdb.1g Tair 内存型 7.0(25.11.0.0) 云原生 单节点 默认设置

Tair 的 watch 支持有问题，只有 lua 模式有实际意义。测试数据仅供参考

- 注： Tair cpu 无法跑满，可能和 tair 自身实现有关

Calls Per Minute (CPM) Statistics:

| benchmark             | 00:01:00  | 00:02:00  | 00:03:00  | 00:04:00  | 00:05:00  | 00:06:00 |
|:----------------------|:----------|:----------|:----------|:----------|:----------|:---------|
| benchmark_lua_version | 1,223,400 | 1,543,845 | 1,533,293 | 1,533,128 | 1,536,916 | 319,376  |
| benchmark_watch_multi | 1,343,757 | 1,860,132 | 1,861,999 | 1,857,989 | 1,863,530 | 514,687  |

Average CPS (Calls Per Second) per Function:

|                       | CPS       |
|:----------------------|:----------|
| benchmark_lua_version | 25,611.31 |
| benchmark_watch_multi | 31,004.29 |

Function Execution Time Statistics:

|                       | Mean |  k50 |   k90 |   k99 | Count     |  Min |   Max | Median |
|:----------------------|-----:|-----:|------:|------:|:----------|-----:|------:|-------:|
| benchmark_lua_version | 7.49 | 6.63 | 11.58 | 13.77 | 7,689,958 | 0.96 | 42.59 |   6.63 |
| benchmark_watch_multi | 6.19 |    7 |  7.95 | 12.22 | 9,302,094 | 1.09 | 50.92 |      7 |

Return Value Distribution Statistics:

|   | benchmark             | return_value |   count | percentage |
|--:|:----------------------|-------------:|--------:|-----------:|
| 0 | benchmark_lua_version |            0 | 7673394 |      99.78 |
| 1 | benchmark_lua_version |            1 |   16511 |       0.21 |
| 2 | benchmark_lua_version |            2 |      52 |          0 |
| 3 | benchmark_lua_version |            3 |       1 |          0 |
| 4 | benchmark_watch_multi |            0 | 9280827 |      99.77 |
| 5 | benchmark_watch_multi |            1 |   21201 |       0.23 |
| 6 | benchmark_watch_multi |            2 |      66 |          0 |
