# HeTu Backend Upsert Benchmark Results

结论：

## 运行命令

```
export REDIS_HOST=r-uf6v7vch86ipmsqmhq.redis.rds.aliyuncs.com
export REDIS_PASSWORD=...
uv run ya .\benchmark\ya_backend_upsert.py
```

Found 1 benchmark(s): benchmark_redis_upsert
Running with 24 workers, 8 tasks per worker, for 5 minute(s)

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