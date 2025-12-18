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

Running benchmark: benchmark_redis_upsert
  Using multiprocessing with 24 workers
  Collected 943560 data points

## 机器

Calls Per Minute (CPM) Statistics:

| benchmark              | 00:01:00   | 00:02:00   | 00:03:00   | 00:04:00   | 00:05:00   | 00:06:00   |
|:-----------------------|:-----------|:-----------|:-----------|:-----------|:-----------|:-----------|
| benchmark_redis_upsert | 177,140    | 192,694    | 190,368    | 183,476    | 188,197    | 11,685     |

Average CPS (Calls Per Second) per Function:

|                        | CPS      |
|:-----------------------|:---------|
| benchmark_redis_upsert | 3,150.72 |

Function Execution Time Statistics:

|                        |   Mean |   k50 |   k90 |    k99 | Count   |   Min |     Max |   Median |
|:-----------------------|-------:|------:|------:|-------:|:--------|------:|--------:|---------:|
| benchmark_redis_upsert |  61.05 | 58.11 | 92.83 | 134.27 | 943,560 |  1.39 | 1131.49 |    58.11 |

Return Value Distribution Statistics:

|    | benchmark              | return_value   |   count |   percentage |
|---:|:-----------------------|:---------------|--------:|-------------:|
|  0 | benchmark_redis_upsert | None           |  943560 |          100 |
================================================================================