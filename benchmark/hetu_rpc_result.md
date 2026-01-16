# HeTu RPC Benchmark Results

结论：使用batch后200%提升

## 运行命令

```
uv run ya ya_hetu_rpc.py -n 1800 -t 2
```

Found 4 benchmark(s): benchmark_get, benchmark_get2_update2, benchmark_get_then_update,
benchmark_hello_world
Running with 64 workers, 3 tasks per worker, for 1.0 minute(s)

Running benchmark: benchmark_ge
Using multiprocessing with 64 workers

## Windows 开发机 9950x3D 32核 + redis:8.0 Windows Docker 默认设置

Average CPS (Calls Per Second) per Function:

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

Average CPS (Calls Per Second) per Function:

|                           | CPS        |
|:--------------------------|:-----------|
| benchmark_get             | 232,257.75 |
| benchmark_get2_update2    | 17,853.76  |
| benchmark_get_then_update | 27,464.93  |
| benchmark_hello_world     | 434,857.21 |

Function Execution Time Statistics:

|                           |  Mean |   k50 |   k90 |    k99 | Count      |  Min |    Max | Median |
|:--------------------------|------:|------:|------:|-------:|:-----------|-----:|-------:|-------:|
| benchmark_get             |  7.72 |  6.47 | 13.35 |  26.53 | 13,922,101 | 0.21 | 344.64 |   6.47 |
| benchmark_get2_update2    | 67.83 | 62.97 | 75.36 | 243.43 | 1,052,346  | 1.25 | 810.44 |  62.97 |
| benchmark_get_then_update |  65.2 | 63.17 | 74.58 | 188.21 | 1,650,167  |  2.7 | 620.28 |  63.17 |
| benchmark_hello_world     |  4.12 |  3.19 |  8.62 |  15.16 | 26,113,439 | 0.07 | 277.02 |   3.19 |

Return Value Distribution Statistics:

|   | benchmark                 | return_value |    count | percentage |
|--:|:--------------------------|:-------------|---------:|-----------:|
| 0 | benchmark_get             | 0            | 13922101 |        100 |
| 1 | benchmark_get2_update2    | 0            |  1031322 |         98 |
| 2 | benchmark_get2_update2    | 1            |    20590 |       1.96 |
| 3 | benchmark_get2_update2    | 2            |      424 |       0.04 |
| 4 | benchmark_get2_update2    | 3            |       10 |          0 |
| 5 | benchmark_get_then_update | 0            |  1625825 |      98.52 |
| 6 | benchmark_get_then_update | 1            |    23950 |       1.45 |
| 7 | benchmark_get_then_update | 2            |      386 |       0.02 |
| 8 | benchmark_get_then_update | 3            |        6 |          0 |
| 9 | benchmark_hello_world     | 世界收到         | 26113439 |        100 |

## debian13 ecs.c8a.16xlarge 64核 + 阿里云最低配Redis: redis.shard.small.2.ce 7.0.2.6 云原生 单节点 读写分离未开启 默认设置

|                           | 手动记录CPU                 |
|:--------------------------|:------------------------|
| benchmark_get             | 河图CPU 99% Redis CPU 52% |
| benchmark_get2_update2    | 河图CPU 73% Redis CPU 96% |
| benchmark_get_then_update | 河图CPU 76% Redis CPU 97% |
| benchmark_hello_world     | 河图CPU 98% Redis CPU 0%  |

Average CPS (Calls Per Second) per Function:

|                           | CPS        |
|:--------------------------|:-----------|
| benchmark_get             | 229,941.26 |
| benchmark_get2_update2    | 29,801.20  |
| benchmark_get_then_update | 48,184.06  |
| benchmark_hello_world     | 436,540.38 |

Function Execution Time Statistics:

|                           |  Mean |   k50 |   k90 |    k99 | Count      |  Min |    Max | Median |
|:--------------------------|------:|------:|------:|-------:|:-----------|-----:|-------:|-------:|
| benchmark_get             |  7.79 |  7.22 |  12.1 |  18.15 | 27,608,276 |  0.5 |  253.4 |   7.22 |
| benchmark_get2_update2    | 60.19 | 55.88 | 69.31 | 242.52 | 3,573,604  | 2.96 | 816.96 |  55.88 |
| benchmark_get_then_update | 37.22 | 35.14 | 43.88 | 126.08 | 5,779,052  | 1.98 | 681.32 |  35.14 |
| benchmark_hello_world     |   4.1 |  3.18 |  8.68 |  15.23 | 52,402,675 | 0.08 |  307.9 |   3.18 |

Return Value Distribution Statistics:

|    | benchmark                 | return_value |    count | percentage |
|---:|:--------------------------|:-------------|---------:|-----------:|
|  0 | benchmark_get             | 0            | 27608276 |        100 |
|  1 | benchmark_get2_update2    | 0            |  3475239 |      97.25 |
|  2 | benchmark_get2_update2    | 1            |    95468 |       2.67 |
|  3 | benchmark_get2_update2    | 2            |     2801 |       0.08 |
|  4 | benchmark_get2_update2    | 3            |       94 |          0 |
|  5 | benchmark_get2_update2    | 4            |        2 |          0 |
|  6 | benchmark_get_then_update | 0            |  5699930 |      98.63 |
|  7 | benchmark_get_then_update | 1            |    78017 |       1.35 |
|  8 | benchmark_get_then_update | 2            |     1091 |       0.02 |
|  9 | benchmark_get_then_update | 3            |       14 |          0 |
| 10 | benchmark_hello_world     | 世界收到         | 52402675 |        100 |

## debian13 ecs.c8a.16xlarge 64核 + 阿里云Tair读写分离：tair.rdb.with.proxy.1g Tair 内存型 7.0(25.11.0.0) 云原生 4节点读写分离 默认设置

需要检查下为什么在读写分离下性能更低，可能是因为batch或者proxy的原因？比如可以读写分离下不开batch

|                           | 手动记录CPU                 |
|:--------------------------|:------------------------|
| benchmark_get             | 河图CPU 99% Redis CPU 50% |
| benchmark_get2_update2    | 河图CPU 41% Redis CPU 59% |
| benchmark_get_then_update | 河图CPU 34% Redis CPU 44% |
| benchmark_hello_world     | 河图CPU 99% Redis CPU 0%  |

Average CPS (Calls Per Second) per Function:

|                           | CPS        |
|:--------------------------|:-----------|
| benchmark_get             | 216,623.78 |
| benchmark_get2_update2    | 13,276.11  |
| benchmark_get_then_update | 23,502.38  |
| benchmark_hello_world     | 434,622.08 |

关闭batch写入有明显提升，而get却降低了，有必要思考下为什么：
| | CPS |
|:--------------------------|:-----------|
| benchmark_get | 173,575.43 |
| benchmark_get2_update2 | 19,620.35 |
| benchmark_get_then_update | 30,915.50 |
| benchmark_hello_world | 434,660.60 |

Function Execution Time Statistics:

|                           |   Mean |   k50 |    k90 |    k99 | Count      |  Min |     Max | Median |
|:--------------------------|-------:|------:|-------:|-------:|:-----------|-----:|--------:|-------:|
| benchmark_get             |   8.26 |  7.68 |  12.41 |  18.66 | 26,022,987 |  0.5 |  242.93 |   7.68 |
| benchmark_get2_update2    | 121.87 | 118.5 | 190.55 | 395.85 | 1,587,435  | 2.74 | 1617.91 |  118.5 |
| benchmark_get_then_update |  76.58 | 76.74 | 105.13 | 210.75 | 2,808,847  |  1.6 |  778.09 |  76.74 |
| benchmark_hello_world     |   4.12 |  3.24 |   8.57 |  14.91 | 52,195,617 | 0.09 |  385.51 |   3.24 |

Return Value Distribution Statistics:

|    | benchmark                 | return_value |    count | percentage |
|---:|:--------------------------|:-------------|---------:|-----------:|
|  0 | benchmark_get             | 0            | 26022987 |        100 |
|  1 | benchmark_get2_update2    | 0            |  1548935 |      97.57 |
|  2 | benchmark_get2_update2    | 1            |    37433 |       2.36 |
|  3 | benchmark_get2_update2    | 2            |     1034 |       0.07 |
|  4 | benchmark_get2_update2    | 3            |       30 |          0 |
|  5 | benchmark_get2_update2    | 4            |        3 |          0 |
|  6 | benchmark_get_then_update | 0            |  2770002 |      98.62 |
|  7 | benchmark_get_then_update | 1            |    38296 |       1.36 |
|  8 | benchmark_get_then_update | 2            |      542 |       0.02 |
|  9 | benchmark_get_then_update | 3            |        7 |          0 |
| 10 | benchmark_hello_world     | 世界收到         | 52195617 |        100 |
