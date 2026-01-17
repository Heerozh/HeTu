# HeTu RPC Benchmark Results

## 运行命令

```
uv run ya ya_hetu_rpc.py -n 1200 -t 1.1
```

Found 4 benchmark(s): benchmark_get, benchmark_get2_update2, benchmark_get_then_update,
benchmark_hello_world
Running with 64 workers, 3 tasks per worker, for 1.1 minute(s)

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

## debian13 ecs.c9ae.16xlarge 64核 + 阿里云最低配Redis: redis.shard.small.2.ce 7.0.2.6 云原生 单节点 读写分离未开启 默认设置

|                           | 手动记录CPU                 |
|:--------------------------|:------------------------|
| benchmark_get             | 河图CPU 40% Redis CPU 95% |
| benchmark_get2_update2    | 河图CPU 29% Redis CPU 98% |
| benchmark_get_then_update | 河图CPU 30% Redis CPU 98% |
| benchmark_hello_world     | 河图CPU 72% Redis CPU 0%  |

Average CPS (Calls Per Second) per Function:

|                           | CPS        |
|:--------------------------|:-----------|
| benchmark_get             | 223,432.15 |
| benchmark_get2_update2    | 23,018.14  |
| benchmark_get_then_update | 39,561.04  |
| benchmark_hello_world     | 889,321.68 |

Function Execution Time Statistics:

|                           | Mean |  k50 |  k90 |  k99 | Count       |  Min |    Max | Median |
|:--------------------------|-----:|-----:|-----:|-----:|:------------|-----:|-------:|-------:|
| benchmark_get             | 0.57 | 0.54 | 0.79 | 1.16 | 26,815,494  | 0.17 |  42.02 |   0.54 |
| benchmark_get2_update2    | 3.42 | 2.81 |  5.1 | 8.03 | 2,707,471   | 1.08 | 351.92 |   2.81 |
| benchmark_get_then_update | 3.23 | 2.85 | 4.25 | 5.39 | 4,750,612   | 0.64 | 307.63 |   2.85 |
| benchmark_hello_world     | 0.14 | 0.08 | 0.37 |  0.6 | 106,786,682 | 0.03 |  17.27 |   0.08 |

Return Value Distribution Statistics:

|   | benchmark                 | return_value |     count | percentage |
|--:|:--------------------------|:-------------|----------:|-----------:|
| 0 | benchmark_get             | 0            |  26815494 |        100 |
| 1 | benchmark_get2_update2    | 0            |   2703196 |      99.84 |
| 2 | benchmark_get2_update2    | 1            |      4269 |       0.16 |
| 3 | benchmark_get2_update2    | 2            |         6 |          0 |
| 4 | benchmark_get_then_update | 0            |   4742876 |      99.84 |
| 5 | benchmark_get_then_update | 1            |      7728 |       0.16 |
| 6 | benchmark_get_then_update | 2            |         8 |          0 |
| 7 | benchmark_hello_world     | 世界收到         | 106786682 |        100 |

## debian13 ecs.c9ae.16xlarge 64核 + 阿里云最低配Redis读写分离: redis.shard.with.proxy.small.ce 7.0.2.6 云原生 4节点 读写分离代理 默认设置

|                           | 手动记录CPU                               |
|:--------------------------|:--------------------------------------|
| benchmark_get             | 河图CPU 98% Redis主节点CPU 41% 只读节点CPU 48% |
| benchmark_get2_update2    | 河图CPU 78% Redis主节点CPU 90% 只读节点CPU 60% |
| benchmark_get_then_update | 河图CPU 88% Redis主节点CPU 97% 只读节点CPU 52% |
| benchmark_hello_world     | 河图CPU 98% Redis主节点CPU 0%              |

Average CPS (Calls Per Second) per Function:

|                           | CPS          |
|:--------------------------|:-------------|
| benchmark_get             | 422,817.37   |
| benchmark_get2_update2    | 54,260.74    |
| benchmark_get_then_update | 90,776.78    |
| benchmark_hello_world     | 1,200,929.39 |

Function Execution Time Statistics:

|                           |  Mean |   k50 |   k90 |    k99 | Count      |  Min |    Max | Median |
|:--------------------------|------:|------:|------:|-------:|:-----------|-----:|-------:|-------:|
| benchmark_get             |   2.7 |  2.31 |  4.77 |    8.4 | 12,814,848 | 0.23 | 123.94 |   2.31 |
| benchmark_get2_update2    | 21.28 | 17.73 | 27.67 | 160.82 | 1,624,608  | 1.49 | 698.39 |  17.73 |
| benchmark_get_then_update | 12.69 | 10.42 | 17.04 |  95.47 | 2,725,078  | 0.94 | 477.27 |  10.42 |
| benchmark_hello_world     |  0.96 |  0.49 |  2.22 |    6.8 | 36,007,652 | 0.03 | 129.85 |   0.49 |

Return Value Distribution Statistics:

|    | benchmark                 | return_value |    count | percentage |
|---:|:--------------------------|:-------------|---------:|-----------:|
|  0 | benchmark_get             | 0            | 12814848 |        100 |
|  1 | benchmark_get2_update2    | 0            |  1583158 |      97.45 |
|  2 | benchmark_get2_update2    | 1            |    40311 |       2.48 |
|  3 | benchmark_get2_update2    | 2            |     1102 |       0.07 |
|  4 | benchmark_get2_update2    | 3            |       33 |          0 |
|  5 | benchmark_get2_update2    | 4            |        4 |          0 |
|  6 | benchmark_get_then_update | 0            |  2682567 |      98.44 |
|  7 | benchmark_get_then_update | 1            |    41842 |       1.54 |
|  8 | benchmark_get_then_update | 2            |      659 |       0.02 |
|  9 | benchmark_get_then_update | 3            |       10 |          0 |
| 10 | benchmark_hello_world     | 世界收到         | 36007652 |        100 |

## debian13 ecs.c8a.16xlarge 64核 + 阿里云Tair读写分离：tair.rdb.with.proxy.1g Tair 内存型 7.0(25.11.0.0) 云原生 4节点 读写分离代理 默认设置

感觉不如原版？单机版Tair表现似乎更好。

Average CPS (Calls Per Second) per Function:

|                           | CPS          |
|:--------------------------|:-------------|
| benchmark_get             | 423,691.78   |
| benchmark_get2_update2    | 37,755.38    |
| benchmark_get_then_update | 58,131.90    |
| benchmark_hello_world     | 1,194,362.75 |

Function Execution Time Statistics:

|                           |  Mean |   k50 |   k90 |    k99 | Count      |  Min |    Max | Median |
|:--------------------------|------:|------:|------:|-------:|:-----------|-----:|-------:|-------:|
| benchmark_get             |   2.7 |   2.2 |  4.95 |   9.28 | 12,808,155 | 0.23 | 220.17 |    2.2 |
| benchmark_get2_update2    | 25.51 |  19.8 |  43.2 | 165.77 | 1,060,136  | 1.49 | 591.47 |   19.8 |
| benchmark_get_then_update | 19.78 | 15.67 | 33.81 | 128.21 | 1,748,140  | 0.93 | 605.67 |  15.67 |
| benchmark_hello_world     |  0.97 |   0.5 |  2.25 |    6.9 | 35,728,092 | 0.03 | 124.35 |    0.5 |

Return Value Distribution Statistics:

|    | benchmark                 | return_value |    count | percentage |
|---:|:--------------------------|:-------------|---------:|-----------:|
|  0 | benchmark_get             | 0            | 12808155 |        100 |
|  1 | benchmark_get2_update2    | 0            |  1037065 |      97.82 |
|  2 | benchmark_get2_update2    | 1            |    22534 |       2.13 |
|  3 | benchmark_get2_update2    | 2            |      523 |       0.05 |
|  4 | benchmark_get2_update2    | 3            |       14 |          0 |
|  5 | benchmark_get_then_update | 0            |  1716777 |      98.21 |
|  6 | benchmark_get_then_update | 1            |    30760 |       1.76 |
|  7 | benchmark_get_then_update | 2            |      589 |       0.03 |
|  8 | benchmark_get_then_update | 3            |       13 |          0 |
|  9 | benchmark_get_then_update | 4            |        1 |          0 |
| 10 | benchmark_hello_world     | 世界收到         | 35728092 |        100 |

