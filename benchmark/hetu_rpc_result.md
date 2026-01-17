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

## debian13 ecs.c9ae.16xlarge 64核 + 本地 Redis-8.4.0 默认设置

Average CPS (Calls Per Second) per Function:

|                           | CPS          |
|:--------------------------|:-------------|
| benchmark_get             | 137,278.00   |
| benchmark_get2_update2    | 18,773.19    |
| benchmark_get_then_update | 32,659.98    |
| benchmark_hello_world     | 1,238,494.10 |

Function Execution Time Statistics:

|                           |  Mean |   k50 |   k90 |    k99 | Count      |  Min |    Max | Median |
|:--------------------------|------:|------:|------:|-------:|:-----------|-----:|-------:|-------:|
| benchmark_get             |  8.39 |  7.94 | 12.14 |  16.42 | 4,118,789  | 0.09 |  99.54 |   7.94 |
| benchmark_get2_update2    | 61.44 | 54.27 | 73.82 | 227.49 | 563,082    | 0.66 | 608.53 |  54.27 |
| benchmark_get_then_update | 35.17 | 29.42 | 47.55 | 108.08 | 983,139    | 0.45 | 505.27 |  29.42 |
| benchmark_hello_world     |  0.94 |  0.49 |  2.12 |   6.72 | 36,835,753 | 0.03 | 122.15 |   0.49 |

Return Value Distribution Statistics:

|   | benchmark                 | return_value |    count | percentage |
|--:|:--------------------------|:-------------|---------:|-----------:|
| 0 | benchmark_get             | 0            |  4118789 |        100 |
| 1 | benchmark_get2_update2    | 0            |   551040 |      97.86 |
| 2 | benchmark_get2_update2    | 1            |    11800 |        2.1 |
| 3 | benchmark_get2_update2    | 2            |      236 |       0.04 |
| 4 | benchmark_get2_update2    | 3            |        6 |          0 |
| 5 | benchmark_get_then_update | 0            |   970938 |      98.76 |
| 6 | benchmark_get_then_update | 1            |    12057 |       1.23 |
| 7 | benchmark_get_then_update | 2            |      143 |       0.01 |
| 8 | benchmark_get_then_update | 3            |        1 |          0 |
| 9 | benchmark_hello_world     | 世界收到         | 36835753 |        100 |

## debian13 ecs.c9ae.16xlarge 64核 + 阿里云Arm倚天Redis: redis.shard.small.2.ce 7.0.2.6 云原生 双节点高可用 默认设置

Average CPS (Calls Per Second) per Function:

|                           | CPS          |
|:--------------------------|:-------------|
| benchmark_get             | 141,058.53   |
| benchmark_get2_update2    | 9,001.79     |
| benchmark_get_then_update | 16,340.51    |
| benchmark_hello_world     | 1,202,890.77 |

Function Execution Time Statistics:

|                           |   Mean |    k50 |    k90 |    k99 | Count      |   Min |     Max | Median |
|:--------------------------|-------:|-------:|-------:|-------:|:-----------|------:|--------:|-------:|
| benchmark_get             |   8.21 |   7.32 |  12.36 |   17.5 | 4,208,701  |  0.58 |  214.73 |   7.32 |
| benchmark_get2_update2    | 128.96 | 122.07 | 147.88 | 366.49 | 268,622    | 33.17 | 1191.56 | 122.07 |
| benchmark_get_then_update |  70.44 |  66.55 |  86.05 | 184.04 | 491,259    |  6.06 |  773.76 |  66.55 |
| benchmark_hello_world     |   0.97 |   0.52 |    2.2 |   6.72 | 35,780,956 |  0.03 |  121.52 |   0.52 |

Return Value Distribution Statistics:

|   | benchmark                 | return_value |    count | percentage |
|--:|:--------------------------|:-------------|---------:|-----------:|
| 0 | benchmark_get             | 0            |  4208701 |        100 |
| 1 | benchmark_get2_update2    | 0            |   262880 |      97.86 |
| 2 | benchmark_get2_update2    | 1            |     5611 |       2.09 |
| 3 | benchmark_get2_update2    | 2            |      125 |       0.05 |
| 4 | benchmark_get2_update2    | 3            |        6 |          0 |
| 5 | benchmark_get_then_update | 0            |   485549 |      98.84 |
| 6 | benchmark_get_then_update | 1            |     5616 |       1.14 |
| 7 | benchmark_get_then_update | 2            |       92 |       0.02 |
| 8 | benchmark_get_then_update | 3            |        2 |          0 |
| 9 | benchmark_hello_world     | 世界收到         | 35780956 |        100 |

## debian13 ecs.c9ae.16xlarge 64核 + 阿里云最低配Redis: redis.shard.small.2.ce 7.0.2.6 云原生 单节点 默认设置

|                           | 手动记录CPU                 |
|:--------------------------|:------------------------|
| benchmark_get             | 河图CPU 40% Redis CPU 95% |
| benchmark_get2_update2    | 河图CPU 32% Redis CPU 98% |
| benchmark_get_then_update | 河图CPU 42% Redis CPU 98% |
| benchmark_hello_world     | 河图CPU 99% Redis CPU 0%  |

Average CPS (Calls Per Second) per Function:

|                           | CPS          |
|:--------------------------|:-------------|
| benchmark_get             | 195,030.89   |
| benchmark_get2_update2    | 20,315.22    |
| benchmark_get_then_update | 35,678.84    |
| benchmark_hello_world     | 1,205,377.44 |

Function Execution Time Statistics:

|                           |  Mean |   k50 |   k90 |    k99 | Count      |  Min |    Max | Median |
|:--------------------------|------:|------:|------:|-------:|:-----------|-----:|-------:|-------:|
| benchmark_get             |  5.93 |  6.03 |  7.36 |   8.67 | 12,831,693 | 0.18 | 192.61 |   6.03 |
| benchmark_get2_update2    | 56.71 |  56.6 | 59.71 |  214.6 | 1,341,201  | 2.13 | 726.92 |   56.6 |
| benchmark_get_then_update | 32.29 | 32.74 | 34.49 | 109.94 | 2,355,115  | 0.72 | 586.39 |  32.74 |
| benchmark_hello_world     |  0.96 |  0.49 |  2.25 |   6.96 | 79,135,563 | 0.03 | 161.48 |   0.49 |

Return Value Distribution Statistics:

|   | benchmark                 | return_value |    count | percentage |
|--:|:--------------------------|:-------------|---------:|-----------:|
| 0 | benchmark_get             | 0            | 12831693 |        100 |
| 1 | benchmark_get2_update2    | 0            |  1311527 |      97.79 |
| 2 | benchmark_get2_update2    | 1            |    28987 |       2.16 |
| 3 | benchmark_get2_update2    | 2            |      672 |       0.05 |
| 4 | benchmark_get2_update2    | 3            |       15 |          0 |
| 5 | benchmark_get_then_update | 0            |  2323616 |      98.66 |
| 6 | benchmark_get_then_update | 1            |    31126 |       1.32 |
| 7 | benchmark_get_then_update | 2            |      365 |       0.02 |
| 8 | benchmark_get_then_update | 3            |        8 |          0 |
| 9 | benchmark_hello_world     | 世界收到         | 79135563 |        100 |

测试TTL `-n 1 -p 1`:

Average CPS (Calls Per Second) per Function:

|                           | CPS       |
|:--------------------------|:----------|
| benchmark_get             | 5,704.82  |
| benchmark_get2_update2    | 827.62    |
| benchmark_get_then_update | 1,498.41  |
| benchmark_hello_world     | 29,921.00 |

Function Execution Time Statistics:

|                           | Mean |  k50 |  k90 |  k99 | Count     |  Min |    Max | Median |
|:--------------------------|-----:|-----:|-----:|-----:|:----------|-----:|-------:|-------:|
| benchmark_get             | 0.18 | 0.17 | 0.18 | 0.21 | 375,917   | 0.15 |   14.8 |   0.17 |
| benchmark_get2_update2    | 1.21 | 1.22 | 1.33 | 1.42 | 54,337    | 0.99 | 207.36 |   1.22 |
| benchmark_get_then_update | 0.67 | 0.66 | 0.69 | 0.83 | 98,766    | 0.43 |   2.97 |   0.66 |
| benchmark_hello_world     | 0.03 | 0.03 | 0.03 | 0.04 | 1,974,514 | 0.03 |   3.77 |   0.03 |

Return Value Distribution Statistics:

|   | benchmark                 | return_value |   count | percentage |
|--:|:--------------------------|:-------------|--------:|-----------:|
| 0 | benchmark_get             | 0            |  375917 |        100 |
| 1 | benchmark_get2_update2    | 0            |   54337 |        100 |
| 2 | benchmark_get_then_update | 0            |   98766 |        100 |
| 3 | benchmark_hello_world     | 世界收到         | 1974514 |        100 |

================================================================================

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

