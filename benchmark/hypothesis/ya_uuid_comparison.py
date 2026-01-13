import uuid

from hetu.common.snowflake_id import SnowflakeID

generator = SnowflakeID()
generator.init(worker_id=1, last_timestamp=0)

# Mock current timestamp to ensure uniqueness in benchmark
# 不需要，因为现在cps就在1附近
# from time import time
# import hetu.common.snowflake_id
# count = 0
# def incrementing_timestamp():
#     global count
#     count += 1
#     return int(time() * 1000) + count
# hetu.common.snowflake_id.time = incrementing_timestamp


async def benchmark_snowflake_id():
    """基准测试SnowflakeID生成速度"""
    for _ in range(4096000):  # 测试雪花允许的1秒最高生成id数
        generator.next_id()
    return generator.next_id()


async def benchmark_uuid6():
    """基准测试UUIDv6生成速度"""
    for _ in range(4096000):
        uuid.uuid6()
    return uuid.uuid6()


async def benchmark_uuid7():
    """基准测试UUIDv7生成速度"""
    for _ in range(4096000):
        uuid.uuid7()
    return uuid.uuid7()


# uv run ya .\benchmark\hypothesis\ya_uuid_comparison.py -t 0.1 -n 1 -p

# Found 3 benchmark(s): benchmark_snowflake_id, benchmark_uuid6, benchmark_uuid7
# Running with 1 workers, 1 tasks per worker, for 0.1 minute(s)

# Running benchmark: benchmark_snowflake_id
#   Running in the main process
#   Collected 5 data points

# Running benchmark: benchmark_uuid6
#   Running in the main process
#   Collected 2 data points

# Running benchmark: benchmark_uuid7
#   Running in the main process
#   Collected 2 data points

# ================================================================================
# Benchmark Results Summary
# ================================================================================

# Calls Per Minute (CPM) Statistics:

# |    | benchmark              | execution_time            |   execution_count |
# |---:|:-----------------------|:--------------------------|------------------:|
# |  0 | benchmark_snowflake_id | 2025-12-15 22:42:00+08:00 |                 6 |
# |  1 | benchmark_uuid6        | 2025-12-15 22:42:00+08:00 |                 1 |
# |  2 | benchmark_uuid6        | 2025-12-15 22:43:00+08:00 |                 1 |
# |  3 | benchmark_uuid7        | 2025-12-15 22:43:00+08:00 |                 2 |

# Average CPS (Calls Per Second) per Function:

# |                        |   CPS |
# |:-----------------------|------:|
# | benchmark_snowflake_id |  1.05 |
# | benchmark_uuid6        |  0.63 |
# | benchmark_uuid7        |  0.6  |

# Function Execution Time Statistics:

# |                        |    Mean |     k50 |     k90 |     k99 |   Count |     Min |     Max |   Median |
# |:-----------------------|--------:|--------:|--------:|--------:|--------:|--------:|--------:|---------:|
# | benchmark_snowflake_id | 1145.26 | 1145.9  | 1148.6  | 1150.06 |       6 | 1139.34 | 1150.22 |  1145.9  |
# | benchmark_uuid6        | 3167.62 | 3167.62 | 3171.01 | 3171.78 |       2 | 3163.38 | 3171.86 |  3167.62 |
# | benchmark_uuid7        | 3331.1  | 3331.1  | 3334.88 | 3335.72 |       2 | 3326.38 | 3335.82 |  3331.1  |

# Return Value Distribution Statistics:

# |    | benchmark              | return_value                         |   count |   percentage |
# |---:|:-----------------------|:-------------------------------------|--------:|-------------:|
# |  0 | benchmark_snowflake_id | 72171396404                          |       1 |           20 |
# |  1 | benchmark_snowflake_id | 77569463017                          |       1 |           20 |
# |  2 | benchmark_snowflake_id | 83672175725                          |       1 |           20 |
# |  3 | benchmark_snowflake_id | 89846190387                          |       1 |           20 |
# |  4 | benchmark_snowflake_id | 95961487320                          |       1 |           20 |
# |  5 | benchmark_uuid6        | 1f0d9be5-7587-6bcf-a927-ac361bf961bc |       1 |           50 |
# |  6 | benchmark_uuid6        | 1f0d9be5-9954-646b-a470-ac361bf961bc |       1 |           50 |
# |  7 | benchmark_uuid7        | 019b224f-ad78-7719-ac04-97ea3702768a |       1 |           50 |
# |  8 | benchmark_uuid7        | 019b224f-baac-76b4-81c2-274d0b1bfd81 |       1 |           50 |
# ================================================================================
