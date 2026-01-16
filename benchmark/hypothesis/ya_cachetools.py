import random
from cachetools import TTLCache
import string


async def cache():
    cache = TTLCache(maxsize=10000, ttl=0.1)
    return cache


async def dict():
    cache = {}
    return cache


async def benchmark_cache(cache):
    rnd_str = "".join(random.choices(string.ascii_uppercase + string.digits, k=3))

    if (cache.get(rnd_str)) is not None:
        return 1

    cache[rnd_str] = random.randint(1, 100000)
    return 0


async def benchmark_dict(dict):
    rnd_str = "".join(random.choices(string.ascii_uppercase + string.digits, k=3))

    if (dict.get(rnd_str)) is not None:
        return 1

    dict[rnd_str] = random.randint(1, 100000)
    return 0


"""
|                 | CPS          |
|:----------------|:-------------|
| benchmark_cache | 293,077.77   |
| benchmark_dict  | 1,159,489.53 |
"""
