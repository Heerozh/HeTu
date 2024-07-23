import argparse
import asyncio
import json
import random
import ssl
import string
import time
import zlib
from collections import defaultdict
from functools import partial
from multiprocessing import Pool

import pandas as pd
import websockets


async def bench_sys_call_routine(address, iters, pid, name, packet):
    count = defaultdict(int)
    if address.startswith('wss://'):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE  # 应该使用ssl_context.load_cert_chain(cert_file)更安全
    else:
        ssl_context = None
    print(name, '开始测试', pid, '号客户端', iters, '次请求')
    async with websockets.connect(address, ssl=ssl_context) as ws:
        for _ in range(iters):
            # 随机读写30w数据中的一个
            raw_call = packet()
            # 组合封包
            call = json.dumps(raw_call).encode()
            call = zlib.compress(call)
            # 为了测试准确的性能，采用call-response模式
            await ws.send(call)
            _ = await ws.recv()
            # 记录当前分钟的执行数
            cur_min = int(time.time() % 3600 // 60)
            count[cur_min] += 1
    return count


async def bench_login(address, iters, name, pid):
    def packet():
        user_id = random.randint(1, 300000)
        return ['sys', 'login_test', user_id]

    return await bench_sys_call_routine(address, iters, pid, name, packet)


async def bench_select_update(address, iters, name, pid):
    def packet():
        row_id = random.randint(1, 300000)
        return ['sys', 'select_and_update', row_id]

    return await bench_sys_call_routine(address, iters, pid, name, packet)


async def bench_exchange_data(address, iters, name, pid):
    def packet():
        rnd_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
        row_id = random.randint(1, 300000)
        return ['sys', 'exchange_data', rnd_str, row_id]

    return await bench_sys_call_routine(address, iters, pid, name, packet)


def run_client(func, pid):
    return asyncio.run(func(pid))


def stat_count(list_of_dict, test_name):
    df = pd.DataFrame(list_of_dict).T  # 行：分钟， 列: client id
    df.columns = df.columns.map(lambda x: f"Client{x}")
    df.sort_index(inplace=True)
    # 分钟normalize
    df.index = df.index - df.index[0]
    df.index = df.index.map(lambda x: f"00:{x:02}")
    df.index.name = 'Time'
    # 去头去尾，保留中间最准确的数据
    if len(df) > 3:
        df = df.iloc[1:-1]
    else:
        print('测试次数不够，数据不准确。')
    print('初步结果：', df.to_markdown())
    series = df.sum(axis=1)
    series.name = test_name
    return series


def run_bench(func, _args, name):
    atest = partial(func, _args.address, _args.iters, name)
    test = partial(run_client, atest)
    with Pool(_args.clients) as p:
        results = p.map(test, list(range(_args.clients)))
    return stat_count(results, name)


if __name__ == '__main__':
    # 通过ws测试完整流程的性能
    # 此方法需要手动部署hetu服务器和backend，和测试机分开
    parser = argparse.ArgumentParser(prog='hetu', description='Hetu full bench')
    parser.add_argument("--address", type=str, default="ws://127.0.0.1:2466/hetu/")
    parser.add_argument("--clients", type=int, default=20)
    parser.add_argument("--iters", type=int, default=10000)
    args = parser.parse_args()

    stat_df = pd.DataFrame([])  # 列：分钟/mean，行:项目

    # 分配clients个进程和连接进行测试
    all_results = [
        run_bench(bench_login, args, 'login'),
        run_bench(bench_select_update, args, 'select + update'),
        run_bench(bench_exchange_data, args, 'select*2 + update*2')
    ]

    # 汇总统计
    stat_df = pd.concat([stat_df, *all_results], axis=1)
    stat_df.loc['Avg/minutes'] = stat_df.mean()
    stat_df.loc['RTT(ms)'] = 60 / stat_df.loc['Avg/minutes'] * 1000
    print(stat_df.to_markdown())
