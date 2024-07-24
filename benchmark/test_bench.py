import argparse
import asyncio
import json
import multiprocessing
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

BENCH_ROW_COUNT = 30000


async def bench_sys_call_routine(address, duration, name: str, pid: str, packet):
    call_count = defaultdict(int)
    retry_count = defaultdict(int)
    if address.startswith('wss://'):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE  # 应该使用ssl_context.load_cert_chain(cert_file)更安全
    else:
        ssl_context = None
    print(name, '开始测试', pid, '号客户端', duration, '分钟运行时间')
    try:
        async with websockets.connect(address, ssl=ssl_context) as ws:
            while True:
                # 随机读写30w数据中的一个
                raw_call = packet()
                # 组合封包
                call = json.dumps(raw_call).encode()
                call = zlib.compress(call)
                # 为了测试准确的性能，采用call-response模式
                await ws.send(call)
                # 统计事务冲突率
                received = await ws.recv()
                received = zlib.decompress(received)
                received = json.loads(received)
                # 记录当前分钟的执行数
                cur_min = int(time.time() // 60)
                call_count[cur_min] += 1
                retry_count[cur_min] += received[0]
                if len(call_count) > duration:
                    del call_count[cur_min]
                    del retry_count[cur_min]
                    break
    except websockets.exceptions.ConnectionClosedError:
        print(pid, '号客户端连接断开，提前结束测试')
        pass
    return call_count, retry_count


async def bench_pubsub_routine(address, duration, name: str, pid: str):
    recv_count = defaultdict(int)
    if address.startswith('wss://'):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE  # 应该使用ssl_context.load_cert_chain(cert_file)更安全
    else:
        ssl_context = None
    print(name, '开始测试', pid, '号客户端', duration, '分钟运行时间')
    try:
        async with websockets.connect(address, ssl=ssl_context) as ws:
            # 组合封包
            raw_call = ['sub', 'IntTable', 'query', 'number', 0, 100, 100]
            call = json.dumps(raw_call).encode()
            call = zlib.compress(call)
            await ws.send(call)
            while True:
                # 统计收到的消息数
                _ = await ws.recv()
                # 记录当前分钟的执行数
                cur_min = int(time.time() // 60)
                recv_count[cur_min] += 1
                if len(recv_count) > duration:
                    del recv_count[cur_min]
                    break
    except websockets.exceptions.ConnectionClosedError:
        print(pid, '号客户端连接断开，提前结束测试')
        pass
    return recv_count


async def bench_select_update(address, duration, name: str, pid: str):
    def packet():
        row_id = random.randint(1, BENCH_ROW_COUNT)
        return ['sys', 'select_and_update', row_id]

    return await bench_sys_call_routine(address, duration, name, pid, packet)


async def bench_exchange_data(address, duration, name: str, pid: str):
    def packet():
        rnd_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
        row_id = random.randint(1, BENCH_ROW_COUNT)
        return ['sys', 'exchange_data', rnd_str, row_id]

    return await bench_sys_call_routine(address, duration, name, pid, packet)


async def gather_clients(func, client_num, pid: int):
    clients = [func(f"{pid}.{i}") for i in range(client_num)]
    return await asyncio.gather(*clients)


def run_client(func, client_num, pid: int):
    return asyncio.run(gather_clients(func, client_num, pid))


def stat_count(list3d, test_name):
    def format_df(list_2d):
        df = pd.DataFrame(list_2d).T  # 行：分钟， 列: client id
        df.columns = df.columns.map(lambda x: f"Client{x}")
        df.sort_index(inplace=True)
        # 删除个别client特别慢导致多出来的行，然后再删除该client列
        df.dropna(thresh=len(df.columns) // 2, inplace=True, axis=0)
        df.dropna(thresh=len(df.index) // 2, inplace=True, axis=1)
        # 分钟normalize
        df.index = df.index - df.index[0]
        df.index = df.index.map(lambda x: f"00:{x:02}")
        df.index.name = 'Time'
        # 去头去尾，保留中间最准确的数据
        if len(df) > 3:
            df = df.iloc[1:-1]
        else:
            print('测试时间不够，数据不准确。')
        return df.sum(axis=1)

    call_count = [x[0] for x in list3d]
    retry_count = [x[1] for x in list3d]
    cpm = format_df(call_count)
    cpm.name = test_name + '(CPM)'
    race = round(format_df(retry_count) / cpm * 100, 3)
    race.name = test_name + '(Race%)'
    return pd.concat([cpm, race], axis=1)


def run_bench(func, _args, name):
    cpu = multiprocessing.cpu_count()
    process_num = cpu * 2
    process_num = _args.clients if process_num > _args.clients else process_num
    clients = min(1, int(_args.clients // process_num))

    atest = partial(func, _args.address, _args.time, name)
    test = partial(run_client, atest, clients)

    with Pool(process_num) as p:
        results = p.map(test, list(range(process_num)))
    results = [y for x in results for y in x]  # flatten clients
    return stat_count(results, name)


if __name__ == '__main__':
    # 通过ws测试完整流程的性能
    # 此方法需要手动部署hetu服务器和backend，和测试机分开
    parser = argparse.ArgumentParser(prog='hetu', description='Hetu full bench')
    parser.add_argument("--address", type=str, default="ws://127.0.0.1:2466/hetu/")
    parser.add_argument("--item", default='call', help="或者填pubsub测试推送能力")
    parser.add_argument("--clients", type=int, default=200, help="启动客户端数")
    parser.add_argument("--time", type=int, default=5,
                        help="每项目测试时间（分钟），测试结果去头尾各1分钟，所以要3分钟起")
    args = parser.parse_args()

    # 分配clients个进程和连接进行测试
    match args.item:
        case 'call':
            all_results = [
                run_bench(bench_select_update, args, 'select + update'),
                run_bench(bench_exchange_data, args, 'select*2 + update*2')
            ]
        case 'pubsub':
            all_results = [
                run_bench(bench_pubsub_routine, args, 'pubsub')
            ]
        case _:
            raise ValueError('--item未知测试项目')

    # 汇总统计
    stat_df = pd.concat(all_results, axis=1)
    stat_df.loc['Avg'] = stat_df.mean()

    cpm_stat = stat_df.loc[:, stat_df.columns.str.contains('CPM')].copy()
    if args.item == 'call':
        cpm_stat.loc['RTT(ms)'] = 60 / cpm_stat.loc['Avg'] * 1000 * args.clients

    race_stat = stat_df.loc[:, stat_df.columns.str.contains('Race')]
    print("各项目每分钟执行次数：")
    print(cpm_stat.to_markdown())

    if args.item == 'call':
        print("各项目事务冲突率：")
        print(race_stat.to_markdown())
