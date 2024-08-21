import argparse
import asyncio
import json
import multiprocessing
import random
import ssl
import string
import time
import zlib

import redis

try:
    import tabulate
    import pandas as pd
except ImportError:
    raise ImportError("压测程序要安装pandas + tabulate库：pip install pandas tabulate")
from collections import defaultdict
from functools import partial
from multiprocessing import Pool, Process
from hetu.data.backend import (
    RedisComponentTable, RedisBackend,
    RaceCondition
)
import websockets
import app
_ = tabulate

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
    print(name, '正在连接', pid, '号客户端')
    try:
        async with websockets.connect(address, ssl=ssl_context) as ws:
            await asyncio.sleep(10) # 等待其他协程连接成功
            print(name, '开始测试', pid, '号客户端', duration, '分钟运行时间')
            while True:
                # 随机读写30w数据中的一个
                raw_call = packet()
                # 组合封包
                call = json.dumps(raw_call).encode()
                # call = zlib.compress(call)
                # 为了测试准确的性能，采用call-response模式
                await ws.send(call)
                # 统计事务冲突率
                received = await ws.recv()
                # received = zlib.decompress(received)
                received = json.loads(received)
                # 记录当前分钟的执行数
                cur_min = int(time.time() // 60)
                call_count[cur_min] += 1
                if type(received[0]) is int:
                    retry_count[cur_min] += received[0]
                else:
                    retry_count[cur_min] += 0
                if len(call_count) > duration:
                    del call_count[cur_min]
                    del retry_count[cur_min]
                    break
    except (websockets.exceptions.ConnectionClosedError, TimeoutError):
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
            await asyncio.sleep(10)
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
                if random.randint(0, 100) == 0:
                    print(f"{pid}号客户端收到数据", len(recv_count), recv_count[cur_min])
                if len(recv_count) > duration:
                    del recv_count[cur_min]
                    break
    except (websockets.exceptions.ConnectionClosedError, TimeoutError):
        print(pid, '号客户端连接断开，提前结束测试')
        pass
    return recv_count, recv_count


async def bench_pubsub_updater(redis_address, instance_name, pid):
    aio = redis.asyncio.from_url(redis_address)
    idx = f"{instance_name}:IntTable:{{CLU0}}:index:number"
    keys = await aio.zrange(idx, start=0, end=100, byscore=True)
    keys = [f"{instance_name}:IntTable:{{CLU0}}:id:{key.decode()}" for key in keys]
    if len(keys) == 0:
        print('没有数据，请先运行call bench生成数据')
        return
    while True:
        rnd_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))
        key = random.choice(keys)
        if random.randint(0, 10000) == 0:
            print(f"{pid}号客户端更新数据", key, rnd_str)
        await aio.hset(key, key='name', value=rnd_str)


async def bench_direct_backend_routine(address, duration, name: str, pid: str):
    call_count = defaultdict(int)
    retry_count = defaultdict(int)

    backend = RedisBackend({"master": address})
    int_tbl = RedisComponentTable(app.IntTable, 'bench1', 0, backend)

    print(name, '开始测试', pid, '号客户端', duration, '分钟运行时间')
    try:
        while True:
            cur_min = int(time.time() // 60)
            try:
                async with backend.transaction(0) as trx:
                    tbl = int_tbl.attach(trx)
                    sel_num = random.randint(1, BENCH_ROW_COUNT)
                    async with tbl.select_or_create(sel_num, 'number') as row:
                        row.name = ''.join(
                            random.choices(string.ascii_uppercase + string.digits, k=3))
                call_count[cur_min] += 1
                retry_count[cur_min] += 0
                if random.randint(0, 10000) == 0:
                    print(f"{pid}号客户端状态：", cur_min, call_count[cur_min], retry_count[cur_min])
                if len(call_count) > duration:
                    del call_count[cur_min]
                    del retry_count[cur_min]
                    break
            except RaceCondition as e:
                retry_count[cur_min] += 1
                continue
    except redis.exceptions.ConnectionError:
        print(pid, '号客户端连接断开，提前结束测试')
        pass
    finally:
        await backend.close()
    return call_count, retry_count


async def bench_direct_redis_routine(address, duration, name: str, pid: str):
    call_count = defaultdict(int)
    retry_count = defaultdict(int)

    aio = redis.asyncio.Redis.from_url(address, decode_responses=True)
    idx_key = 'bench1:IntTable:{CLU0}:index:number'

    print(name, '开始测试', pid, '号客户端', duration, '分钟运行时间')
    try:
        while True:
            cur_min = int(time.time() // 60)
            # 直接 ZRANGE, WATCH, HGETALL, MULTI, HSET, EXEC 指令测试
            sel_num = random.randint(1, BENCH_ROW_COUNT)

            pipe = aio.pipeline()
            pipe.watching = True
            # 1.zrange->select row
            row_ids = await pipe.zrange(
                idx_key, sel_num, sel_num,
                byscore=True, offset=0, num=1, withscores=True)

            # 以下代码不正确，只是为了模拟插入
            insert = False
            if len(row_ids) == 0:
                row_ids = await pipe.zrange(idx_key, 0, 0, desc=True, withscores=True)
                if len(row_ids) == 0:
                    row_id = 1
                else:
                    row_id = int(row_ids[0][1]) + 1
                insert = True
            else:
                row_id = int(row_ids[0][1])
                if row_id == 0:
                    print(row_ids)

            # 2.watch
            row_key = f"bench1:IntTable:{{CLU0}}:id:{row_id}"
            await pipe.watch(row_key)
            # 3.hgetall
            if insert:
                row = {'number': sel_num, 'id': row_id}
            else:
                row = await pipe.hgetall(row_key)
            row['name'] = ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))

            # 4.multi
            pipe.multi()
            # 5.hset
            pipe.hset(row_key, mapping=row)

            # 6.exec
            try:
                await pipe.execute()
            except redis.exceptions.WatchError:
                retry_count[cur_min] += 1
                delay = random.random() / 5
                await asyncio.sleep(delay)
                continue

            call_count[cur_min] += 1
            retry_count[cur_min] += 0
            if random.randint(0, 10000) == 0:
                print(f"{pid}号客户端状态：", cur_min, call_count[cur_min], retry_count[cur_min])
            if len(call_count) > duration:
                del call_count[cur_min]
                del retry_count[cur_min]
                break
    except redis.exceptions.ConnectionError:
        print(pid, '号客户端连接断开，提前结束测试')
        pass
    finally:
        await aio.aclose()
    return call_count, retry_count

async def bench_just_select(address, duration, name: str, pid: str):
    def packet():
        row_id = random.randint(1, BENCH_ROW_COUNT)
        return ['sys', 'just_select', row_id]

    return await bench_sys_call_routine(address, duration, name, pid, packet)

async def bench_select_update(address, duration, name: str, pid: str):
    def packet():
        row_id = random.randint(1, BENCH_ROW_COUNT)
        return ['sys', 'select_and_update', row_id]

    return await bench_sys_call_routine(address, duration, name, pid, packet)


async def bench_exchange_data(address, duration, name: str, pid: str):
    def packet():
        rnd_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))
        row_id = random.randint(1, BENCH_ROW_COUNT)
        return ['sys', 'exchange_data', rnd_str, row_id]

    return await bench_sys_call_routine(address, duration, name, pid, packet)


async def bench_hello_world(address, duration, name: str, pid: str):
    def packet():
        return ['sys', 'hello_world']

    return await bench_sys_call_routine(address, duration, name, pid, packet)


async def gather_clients(func, client_num, pid: int):
    client_num = max(client_num, 1)
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
        if len(df) > 1:
            df = df.iloc[1:]
        if len(df) > 3:
            df = df.iloc[:-1]
        else:
            print('测试时间不够，数据不准确。')
        return df.sum(axis=1)

    call_count = [x[0] for x in list3d]
    retry_count = [x[1] for x in list3d]
    cpm = format_df(call_count)
    cpm.name = test_name + '(Calls)'
    race = round(format_df(retry_count) / cpm * 100, 3)
    race.name = test_name + '(Race%)'
    return pd.concat([cpm, race], axis=1)


def run_bench(func, _args, name):
    cpu = multiprocessing.cpu_count()
    process_num = cpu * 2
    process_num = _args.clients if process_num > _args.clients else process_num
    clients = max(1, int(_args.clients // process_num))

    atest = partial(func, _args.address, _args.time, name)
    test = partial(run_client, atest, clients)

    with Pool(process_num) as p:
        results = p.map(test, list(range(process_num)))
    print("所有进程推出，执行完毕")
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
    parser.add_argument("--redis", default='redis://127.0.0.1:6379/0',
                        help="pubsub bench用，服务器redis url")
    parser.add_argument("--inst_name", default='bench1',
                        help="pubsub bench用，hetu实例名称")

    args = parser.parse_args()

    # 分配clients个进程和连接进行测试
    match args.item:
        case 'call':
            all_results = [
                run_bench(bench_hello_world, args, 'hello world'),
                run_bench(bench_just_select, args, 'just select'),
                run_bench(bench_select_update, args, 'select + update'),
                run_bench(bench_exchange_data, args, 'select*2 + update*2'),
            ]
        case 'pubsub':
            # 再开一个写入进程
            updater = partial(bench_pubsub_updater, args.redis, args.inst_name)
            async_updater = partial(run_client, updater, args.clients // 10)
            p_uper = Process(target=async_updater, args=(1,))
            p_uper.start()
            # 开始等待消息
            all_results = [
                run_bench(bench_pubsub_routine, args, 'pubsub')
            ]
        case 'direct':
            all_results = [
                run_bench(bench_direct_redis_routine, args, 'direct redis'),
                run_bench(bench_direct_backend_routine, args, 'direct backend'),
            ]
        case _:
            raise ValueError('--item未知测试项目')

    # 汇总统计
    stat_df = pd.concat(all_results, axis=1)
    avg = stat_df.mean()
    stat_df.loc['Avg'] = avg
    stat_df.loc['Avg(每秒)'] = avg / 60

    cpm_stat = stat_df.loc[:, stat_df.columns.str.contains('Calls')].copy()
    # 如果只启动1个客户端（服务器压力不大时），显示RTT数据
    if args.item == 'call' and args.clients == 1:
        cpm_stat.loc['RTT(ms)'] = 60 / cpm_stat.loc['Avg'] * 1000 * args.clients

    print("各项目每分钟执行次数：")
    print(cpm_stat.to_markdown())

    match args.item:
        case 'call' | 'direct':
            print("各项目事务冲突率：")
            race_stat = stat_df.loc[:, stat_df.columns.str.contains('Race')]
            race_stat = race_stat.loc[~race_stat.index.str.contains('秒')]
            print(race_stat.to_markdown())
        case 'pubsub':
            p_uper.kill()

