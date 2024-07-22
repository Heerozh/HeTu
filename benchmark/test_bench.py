import argparse
import json
import pickle
import random
import time
import websockets
from multiprocessing import Pool
from functools import partial
import zlib
import ssl
from collections import defaultdict
import asyncio


def test_login():
    pass


async def test_select_update(address, iters, pid):
    raw_call = ['sys', 'select_and_update', 1]
    count = defaultdict(int)
    if address.startswith('wss://'):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE  # 应该使用ssl_context.load_cert_chain(cert_file)更安全
    else:
        ssl_context = None
    print('开始测试', pid, '号客户端', iters, '次请求')
    async with websockets.connect(address, ssl=ssl_context) as ws:
        for _ in range(iters):
            # 随机读写30w数据中的一个
            row_id = random.randint(1, 300000)
            raw_call[2] = row_id
            # 组合封包
            call = json.dumps(raw_call).encode()
            call = zlib.compress(call)
            # 为了测试准确的性能，采用call-response模式
            await ws.send(call)
            _ = await ws.recv()
            # 记录当前分钟的执行数
            cur_min = time.time() % 3600 // 60
            count[cur_min] += 1
    # # 存到当前目录
    # pkl = pickle.dumps(count, protocol=5)
    # with open(f"count_{pid}.pkl", 'wb') as f:
    #     f.write(pkl)
    return count


def test_exchange_data():
    pass


def test_pubsub():
    pass


def run_test(func, pid):
    return asyncio.run(func(pid))


def stat_count():
    # 然后运行结束后读取并汇总
    pass


if __name__ == '__main__':
    # 通过ws测试完整流程的性能
    # 此方法需要手动部署hetu服务器和backend，和测试机分开
    parser = argparse.ArgumentParser(prog='hetu', description='Hetu full bench')
    parser.add_argument("--address", type=str, default="ws://127.0.0.1:2466/hetu/")
    parser.add_argument("--clients", type=int, default=30)
    parser.add_argument("--iters", type=int, default=5000)
    args = parser.parse_args()

    # 分配clients个进程和连接进行测试
    atest = partial(test_select_update, args.address, args.iters)
    test = partial(run_test, atest)
    with Pool(args.clients) as p:
        print(p.map(test, list(range(args.clients))))


