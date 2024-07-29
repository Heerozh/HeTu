import argparse
import pandas as pd
import re
import matplotlib.pyplot as plt

# 必须在Redis CPU 100%时，保存的MONITOR的日志（或者用Insight工具保存），然后用这个脚本分析

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='hetu', description='Hetu redis monitor log 绘图')
    parser.add_argument("--file", type=str)
    args = parser.parse_args()

    with open(args.file) as f:
        data = [list(re.match(r'^(\d*\.\d*) \[.*] "(.+?)" ?.*', line).groups())
                for line in f]

    df = pd.DataFrame(data)
    df.rename(columns={0: 'cost', 1: 'cmd'}, inplace=True)
    df.cost = pd.to_datetime(df.cost.astype(float), unit='s')
    df.cost = -(df.cost.diff(-1)).dt.total_seconds()
    df.cmd = df.cmd.str.lower()

    df.groupby('cmd').sum().sort_values('cost', ascending=False).plot(kind='barh', legend=False)
    plt.title('Redis命令总耗时', family=['SimHei'])
    plt.show()

    count = df.cmd.value_counts()
    less_item = count.index[count.gt(count.quantile(0.15))]
    df = df[df.cmd.isin(less_item)]
    df.groupby('cmd').mean().sort_values('cost', ascending=False).plot(kind='barh', legend=False)
    plt.title('Redis命令平均耗时', family=['SimHei'])
    plt.show()
