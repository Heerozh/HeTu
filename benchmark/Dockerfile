# 阿里云ECS内网镜像
FROM registry-vpc.cn-shanghai.aliyuncs.com/heerozh/hetu:latest-nightly

WORKDIR .

COPY app.py .

CMD [ \
    "start",\
    "--app-file=./app.py",\
    "--db=${REDIS_URL}",\
    "--namespace=bench",\
    "--instance=docker_bench",\
    "--workers=76"\
]

# 使用方式：
# REDIS_URL="redis://:password@host:6379/0"
# docker build -t hetu_bench_srv .
# docker run -it --rm -p 2466:2466 --name my-running-bench hetu_bench_srv