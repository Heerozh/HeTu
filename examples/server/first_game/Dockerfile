# 如果是阿里云内网请用 registry-vpc.cn-shanghai.aliyuncs.com/heerozh/hetu:latest
FROM registry.cn-shanghai.aliyuncs.com/heerozh/hetu:latest

WORKDIR /app

COPY . .
COPY ./config-for-docker.yml ./config.yml
RUN pip install .

ENTRYPOINT ["hetu", "start", "--config=./config.yml"]
# CMD [ "start", "--config ./config.yml" ]