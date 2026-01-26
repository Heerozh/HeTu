# 河图的image
FROM python:3.14-slim

# 从项目文件安装Hetu
WORKDIR /src/hetu
COPY ./pyproject.toml ./README.md ./
COPY ./hetu ./hetu
RUN pip install --no-cache-dir . && rm -rf /src/hetu/

# 创建一个非 root 用户和工作目录
RUN addgroup --system --gid 1001 hetu
RUN adduser --system --uid 1001 hetu

# 用户应用目录
RUN mkdir /app /logs
VOLUME /app /logs
RUN chown -R hetu:hetu /app /logs
WORKDIR /

# 切换用户
USER hetu

# 入口
EXPOSE 2466/tcp

ENTRYPOINT ["python -m hetu"]
#ENTRYPOINT ["python -O -m hetu"]  项目成熟后再开-O
CMD ["start", "--config /app/config.yml"]