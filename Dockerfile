# 河图的image
FROM python:3.13-slim

RUN apt-get update && apt-get install -y git

# RUN pip install git+https://github.com/Heerozh/HeTu.git 用copy才能判断文件是否已更改
WORKDIR /src
COPY ./pyproject.toml ./README.md ./
COPY ./hetu ./hetu

RUN pip install .

RUN mkdir /app /logs
VOLUME /app /logs
WORKDIR /

EXPOSE 2466/tcp

ENTRYPOINT ["hetu"]
CMD ["start", "--config /app/config.yml"]