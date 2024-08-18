# 河图的image
FROM python:3.11-alpine3.20

RUN apk add git
RUN apk add redis

# RUN pip install git+https://github.com/Heerozh/HeTu.git 用copy才能判断文件是否已更改
COPY ./hetu /hetu
COPY ./setup.py /setup.py
COPY ./requirements.txt /requirements.txt
COPY ./README.md /README.md
RUN pip install /

ENV HETU_RUN_REDIS=1

RUN mkdir /data /app /logs
VOLUME /data /app /logs
WORKDIR /

EXPOSE 2466/tcp

ENTRYPOINT ["hetu"]
CMD ["start", "--config /app/config.yml"]