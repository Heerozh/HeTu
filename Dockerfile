# 河图的image
FROM sanicframework/sanic:latest-py3.11

RUN apk add git
RUN apk add redis

RUN pip install https://github.com/Heerozh/HeTu.git
# COPY ./ /
# RUN rm -rf /logs
# RUN pip install /

ENV HETU_RUN_REDIS=1

RUN mkdir /data /app /logs
VOLUME /data /app /logs
WORKDIR /

EXPOSE 2466/tcp

ENTRYPOINT ["hetu"]
CMD ["start"]