# 河图的image
FROM python:3.14-slim

WORKDIR /src/hetu
COPY ./pyproject.toml ./README.md ./
COPY ./hetu ./hetu

RUN pip install . && rm -rf /src/hetu/

RUN mkdir /app /logs
VOLUME /app /logs
WORKDIR /

EXPOSE 2466/tcp

ENTRYPOINT ["python -O -m hetu"]
CMD ["start", "--config /app/config.yml"]