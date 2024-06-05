# 河图的image，通过compose启动

FROM sanicframework/sanic:latest-py3.11

CMD ["hetu", "start", "--config CONFIG.py"]