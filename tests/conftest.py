from fixtures.backends import *
from fixtures.testdata import *
from fixtures.defines import *
from fixtures.testapp import *


import logging
import sys


@pytest.fixture(autouse=True, scope="session")
def force_print_logging():
    """
    修复PyCharm测试控制台不显示错误日志的问题
    """
    # 1. 获取根记录器
    root_logger = logging.getLogger()

    # 2. 创建一个流处理器，直接指向 stderr (PyCharm 控制台能捕获 stderr)
    stream_handler = logging.StreamHandler(sys.stderr)

    # 3. 设置格式 (可选，根据你的需要调整)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    stream_handler.setFormatter(formatter)

    # 4. 设置级别 (确保它能打印出来，或者跟随 root 的级别)
    stream_handler.setLevel(logging.INFO)

    # 5. 将处理器添加到 root logger
    root_logger.addHandler(stream_handler)

    yield

    # 6. 清理：测试结束后移除这个 handler，防止污染
    root_logger.removeHandler(stream_handler)
