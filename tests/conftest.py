import logging
import os
import sys

import pytest

os.environ["LANGUAGE"] = "zh_CN"
os.environ["LANG"] = "zh_CN.UTF-8"
os.environ["LC_ALL"] = "zh_CN.UTF-8"

from fixtures.backends import *
from fixtures.contexts import *
from fixtures.defines import *
from fixtures.redis_service import *
from fixtures.sql_service import *
from fixtures.testapp import *
from fixtures.testdata import *

# set default lang


@pytest.hookimpl(tryfirst=True)
def pytest_cmdline_main(config):
    """`-n` 并行（pytest-xdist）且没指定 `--dist` 时，默认用 loadgroup 调度"""
    if getattr(config.option, "numprocesses", None) and (
        getattr(config.option, "dist", None) == "no"
    ):
        config.option.dist = "loadgroup"


@pytest.hookimpl(optionalhook=True)
def pytest_configure_node(node):
    """worker 只会从命令行参数解析 `--dist`，上面改的调度模式要另外传过去"""
    node.workerinput["hetu_loadgroup"] = node.config.getvalue("dist") == "loadgroup"


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config, items):
    """
    loadgroup 调度时以（测试文件, 后端）为单位分给 xdist worker：同一单位的测试在同一
    worker 上连续跑完，模块级夹具不会在多个 worker 上重复初始化。每个 worker 是独立
    进程，只启动自己用到的后端容器（见 fixtures/docker_infra.py）。
    """
    if getattr(config, "workerinput", {}).get("hetu_loadgroup"):
        # xdist 自己的 collection 钩子看到这个开关，才会按 xdist_group 标记分组
        config.option.loadgroup = True
    if not getattr(config.option, "loadgroup", False):
        return
    for item in items:
        group = item.nodeid.split("::", 1)[0]
        callspec = getattr(item, "callspec", None)
        backend = callspec.params.get("backend_name") if callspec else None
        # HETU_TEST_BACKENDS 把某个参数化列表滤空时，pytest 生成的跳过项参数是 NOTSET
        if isinstance(backend, str):
            group += ":" + backend
        item.add_marker(pytest.mark.xdist_group(group))


@pytest.fixture(autouse=True, scope="module")
def reset_snowflake_lease():
    """
    worker_main 起的服务会给进程级单例 SnowflakeID 挂上 WorkerKeeper 租约，服务停了就
    不再续约，60 秒后同一进程里别的测试一发号就 WorkerLeaseExpired。串行时这类测试恰好
    排在最后才没暴露；xdist 下 worker 跑测试文件的顺序不定，所以每个模块开始前清掉。
    """
    from hetu.common.snowflake_id import SnowflakeID

    SnowflakeID().lease = None


@pytest.fixture(autouse=True, scope="session")
def force_print_logging():
    """
    修复PyCharm测试控制台不显示错误日志的问题
    """
    # 1. 获取根记录器 & replay 记录器
    root_logger = logging.getLogger()
    replay_logger = logging.getLogger("HeTu.replay")
    replay_logger.setLevel(logging.DEBUG)

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
    replay_logger.addHandler(stream_handler)

    yield

    # 6. 清理：测试结束后移除这个 handler，防止污染
    root_logger.removeHandler(stream_handler)
    replay_logger.removeHandler(stream_handler)
