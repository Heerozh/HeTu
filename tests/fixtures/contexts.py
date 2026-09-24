"""
测试用的连接上下文（`SystemContext`）与几个等待小工具。

`SystemContext` 的构造参数多且大部分测试都不关心，每个模块各抄一份容易走形；
放这里统一造，需要别的 caller / group 时传参即可。
"""

import asyncio
from collections.abc import Callable

import pytest

from hetu.data.backend.base import MQClient
from hetu.data.sub import SubscriptionBroker
from hetu.system import SystemContext


def make_ctx(caller: int = 0, group: str = "", connection_id: int = 0) -> SystemContext:
    """造一个连接上下文。默认是未登录（caller=0、无 group）"""
    return SystemContext(
        caller=caller,
        connection_id=connection_id,
        address="NotSet",
        group=group,
        user_data={},
        timestamp=0,
        request=None,  # type: ignore
        systems=None,  # type: ignore
    )


def admin_ctx_() -> SystemContext:
    """管理员权限的连接上下文：不受 Component 权限与 RLS 限制"""
    return make_ctx(group="admin")


def user_ctx_(caller: int) -> SystemContext:
    """某个已登录用户的连接上下文"""
    return make_ctx(caller=caller)


@pytest.fixture
def admin_ctx() -> SystemContext:
    """管理员权限的ctx（连接上下文）"""
    return admin_ctx_()


@pytest.fixture
def user_id10_ctx() -> SystemContext:
    """用户ID为10的ctx（连接上下文）"""
    return user_ctx_(10)


@pytest.fixture
def user_id11_ctx() -> SystemContext:
    """用户ID为11的ctx（连接上下文）"""
    return user_ctx_(11)


async def wait_until(pred: Callable[[], object], timeout: float = 3.0) -> None:
    """轮询到 pred() 为真（按真值判断，不要求返回 bool），超时抛 TimeoutError。

    用来等后端 hub 把通知投递到 mq 的本地队列这类"迟早会发生但没有钩子"的事。
    """
    async with asyncio.timeout(timeout):
        while not pred():
            await asyncio.sleep(0.01)


async def settled_updates(broker: SubscriptionBroker, timeout: float = 5.0) -> dict:
    """等订阅推送安静下来，返回期间全部推送合并后的结果 {sub_id: {row_id: 最后推的行}}。

    一次写入引起的推送可能分几批到：合并进队头的通知会在一个 interval 后尾随重读，订阅
    生效后也会补读一次，它们和下一轮写入的通知谁先弹出取决于时序。断言"写入之后客户端
    最终看到什么"时用它，别假设一次 get_updates 就拿全。第一批最多等 timeout 秒，之后
    连续 2.5 个 interval 没有新推送就算安静。
    """
    updates = await broker.get_updates(timeout=timeout)
    quiet = 2.5 / MQClient.UPDATE_FREQUENCY
    while more := await broker.get_updates(timeout=quiet):
        for sub_id, rows in more.items():
            updates.setdefault(sub_id, {}).update(rows)
    return updates
