"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

from typing import TYPE_CHECKING, cast, Callable, AsyncIterator
import asyncio
from contextlib import AbstractAsyncContextManager
from .base import RaceCondition

from .idmap import IdentityMap
from .repo import SessionRepository

if TYPE_CHECKING:
    from . import Backend, BackendClient
    from hetu.data.component import BaseComponent


class Session(AbstractAsyncContextManager):
    """
    数据库事务类，用于缓存数据修改，并最终提交到数据库。

    Examples
    --------
    标准写法::
        async with backend.session() as session:
            repo = session.using(ComponentClass)
            # do something with repo
            ...
            # 离开时会自动提交数据

    重试体写法::

        async for attempt in backend.session().retry(3):
            async with attempt as session:
                repo = session.using(ComponentClass)
                # do something with repo
                ...

    """

    def __init__(self, backend: Backend, instance: str, cluster_id: int) -> None:
        self.instance_name = instance
        self.cluster_id = cluster_id
        self._backend = backend
        self._master = backend.master
        self._idmap = cast(IdentityMap, object())
        self._entered = False
        self.only_master = False

        self.clean()
        # todo 要检测是否在session中又开了一个session，如果是，应该报错，毕竟嵌套session没意义
        #      或者只是在system中不允许开session，因为其他地方他开是明确的，能知道自己嵌套了
        #      或者允许嵌套，但是内部的必须加上lock防止重复运行。但这也意义不大，2者数据互通还是会很多问题

    def clean(self):
        """清除Session缓存的数据状态"""
        self._idmap = IdentityMap()
        self._entered = False

    def using(self, comp_cls: type[BaseComponent]):
        return SessionRepository(self, comp_cls)

    @property
    def master(self) -> BackendClient:
        assert self._entered, "Session must be used in `async with` block"
        return self._master

    @property
    def master_or_servant(self) -> BackendClient:
        assert self._entered, "Session must be used in `async with` block"
        if self.only_master:
            return self._master
        else:
            return self._backend.master_or_servant

    @property
    def idmap(self) -> IdentityMap:
        assert self._entered, "Session must be used in `async with` block"
        return self._idmap

    async def commit(self) -> None:
        """
        向数据库提交Session中的所有数据修改

        Exceptions
        --------
        RaceCondition
            当提交数据时，发现数据已被其他事务修改，抛出此异常
        """
        # 如果数据库不具备写入通知功能，要在此手动往MQ推送数据变动消息。
        if self._idmap.is_dirty:
            await self._master.commit(self._idmap)
        self.clean()

    def discard(self) -> None:
        """放弃Session中的所有数据修改"""
        self.clean()

    async def __aenter__(self):
        self._entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self.commit()
        else:
            self.discard()

    def retry(self, count: int = 5) -> AsyncSessionRetryGenerator:
        """
        返回一个可重试的生成器，当提交数据时遇到RaceCondition异常会自动重试。

        Examples
        --------
        ::
            async for attempt in backend.session().retry(3):
                async with attempt as session:
                    repo = session.using(ComponentClass)
                    # do something with repo
                    ...

        Parameters
        ----------
        count : int
            最大重试次数，默认5次

        Returns
        -------
        一个可以sync for的生成器，每次迭代返回一个attempt对象，可以在async with中使用。
        """
        return AsyncSessionRetryGenerator(
            session=self, times=count, backoff=lambda attempt: (2**attempt) * 0.1
        )


class RetryAttempt(AbstractAsyncContextManager):
    def __init__(self, session, index: int, is_last: bool):
        self.session = session
        self.is_last = is_last
        self.index: int = index  # 记录重试次数，只有在retry中使用才会增加
        self.success = False  # 标记本次尝试是否成功提交

    async def __aenter__(self):
        # 可以在这里做一些清理或 begin 操作
        await self.session.__aenter__()
        return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # 1. 如果 Body 内发生了 RaceCondition
        if exc_type and issubclass(exc_type, RaceCondition):
            self.session.discard()
            if self.is_last:
                return False
            else:
                return True  # 抑制异常，不让循环中断

        # 2. 如果 Body 正常，尝试 Commit
        if exc_type is None:
            try:
                await self.session.commit()
                self.success = True
            except RaceCondition as e:
                self.session.discard()
                if self.is_last:
                    raise RuntimeError("Exceeded maximum retry attempts") from e
                return True  # 同样抑制异常，触发重试

        # 其他异常正常抛出
        return False


class AsyncSessionRetryGenerator:
    def __init__(
        self, *, session: Session, times: int, backoff: Callable[[int], float] | None
    ):
        if times <= 0:
            raise ValueError("times must be > 0")
        self._session = session
        self._times = times
        self._backoff = backoff

    def __aiter__(self) -> AsyncIterator[RetryAttempt]:
        return self._gen()

    async def _gen(self) -> AsyncIterator[RetryAttempt]:
        session = self._session

        for i in range(self._times):
            is_last = i == self._times - 1

            # 把本次 session 交给用户的循环体
            attempt = RetryAttempt(session, i, is_last)
            yield attempt

            if attempt.success:
                return
            else:
                if self._backoff is not None:
                    delay = float(self._backoff(i))
                    if delay > 0:
                        await asyncio.sleep(delay)

        # 理论上走不到这；RetryAttempt最后一次不会抑制异常
        raise RuntimeError("Unreachable code in AsyncSessionRetryGenerator")
