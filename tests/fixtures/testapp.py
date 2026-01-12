import pytest
from hetu.endpoint.executor import EndpointExecutor


@pytest.fixture(scope="module")
def mod_test_app():
    import hetu
    import app
    import importlib

    hetu.data.ComponentDefines().clear_()
    hetu.endpoint.definer.EndpointDefines()._clear()
    hetu.system.SystemClusters()._clear()

    importlib.reload(app)

    # 初始化SystemCluster
    hetu.system.SystemClusters().build_clusters("pytest")
    hetu.system.SystemClusters().build_endpoints()
    return app


def comp_mgr_factory(mod_auto_backend):
    # 为每个test初始化comp_mgr，因为每个test的线程不同
    backends = {"default": mod_auto_backend()}

    from hetu.manager import ComponentTableManager
    from hetu.system.definer import SystemClusters

    if SystemClusters().get_clusters("pytest") is None:
        raise RuntimeError("需要至少含有一个test_app，比如添加mod_test_app夹具")

    comp_mgr = ComponentTableManager("pytest", "server1", backends)
    comp_mgr._flush_all(force=True)

    return comp_mgr


@pytest.fixture(scope="module")
def mod_comp_mgr(mod_auto_backend):
    return comp_mgr_factory(mod_auto_backend)


@pytest.fixture(scope="function")
def comp_mgr(mod_auto_backend):
    return comp_mgr_factory(mod_auto_backend)


@pytest.fixture
async def new_ctx(comp_mgr):
    """SystemContext factory"""
    from hetu.system import SystemContext
    from hetu.system.caller import SystemCaller

    def create_ctx() -> SystemContext:
        ctx = SystemContext(
            caller=0,
            connection_id=0,
            address="NotSet",
            group="",
            user_data={},
            timestamp=0,
            request=None,  # type: ignore
            systems=None,  # type: ignore
        )
        systems = SystemCaller("pytest", comp_mgr, ctx)
        ctx.systems = systems
        return ctx

    return create_ctx


@pytest.fixture(scope="module")
async def mod_new_ctx(mod_comp_mgr):
    """SystemContext factory"""
    from hetu.system import SystemContext
    from hetu.system.caller import SystemCaller

    def create_ctx() -> SystemContext:
        ctx = SystemContext(
            caller=0,
            connection_id=0,
            address="NotSet",
            group="",
            user_data={},
            timestamp=0,
            request=None,  # type: ignore
            systems=None,  # type: ignore
        )
        systems = SystemCaller("pytest", mod_comp_mgr, ctx)
        ctx.systems = systems
        return ctx

    return create_ctx


@pytest.fixture(scope="module")
async def mod_executor(mod_comp_mgr, mod_new_ctx):
    from hetu.endpoint.executor import EndpointExecutor

    executor = EndpointExecutor("pytest", mod_comp_mgr, mod_new_ctx())
    await executor.initialize("")
    yield executor

    # 结束连接
    await executor.terminate()


@pytest.fixture(scope="function")
async def executor(comp_mgr, new_ctx):
    executor = EndpointExecutor("pytest", comp_mgr, new_ctx())
    await executor.initialize("")
    yield executor

    # 结束连接
    await executor.terminate()
