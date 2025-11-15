import pytest


@pytest.fixture(scope="module")
def mod_test_app():
    import hetu

    hetu.data.ComponentDefines().clear_()
    hetu.system.SystemClusters()._clear()

    import app

    # 初始化SystemCluster
    hetu.system.SystemClusters().build_clusters("pytest")
    return app


@pytest.fixture(scope="module")
def mod_comp_mgr(mod_auto_backend):
    backend_component_table, get_or_create_backend = mod_auto_backend

    # 为每个test初始化comp_mgr，因为每个test的线程不同
    backends = {"default": get_or_create_backend()}
    comp_tbl_classes = {"default": backend_component_table}

    import hetu

    return hetu.ComponentTableManager("pytest", "server1", backends, comp_tbl_classes)


@pytest.fixture(scope="function")
def comp_mgr(mod_comp_mgr):
    return mod_comp_mgr


@pytest.fixture(scope="module")
async def mod_executor(mod_comp_mgr):
    import hetu

    executor = hetu.system.SystemExecutor("pytest", mod_comp_mgr)
    await executor.initialize("")
    yield executor

    # 结束连接
    await executor.terminate()


@pytest.fixture(scope="function")
async def executor(comp_mgr):
    import hetu

    executor = hetu.system.SystemExecutor("pytest", comp_mgr)
    await executor.initialize("")
    yield executor

    # 结束连接
    await executor.terminate()
