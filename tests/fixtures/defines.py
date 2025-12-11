import pytest


@pytest.fixture
def new_clusters_env():
    """
    清理System定义，保证每个测试用例使用干净的System定义环境
    """
    from hetu.system import SystemClusters

    SystemClusters()._clear()
    return None


@pytest.fixture
def new_component_env():
    """
    清理Component定义，保证每个测试用例使用干净的Component定义环境
    """
    from hetu.data import ComponentDefines

    ComponentDefines().clear_()
    return None


@pytest.fixture(scope="module")
def mod_new_component_env():
    """
    清理Component定义，保证每个测试用例使用干净的Component定义环境
    """
    from hetu.data import ComponentDefines

    ComponentDefines().clear_()
    return None
