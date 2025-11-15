import pytest

@pytest.fixture
def new_clusters_env():
    from hetu.system import SystemClusters
    SystemClusters()._clear()
    return None


@pytest.fixture
def new_component_env():
    from hetu.data import ComponentDefines
    ComponentDefines().clear_()
    return None