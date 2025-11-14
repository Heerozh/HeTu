import pytest

@pytest.fixture
def new_clusters_env():
    from hetu.system import SystemClusters
    SystemClusters()._clear()
    return None