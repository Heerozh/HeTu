import os
import sys

import pytest

from hetu.__main__ import main
from hetu.data.backend import HeadLockFailed


def test_required_parameters():
    sys.argv[1:] = []
    with pytest.raises(SystemExit):
        main()

    sys.argv[1:] = ["start"]
    with pytest.raises(SystemExit):
        main()

    sys.argv[1:] = ["start", "--namespace=ssw"]
    with pytest.raises(SystemExit):
        main()

    sys.argv[1:] = ["start", "--namespace=ssw", "--instance=unittest1", "--debug=True"]
    with pytest.raises(FileNotFoundError):
        main()


@pytest.mark.timeout(10, method="thread")
def test_start_with_redis_head_lock(new_clusters_env, mod_redis_backend):
    # 使用CONFIG_TEMPLATE.yml启动防止模板内容错误
    cfg_file = os.path.join(os.path.dirname(__file__), "../CONFIG_TEMPLATE.yml")
    sys.argv[1:] = ["start", "--config", cfg_file]
    os.chdir(os.path.join(os.path.dirname(__file__)))
    # 启动6379的redis服务
    backend_component_table, get_or_create_backend = mod_redis_backend
    backend = get_or_create_backend(port=6379)
    backend.requires_head_lock()
    # 如果HeadLockFailed没触发，会导致服务器启动成功，然后就卡死了，所以要timeout
    # RuntimeError是sanic在pypi下会报错，也许未来升级了会修复
    try:
        with pytest.raises(HeadLockFailed):
            main()
    except RuntimeError as e:
        if "This event loop is already running" in str(e):
            pytest.xfail(f"Temporary failure: {e}")
        else:
            raise
    backend.close()
