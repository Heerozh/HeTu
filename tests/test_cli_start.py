import os
import sys

import pytest

from hetu.__main__ import main
from hetu.data.backend import HeadLockFailed

def test_required_parameters():
    sys.argv[1:] = []
    with pytest.raises(SystemExit):
        main()

    sys.argv[1:] = ['start']
    with pytest.raises(SystemExit):
        main()

    sys.argv[1:] = ['start', '--namespace=ssw']
    with pytest.raises(SystemExit):
        main()

    sys.argv[1:] = ['start', '--namespace=ssw', '--instance=unittest1', '--debug=True']
    with pytest.raises(FileNotFoundError):
        main()


def test_head_lock(new_clusters_env, mod_auto_backend):
    # 使用CONFIG_TEMPLATE.yml启动防止模板内容错误
    cfg_file = os.path.join(os.path.dirname(__file__), '../CONFIG_TEMPLATE.yml')
    sys.argv[1:] = ['start', '--config', cfg_file]
    os.chdir(os.path.join(os.path.dirname(__file__)))
    # 阻止启动的最后一步，不然就卡死了
    backend_component_table, get_or_create_backend = mod_auto_backend
    backend = get_or_create_backend()
    backend.requires_head_lock()
    # 如果卡在这里，说明启动成功了，不应该，考虑加个timeout
    with pytest.raises(HeadLockFailed):
        main()
