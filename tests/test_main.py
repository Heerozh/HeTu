import os
import sys
import unittest
import redis

from backend_mgr import UnitTestBackends
from hetu.data.backend import HeadLockFailed
from hetu.__main__ import main
from hetu.system import SystemClusters


class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        SystemClusters()._clear()
        cls.backend_mgr = UnitTestBackends()
        cls.backend_mgr.start_redis_server(6379)

    @classmethod
    def tearDownClass(cls):
        cls.backend_mgr.teardown()

    def test_required_parameters(self):
        sys.argv[1:] = []
        with self.assertRaises(SystemExit):
            main()

        sys.argv[1:] = ['start']
        with self.assertRaises(SystemExit):
            main()

        sys.argv[1:] = ['start', '--namespace=ssw']
        with self.assertRaises(SystemExit):
            main()

        sys.argv[1:] = ['start', '--namespace=ssw', '--instance=unittest1', '--debug=True']
        with self.assertRaises(FileNotFoundError):
            main()

        # 正常启动
        cfg_file = os.path.join(os.path.dirname(__file__), 'config.py')
        sys.argv[1:] = ['start', '--config', cfg_file]
        SystemClusters()._clear()
        # 阻止启动的最后一步，不然就卡死了
        r = redis.Redis(host='127.0.0.1', port=6379)
        r.set('head_lock', '1')
        with self.assertRaises(HeadLockFailed):
             main()


if __name__ == '__main__':
    unittest.main()
