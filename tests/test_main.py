import os
import sys
import unittest
import redis

from hetu.__main__ import main
from hetu.system import SystemClusters


class MyTestCase(unittest.TestCase):

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

        sys.argv[1:] = ['start', '--namespace=ssw', '--instance=unittest1']
        with self.assertRaises(FileNotFoundError):
            main()

        # 正常启动
        cfg_file = os.path.join(os.path.dirname(__file__), 'config.py')
        sys.argv[1:] = ['start', '--config', cfg_file]
        SystemClusters()._clear()
        with self.assertRaises(redis.exceptions.ConnectionError):
             main()


if __name__ == '__main__':
    unittest.main()
