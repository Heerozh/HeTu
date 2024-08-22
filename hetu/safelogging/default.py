"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""

import sys

DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'generic',
            'stream': sys.stdout,
        },
    },
    'loggers': {
        'root': {
            'handlers': ['console'],
            'level': 'INFO',
        },
        'HeTu.root': {
            'level': 'INFO',
        },
        'HeTu.replay': {
            'level': 'ERROR',
            'propagate': False,
        },
    },
    'formatters': {
        'generic': {
            'class': 'sanic.logging.formatter.AutoFormatter',
        },
    },
}
