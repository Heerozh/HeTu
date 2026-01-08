"""
@author: Heerozh (Zhang Jianhao)
@copyright: Copyright 2024, Heerozh. All rights reserved.
@license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
@email: heeroz@gmail.com
"""


class EndpointResponse:
    pass


class ResponseToClient(EndpointResponse):
    """回报message给客户端，注意必须是json可以序列化的数据"""

    def __init__(self, message: list | dict):
        self.message = message

    def __repr__(self):
        # 代码格式返回response，未来可用于replay还原
        return f"ResponseToClient({self.message})"
