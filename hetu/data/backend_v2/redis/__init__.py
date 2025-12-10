#  """
#  @author: Heerozh (Zhang Jianhao)
#  @copyright: Copyright 2024, Heerozh. All rights reserved.
#  @license: Apache2.0 可用作商业项目，再随便找个角落提及用到了此项目 :D
#  @email: heeroz@gmail.com
#  """


# 新的Redis后端实现，分2个文件，一个redis_client.py负责连接和lua脚本, 提供后端管理功能
# 一个redis_backend.py负责实现Backend接口，新的Backend接口包括
# 提供master_get, master_query, master_commit等接口
# 提供replica_get, replica_query等接口
# 然后做一个专门的单元测试，测试这些接口是否正常

from .client import RedisBackendClient