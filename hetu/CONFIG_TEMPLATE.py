# HeTu配置文件模板

# 你的游戏服务器端代码文件，也就是System和Component的定义
APP_FILE = "app.py"

# 项目名称，也代表加载APP_FILE中的哪个namespace
NAMESPACE = "game_short_name"

# 服务器实例名称，每个实例是一个副本
INSTANCE_NAME = "game_name_region1234"

# 开启服务器的debug模式，打印debug消息，并且自动生成调试用的https证书
DEBUG = False

# SSL配置
# CERT_CHAIN = [
#     None,  # 禁止通过IP查看证书
#     {
#         "cert": "/path/to/example.com/fullchain.pem",
#         "key": "/path/to/example.com/privkey.pem",
#         "password": "可选，如果密钥有密码的话",
#     },
#     {
#         "cert": "/path/to/example2.cn/fullchain.pem",
#         "key": "/path/to/example2.cn/privkey.pem",
#         "password": "可选，如果密钥有密码的话",
#     },
# ]

# 服务器Websocket监听地址
LISTEN = "0.0.0.0:2466"

# 工作进程数，-1 为自动按cpu核心数
WORKER_NUM = 4

# 反正是Websocket，可以关了加性能
ACCESS_LOG = False

# 传入消息的最大字节、ping间隔秒、ping无响应关闭连接秒
WEBSOCKET_MAX_SIZE = 2 ^ 19  # 0.5MB
# WEBSOCKET_PING_INTERVAL = 20
# WEBSOCKET_PING_TIMEOUT = 20

# 闲置多少秒未调用System则视为断线
SYSTEM_CALL_IDLE_TIMEOUT = 60 * 2
# 限制每个IP可以创建多少匿名连接（提权登陆后不受此限制）
MAX_ANONYMOUS_CONNECTION_BY_IP = 10

# 限制未登录客户端发送消息的频率，可以设置多个指标，格式为[[最大数量，统计时间（秒）], ...]，默认值目标是限制每秒3条消息
# 登录提权后限制次数*10，或自己在login逻辑中修改ctx.client_send_limits值来实现
CLIENT_SEND_LIMITS = [[10, 1], [27, 5], [100, 50], [300, 300]]
# 限制服务器端发送给未登录客户端消息的频率，格式同上
SERVER_SEND_LIMITS = [[10, 1], [27, 5], [100, 50], [300, 300]]
# 限制未登录客户端最大允许订阅的行数。每次Select算1行，每次Query后算返回的行数。
# 登录提权后限制数*100，或自己在login逻辑中修改ctx.max_row_sub值来实现
MAX_ROW_SUBSCRIPTION = 10
# 限制未登录客户端最大允许订阅的Index数(Query订阅)。每次Query订阅+1。
MAX_INDEX_SUBSCRIPTION = 1


# 消息协议，可以在app代码中自己包装class，在内部实现自定义协议
PACKET_COMPRESSION_CLASS = 'zlib'   # 通过该class的compress和decompress方法进行压缩和解压缩
PACKET_CRYPTOGRAPHY_CLASS = None    # 通过该class的encrypt和decrypt方法进行加密和解密

# ！！ 注意 ！！
# 如果从容器启动standalone模式河图，redis的地址不能用127.0.0.1，因为这是容器内部地址
# 要使用redis所在机器的实际内网ip。比如192.168.1.100

# 后端数据库地址，component中的backend可以指定用这里的哪个后端
BACKENDS = {
    # 第一条是默认后端，@define_component不指定backend时会用这个
    'backend_name': {
        "type": "Redis",                        # 指定backend的类型，目前只支持Redis
        "master": "redis://127.0.0.1:6379/0",   # 指定master服务器，只能一个地址
        "servants": [],                         # 只读副本服务器，如果设置了，所有客户端查询随机在只读副本上进行
        # url格式：redis://[[username]:[password]]@localhost:6379/0
    },
}
