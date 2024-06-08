# HeTu配置文件模板

# 你的游戏服务器端代码文件，也就是System和Component的定义
APP_FILE = "app.py"

# 项目名称，也代表加载APP_FILE中的哪个namespace
NAMESPACE = "game_short_name"

# 服务器实例名称，每个实例是一个副本
INSTANCE_NAME = "game_name_region1234"

# 是否为主节点，主节点负责分发跨节点数据
HEAD_NODE = True

# 开启服务器的debug模式，打印debug消息，并且自动生成调试用的https证书
DEBUG = False

# 服务器Websocket监听地址
LISTEN = "0.0.0.0:2466"

# 工作进程数，-1 为自动按cpu核心数
WORKER_NUM = 4

# 反正是Websocket，可以关了加性能，要加也应该在反向代理里加
ACCESS_LOG = False

# 传入消息的最大字节、ping间隔秒、ping无响应关闭连接秒
# WEBSOCKET_MAX_SIZE = 2 ^ 20
# WEBSOCKET_PING_INTERVAL = 20
# WEBSOCKET_PING_TIMEOUT = 20

# 后端数据库地址，component中的implement可以指定用这里的哪个后端
BACKENDS = {
    'db_name': {
        "type": "Redis",
        "addr": "127.0.0.1:6379",
        "user": "root",
        "pass": "",
        "db": 0,
    },
}
