# Sql系后端SQLBackendClient实现计划

库hetu目前已有redis的数据库后端，现在需要再实现一个sql系的后端SQLBackendClient。

- 请参考 @/hetu/data/backend/redis/client.py , 实现sql系的后端，请保持结构尽量一致，和redis后端的代码差别不要太大。
    - redis专用的_schema_checking_for_redis，load_commit_scripts，to_sortable_bytes等方法不需要
    - 但注意range_normalize_中的inf边界处理和检查保留。
    - pubsub是redis特有的，不需要实现。以及worker_keeper目前无须实现。

- 实现时请使用SQLAlchemy core库，这样可以在一个SQLBackendClient满足各种不同sql数据库，直接通过url可以连接不同种类的数据库；
    - 只用sqla core，不用 ORM。
    - SQLBackendClient要支持各种sql系的数据库，考虑扩展性。
    -
    不要每个dialects都去BackendClientFactory注册新的alias，对hetu来说，SQLBackendClient会自动处理连接url，不关心具体的dialects。

- 重要，注意我们有些维护/初始化方法是sync的，需要维护2套io，一个使用sync的engine，一个使用async的engine。
  不能使用asyncio.run等方法执行async的方法，因为hetu基于异步网页框架，即使能跑通tests，也会导致实际loop混乱。
    - 用户传入的连接url(dsn)不能带driver，我们自己给用户拼上。如果用户带了driver，报错
    - postgre可以asyncpg驱动作为async io，和create_engine() with postgresql+psycopg://...
      作为同步sync io
    - MariaDB/MySQL可以使用aiomysql驱动作为async io，和pymysql作为同步sync io
    - SQLite可以使用aiosqlite驱动作为async io，同步io就是无驱动python标准库自带的即可
    - 以上依赖库都已添加，其他Oracle，MSSQL暂时不处理，因为我不想项目加太多依赖库，以后需要再说。

- 保持原有version乐观锁设计，commit时使用事务来实现一起提交成功或一起失败
    - 注意commit时redis是用lua实现的，这里用sql事务
    - 注意sql要先调用delete操作（不然先insert可能造成索引unique冲突，而redis并无索引）

- @/hetu/data/backend/redis/maint.py 实现数据库的表创建，以及alter等操作，注意maint都是sync操作，使用同步的库。

- @/hetu/data/backend/redis/mq.py 实现数据库写入通知，可以使用通知表来实现，在commit时把写入的频道计入通知表，然后mq获得更新消息。
    - 记得通知表也要定时清理过期内容防止无限增大，可以每15分钟，在commit时顺手清理掉，注意抖动时间，因为worker进程可能有很多个。

- 目前项目带有backend的所有功能的pytest，测试会跑在所有后端夹具上。全部实现完成后，请通过测试。
    - 可通过HETU_TEST_BACKENDS环境变量指定测试特定后端
    - 夹具可能不完整，请完善postgres, sqlite, mariadb夹具并让所有测试通过
    - 如果测试单元有不合理的地方，比如漏掉了某些初始化，导致在当前后端下有问题，请询问用户是否需要修改。