# MMapBackendClient实现计划

库hetu目前已有redis/sql的数据库后端，现在需要再实现一个追求极致性能的共享内存后端MMapBackendClient。
由于hetu是多进程跨主机的分布式系统，内存后端需要使用内存映射文件来实现数据共享，因此功能仅限单机多进程模式。

hetu的数据是ECS架构的，且基于numpy record array，非常适合用np.memmap来存储，读取方可以直接把数据文件，
映射成numpy record array内存，来获取所有数据，性能极佳，因此mmap是最佳选择。

由于是共享内存，写入要提交给一个写入进程统一执行，来防止竞态冲突。
让我们先来实现mmap数据库写入类MmapWriter，此类会启动为一个进程作为服务(通过SyncManager)
，拥有以下功能：

- 管理方法：
    - 对外提供表创建，rename，drop, flush(truncate), rebuild_index等函数，且这些函数接受进程间调用。
    - 每次创建table mmap文件时，预留一定的空行数，初始行数可以是100行。
    - 对内提供扩容方法，扩容时直接复制当前table mmap文件，到新的table mmap文件，只是文件末尾序号+1
      之后np.memmap重新加载新的文件，指定一个更大的行数（扩容算法可以类似于std::
      vector），来完成扩容。换文件的原因是可能有读取方占用旧文件映射。
    - rename只是改table mmap文件名，只改末尾序号最大的，其他的删除。这是维护方法，没有读取方占用
    - flush删除table mmap所有版本的文件，创建一个新的一定空行数的table
      mmap文件，末尾序号归0。这是维护方法，没有读取方占用
    - drop为删除table mmap文件的所有版本。这是维护方法，没有读取方占用
- 写入方法：
    - 对外提供commit函数，且此函数接受进程间调用。
    - commit时mmap数组行数不够则立即flush后自动调用扩容
    - 写入完成后要保证数据持久化，可以调用mmap的flush写入到磁盘的功能
    - 写入后要维护索引文件的正确性。
    - 保持原有version乐观锁设计，commit时务必保证一起提交成功或一起失败
    - 注意commit时redis是用lua实现的，等于要实现lua相同逻辑
- 索引维护：
    - 一个table有些数据列有索引，因此也维护每个索引的数组
    - 采用 orderedstructs的skiplist来实现索引，来保证索引的插入和删除性能。
    - 然后通过multiprocessing.shared_memory来共享给所有读取者
    - 每个索引都是一个按值排序好的 array，可以沿用redis，把数据和id转换为bytes，用lex排序的方法。
    - 每次commit时，如果有数据更新，就会同步删除旧数据对应的索引行，并插入新数据对应的索引行，保持索引文件的正确性。
    - 以后读取方通过共享内存，二分查找来找到对应行号，再去table mmap读取对应行的数据。
- 订阅Pub/Sub：
    - 主要用于管理所有MQClient的订阅
    - 对外提供订阅/反订阅函数，且此函数接受进程间调用。
    - 使用 multiprocessing.Queue 来通知
    - 每次commit时，会往订阅管理器发送哪个频道更新了，订阅管理器看看有哪些MQClient订阅了这个频道，
      就把更新通知压入对方客户端的multiprocessing.Queue 里。
    - 注意每次发生扩容，也要把扩容消息压入所有的MQClient的Queue里，让他们知道有新版本了。

可以先从最小的单元测试开始写，然后逐步迭代，实现完整功能并全部覆盖测试。

============

现在让我们实现读取方的mmap读取管理器MmapReader，MMapBackendClient通过它读取数据，这个管理器是进程内调用的，负责查询数据。

- 此管理器负责直接加载只读的memmap文件，不和写入管理器通信。
- lazy加载table mmap文件，需要时才加载，并保留
- 提供一个reload public方法，需要查询mmap文件是否已有新版本，如果有新版本，就重新加载新的mmap文件，来保证数据是最新的。
  这个reload方法由之后的MQClient收到消息后调用。
- 提供range public方法，先查询共享内存的索引，找到对应行号范围，再去table
  mmap文件里读取对应行的数据切片返回。回给用户时也无须拷贝，因为只读的memmap如果修改会报错。

可以先从最小的单元测试开始写，然后逐步迭代，实现完整功能并全部覆盖测试。

============

现在终于我们需要实现MMapBackendClient了。

- 请参考 @/hetu/data/backend/redis/client.py , 实现mmap的后端，请保持结构尽量一致，和redis后端的代码差别不要太大。
    - redis专用的_schema_checking_for_redis，load_commit_scripts等方法不需要
    - 但注意range_normalize_中的inf边界处理和检查保留。
    - pubsub是redis特有的，不需要实现。
    - mmap数据库写入类的服务进程可以在client初始化时，创建一个，需要判断是否已有创建，通过加锁防止竞态创建
    - 大多数操作可以通过之前写的2个类实现

- 参考 @/hetu/data/backend/redis/maint.py 实现mmap创建，以及alter等操作。大多数操作可以通过之前写的2个类实现

- 参考 @/hetu/data/backend/redis/mq.py 实现数据库写入通知，大多数操作通过之前写的写入进程服务实现

- 目前项目带有backend的所有功能的pytest，测试会跑在所有后端夹具上。全部实现完成后，请通过测试。
    - 可通过HETU_TEST_BACKENDS环境变量指定测试特定后端
    - 夹具可能不完整，请完善夹具并让所有测试通过
    - 如果测试单元有不合理的地方，比如漏掉了某些初始化，导致在当前后端下有问题，请询问用户是否需要修改。

- 重要提醒，注意我们有些维护/初始化方法是sync的， 不能使用asyncio.run等方法执行async的方法，
  因为hetu基于异步网页框架，即使能跑通tests，也会导致实际loop混乱。

=======================

最后我们实现mmap的worker_keeper，就叫SingleMachineWorkerKeeper吧。

- 参考 @hetu/data/backend/redis/worker_keeper.py
- 由于worker_keeper本质是用来管理跨主机的worker id，因此我们这种单机多进程模式就简单多了
- 直接返回累加的worker id即可，可以通过跨进程的全局变量来实现
- keep_alive只需记录last_timestamp即可，记录到当前目录本地文件中即可，可以每个worker
  id一个文件