# 节点发现：Redis 注册表 + 网关上的 DNS — 设计稿

- 日期：2026-11-15；2026-11-16 按之后合并进 dev 的 PR #235、#236 修订（§15）
- 状态：已实现（分支 `feat/node-discovery`），实现记录和手工测试见 §16。已定事项见 §13，没有待定问题。
- 基线：dev `2a144728`（初稿是 `8caa6a48`；`7a6a353a` 之后的几个提交只改了文档和翻译脚本）
- 影响范围：`hetu/server/runtime.py`、`hetu/server/adapters/aiohttp_app.py`、`hetu/server/supervisor.py`
  （§9 第 0 步；关服上限加上注销等待，§4.3）、`hetu/cli/start.py`；新增 `hetu/discovery/`、`hetu/cli/dns.py`、
  `hetu/cli/nodes.py`；`hetu/CONFIG_TEMPLATE.yml`、`pyproject.toml`（dev 组加 dnspython）、测试、文档。
- **不改**：线协议、客户端 SDK、雪花 ID 租约（`RedisWorkerKeeper` 的 key 和值，只读它的持有者标识和发号围栏）、
  Session / 订阅。后端读写：每个 worker 每 5 秒多一条 master 写（HSET），和续约同频；不新增 master 读。

## 1. 背景

### 1.1 现在的参考部署

operations.md「负载均衡」：一台固定的 Caddy 网关，HeTu 跑在弹性伸缩组里（可以用抢占式实例），Caddy 用
`dynamic a` 定时查内网 DNS 里的 `hetu.internal`。要人维护的环节：

- 节点开好服之后（根路径 `/` 能访问了）往云厂商的内网 DNS（PrivateZone、Route 53 私有托管区等）加记录；停服前
  删记录，等 Caddy 刷新过（`refresh` 加上记录的 TTL）再停服。每台机器要带 DNS API 的凭据，脚本每家云都不一样；
- 没来得及删记录就没了的机器（抢占回收、宕机）留下残留记录，要定时对账或靠伸缩组的缩容钩子清掉；
- 顺序靠脚本保证，错了就会被 Caddy 摘掉：开机就加记录、或者删了记录马上停服，Caddy 连过去被拒一次，就把这台
  摘掉 30 秒（`fail_duration`）。原地重启的节点开好服之后还要再等这么久才有新连接；只有一台、或者所有节点一起
  重启时，玩家要多等 30 多秒（aiohttp 迁移设计稿 §12.5；PR #236 因此把文档改成了上面的顺序）。

### 1.2 思路

worker 本来就在 Redis 里租雪花 ID：每 5 秒续约一次，TTL 60 秒，正常停服时释放，CAS 保证归属，这已经是一个带
心跳的注册中心。节点在同一个 Redis 里登记自己的地址，网关从注册表里找节点，上面这些环节就都没有了：节点不需要
任何云 API 凭据，宕机的条目自己过期，换一家云、自建机房、单机 compose 都一样。等于拿已有的 Redis 当 Consul 用。

Caddy 读不了 Redis。网关机上跑一个小 DNS 服务（`hetu dns`），把注册表答成 `hetu.internal` 的 A 记录，Caddy
的 `dynamic a` 用 `resolvers` 指向它，Caddy 继续用官方镜像。这和 Caddy 接 Consul DNS 是同一个套路。

## 2. 目标与非目标

目标：

- 节点打开 `DISCOVERY` 就自动登记、自动注销，宕机后 15 秒内在注册表里失效；不需要任何脚本和云 API 凭据。
- 计划内的开服、停服、重启不会被 Caddy 摘掉：开好服才登记，注销后等 Caddy 刷新过才停止监听（§4.3）。
- 网关上只多一个 `hetu dns` 进程。Caddy 用官方镜像，Caddyfile 只多 `resolvers`、`versions` 两行。
- 傻瓜式检查：部署方式不对（bridge 网络、LISTEN 绑在回环上、PROXIES_COUNT 为 0、Redis 用户权限过大……）时，
  开服或启动时直接报错退出（或告警），说明怎么改，而不是开起来之后玩家连不上。
- 只支持 host 网络（§5.4）。
- 不增加运行时依赖。

非目标（后续工作见 §11）：主动再平衡；抢占通知；客户端直连节点；Caddy 插件；IPv6、SRV、各节点不同端口；
`hetu dns` 连 Redis 原生集群；多个网关之间的全局 least_conn。

## 3. 总体结构

```
玩家 ──wss──► Caddy ──ws（内网）──► HeTu 节点 ×N（host 网络，伸缩组）
               │                        │ 每 5 秒 HSET hetu:nodes
               │ DNS 查询               ▼
               ▼                    Redis master ──复制──► 副本
           hetu dns ◄────── 每秒 HGETALL（只读 ACL 用户）──────┘
   （网关机上，host 网络，127.0.0.1:1053）
```

## 4. 注册表

### 4.1 为什么不写进租约

- **时机**：租约在开服最早就拿（启动 System 之前，见 `WorkerRuntime.start_backends`），关服最后才释放
  （`close_backends`）。登记要等开好服才出现，关服第一步就要变成 draining，不然网关会在启动 System 还没跑完、
  或者已经开始排空时把新连接发过来。
- **值**：租约的值是 CAS 令牌（`持有者标识#pid.随机数`，见 `lease_value`；续约用 GETEX 比整个值，接手、释放用
  Lua 比值），放不了会变的连接数和状态。开服时还要按其中的持有者标识认出自己重启前留下的租约，Windows 上
  `lease_owner_exited` 也按冒号分段解析它，掺进 `ip:端口` 就会认错、解析错。
- **TTL**：租约 60 秒是为了不被误接手，宁长勿短；登记要让死节点尽快消失（15 秒）。
- **持有者**：工具进程（`hetu call` / `shell`）也持有租约，但不对外服务。

当初把时间戳水位拆给 `SnowflakeTimestampKeeper` 也是这个理由：租约要强协调，登记只需尽力而为，两者的存活互不
牵连。

### 4.2 数据布局

一个 hash：key 是 `hetu:nodes`，field 是 worker id（十进制），value 是 JSON：

```json
{"v": 1, "node": "<持有者标识>", "ns": "my_game", "inst": ["server1"],
 "addr": "10.0.3.17", "port": 2466, "state": "ready", "conns": 132,
 "ts": 1763190000.123, "up": 1763180000.0, "ver": "1.4.0"}
```

| 字段 | 含义 |
|---|---|
| `v` | 格式版本。读的一方遇到不认识的版本时跳过这条并告警 |
| `node` | 租约的持有者标识（`RedisWorkerKeeper.identity`：`机器码[:节点 token]:w<序号>:<管理进程 pid>`，单进程时是 `机器码[:节点 token]:<pid>`，见 `lease_owner_id`），排查用。不带租约值里每个进程随机的那部分：同一个容器重启后不变，和 `[❄️ID]` 日志里的进程码对得上 |
| `ns` / `inst` | NAMESPACE、INSTANCES |
| `addr` / `port` | 广播地址（§5.2）、LISTEN 的端口 |
| `state` | `ready` 或 `draining` |
| `conns` | 本 worker 的在线连接数（`len(runtime.live_connections)`，含匿名连接） |
| `ts` | 写入时的本机时钟（秒） |
| `up` | 本 worker 就绪的时刻 |
| `ver` | HeTu 版本 |

- **field 用 worker id**：同一时刻一个 id 只有一个持有者，field 天然不会撞；整个 hash 最多 1024 个 field，不用
  清理。死掉的 worker 的 field 留在原处，读的一方判它过期；这个 id 被别人租到之后，新的持有者会覆盖它。非正常
  退出后同一个容器重启、或者管理进程以同一序号重启 worker，会接回原来的 id（最多等 20 秒，`REUSE_STALE_SEC`），
  覆盖的就是自己原来的 field，地址也相同。
- **为什么不是每个 worker 一个带 TTL 的 key**：那样读的一方要么 pipeline 1024 个 GET，要么 SCAN；一个 hash 每秒
  一条 HGETALL 就够了。Redis 7 没有 hash field 的过期（HEXPIRE 要 7.4，HSETEX 要 8.0），所以过期由读的一方判断
  （§4.5）。
- 集群模式下这一个 key 落在一个分片上，写入量很小（§4.4），没关系。
- JSON 用 msgspec 编码（已经是依赖），`redis-cli HGETALL hetu:nodes` 直接能看。

### 4.3 写入时机

挂在 `WorkerRuntime` 上，和 Web 框架无关。是一个尽力而为的后台循环，照水位预留循环的写法：异常只告警，不影响
续约。

1. 开服完成后（`AiohttpServer.start()` 末尾，开始监听、`start_background()` 之后），立刻写一次 `ready`，之后每
   5 秒写一次。operations.md 现在要求脚本做的"开好服之后才加进 DNS"，这里自动做到了；
2. 关服第一步（`AiohttpServer.stop()` 里、`stopping` 置位之前），立刻写一次 `draining`（限时 1 秒，写不进去不挡
   关服）。写成功了就**照常服务 4 秒再往下关**（停止监听、排空……），见下面的"注销等待"。之后照样每 5 秒写一次
   `draining`：排空期间连接数往下走，`hetu nodes` 看得到；
3. `close_backends` 里释放租约之前：停掉循环，`HDEL hetu:nodes <worker id>`。必须在 `lease.release()` 之前：
   释放的第一步就是关掉发号围栏（`close_fence`），之后第 4 条就不让删了；
4. 每次写、删之前先看发号围栏（`keeper.lease_deadline`）：已经关了（续约发现租约被接手、开始释放时，keeper 马上
   关掉围栏），或者过了安全期（最近一次确认租约之后 `LEASE_SAFE_SEC`，15 秒），就不写，关服时也不删、不做注销
   等待。安全期比别的进程能接手这个 id 的最早时刻短（同一持有者标识要等 `REUSE_STALE_SEC`，20 秒；不同标识要等
   key 过期，60 秒），所以围栏开着时 field 还是自己的；过了安全期，这个 id 可能已经被接手，field 是对方的。
   `lease.lost` 不用另外看：续约发现租约丢失时，围栏已经关了。

拿到租约后开服失败的，还没写过，不用删，也不等。进程被强杀时 field 留着，15 秒后被判过期。

**注销等待**：Caddy 要等下一次查 DNS 才知道这台没了。`hetu dns` 每秒重算一次答案（§6.3），Caddy 按 `refresh 2s`
缓存答案，加上副本延迟，最多约 3 秒，所以取 4 秒。这期间就停止监听的话，Caddy 连过来被拒，`fail_duration` 把这台
摘掉 30 秒。要走的节点无所谓（Caddy 在 `lb_try_duration` 内换别的节点重试），原地重启的节点（滚动升级、所有节点
一起重启）开好服之后却要再等 30 秒才有新连接（§1.1）。

- 等待期间 `stopping` 还是假，新连接照常接、照常服务。赶上的少数客户端几秒后在排空时收到 1001，重连到别的节点。
  不改成拒绝新连接：Caddy 只在连不上时换节点重试，HTTP 层的拒绝会原样回给客户端。
- 多进程时每个 worker 各自写、各自等。同一个地址只要还有一个 worker 是 `ready` 就照答（§6.3）；各 worker 共用
  一个监听 socket，要等所有 worker 都停止监听，它才真正关掉（第 0 步之后管理进程不再拖住它），所以从最后一个
  `draining` 写进去算起，至少还监听 4 秒。
- 写 `draining` 失败就不等：网关看到的还是 `ready`，等了也没用；写不进 master 的节点多半本来就干不了活了。
- 关服被打断后再调 `stop()`，不重复等（和排空的截止时刻一样只定一次）。
- 等待时长写死，不设配置项；文档要求 Caddyfile 的 `refresh` 保持 2s（§7）。调大了只是退回到现在的情况：注销后
  连过来的被拒、这台被摘掉 30 秒。
- 管理进程等 worker 关完服的上限 `worker_stop_timeout` 在 `DISCOVERY` 打开时加 5 秒（写 `draining` 的 1 秒上限
  加 4 秒等待），默认配置下是 65 秒，operations.md 要求的 70 秒宽限期仍然够。

### 4.4 读写负担

- **写**：每个 worker 每 5 秒一条 HSET（约 250 字节），1000 个 worker 是每秒 200 条；关服时一条 HDEL。续约本来就
  是每 5 秒一条 GETEX。HSET 在 master 和各副本上各产生一条 keyspace 通知（HeTu 开了 `Kghz`，含 h 类），量很小。
- **读**：`hetu dns` 每秒一条 HGETALL，读副本；`hetu nodes` 默认也读副本。不新增 master 读。

### 4.5 过期判定

- **`hetu dns`（常驻）**：按"`ts` 有没有往前走"判断，不比较时钟。记住每个 field 上见过的最大 `ts` 和它前进的
  时刻（本机单调时钟），15 秒没往前走就算过期。活着的 worker 每 5 秒写一次，`ts` 一次比一次大。换了一个进程
  （持有者标识或 `up` 变了：重启、worker id 被接手）算一次新的写入，`ts` 不用比原来的大。启动后第一次读到的
  field 还没有历史，按本机时钟和 `ts` 的差判断：在 15 秒以内的算活着，更早的按 `ts` 算出它多久没写了。这样无论
  节点和网关的时钟差多少，都不影响结果。
  - 只看"值有没有在变"不够（实现审查指出，§17）：经代理按请求轮询几台副本、其中一台和 master 断开了还在给旧
    数据时，死节点的值会在新旧两份之间来回变，永远算活着。
- 看到一次新的写入时，节点的 `ts` 和本机时钟差超过 5 秒，就对这个 field 告警一次（雪花 ID 本来就要求 NTP 同步）。
  第一次见到的只认 `ts` 在未来的：`ts` 在过去可能只是它很久没写了，死掉的 worker 的登记会一直留着。
- **`hetu nodes`（一次性）**：没有历史，只按 `ts` 和本机时钟判断，同时列出每条的年龄。

## 5. 节点这边

### 5.1 开关

配置项 `DISCOVERY`，默认 false。打开后，开服前做 §5.3 的检查，开服后按 §4.3 登记。默认关闭，是为了让现有的
bridge 网络部署（operations.md 里的 `docker run -p`、仓库的 docker-compose.yaml）照常能开。

### 5.2 广播地址

不另设配置项：

- `LISTEN` 写了具体的 IP（比如 `${HOST_IP}:2466`）：就用它；
- `LISTEN` 是 `0.0.0.0`：取"连默认后端 master 时用的本机源地址"。做法是对 master 的地址 `connect` 一个 UDP
  socket（不发包），再 `getsockname()`。网关和 Redis 一般在同一个 VPC，这个地址基本一定是网关连得到的内网地址。
  多网卡、到 Redis 和到网关不走同一张网卡时，在 `LISTEN` 里写明。
- 端口取 `LISTEN` 的端口。

### 5.3 开服检查

都在 `hetu start` 的主进程里、开服之前做：不需要加载 app 文件，多进程时也只报一次。任何一条不过，都打印原因和
改法，以 2 退出（和 LOGGING 检查一样），什么都没开。算出的广播地址经配置传给各 worker（和 boot uuid 的做法一样）。

| 检查 | 不过时的说明 |
|---|---|
| 默认后端是 Redis | SQLite 只用于单机开发，用不着节点发现 |
| `PROXIES_COUNT ≥ 1` | 节点在网关后面。为 0 时所有玩家的地址都是网关的，同 IP 匿名连接上限（`MAX_ANONYMOUS_CONNECTION_BY_IP`，默认 10）会把玩家挡在外面 |
| `LISTEN` 的主机是 `0.0.0.0`，或本机的一个 IPv4 地址 | 回环地址网关连不上；不能写主机名；这个网络命名空间里没有这个 IP 时，在容器里多半是没用 host 网络 |
| `LISTEN` 是 `0.0.0.0` 时：master 的地址能解析出来、不是 unix socket，推出来的源地址是 IPv4、不是回环或链路本地（169.254.0.0/16） | 推不出广播地址，请在 `LISTEN` 里写明对外 IP。源地址是回环，说明 Redis 在本机 |
| 广播地址所在的网卡在 host 网络里（§5.4） | 只支持 host 网络 |

### 5.4 只支持 host 网络

在 bridge 网络里，容器看到的是 docker 网桥分的地址，别的机器上的网关连不上；host 网络还省掉了端口映射这一层
转发，性能最好。所以只支持 host 网络（docker `--network host`、compose `network_mode: host`、k8s
`hostNetwork: true`），检测到不是就报错退出，例如：

```
❌ 节点发现只支持 host 网络：广播地址 172.17.0.2 在 eth0 上，eth0 是容器网络的虚拟网卡（veth），
   别的机器上的网关连不到。请用 host 网络运行：docker run --network host ...（compose：
   network_mode: host；k8s：hostNetwork: true）。host 网络还省掉了端口映射，性能最好。
```

怎么判断（只在 Linux 上做，别的系统跳过。Docker Desktop 里的容器本身就是 Linux）：

1. 找出广播地址所在的网卡：用 `SIOCGIFCONF` 列出本机的 IPv4 地址和所属网卡。
2. 网卡是 veth 这类成对的虚拟网卡，就不是 host 网络：判断依据是 `/sys/class/net/<网卡>/iflink` 和 `ifindex`
   不相等。veth 的 iflink 指向另一个命名空间里的对端；macvlan / ipvlan 指向父网卡，同样不支持。例外是 VLAN 子
   接口（`uevent` 里 `DEVTYPE=vlan`），它的 iflink 指向宿主机上的父网卡，属于 host 网络。
3. 网卡是本机的容器网桥（`docker0`、`br-<12 位十六进制>`、`podman*`、`cni*`、`virbr*`）：说明到 Redis 走的是
   本机网桥（Redis 跑在本机的 bridge 容器里），这个地址别的机器也连不上。报错，请在 `LISTEN` 里写明对外 IP。

不按 IP 段判断：Docker 默认的地址池（172.17.0.0/16 到 172.31.0.0/16、192.168.0.0/16）和 AWS 默认 VPC
（172.31.0.0/16）以及常用的 172.16.0.0/12 VPC 网段重叠，按段判会误杀。

检查函数接受一个 sysfs 根目录参数，测试用假目录模拟 veth、物理网卡、VLAN、bond、网桥。

已知会被拒绝的情况：跑在 LXC/LXD 这类系统容器里（eth0 也是 veth），k8s 的普通 Pod（即使 Pod IP 在 VPC 里
可路由，比如 AWS VPC CNI）。按"只支持 host 网络"的决定（§13），不留绕过检查的开关。

同一台宿主机上跑几个 host 网络的 HeTu 容器时，它们的主机名相同、pid 都是 1，PR #235 之前租约的持有者标识
（`机器码:pid`）会撞；现在每个容器有自己的节点 token（`/var/tmp/hetu-node-token`），标识不再相同。这几个容器
还要 LISTEN 在不同的 IP 上：端口都得是 `--port`（§6.3）。

### 5.5 `hetu nodes`

排查用。读注册表（和 `hetu get` 一样，默认读副本，`--master` 读主），stdout 输出一行 JSON：按地址汇总（worker
数、连接数、状态、最大年龄、版本），另外列出过期的条目和注册表里出现过的 namespace。它用 HeTu 的配置文件，在
节点或运维机上跑，不在网关上跑；网关上用 `hetu dns --once`（§6.6）。

## 6. `hetu dns`

### 6.1 用法与部署

```
hetu dns --namespace my_game [--name hetu.internal] [--port 2466] [--listen 127.0.0.1:1053] [--once]
```

- Redis 地址从环境变量 `HETU_DNS_REDIS` 读（也可以用 `--redis`，但口令会出现在进程列表里）。它应当指向副本，用
  只读 ACL 用户（§6.5）。支持 `?protocol=2` 等参数，单机 Redis 和代理层都可以；原生集群第一版不支持（§13），
  连上了启动检查会报错（§6.5）。
- `--namespace` 必填：只答这个项目的节点。
- `--port` 要和 Caddyfile 里 `dynamic a` 的 `port`、节点的 LISTEN 端口一致（A 记录不带端口）。
- 不读 HeTu 的配置文件：网关机上不该有 master 的口令。
- 退出码沿用 CLI 的约定：2 是用法或部署错误，3 是环境未就绪（Redis 连不上、被 ACL 拒绝、端口被占）。

网关机（compose）：

```yaml
services:
  caddy:
    image: caddy:2
    network_mode: host
    restart: always
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile
      - caddy_data:/data
  hetu-dns:
    image: heerozh/hetu:latest
    network_mode: host
    restart: always
    command: ["dns", "--namespace", "my_game"]
    environment:
      HETU_DNS_REDIS: redis://hetu-dns:${DNS_REDIS_PASSWORD}@10.0.1.20:6379/0
volumes:
  caddy_data:
```

Caddyfile 相对 operations.md 现在的示例，只在 `dynamic a` 里多两行：

```
dynamic a {
	name hetu.internal
	port 2466
	refresh 2s
	resolvers 127.0.0.1:1053
	versions ipv4
}
```

`versions` 不写时，Caddy 用 `"ip"` 同时查 A 和 AAAA；写 `ipv4` 省掉一半查询。

### 6.2 协议子集

只做 Caddy 用得到的那一点，用 `struct` 手写，跑在 asyncio 的 UDP 端点（`loop.create_datagram_endpoint`）上：

- 只收 QUERY（其他 opcode 回 NOTIMP）；QDCOUNT 必须是 1，问题里不能带压缩指针，QCLASS 不是 IN 的回 NOTIMP；
  解析不了的回 FORMERR，连报头都不完整的直接丢弃。
- 名字等于 `--name`（不分大小写）：A 和 ANY 回地址（TTL 0）；AAAA 和其他类型回 NODATA（NOERROR，没有回答）。
  别的名字回 NXDOMAIN。
- 回答带 AA、不带 RA，回显 ID、RD 和问题；回答里的名字用压缩指针指向问题。查询带 EDNS OPT 时，回答也带。
- **从不设 TC**：Caddy 配了 `resolvers` 之后，它的 Dial 会忽略 Go 解析器传进来的网络类型，默认总是拨 UDP。Go
  收到 TC 后按"tcp"重试，拿到的还是 UDP 连接，第二次的回答即使是截断的也照单全收（2026-11-15 读的 master：
  caddy `modules/caddyhttp/reverseproxy/upstreams.go`，Go `src/net/dnsclient_unix.go`）。所以回答必须一个包就装
  下，上限是查询声明的 UDP 大小：没带 OPT 按 512 字节，带了按它声明的大小、最多 1232 字节（Go 声明的就是 1232）。
  `hetu.internal` 一条 A 记录 16 字节，1232 字节放得下 74 条，512 字节放得下 30 条。
- 不做 TCP：Caddy 用不上。

### 6.3 答哪些地址

每秒读一次注册表，算好答案；查询时直接用算好的答案，不碰 Redis。

1. 留下 `ns` 等于 `--namespace`、没过期（§4.5）、`state` 为 `ready` 的条目；
2. 按 `addr` 去重：同一台机器上的多个 worker 合成一个地址；
3. 排除 `port` 不等于 `--port` 的条目，每个节点报一次错：网关按 `--port` 去连，连不到它；
4. 排除探测不通的（§6.6）；
5. 一个包装不下时，按每个 worker 的平均连接数从少到多取（伸缩组里可能混着不同规格的机器），平均数相同的随机取。

答案变了就打一行日志：加了哪些、去了哪些、一共几台。

### 6.4 容错

- **读 Redis 失败**：一直答上一次的结果，每次失败都打错误日志。
- **注册表里有条目，但全都过期了**：多半是副本落后，或者和 master 断开了（`replica-serve-stale-data` 默认为
  yes，断开时副本照样给旧数据），而不是所有节点同时死了。沿用上一次的结果并报错。就算节点真的全死了，沿用的结果也
  只是让 Caddy 连不上，和答空的效果一样。
- **注册表是空的**（一个 field 都没有）：照实答 NODATA。这说明节点都正常停服，删掉了自己的条目。
- **条目都没过期，但没有 `ready` 的**（所有节点一起停服、重启）：同样照实答 NODATA。Go 解析器把 NODATA 当成查不到，
  Caddy 退回静态上游，新连接等满 `lb_try_duration` 拿到 503，客户端按退避重连；节点开好服、登记 `ready` 之后就能
  连上，不会被摘掉（§4.3）。
- **进程挂了**：按 Caddy 文档，查询出错时退回静态上游，没配静态上游的话新连接进不来，已有的连接不受影响。compose
  里配 `restart: always`。

### 6.5 启动检查

| 检查 | 不过时 |
|---|---|
| 网络模式（同 §5.4，看连 Redis 时用的网卡） | 退出（2）。Caddy 用 host 网络时查的 127.0.0.1 是宿主机的回环，bridge 网络里的进程收不到 |
| 连得上 Redis，HGETALL `hetu:nodes` 没被拒绝 | 退出（3）；被 ACL 拒绝时，把要授权的命令打出来 |
| HGETALL 没有收到 MOVED / ASK（连的不是原生集群） | 退出（2）：第一版不支持原生集群，请经代理层连（§13）。恰好连到持有这个 key 的分片主节点时查不出来；之后槽位迁走了，运行时读失败的错误日志里同样这么说 |
| 权限是不是太大：HGETALL 一个注册表以外的 key | 没被拒绝就告警：网关暴露在公网，请用只读、只能读 `hetu:nodes` 的用户，并打出建用户的命令 |
| 不带口令也能连上：去掉地址里的用户名、口令另连一次，PING 得通（地址里本来就没口令时直接算） | 告警：default 用户没设口令时，网关被攻破的话攻击者直接用它，只读用户形同虚设。请给 default 设口令或禁用它（master 和每台副本都要设） |
| 连的是不是 master（`ROLE`，被 ACL 拒绝就跳过这条） | 是 master 就告警，请指向副本 |
| `--listen` 是回环地址 | 不是就告警：别人也能查到节点地址 |
| bind 成功 | 失败（比如端口被占）就退出（3） |

建用户的命令。ACL 不随复制同步，要在 DNS 连的那台副本上建；云厂商的托管 Redis 在控制台建账号：

```
ACL SETUSER hetu-dns on >口令 resetkeys ~hetu:nodes resetchannels -@all +hgetall +ping
```

这条命令以实测为准（包括 redis-py 建连接时发的 CLIENT SETINFO 等命令）：测试里用真的受限用户跑一遍，文档里写
实测过的版本。用的不是 0 号库时要加 `+select`。

### 6.6 运行时检查

- **探测**：一个地址从不答变成要答时（第一次出现，或者去掉之后又回来：云上回收的实例的内网 IP 会分给新实例），
  从网关 TCP 连一次 `addr:port`（超时 1 秒）。连不上就先不答，打错误日志，提示检查安全组、LISTEN 和网络模式；
  之后每 10 秒重试一次。正在答的地址不再探测，掉线交给 Caddy 的被动健康检查（`fail_duration`）。
- **名字不对**：收到别的名字的查询时，每个名字告警一次，提示检查 Caddyfile 的 `name` 和 `--name`。按 resolv.conf
  搜索域拼出来的 `<name>.<后缀>` 不算，Go 解析器在答空时会拼这些名字再问。
- **第一个查询**：收到时打一行 info，说明 Caddy 已经在用它。不做"很久没收到查询"的告警：Caddy 只在有请求进来时
  才查，没人连的时候本来就没有查询。
- **`--once`**：跑一遍启动检查，读一次注册表并探测，打印会答哪些地址、排除了哪些、各自为什么，然后退出。网关上
  排查用。

### 6.7 实现

- 代码放在 `hetu/discovery/`：`registry.py`（读写和过期判定，节点、`hetu nodes`、`hetu dns` 共用）、
  `netcheck.py`（广播地址和网络模式检查）、`dns.py`（报文和服务）。CLI 在 `hetu/cli/dns.py`、`hetu/cli/nodes.py`。
- 不增加运行时依赖：只用 `struct`、asyncio、`redis.asyncio`、msgspec。dnspython 只进 dev 组，测试里当对照实现，
  和 pynacl 给加密层做对照是同一个做法。
- 不选 dnspython / dnslib 当运行时依赖：只有网关机用得上，却要每个 hetudb 用户都装；dnslib 自带的服务器是线程
  模型，和 asyncio 不搭。不选 Go（miekg/dns）或 CoreDNS：多一门语言、多一个自己编的镜像。

## 7. 文档

- operations.md（中英文）「负载均衡」重写，以 PR #236 改过的版本为底：
  - "发现后端"换成注册表 + `hetu dns`：节点用 host 网络、打开 `DISCOVERY`；网关的 compose 和 Caddyfile（`refresh`
    保持 2s，节点的注销等待按它算）；建 ACL 用户；开服和启动检查各管什么；排查用 `hetu dns --once` 和 `hetu nodes`。
    "开好服之后才加、不要开机就加"不用再写，节点自己保证；
  - "停一台 HeTu"改成直接停服：节点先注销、照常服务 4 秒再停止监听，不再有删记录、等 Caddy 刷新这两步；
  - "没来得及删记录就没了的机器"改成 15 秒内自动失效，不用对账；
  - "大批玩家同时重连"一条保留（`LISTEN_BACKLOG`、`net.core.somaxconn`、SDK 的重连抖动），补一句：计划内的
    重启不会被摘掉，会被摘掉 30 秒的只剩崩溃、被强杀的节点；
  - 可以提一句：只在一家云上跑的话，伸缩组直接关联云负载均衡也不用写脚本（§12）。
- operations.md「Docker」：多机部署用 `--network host`。
- operations.md「停服」：打开 `DISCOVERY` 时，关服第一步先把登记改成 draining、照常服务 4 秒；多进程时每个 worker
  的上限多 5 秒（默认 65 秒），70 秒的宽限期仍然够。
- README（中英文）的负载均衡一段、`hetu/llms.txt` 里 Operations 那一条同步改。
- CONFIG_TEMPLATE.yml：加 `DISCOVERY` 的说明。
- docs/api 照规矩不提交。

## 8. 测试计划

单元测试（不需要 Docker）：

- DNS 报文：用 dnspython 构造查询、解析回答。覆盖 A、AAAA 回 NODATA、别的名字回 NXDOMAIN、大小写、ID 和 RD 回显、
  带和不带 OPT 时的条数上限（74 / 30）、从不设 TC、FORMERR、NOTIMP、不完整的包被丢弃。
- 选哪些地址（假时钟）：按变化判过期、启动时按 `ts` 判、排除 draining、按地址去重、排除端口不符的、超出上限时按
  平均连接数取、按 namespace 过滤、时钟差告警。
- 容错：读失败时沿用、全部过期时沿用、注册表空时答空、都没过期但没有 ready 时答空。
- 广播地址：LISTEN 写了 IP；`0.0.0.0` 时按到 master 的源地址（对一个本机地址 connect）；回环、unix socket、
  IPv6 都报错。
- 网络模式：用假 sysfs 各造一个 veth、物理网卡、VLAN、bond、网桥、docker0、br-xxx。
- `hetu start` 的检查：每一条不过都以 2 退出、没有 bind 端口；包括 PROXIES_COUNT=0、SQLite。

集成测试（Redis 后端）：

- 真开服（用现有 harness，配置里直接给广播地址，跳过网络检查）：就绪后出现 ready 条目；连上一个 ws 后 conns 变成
  1；关服时先变成 draining，注销等待期间（测试里调短）监听 socket 还开着、新连接照常服务，之后才停止监听；释放
  租约之前被删掉；关掉发号围栏（模拟续约发现租约被接手）后不再写，关服时也不删、不等；写 draining 失败时不等。
- `worker_stop_timeout` 在 `DISCOVERY` 打开时多出 5 秒。
- `hetu dns` 对真 Redis：用受限的 ACL 用户（测试里 `ACL SETUSER` 建）能跑，dnspython 查得到节点地址；用 default
  用户时告警权限过大；ACL 拒绝 HGETALL 时以 3 退出；连 `redis_cluster` 测试后端里不持有这个 key 的节点时以 2 退出。
- 探测：注册在没人监听的端口上的地址不被回答，开始监听后，在重试间隔内被回答（测试里把间隔调短）；地址去掉后
  又回来时重新探测。

手工测试（Linux 或 WSL2）：用 dummy 网卡加几个本机地址，几个 HeTu 分别 LISTEN 在这些地址上，再起 Caddy（host 网络）
和 `hetu dns`。验证：连接按 least_conn 分开；停一台，它上面的连接收到 1001 后重连到别的机器，Caddy 的日志里没有
连它被拒的记录；原地重启一台（以及所有节点一起重启），开好服之后马上就有新连接，没有被摘掉 30 秒；加一台后，新连接
落到它上面；kill -9 一台，15 秒内从答案里消失。另外用 bridge 网络开一个节点，确认它报错退出。

## 9. 分步与提交计划

0. **现存问题**（和本设计无关，单独一个提交，先写测试确认）：多进程时，管理进程一直拿着监听 socket
   （`hetu/cli/start.py` 在 `supervisor.run()` 返回之后才 `sock.close()`）。worker 关服时各自 `site.stop()`，
   关掉的只是自己那份 fd，socket 仍在监听：新连接在内核里完成握手、进了 accept 队列却没人接，一直挂到管理进程退出
   时才被 RST，网关没法马上换一台。修法：管理进程开始关服时关掉自己那份（worker 手里的还开着，§4.3 的注销等待
   期间照常接连接）。以上是按代码推断，实现时先测。PR #236 没有改管理进程持有监听 socket 的方式（SO_REUSEPORT
   的原型没合并），这一步照旧。
1. 注册表与节点这边（§4、§5）：配置项、开服检查、登记循环、关服挂钩、`hetu nodes`。
2. `hetu dns`（§6）。
3. 文档（§7）；手工测试的结果记进本稿。

每步一个 PR，base 为 dev。

## 10. 风险

- **veth 判断误杀**：宿主机上默认路由走 macvlan 之类的少见配置，会被当成容器网络。报错信息里写明判断依据，有人
  遇到再加例外。
- **副本延迟**：注册表经副本读，副本落后时新节点会晚几秒才被回答（可以接受）；全部过期时沿用上次的结果（§6.4）。
- **节点超过 74 台**：只答最闲的 74 台。雪花 ID 本身就限 1024 个 worker，每台 8 个 worker 时最多 128 台。
- **Caddy 的 `refresh` 被调大**：注销等待按 2s 算，不够时注销后连过来的被拒、这台被摘掉 30 秒，只影响原地重启
  （§4.3）。
- **`hetu dns` 是网关上又一个进程**：它挂了，新连接就进不来（§6.4），靠 `restart: always` 拉起。它和 Caddy 在同一
  台机器上，不增加新的单点机器。

## 11. 后续工作（不在本次范围内）

- **主动再平衡**：节点对照注册表里的平均连接数，把超出的部分慢慢以 1001 断开，让它们经 least_conn 落到新机器上。
  要限速：被断开的玩家如果正有在途调用，会收到 `CallOutcomeUnknownException`。
- **抢占通知**：可选地轮询云厂商的元数据服务，收到回收通知就开始关服。GCP、Azure 只提前 30 秒，比默认配置下关服
  的最坏耗时（60 秒，打开 `DISCOVERY` 时 65 秒）还短。
- **机器无预警消失**：这时只订阅、不发请求的客户端发现不了断线。Caddy 到上游的连接要等 TCP keepalive 判定断开；
  服务器的 ping 是协议层的，SDK 看不到。需要 SDK 侧的应用层心跳，另立 spec。
- IPv6（AAAA）、SRV（各节点不同端口）、`hetu dns` 连原生集群。

## 12. 备选方案与否决理由

- **地址写进租约的值**：见 §4.1。
- **每个 worker 一个带 TTL 的 key**：见 §4.2。
- **用 Caddy admin API 推上游列表**：等于重载配置，会断掉所有 ws（这也是弃用 caddy-docker-proxy 的原因之一）。
- **Caddy 插件（`dynamic hetu`）**：要维护 Go 代码和自己编的 Caddy 镜像。
- **选举一个节点，把注册表同步到云厂商的内网 DNS**：每家云一套 API，节点要带凭据。
- **客户端直连节点**：每台都要公网 IP 和证书（WebGL 必须用 wss），节点 IP 暴露给 DDoS。
- **伸缩组直接关联云负载均衡**：不用写脚本，但要付负载均衡的月费、绑死一家云，也拿不到注册表里的负载信息。只在
  一家云上跑的话，这也是可行的选择，文档里提一句。
- **按 IP 段判断 bridge 网络**：见 §5.4。
- **另设 `ADVERTISE` 配置项**：LISTEN 写具体 IP 就够了（§5.2），少一个配置项。
- **留绕过网络检查的开关**：只支持 host 网络（§13）。

## 13. 已定事项

2026-11-15：

1. 注册表另起一个 key，不写进租约（§4.1）。
2. 网关上用 DNS 桥，Caddy 用官方镜像；`hetu dns` 用 Python 写在 hetu 包里，报文用 `struct` 手写，不增加运行时依赖。
3. 只支持 host 网络，检测到不是就报错退出，不留开关（用户定）。
4. 从不设 TC，不做 TCP（§6.2）。
5. `DISCOVERY` 默认关闭（§5.1）。

2026-11-16（原 §14 的三个问题，按倾向定）：

6. `hetu dns` 第一版不支持 Redis 原生集群（HeTu 本来就推荐代理层），用到时再加；连到原生集群时启动检查报错退出
   （§6.5）。
7. 命名：配置项 `DISCOVERY`，命令 `hetu dns`、`hetu nodes`，Redis key `hetu:nodes`。
8. `hetu dns` 的 `--namespace` 必填（§6.1）：写错了的话，`--once` 和日志会说清楚注册表里实际有哪些 namespace。

## 14. 待定问题

无。

## 15. 修订记录

**2026-11-16：对照 PR #235、#236**（都在初稿基线 `8caa6a48` 之后合并进 dev）

- PR #235（租约持有者标识）：
  - 租约值从 `机器码:pid` 变成 `持有者标识#pid.随机数`，开服时按持有者标识接回自己重启前留下的 id。§4.1 不写进
    租约的理由照旧成立，措辞按新格式改了；
  - `node` 字段改存持有者标识（§4.2）。初稿写的 `机器码:pid` 在 host 网络下会撞（同一宿主机上的容器主机名相同、
    pid 都是 1），现在由节点 token 区分（§5.4），只支持 host 网络不再带着这个隐患；
  - 发号围栏改成每次确认租约后 15 秒（`LEASE_SAFE_SEC`），租约丢失、开始释放时马上关（`close_fence`）。§4.3 第 4 条
    改成只看围栏，并写明 HDEL 必须在 `lease.release()` 之前；
  - 接回原来的 id 最多等 20 秒，旧 field 被自己覆盖（§4.2）。
- PR #236（监听队列、前置 Caddy 时的建连风暴）：
  - 运维文档改成开好服之后才加 DNS 记录，停服前删记录、等 Caddy 刷新之后再停（aiohttp 迁移设计稿 §12.5 第 4 条）。
    本设计的登记时机本来就是开好服之后；注销原来是写完 `draining` 马上停止监听，会被 Caddy 摘掉 30 秒，改成照常
    服务 4 秒再停（§4.3），`worker_stop_timeout` 跟着加 5 秒。§1.1、§2、§6.4、§7 按新的文档改写；
  - `LISTEN_BACKLOG`、SDK 的重连抖动不影响本设计，文档里"大批玩家同时重连"那一条保留；
  - 第 0 步的现存问题照旧（§9）。
- 原 §14 的三个问题定了（§13 第 6~8 条），§6.5 加上连到原生集群时的检查。§6.6 的探测改成地址每次重新出现都探：
  云上回收的实例的内网 IP 会分给新实例。

## 16. 实现记录（2026-11-16，分支 `feat/node-discovery`）

按 §9 分步提交：

| 步 | 提交 | 内容 |
|---|---|---|
| 0 | `fix(supervisor)` | 管理进程在 `_shutdown` 开始时关掉自己那份监听 socket。先写的测试在修之前失败（Windows 上，两个假 worker 都关掉自己那份之后新连接照样连得上），按代码的推断成立 |
| 1 | `feat(discovery)` × 2、`feat(cli)` | `hetu/discovery/netcheck.py`（开服检查）、`registry.py`（注册表、`NodeRegistrar`），`WorkerRuntime.start_registration` / `withdraw`、`close_backends` 里删登记，`worker_stop_timeout`，`hetu nodes` |
| 2 | `feat(discovery)` | `hetu/discovery/dns.py`、`hetu dns`；dnspython 进 dev 组 |
| 3 | `fix(build)`、`docs` | 见下面的"顺带修的"；operations.md（中英）、README（中英）、`hetu/llms.txt`、AGENTS.md、CONFIG_TEMPLATE.yml |

和设计不一样、或者设计里没写细的地方：

- 广播地址经内部配置项 `DISCOVERY_ADVERTISED_ADDRESS` 传给 worker（同 `ON_START_UUID`）。`hetu start` 打开
  DISCOVERY 时总是用检查算出的值覆盖它，所以它不是给用户的 ADVERTISE（§12）。不是经 `hetu start` 开的服（嵌入、
  测试）没有这一项时，worker 在 `WorkerRuntime.start()` 里自己做开服检查，不过就 `StartupAborted`。
- `PROXIES_COUNT ≥ 1` 的检查，配了 `REAL_IP_HEADER` 也算过：那时客户端地址从它取。`DISCOVERY` 不是布尔值
  （比如写成 `"true"`、`${VAR}` 插值）以 2 退出，同 `LISTEN_BACKLOG`。
- 登记的写、删在一把锁里排队：关服时登记循环正好在写 `ready`，`draining` 会排在它后面落地，不会被它盖掉（有测试）。
  写 `draining` 失败就不做注销等待。
- `hetu nodes` 用 `--config` 或 `--db`，过期只按 `ts` 判断（`|本机时钟 - ts| > 15 秒`），坏的登记单独列出。
- 原生集群的判断按异常类型（`MovedError`、`ClusterDownError` 等）：redis-py 解析错误时会去掉开头的错误码，按消息
  前缀认不出来。刚建好的集群里，没收敛完的节点回的是 CLUSTERDOWN，也当成集群。
- §6.5 的 ACL 命令（加 `+select`，用的是 12 号库）用 Redis 8.10.3 实测：redis-py 8 默认的 RESP3 握手（HELLO）、
  `CLIENT SETINFO` 都不受影响，`HGETALL hetu:nodes` 能读，别的 key、`ROLE` 被拒（测试里按 `ACL_COMMAND` 建用户）。
- `--once` 打印的是给人看的报告（会答哪些地址、哪些不答和原因、解析不了的登记、别的 namespace），不是一行 JSON。
- 审查后加的：不带口令也能连上 Redis 就告警（§6.5）。hetu dns 自己的用户权限再小，副本的 default 用户没设口令时
  网关一旦被攻破照样全开；文档写明副本的 default 用户要设口令或禁用。

**顺带修的现存问题**：用 Dockerfile 构建的镜像里没有 `hetu/data/backend/redis/commit_v2.lua`（构建时没有 .git，
setuptools-scm 列不出文件，package-data 又没列它），已发布的 `heerozh/hetu:latest`（2026-11-09 构建）也是这样，
Redis 后端开服就报 FileNotFoundError。手工测试时发现，单独一个提交加进 package-data，`tests/test_packaging.py`
守门。同一个原因，镜像里的版本号是 setuptools-scm 的兜底值 0.0.0（注册表里的 `ver` 也是它），要在 CD 里把 tag 作为
`SETUPTOOLS_SCM_PRETEND_VERSION` 传进构建，没改。

### 16.1 手工测试（2026-11-16）

Docker Desktop（WSL2）的虚拟机里全用 host 网络：dummy 网卡上的 10.99.0.11~13 三个地址各跑一个节点（这个分支构建的
镜像，`WORKER_NUM: 2`，`LISTEN` 写明 IP），Redis 8.10.3 一主一从，`hetu dns`（受限 ACL 用户读副本，
`--listen 127.0.0.1:11053`），Caddy 2.11.9 官方镜像（参考配置，HTTP），客户端经 Caddy 保持 30~60 个 ws 连接
（握手后调一个返回节点地址的 Endpoint，断了 0.2~0.5 秒后重连）。

| 场景 | 结果 |
|---|---|
| 三台都在 | 30 个连接 10 / 10 / 10 |
| 正常停一台（SIGTERM） | 停服开始约 0.5 秒后 `hetu dns` 去掉它；4 秒后它的 10 个连接收到 1001，1 秒内都重连到另外两台；整个停服约 4.7 秒 |
| 原地重启一台 | 停止监听 18:48:41、就绪 :42、`hetu dns` 加回 :43.9（登记、每秒读、探测），:47.1 开的 10 个新连接全落在它上面：离它停止监听 6 秒，没有被 Caddy 摘掉 30 秒。就绪后 1 秒内开的连接还落在别的节点上（Caddy 的 2 秒缓存），属于正常的传播延迟 |
| `kill -9` 一台 | 12.0 秒后从答案里去掉；它的 25 个连接收到 1006，都重连到另外两台，没有失败 |
| 三台一起重启 | 0.9 秒后答空；4.3 秒后 60 个连接收到 1001；三台 +5 秒就绪，`hetu dns` +6.9 秒答回三台，+7.9 秒 60 个连接全部连回（就绪后约 3 秒），没有一次连接失败：Caddy 在 `lb_try_duration` 的 5 秒内等到了答案。对照 aiohttp 迁移设计稿 §12.5 按旧文档的流程是 35 秒以上 |
| bridge 网络的节点 | 以 2 退出：`广播地址 172.17.0.3 在 eth0 上，eth0 是容器网络的虚拟网卡（veth 之类：iflink 77 ≠ ifindex 2）……` |
| bridge 网络的 `hetu dns` | 以 2 退出，说明 Caddy 查的 127.0.0.1 是宿主机的回环 |
| host 网络、Redis 在本机 bridge 容器里 | 节点以 2 退出：广播地址 172.17.0.1 在本机的容器网桥 docker0 上 |
| `hetu dns --once`、`hetu nodes` | 报告和汇总都对；全程 `hetu dns` 和节点的日志里没有告警、错误 |

没测的：k8s 的 hostNetwork、AWS / 阿里云上真实的伸缩组和安全组、多个网关。
