-- 权威读：在 master 上批量 HGETALL，KEYS 是行 key（同一张表的行带同一个 {CLU} hash tag，同 slot）。
-- 用脚本而不是直接 HGETALL，是因为读写分离代理会把普通读命令送到副本，脚本才一定到主节点
-- （代理和 redis-py cluster 都按命令名路由 EVALSHA）。
-- 故意不写 `#!lua` shebang：带 shebang 而不声明 no-writes 的脚本在启动前就要过写检查
--（副本 READONLY / MISCONF / NOREPLICAS），兼容模式只在真的执行写命令时才检查，
-- 只读脚本就不会被 min-replicas-to-write 之类拒掉。
local res = {}
for i, key in ipairs(KEYS) do
    res[i] = redis.call('HGETALL', key)
end
return res
