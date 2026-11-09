local cmsgpack = cmsgpack
local unpack = unpack
local redis_call = redis.call
local string_match = string.match
local ipairs = ipairs

-- ARGV[1] 是 msgpack 序列化的 payload
-- 结构: [ [checks...], [pushes...], {deleted...}, [table_pubs...], [value_chans...], [seq_allocs...] ]
local payload = cmsgpack.unpack(ARGV[1])
local checks = payload[1]
local pushes = payload[2]
local deleted = payload[3]
local table_pubs = payload[4]
local value_chans = payload[5]
-- 提交序号列（commit_seq）这次要发的号：[[计数器 key, 个数], ...]。没有号要发时不带这一段，
-- 下面相关的表、循环都不建：所有提交都走这个脚本，不能让不用提交序号列的也多付开销
local seq_allocs = payload[6]

-- ============================================================================
-- Phase 1: Checks
-- 顺序即优先级：Python 把竞态类（VER、带 RACE 标记的 NX/UNIQ、区间的 CNT）排在
-- 确定性类前面，这里首个失败即返回，所以同时存在两类冲突时先报 RACE。
-- ============================================================================
if checks then
    -- 同一 payload 内两条 UNIQ 指向同一 (索引, 值) 的兜底（正常由本地 IdentityMap 拦住）
    local seen_uniq = {}
    -- 本次 pushes 要 ZREM 的 (索引 key, member)，UNIQ 查到已有的 member 时才建（常见路径用不到）。
    -- 键带索引 key：同一行两个 unique 列同值时 member 字节一样，只按 member 会把另一列一起豁免
    local removed = nil
    for _, check in ipairs(checks) do
        local op = check[1]

        -- 检查版本号 (乐观锁)
        -- 格式: ["VER", key, expected_version]
        if op == "VER" then
            local key = check[2]
            local expected = check[3]
            local current = redis_call("HGET", key, "_version")
            -- 注意: HGET 返回的是 string，如果 key 不存在返回 false/nil
            if current ~= expected then
                return "RACE: Version mismatch " .. key .. " exp:" .. tostring(expected) .. " got:" .. tostring(current)
            end

            -- 检查 Key 不存在 (用于 Insert 主键，以及本事务按 id 读空过的行)
            -- 格式: ["NX", key, code, label]
            -- code 为 "RACE"/"UNIQUE"：本事务曾 get 观察该 id 不存在则为竞态，否则为确定性冲突；
            -- label 只做定位信息，原样回显
        elseif op == "NX" then
            if redis_call("EXISTS", check[2]) == 1 then
                return check[3] .. ": Key already exists " .. check[4]
            end

            -- 检查 Key 存在 (用于 Update/Delete)
            -- 格式: ["EX", key]
        elseif op == "EX" then
            local key = check[2]
            if redis_call("EXISTS", key) == 0 then
                return "RACE: Key does not exist " .. key
            end

            -- 检查唯一索引
            -- 格式: ["UNIQ", index_key, start_val, end_val, code, label]
        elseif op == "UNIQ" then
            local idx_key, start_val, end_val = check[2], check[3], check[4]
            local code, label = check[5], check[6]
            local dup = idx_key .. "\0" .. start_val
            if seen_uniq[dup] then
                return "UNIQUE: Duplicate unique value within transaction " .. label
            end
            seen_uniq[dup] = true
            -- ZRANGE key [val\x00 [val\x00\xff BYLEX LIMIT 0 1
            local res = redis_call("ZRANGE", idx_key, start_val, end_val, "BYLEX", "LIMIT", 0, 1)
            if #res > 0 then
                -- member 是 值段\x00id 段，id 段是定宽的 hex、不含 0x00，故最后一个 0x00 即终止符；
                -- deleted 的键也是 id 段（见 redis_model.py 的 ROW_ID_TOKEN_LEN）
                local row_id = string_match(res[1], ".*%z(.*)$")
                -- 唯一索引指向的行在本次事务中被删除了，则不算冲突
                if not deleted[row_id] then
                    -- 本次提交会把这个 member 移走（同一事务里把值挪到了别的行，比如换槽位）也不算：
                    -- 被移走的行都有 VER 钉住，库里它的 member 就是本次要 ZREM 的那个
                    if removed == nil then
                        removed = {}
                        if pushes then
                            for _, cmd in ipairs(pushes) do
                                if cmd[1] == "ZREM" then
                                    for i = 3, #cmd do
                                        removed[cmd[2] .. "\0" .. cmd[i]] = true
                                    end
                                end
                            end
                        end
                    end
                    if not removed[idx_key .. "\0" .. res[1]] then
                        return code .. ": Unique violation " .. label
                    end
                end
            end

            -- 检查区间行数 (range 读的防幻读校验)
            -- 格式: ["CNT", index_key, min, max, expected_count, label]
            -- 读到的行由 VER 保证没被删改，区间里行数不变就说明没有新行插进来
        elseif op == "CNT" then
            if redis_call("ZLEXCOUNT", check[2], check[3], check[4]) ~= check[5] then
                return "RACE: Range changed " .. check[6]
            end
        end
    end
end

-- ============================================================================
-- Phase 2: Execute Commands (批量写入)
-- ============================================================================
-- 先把提交序号列这次要发的号全部拿到，再写：Lua 运行时出错不回滚已经执行的写入，号排在所有
-- 写入之前，写到一半出错时坏情况是留下缺口而不是发出重号。发号本身全成或全不发：哪个计数器
-- 出错，或者发出的号不在 [1, 2^53) 里（再往上 Lua 的 double 不精确；计数器被手工改成负数时
-- 发出的号 <= 0，和"没有号"分不开），就把已经加上去的减回去，一个号都不发、什么都不写（原来
-- 没有的计数器 key 减回之后留着，值是 0，和不存在等价）
local seq_next = nil
if seq_allocs then
    seq_next = {}
    for i, alloc in ipairs(seq_allocs) do
        local last = redis.pcall("INCRBY", alloc[1], alloc[2])
        local failed = type(last) == "table"
        if failed or last >= 9007199254740992 or last < alloc[2] then
            for j = 1, failed and i - 1 or i do
                redis_call("DECRBY", seq_allocs[j][1], seq_allocs[j][2])
            end
            if failed then
                -- 原样返回 Redis 的错误（WRONGTYPE、not an integer、OOM、READONLY……），调用方
                -- 拿到的还是原来的异常
                return last
            end
            return "SEQ: Counter out of range " .. alloc[1]
        end
        seq_next[i] = last - alloc[2] + 1
    end
end

if pushes then
    for _, cmd in ipairs(pushes) do
        if seq_next and cmd[1] == "HSETSEQ" then
            -- ["HSETSEQ", 下标, 组 key, P, id 段, 行 key, field, val, ..., 号的字段名, 占位]：
            -- 按 seq_allocs 的第"下标"项取号，号替换最后的占位写进行（同名字段取最后一个值），
            -- 再把 P + 号的编码 + 0x00 + id 段加进组 key。号用 %.0f 转成字符串：%d 要先转成 C 的
            -- long（32 位的平台上会截断），tostring 是 %.14g（>= 1e14 变成科学计数法）
            local slot = cmd[2]
            local n = seq_next[slot]
            seq_next[slot] = n + 1
            cmd[#cmd] = string.format("%.0f", n)
            redis_call("HSET", unpack(cmd, 6))
            -- 号的索引编码（int64 的 to_sortable_bytes）：n + 2^63 的 8 字节大端。1 <= n < 2^53
            -- 时最高字节是 0x80，后面是 n 的低 7 字节。纯算术拼，n < 2^53 时 % 和除法都精确
            local b, m = {}, n
            for k = 8, 2, -1 do
                local r = m % 256
                b[k] = r
                m = (m - r) / 256
            end
            b[1] = 128 + m
            redis_call("ZADD", cmd[3], 0, cmd[4] .. string.char(unpack(b)) .. "\0" .. cmd[5])
        else
            -- cmd 格式: ["HSET", key, field, val, ...] / ZADD / ZREM / DEL
            redis_call(unpack(cmd))
        end
    end
end

-- ============================================================================
-- Phase 3: 表频道 / 索引值频道通知（整表订阅、点查询订阅用）
-- ============================================================================
-- 行 / 整个索引的变更由各副本应用写入时自己产生 keyspace 通知，不占 master。
-- 这里的 PUBLISH 很贵：master 上每条约 1 万条指令（redis.call 调度 + 强制写进复制流 +
-- payload 搬运），而且每个副本都要再执行一遍（数据见 benchmark/redis_publish_cost_result.md）。
-- 所以只保留这两个调用点，Python 侧只给声明了的组件 / 索引准备数据：
-- - 表频道：table_sub 组件，一个事务一张表一条，消息是 msgpack 的 row_id 列表；
-- - 值频道：point_sub 索引的"进入"，一个事务每个 (索引, 值) 一条，消息为空串。
-- 复合索引（groupby）不在这里发：每组一个 zset，按组订阅订这一组 key 的 keyspace 通知。
-- 不要在这里加新的调用点、也不要往消息里塞内容：tests/test_arch_publish.py 守门。
-- （原生 cluster 下 PUBLISH 会经 cluster bus 发到所有节点；推荐用 proxy 反代，没有这个问题）
if table_pubs then
    for _, pub in ipairs(table_pubs) do
        redis_call("PUBLISH", pub[1], pub[2])
    end
end
if value_chans then
    for _, ch in ipairs(value_chans) do
        redis_call("PUBLISH", ch, "")
    end
end

return "committed"