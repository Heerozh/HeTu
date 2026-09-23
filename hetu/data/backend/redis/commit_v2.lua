local cmsgpack = cmsgpack
local unpack = unpack
local redis_call = redis.call
local string_match = string.match
local ipairs = ipairs

-- ARGV[1] 是 msgpack 序列化的 payload
-- 结构: [ [checks...], [pushes...], {deleted...}, [table_pubs...], [value_chans...] ]
local payload = cmsgpack.unpack(ARGV[1])
local checks = payload[1]
local pushes = payload[2]
local deleted = payload[3]
local table_pubs = payload[4]
local value_chans = payload[5]

-- ============================================================================
-- Phase 1: Checks
-- 顺序即优先级：Python 把竞态类（VER、带 RACE 标记的 NX/UNIQ）排在确定性类前面，
-- 这里首个失败即返回，所以同时存在两类冲突时先报 RACE。
-- ============================================================================
if checks then
    -- 同一 payload 内两条 UNIQ 指向同一 (索引, 值) 的兜底（正常由本地 IdentityMap 拦住）
    local seen_uniq = {}
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

            -- 检查 Key 不存在 (用于 Insert 主键)
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
                -- member 是 value\x00row_id，row_id 不含 0x00，故最后一个 0x00 即终止符
                local row_id = string_match(res[1], ".*%z(.*)$")
                -- 唯一索引指向的行在本次事务中被删除了，则不算冲突
                if not deleted[row_id] then
                    return code .. ": Unique violation " .. label
                end
            end
        end
    end
end

-- ============================================================================
-- Phase 2: Execute Commands (批量写入)
-- ============================================================================
if pushes then
    for _, cmd in ipairs(pushes) do
        -- cmd 格式: ["HMSET", key, field, val, ...]
        redis_call(unpack(cmd))
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