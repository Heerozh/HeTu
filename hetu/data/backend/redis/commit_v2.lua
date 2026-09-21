local cmsgpack = cmsgpack
local unpack = unpack
local redis_call = redis.call
local string_match = string.match
local ipairs = ipairs

-- ARGV[1] 是 msgpack 序列化的 payload
-- 结构: [ [checks...], [pushes...], {deleted...}, [publishes...] ]
local payload = cmsgpack.unpack(ARGV[1])
local checks = payload[1]
local pushes = payload[2]
local deleted = payload[3]
local publishes = payload[4]

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
-- Phase 3: 表级 / 索引值频道通知 (整表订阅、点查询订阅用)
-- ============================================================================
-- 行/整个索引的变更由 keyspace notification 自动发出；这里额外对每张被改动的表、
-- 每个被改动的 (索引, 值) 各 PUBLISH 一条带 payload 的消息，payload 是 msgpack 的 row_id 列表。
if publishes then
    for _, pub in ipairs(publishes) do
        -- pub 格式: [channel, packed_row_ids]
        -- PUBLISH会在Redis cluster下，对所有node发送，而我们只需要"本分片"收到这条消息
        -- SPUBLISH可以解决这个问题，但是订阅复杂度上升，且我们并不推荐cluster模式
        -- 应使用proxy反代，就没有这个问题了
        redis_call("PUBLISH", pub[1], pub[2])
    end
end

return "committed"