local cmsgpack = cmsgpack
local redis_call = redis.call
local unpack = unpack

-- ARGV[1] 是 msgpack 序列化的 payload
-- 结构: [ [checks...], [commands...] ]
local payload = cmsgpack.unpack(ARGV[1])
local checks = payload[1]
local commands = payload[2]

-- ============================================================================
-- Phase 1: Atomic Checks (原子性检查)
-- ============================================================================
if checks then
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

        -- 检查 Key 不存在 (用于 Insert)
        -- 格式: ["NX", key]
        elseif op == "NX" then
            local key = check[2]
            if redis_call("EXISTS", key) == 1 then
                return "RACE: Key already exists " .. key
            end

        -- 检查 Key 存在 (用于 Update/Delete)
        -- 格式: ["EX", key]
        elseif op == "EX" then
            local key = check[2]
            if redis_call("EXISTS", key) == 0 then
                return "RACE: Key does not exist " .. key
            end

        -- 检查唯一索引 (数值型)
        -- 格式: ["UNIQ_NUM", index_key, value]
        elseif op == "UNIQ_NUM" then
            local idx_key = check[2]
            local val = check[3]
            -- ZRANGE key min max BYSCORE LIMIT 0 1
            local res = redis_call("ZRANGE", idx_key, val, val, "BYSCORE", "LIMIT", 0, 1)
            if #res > 0 then
                return "UNIQUE: Constraint violation (num) on " .. idx_key
            end

        -- 检查唯一索引 (字符串型)
        -- 格式: ["UNIQ_STR", index_key, value]
        elseif op == "UNIQ_STR" then
            local idx_key = check[2]
            local val = check[3]
            -- ZRANGE key [val: [val:\xff BYLEX LIMIT 0 1
            local min = "[" .. val .. ":"
            local max = "[" .. val .. ":\255"
            local res = redis_call("ZRANGE", idx_key, min, max, "BYLEX", "LIMIT", 0, 1)
            if #res > 0 then
                return "UNIQUE: Constraint violation (str) on " .. idx_key
            end
        end
    end
end

-- ============================================================================
-- Phase 2: Execute Commands (批量写入)
-- ============================================================================
if commands then
    for _, cmd in ipairs(commands) do
        -- cmd 格式: ["HMSET", key, field, val, ...]
        redis_call(unpack(cmd))
    end
end

return "committed"