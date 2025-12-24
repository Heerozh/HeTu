local cmsgpack = cmsgpack
local unpack = unpack
local redis_call = redis.call
local ipairs = ipairs

-- ARGV[1] 是 msgpack 序列化的 payload
-- 结构: [ [checks...], [pushes...] ]
local payload = cmsgpack.unpack(ARGV[1])
local checks = payload[1]
local pushes = payload[2]

-- ============================================================================
-- Phase 1: Checks
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

            -- 检查唯一索引 (字符串型)
            -- 格式: ["UNIQ", index_key, value]
        elseif op == "UNIQ" then
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
if pushes then
    for _, cmd in ipairs(pushes) do
        -- cmd 格式: ["HMSET", key, field, val, ...]
        redis_call(unpack(cmd))
    end
end

return "committed"