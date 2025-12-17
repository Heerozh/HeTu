--local table_concat = table.concat
local table_insert = table.insert
local unpack = unpack
local string_match = string.match
local type = type
local redis_call = redis.call
local ipairs = ipairs
local pairs = pairs
local tonumber = tonumber
local tostring = tostring

-- ============================================================================
-- Redis Lua ORM Transaction Script
-- ============================================================================

-- 依赖库
local cmsgpack = cmsgpack

-- 1. SCHEMA 定义 (由应用层硬编码注入，这里仅作示例结构)
local SCHEMA = PLACEHOLDER_SCHEMA
-- 示例结构
-- local SCHEMA = {
--   ["User:{CLU1}"] = {
--     unique = { ["email"] = true, ["phone"] = true },
--     indexes = { ["email"] = true, ["age"] = true, ["phone"] = true }
--     -- 注意: unique 字段也必须在 indexes 里列出
--   }
-- }

-- 获取 payload
local payload = cmsgpack.unpack(ARGV[1])

-- 上下文缓存，用于在 Check 阶段保存读取到的旧数据，供 Write 阶段使用
-- 结构: context[table_name][key_name] = old_hash_data
local context = {}

-- ============================================================================
-- 辅助函数
-- ============================================================================

-- 分割 "instance_name:User:{CLU0}" 获取 tablename
local function get_table_ref(key)
    local i, t, c = string_match(key, "^(.-):(.-):{CLU(%d+)}$")
    return i, t, c  -- instance_name, table_name, cluster_id
end

local function row_key(table_prefix, row_uuid)
    return table_prefix .. ":id:" .. row_uuid
end

-- 生成索引的 Key
local function index_key(table_prefix, field_name)
    return table_prefix .. ":index:" .. field_name
end

-- 生成 ZSet 的 Member 和 Score
-- String: Score=0, Member="val:uid"
-- Number: Score=val, Member="uid"
local function get_index_member_score(is_str, val, uid)
    if not is_str then
        return uid, val -- member, score
    else
        return tostring(val) .. ":" .. uid, 0 -- member, score
    end
end

-- 检查 Unique 约束
-- 返回 true 表示冲突(失败), false 表示通过
local function check_unique_constraint(table_prefix, field, new_val)
    local key = index_key(table_prefix, field)

    if type(new_val) == "number" then
        -- 数字类型 Unique 检查: Score = val
        -- 检查 range [val, val] 是否存在元素，且元素不等于 current_uid
        local res = redis.call('zrange', key, new_val, new_val, 'BYSCORE', 'LIMIT', 0, 1)
        if #res > 0 then
            return true
        end
    else
        -- 字符串类型 Unique 检查: Member prefix = "val:"
        -- 使用 ZRANGE BYLEX [val: [val:\xff
        local search_start = "[" .. tostring(new_val) .. ":"
        local search_end = "[" .. tostring(new_val) .. ":\255"
        local res = redis.call('zrange', key, search_start, search_end, 'BYLEX', 'LIMIT', 0, 1)

        if #res > 0 then
            return true
        end
    end
    return false
end

-- ============================================================================
-- Phase 1: Check & Prepare (只读/验证)
-- ============================================================================

-- payload示例结构：
-- payload = {
--     insert = {
--         ["instance_name:User:{CLU0}"] = {
--             {属性1 = value, ...},
--         },
--     },
--     update = {
--         ["instance_name:User:{CLU0}"] = {
--             {属性1 = value, ...}
--         },
--     },
--     delete = {
--         ["instance_name:User:{CLU0}"] = {
--             {version = 1},
--         },
--     },
-- }

-- 1.1 Check Insert
if payload["insert"] then
    for table_prefix, rows in pairs(payload["insert"]) do
        -- 解析 table_ref
        local _, table_name, _ = get_table_ref(table_prefix)
        -- 获取表结构
        local table_schema = SCHEMA[table_name]
        if not table_schema then
            error("Schema not found for table: " .. table_name)
        end
        -- 开始检查insert
        for _, row in ipairs(rows) do
            local uid = row.id
            local key = row_key(table_prefix, uid)

            -- 检查 payload 里的 version 必须是0
            local payload_ver = row["_version"]
            if payload_ver ~= 0 then
                error("ASSERT: Insert payload version must be 0: " .. key)
            end

            -- 检查 1: Key 是否已存在
            if redis_call('EXISTS', key) == 1 then
                return {
                    err = "RACE: Try insert but key exists: " .. key
                }
            end

            -- 检查 2: Unique 约束
            for field, _ in pairs(table_schema.unique) do
                if check_unique_constraint(table_prefix, field, row[field]) then
                    return {
                        err = "UNIQUE: Constraint violation: " .. table_name .. "." .. field .. "=" ..
                                tostring(row[field])
                    }
                end
            end
        end
    end
end

-- 1.2 Check Update
if payload["update"] then
    for table_prefix, rows in pairs(payload["update"]) do
        -- 确保 Context 结构存在
        if not context[table_prefix] then
            context[table_prefix] = {}
        end
        -- 解析 table_ref
        local _, table_name, _ = get_table_ref(table_prefix)
        -- 获取表结构
        local table_schema = SCHEMA[table_name]
        if not table_schema then
            error("Schema not found for table: " .. table_name)
        end

        for rows_i, row in ipairs(rows) do
            local uid = row.id
            local key = row_key(table_prefix, uid)

            -- 获取旧数据 (Snapshot)
            local old_row_raw = redis_call('HGETALL', key)
            if #old_row_raw == 0 then
                return {
                    err = "RACE: Try update key but not exists: " .. key
                }
            end

            -- 将 HGETALL 的 array 转换为 map
            local old_row = {}
            for i = 1, #old_row_raw, 2 do
                old_row[old_row_raw[i]] = old_row_raw[i + 1]
            end

            -- 检查 1: 乐观锁 (Version)
            -- 数据库里的 version 是 string，需要转 number 对比
            local payload_ver = row["_version"]
            local db_ver = tonumber(old_row["_version"])
            if db_ver ~= payload_ver then
                return {
                    err = "RACE: Optimistic lock failure(update): " .. key .. " DB=" .. db_ver .. " Req=" ..
                            tostring(payload_ver)
                }
            end

            -- 检查 2: 获取改变的字段，和Unique 约束检查 (仅检查被修改的字段)
            local changed_fields = {}
            for field, new_val in pairs(row) do
                -- 跳过未改变的字段
                if tostring(new_val) ~= tostring(old_row[field]) then
                    changed_fields[field] = new_val
                    if table_schema.unique and table_schema.unique[field] then
                        if check_unique_constraint(table_name, field, new_val) then
                            return {
                                err = "UNIQUE: Constraint violation: " .. table_name .. "." .. field .. "=" ..
                                        tostring(new_val)
                            }
                        end
                    end
                end
            end

            -- 保存到 context 供 Phase 2 使用
            context[table_prefix][key] = old_row

            -- 只保留变更字段，供 Phase 2 使用
            rows[rows_i] = changed_fields
            if next(changed_fields) == nil then
                error("ASSERT: No fields changed in update payload for key: " .. key)
            end
            if changed_fields["id"] ~= nil then
                error("ASSERT: 'id' field should not be changed in update payload for key: " .. key)
            end
            -- 保留 id  字段用于 Phase 2 判断写到哪里
            changed_fields["id"] = row.id
        end
    end
end

-- 1.3 Check Delete
if payload["delete"] then
    for table_prefix, rows in pairs(payload["delete"]) do
        if not context[table_prefix] then
            context[table_prefix] = {}
        end

        for _, row in ipairs(rows) do
            local uid = row.id
            local key = row_key(table_prefix, uid)
            -- 获取旧数据 (Snapshot) 以便后续删除索引
            local old_row_raw = redis_call('HGETALL', key)
            if #old_row_raw == 0 then
                -- 如果已经不存在，说明冲突
                return {
                    err = "RACE: Try delete key but not exists: " .. key
                }
            end

            -- 将 HGETALL 的 array 转换为 map
            local old_row = {}
            for i = 1, #old_row_raw, 2 do
                old_row[old_row_raw[i]] = old_row_raw[i + 1]
            end

            -- 转换 version 为 number 用于后续计算和对比
            local payload_ver = row["_version"]
            local db_ver = tonumber(old_row["_version"])

            -- 检查: 乐观锁
            if db_ver ~= payload_ver then
                return {
                    err = "RACE: Optimistic lock failure (delete): " .. key .. " DB=" .. db_ver .. " Req=" ..
                            tostring(payload_ver)
                }
            end

            -- 保存到 context 供 Phase 2 使用
            context[table_prefix][key] = old_row
        end
    end
end

-- ============================================================================
-- Phase 2: Execute (写入/修改) - 此时已无报错可能
-- ============================================================================

-- 2.1 Execute Insert
if payload["insert"] then
    for table_prefix, rows in pairs(payload["insert"]) do
        local _, table_name, _ = get_table_ref(table_prefix)
        local table_schema = SCHEMA[table_name]

        for _, row in ipairs(rows) do
            local uid = row.id
            local key = row_key(table_prefix, uid)

            -- 设置 Version = 1
            row["_version"] = row["_version"] + 1

            -- 构建 HMSET 参数
            local args = {}
            for k, v in pairs(row) do
                table_insert(args, k)
                table_insert(args, v)
            end
            redis_call('HMSET', key, unpack(args))

            -- 更新索引
            for field, is_str in pairs(table_schema.indexes) do
                local idx_key = index_key(table_prefix, field)
                local member, score = get_index_member_score(is_str, row[field], uid)
                redis_call('ZADD', idx_key, score, member)
            end
        end
    end
end

-- 2.2 Execute Update
if payload["update"] then
    for table_prefix, rows in pairs(payload["update"]) do
        local _, table_name, _ = get_table_ref(table_prefix)
        local table_schema = SCHEMA[table_name]

        for _, row in ipairs(rows) do
            local uid = row.id
            local key = row_key(table_prefix, uid)
            local old_row = context[table_prefix][key] -- 从上下文获取旧值

            -- 设置 Version += 1，因为update只有修改的字段，所以从old获取
            row["_version"] = tonumber(old_row["_version"]) + 1

            -- 准备更新数据
            local update_args = {}

            -- 处理普通字段和索引更新
            for field, new_val in pairs(row) do
                if field ~= "id" then
                    table_insert(update_args, field)
                    table_insert(update_args, new_val)

                    -- 处理索引更新
                    local is_str = table_schema.indexes[field]
                    local is_indexed = is_str ~= nil
                    if is_indexed then
                        local idx_key = index_key(table_name, field)
                        local old_val = old_row[field]

                        -- 1. 删除旧索引
                        if old_val then
                            -- 只有旧值存在才删
                            -- 需要判断旧值的类型来决定 score/member 格式
                            local old_member, _ = get_index_member_score(is_str, old_val, uid)
                            redis_call('ZREM', idx_key, old_member)
                        end

                        -- 2. 添加新索引
                        local new_member, new_score = get_index_member_score(is_str, new_val, uid)
                        redis_call('ZADD', idx_key, new_score, new_member)
                    end
                end
            end

            redis_call('HMSET', key, unpack(update_args))
        end
    end
end

-- 2.3 Execute Delete
if payload["delete"] then
    for table_prefix, rows in pairs(payload["delete"]) do
        local _, table_name, _ = get_table_ref(table_prefix)
        local table_schema = SCHEMA[table_name]

        for _, row in ipairs(rows) do
            local uid = row.id
            local key = row_key(table_prefix, uid)
            local old_row = context[table_prefix][key]

            -- 删除索引
            for field, is_str in pairs(table_schema.indexes) do
                local idx_key = index_key(table_name, field)
                local member, _ = get_index_member_score(is_str, old_row[field], uid)
                redis_call('ZREM', idx_key, member)
            end

            -- 删除 Hash
            redis_call('DEL', key)
        end
    end
end

-- 全部成功
return {
    ok = "committed"
}
