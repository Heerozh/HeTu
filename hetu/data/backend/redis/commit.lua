-- 我在写一个redis的orm框架，事务采用version属性值作为乐观锁+lua脚本的机制实现。
-- 现在我在写这个框架的redis lua脚本，用于处理事务commit。
-- 因为没有使用任何watch机制，所以lua脚本需要做完整的检查和操作。
-- ```
-- 数据库设计信息：
--    - 数据库数据是以hash存储的，key_name是"table_name:雪花算法的uid"的形式。
--    - 有些属性会有zset索引，储存在"table_name:index:属性名"里，有些属性会要求有unique约束。
--      所有索引包括unique约束都是通过zset实现的。
--    - 如果值是字符串类型，score是0, member是"字符串值:雪花算法的uid"。如果为数字，score是数字值，member是"雪花算法的uid"。
--    lua事先会有个schema全局变量，里面有每个table的unique和index属性定义，所有unique属性也会在index里列出。
--      应用层会把 Schema 硬编码进 Lua 脚本字符串中，所以这里不用处理，直接使用即可。
--    key的version属性是数据库管理的，用户无法修改。所以提交数据都是原样获取后原样传入，如果version变了则说明有竞态
--    key_name是调用方按“table_name:雪花算法的uid”组合出来的，且不允许rename操作
--
-- 首先使用cmsgpack来获取提交的payload。
-- payload是dict包含
--{
--    "insert": {
--        table_name1 = {
--            key_name1: {属性1:value,...} , key_name2: {}, ...
--        }, ...
--    },
--    "update": {
--        table_name1 = {
--            key_name1: {属性1:value,...} , key_name2: {}, ...
--        }, ...
--    },
--    "delete": {
--        table_name1 = {
--            key_name1: {version:1} , key_name2: {version:2}, ...
--        }, ...
--    },
--}
--
-- 由于lua脚本失败不会回滚，所以我们要先做检查。
-- 对于3种操作，检查逻辑如下：
-- * insert:
--   - insert提交的主要问题有: 可能有其他人预先竞态插入了相同的unique值。
--   - 所以处理前要检查unique的值是否已存在，table有哪些unique属性可以从lua的schema全局变量里获取，此变量
--     事先就有，格式为{table_name: {unique: {set}, indexes{set}}}。
--   - 然后检查payload的version应该是0，且数据库中不应该exists该key。
--
-- * update:
--   - 首先获取完整的旧值并保存，检测旧值version和数据库中的是否相同，不同的话就返回竞态错误。
--   - 然后获取值变化的属性，检查这些属性里是否有unique约束的，有的话检查新的值是否已存在。
--
-- * delete:
--   首先获取完整的旧值，检测旧值version和数据库中的是否相同，不同的话就返回竞态错误。
--
-- 然后开始写入操作，这些操作必须全部成功，不能有失败的可能
-- 对于3种操作，写入逻辑如下：
-- * insert:
--   - payload version+1后，写入数据，同时循环所有标记为index的属性，更新zset索引。
--
-- * update:
--   - 获取值变化的属性，如果这些属性标记为index，根据旧 Hash 值从 ZSET 中移除旧索引
--   - zadd新值到索引
--   - version+1，更新数据。
-- * delete:
--   循环所有标记为index的属性，用zrem旧值从zset删除索引
--   最后删除hash key。
-- ```

local table_concat = table.concat
local string_match = string.match
local type = type
local redis = redis
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
--}

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
    return i, t, c
end

local function row_key(table_prefix, row_uuid)
    return table_prefix .. ":id:" .. row_uuid
end

-- 生成索引的 Key
local function index_key(table_prefix, field_name)
    return table_prefix .. ":index:" .. field_name
end

-- 判断是否为数字
local function is_number(val)
    return type(val) == "number"
end

-- 生成 ZSet 的 Member 和 Score
-- String: Score=0, Member="val:uid"
-- Number: Score=val, Member="uid"
local function get_index_member_score(val, uid)
    if is_number(val) then
        return uid, val                       -- member, score
    else
        return tostring(val) .. ":" .. uid, 0 -- member, score
    end
end

-- 检查 Unique 约束
-- 返回 true 表示冲突(失败), false 表示通过
local function check_unique_constraint(table_prefix, field, new_val)
    local key = index_key(table_prefix, field)

    if is_number(new_val) then
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
        local instance_name, table_name, cluster_id = get_table_ref(table_prefix)
        -- 获取表结构
        local table_schema = SCHEMA[table_name]
        if not table_schema then
            error("Schema not found for table: " .. table_name)
        end
        -- 开始检查insert
        for _, row in ipairs(rows) do
            local uid = row.id
            local key = row_key(table_prefix, uid)

            -- 检查 1: Key 是否已存在
            if redis.call('EXISTS', key) == 1 then
                return { err = "RACE: Try insert but key exists: " .. key }
            end

            -- 检查 2: Unique 约束
            for field, _ in pairs(table_schema.unique) do
                if check_unique_constraint(table_prefix, field, row[field]) then
                    return {
                        err = "UNIQUE: Constraint violation: " ..
                                table_name .. "." .. field .. "=" .. tostring(row[field])
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
        local instance_name, table_name, cluster_id = get_table_ref(table_prefix)
        -- 获取表结构
        local table_schema = SCHEMA[table_name]
        if not table_schema then
            error("Schema not found for table: " .. table_name)
        end

        for rows_i, row in ipairs(rows) do
            local uid = row.id
            local key = row_key(table_prefix, uid)

            -- 获取旧数据 (Snapshot)
            local old_row_raw = redis.call('HGETALL', key)
            if #old_row_raw == 0 then
                return { err = "RACE: Try update key but not exists: " .. key }
            end

            -- 将 HGETALL 的 array 转换为 map
            local old_row = {}
            for i = 1, #old_row_raw, 2 do
                old_row[old_row_raw[i]] = old_row_raw[i + 1]
            end

            -- 保存到 context 供 Phase 2 使用
            context[table_prefix][key] = old_row

            -- 检查 1: 乐观锁 (Version)
            -- 数据库里的 version 可能是 string，需要转 number 对比
            local db_ver = tonumber(old_row["_version"] or "0")
            local payload_ver = tonumber(row["_version"])
            if db_ver ~= payload_ver then
                return { err = "RACE: Optimistic lock failure(update): " .. key .. " DB=" .. db_ver .. " Req=" .. tostring(payload_ver) }
            end

            -- 检查 2: 获取改变的字段，和Unique 约束检查 (仅检查被修改的字段)
            local changed_fields = {}
            for field, new_val in pairs(row) do
                -- 跳过未改变的字段
                if tostring(new_val) ~= old_row[field] then
                    changed_fields[field] = new_val
                    if table_schema.unique and table_schema.unique[field] then
                        if check_unique_constraint(table_name, field, new_val) then
                            return {
                                err = "UNIQUE: Constraint violation: " ..
                                        table_name .. "." .. field .. "=" .. tostring(new_val)
                            }
                        end
                    end
                end
            end
            rows[rows_i] = changed_fields -- 只保留变更字段，供 Phase 2 使用
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
            local old_row_raw = redis.call('HGETALL', key)
            if #old_row_raw == 0 then
                -- 如果已经不存在，说明冲突
                return { err = "RACE: Try delete key but not exists: " .. key }
            end

            local old_row = {}
            for i = 1, #old_row_raw, 2 do
                old_row[old_row_raw[i]] = old_row_raw[i + 1]
            end
            context[table_prefix][key] = old_row

            -- 检查: 乐观锁
            local db_ver = tonumber(old_row["_version"] or "0")
            local payload_ver = tonumber(row["_version"])

            if db_ver ~= payload_ver then
                return { err = "RACE: Optimistic lock failure (delete): " .. key .. " DB=" .. db_ver .. " Req=" .. tostring(payload_ver) }
            end
        end
    end
end

-- ============================================================================
-- Phase 2: Execute (写入/修改) - 此时已无报错可能
-- ============================================================================

-- 2.1 Execute Insert
if payload["insert"] then
    for table_name, rows in pairs(payload["insert"]) do
        local table_schema = SCHEMA[table_name] or { unique = {}, indexes = {} }

        for key, fields in pairs(rows) do
            local _, uid = get_key_parts(key)

            -- 设置 Version = 1
            fields["version"] = 1

            -- 构建 HMSET 参数
            local args = {}
            for k, v in pairs(fields) do
                table.insert(args, k)
                table.insert(args, v)
            end
            redis.call('HMSET', key, unpack(args))

            -- 更新索引
            for field, val in pairs(fields) do
                if table_schema.indexes[field] then
                    local index_key = get_index_key(table_name, field)
                    local member, score = get_index_member_score(val, uid)
                    redis.call('ZADD', index_key, score, member)
                end
            end
        end
    end
end

-- 2.2 Execute Update
if payload["update"] then
    for table_name, rows in pairs(payload["update"]) do
        local table_schema = SCHEMA[table_name] or { unique = {}, indexes = {} }

        for key, new_fields in pairs(rows) do
            local _, uid = get_key_parts(key)
            local old_row = context[table_name][key] -- 从上下文获取旧值

            -- 准备更新数据
            local update_args = {}
            local has_updates = false

            -- 处理普通字段和索引更新
            for field, new_val in pairs(new_fields) do
                if field ~= "version" then
                    -- version 后面单独处理
                    local old_val = old_row[field]
                    -- 类型统一转 string 对比 (Redis Hash 存储均为 string)
                    if tostring(new_val) ~= old_val then
                        table.insert(update_args, field)
                        table.insert(update_args, new_val)
                        has_updates = true

                        -- 处理索引更新
                        if table_schema.indexes[field] then
                            local index_key = get_index_key(table_name, field)

                            -- 1. 删除旧索引
                            if old_val then
                                -- 只有旧值存在才删
                                -- 需要判断旧值的类型来决定 score/member 格式
                                -- 注意: old_row 里取出来全是 string，尝试转 number
                                local old_val_num = tonumber(old_val)
                                local final_old_val = old_val_num or old_val
                                local old_member, _ = get_index_member_score(final_old_val, uid)
                                redis.call('ZREM', index_key, old_member)
                            end

                            -- 2. 添加新索引
                            local new_member, new_score = get_index_member_score(new_val, uid)
                            redis.call('ZADD', index_key, new_score, new_member)
                        end
                    end
                end
            end

            if has_updates then
                -- Version + 1
                local new_ver = (tonumber(old_row["version"] or "0") + 1)
                table.insert(update_args, "version")
                table.insert(update_args, new_ver)

                redis.call('HMSET', key, unpack(update_args))
            end
        end
    end
end

-- 2.3 Execute Delete
if payload["delete"] then
    for table_name, rows in pairs(payload["delete"]) do
        local table_schema = SCHEMA[table_name] or { unique = {}, indexes = {} }

        for key, _ in pairs(rows) do
            local _, uid = get_key_parts(key)
            local old_row = context[table_name][key]

            -- 删除索引
            for field, val in pairs(old_row) do
                if table_schema.indexes[field] then
                    local index_key = get_index_key(table_name, field)
                    local val_num = tonumber(val)
                    local final_val = val_num or val

                    local member, _ = get_index_member_score(final_val, uid)
                    redis.call('ZREM', index_key, member)
                end
            end

            -- 删除 Hash
            redis.call('DEL', key)
        end
    end
end

-- 全部成功
return { ok = "committed" }
