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

-- 分割 key_name 获取 table 和 uid (格式 "table:uid")
local function get_key_parts(key)
    local t, u = string.match(key, "^(.-):(%d+)$")
    return t, u
end

-- 生成索引的 Key
local function get_index_key(table_name, field)
    return table_name .. ":index:" .. field
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
local function check_unique_constraint(table_name, field, val, current_uid)
    local index_key = get_index_key(table_name, field)

    if is_number(val) then
        -- 数字类型 Unique 检查: Score = val
        -- 检查 range [val, val] 是否存在元素，且元素不等于 current_uid
        local res = redis.call('ZRANGEBYSCORE', index_key, val, val)
        if #res > 0 then
            for _, existing_uid in ipairs(res) do
                if existing_uid ~= current_uid then
                    return true
                end -- 冲突
            end
        end
    else
        -- 字符串类型 Unique 检查: Member prefix = "val:"
        -- 使用 ZRANGEBYLEX [val: [val:\xff
        local search_start = "[" .. tostring(val) .. ":"
        local search_end = "[" .. tostring(val) .. ":\255"
        local res = redis.call('ZRANGEBYLEX', index_key, search_start, search_end)

        if #res > 0 then
            for _, member in ipairs(res) do
                -- member 格式为 "val:uid"，需要提取 uid
                local _, existing_uid = string.match(member, "^(.-):(%d+)$")
                if existing_uid ~= current_uid then
                    return true
                end -- 冲突
            end
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
    for table_name, rows in pairs(payload["insert"]) do
        -- 确保 Context 结构存在
        if not context[table_name] then
            context[table_name] = {}
        end

        local table_schema = SCHEMA[table_name] or { unique = {}, indexes = {} }

        for key, fields in pairs(rows) do
            local _, uid = get_key_parts(key)
            if not uid then
                return { err = "Invalid key format: " .. key }
            end

            -- 检查 1: Key 是否已存在
            if redis.call('EXISTS', key) == 1 then
                return { err = "Duplicate key entry: " .. key }
            end

            -- 检查 2: Unique 约束
            for field, val in pairs(fields) do
                if table_schema.unique and table_schema.unique[field] then
                    if check_unique_constraint(table_name, field, val, uid) then
                        return {
                            err = "Unique constraint violation: " ..
                                    table_name .. "." .. field .. "=" .. tostring(val)
                        }
                    end
                end
            end
        end
    end
end

-- 1.2 Check Update
if payload["update"] then
    for table_name, rows in pairs(payload["update"]) do
        if not context[table_name] then
            context[table_name] = {}
        end
        local table_schema = SCHEMA[table_name] or { unique = {}, indexes = {} }

        for key, new_fields in pairs(rows) do
            local _, uid = get_key_parts(key)

            -- 获取旧数据 (Snapshot)
            local old_row_raw = redis.call('HGETALL', key)
            if #old_row_raw == 0 then
                return { err = "Key not found for update: " .. key }
            end

            -- 将 HGETALL 的 array 转换为 map
            local old_row = {}
            for i = 1, #old_row_raw, 2 do
                old_row[old_row_raw[i]] = old_row_raw[i + 1]
            end

            -- 保存到 context 供 Phase 2 使用
            context[table_name][key] = old_row

            -- 检查 1: 乐观锁 (Version)
            -- 数据库里的 version 可能是 string，需要转 number 对比
            local db_ver = tonumber(old_row["version"] or "0")
            local payload_ver = tonumber(new_fields["version"] or "-1")

            -- 注意：Payload 里的 update 对象通常包含要做检查的 version
            -- 这里假设 payload 里的 new_fields 包含 "version" 字段用于乐观锁检查
            -- 如果 payload 结构是 {field: val}, 且 version 单独传，请调整此处逻辑
            -- 按照 ORM 惯例，提交上来的数据包含旧版本号
            if db_ver ~= payload_ver then
                return { err = "Optimistic lock failure: " .. key .. " DB=" .. db_ver .. " Req=" .. tostring(payload_ver) }
            end

            -- 检查 2: Unique 约束 (仅检查被修改的字段)
            for field, new_val in pairs(new_fields) do
                -- 跳过 version 字段和未改变的字段
                if field ~= "version" and tostring(new_val) ~= old_row[field] then
                    if table_schema.unique and table_schema.unique[field] then
                        if check_unique_constraint(table_name, field, new_val, uid) then
                            return {
                                err = "Unique constraint violation: " ..
                                        table_name .. "." .. field .. "=" .. tostring(new_val)
                            }
                        end
                    end
                end
            end
        end
    end
end

-- 1.3 Check Delete
if payload["delete"] then
    for table_name, rows in pairs(payload["delete"]) do
        if not context[table_name] then
            context[table_name] = {}
        end

        for key, criteria in pairs(rows) do
            -- 获取旧数据 (Snapshot) 以便后续删除索引
            local old_row_raw = redis.call('HGETALL', key)
            if #old_row_raw == 0 then
                -- 如果已经不存在，根据业务逻辑可能报错或忽略，这里选择报错
                return { err = "Key not found for delete: " .. key }
            end

            local old_row = {}
            for i = 1, #old_row_raw, 2 do
                old_row[old_row_raw[i]] = old_row_raw[i + 1]
            end
            context[table_name][key] = old_row

            -- 检查: 乐观锁
            local db_ver = tonumber(old_row["version"] or "0")
            local req_ver = tonumber(criteria["version"])

            if db_ver ~= req_ver then
                return { err = "Optimistic lock failure (delete): " .. key }
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
