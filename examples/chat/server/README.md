# Chat Server Example

一个最小聊天服务器示例，放在 `examples/chat/server`，核心只有 3 个业务 System：

- `user_login(user_id, name)`：登录并写入在线用户列表
- `user_quit()`：将用户标记为离线并写入离开系统消息
- `user_chat(text)`：发送一条带时间戳的聊天消息

断线清理由固定 hook `on_disconnect` 触发，它内部调用 `user_quit()`。

## Component Behavior

- `OnlineUser`
  - `owner` / `name`
  - `online`：当前是否在线
  - `last_seen_ms`：最后活动时间戳（毫秒）
- `ChatMessage`
  - `owner` / `name` / `text`
  - `kind`：`chat` 或 `system`
  - `created_at_ms`：消息创建时间戳（毫秒）

`user_login` 会写入 `"<name> joined the chat"` 系统消息，`user_quit` 会写入
`"<name> left the chat"` 系统消息。

## Run

```bash
cd examples/chat/server
# 如果从旧版本升级，先删除旧数据文件：
# rm -f ./data/chat.sqlite3
hetu start --config=./config.yml
```

## Client Mapping

客户端可按传统布局实现：

- 左侧在线用户：订阅/查询 `OnlineUser`
- 右侧聊天气泡：订阅/查询 `ChatMessage`
- 下方输入框：调用 `user_chat(text)`

登录流程：先调用 `user_login(user_id, name)`，断线时服务端会自动回调 `on_disconnect -> user_quit`。
