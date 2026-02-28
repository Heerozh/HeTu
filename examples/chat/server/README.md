# Chat Server Example

一个最小聊天服务器示例，放在 `examples/server/chat`，核心只有 3 个业务 System：

- `user_login(user_id, name)`：登录并写入在线用户列表
- `user_quit()`：从在线用户列表删除
- `user_chat(text)`：发送一条聊天消息

断线清理由固定 hook `on_disconnect` 触发，它内部调用 `user_quit()`。

## Run

```bash
cd examples/server/chat
hetu start --config=./config.yml
```

## Client Mapping

客户端可按传统布局实现：

- 左侧在线用户：订阅/查询 `OnlineUser`
- 右侧聊天气泡：订阅/查询 `ChatMessage`
- 下方输入框：调用 `user_chat(text)`

登录流程：先调用 `user_login(user_id, name)`，断线时服务端会自动回调 `on_disconnect -> user_quit`。
