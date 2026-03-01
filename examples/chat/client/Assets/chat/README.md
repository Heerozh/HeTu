# Unity Chat Client (UI Toolkit + R3)

极简示例：一个文件级可落地的 Unity 客户端，连接 `examples/server/chat`。

## Files

- `ChatClient.cs`：连接、3 个 System 调用、R3 绑定逻辑
- `ChatWindow.uxml`：左在线用户 / 右聊天气泡 / 下输入框
- `ChatWindow.uss`：现代炫彩风格

## Quick Use

1. 把这 3 个文件复制到 Unity 工程（例如 `Assets/ChatExample/`）。
2. 新建场景对象并挂 `UIDocument` + `ChatClient`。
3. `UIDocument` 指向 `ChatWindow.uxml`，并把 `ChatWindow.uss` 加到根样式。
4. 启动服务端：`examples/server/chat/config.yml`。
5. Play。

## Reactive Binding

- 在线用户列表：`IndexSubscription<OnlineUser>.ObserveAdd()`
- 聊天气泡列表：`IndexSubscription<ChatMessage>.ObserveAdd()`
- 单条消息与本人信息实时刷新：`RowSubscription<T>.Subject`

所有 UI 更新都走 R3 流，不走轮询。
