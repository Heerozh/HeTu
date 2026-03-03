# Unity Chat Client (UI Toolkit + R3)

极简示例：一个可落地的 Unity 客户端，连接 `examples/chat/server`。

## Files

- `ChatClient.cs`：View 层，负责 UI 查询、HeTu 订阅流消费与渲染
- `ChatViewModel.cs`：MVVM-Lite 透传层，只暴露 Observable 与调用入口
- `ChatRepository.cs`：Model 层，负责 HeTu 连接、系统调用与订阅桥接
- `ChatRenderers.cs`：消息/系统事件/成员项 UI 渲染器
- `ChatModels.cs`：View 的展示模型
- `Components.cs`：MessagePack 组件映射
- `ChatWindow.uxml` + `ChatWindow.uss`：Discord 风格 UI

## Quick Use

1. 把 `chat` 目录下的脚本与 UXML/USS 一起复制到 Unity 工程（例如 `Assets/ChatExample/`）。
2. 新建场景对象并挂 `UIDocument` + `ChatClient`。
3. `UIDocument` 指向 `ChatWindow.uxml`，并把 `ChatWindow.uss` 加到根样式。
4. 启动服务端：`examples/chat/server/config.yml`。
5. Play。

## Reactive Binding

- 在线成员列表：订阅 `OnlineUser`，按 `online` 分组为在线/离线
- 聊天流：订阅 `ChatMessage`，`kind=chat/system` 分别渲染
- 时间显示：使用 `created_at_ms` 格式化为 `HH:mm`

所有 UI 更新都走 R3 响应流，不走轮询。
