# Unity Chat Client (UI Toolkit + R3)

极简示例：一个可落地的 Unity 客户端，连接 `examples/chat/server`。
展示如何用 HeTu 客户端 SDK 的响应式功能，几行代码写出界面。

## Architecture

```
HeTu Components (= Model)
     ↓
ChatViewModel (组合 + ReactiveCommand)
     ↓ Observable
ChatView (订阅 → UIToolkit 控件)
```

## Files

- `LoginView.cs`：登录界面，连接 HeTu 服务器并执行 `user_login` RPC
- `ChatViewModel.cs`：ViewModel，直接包装 HeTu 订阅，暴露 Observable + ReactiveCommand
- `ChatView.cs`：View，订阅 ViewModel 响应流，驱动 ListView 刷新
- `ChatRenderers.cs`：消息/事件/成员 UI 渲染器
- `Components.cs`：MessagePack 组件映射（= Model）
- `ChatWindow.uxml` + `ChatWindow.uss`：Discord 风格 UI（Login + Chat）

## Quick Use

1. 把 `Chat` 目录下的文件复制到 Unity 工程（例如 `Assets/Chat/`）。
2. 新建场景对象并挂 `UIDocument` + `LoginView` + `ChatView`。
3. `UIDocument` 指向 `ChatWindow.uxml`。
4. 在 `ChatView` Inspector 中将 `LoginView` 拖到 `loginView` 字段。
5. 启动服务端：`examples/chat/server/config.yml`。
6. Play — Login → 进入聊天。

## Reactive Highlights

- **ReactiveCommand**：输入为空时 Send 按钮自动禁用
- **ObserveAdd / ObserveRemove**：成员和消息的增删自动推送到 ListView
- **Two-way binding**：TextField ↔ BindableReactiveProperty 双向同步
