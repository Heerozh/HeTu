# HeTu JavaScript Client SDK

HeTu(河图) game database 的 JavaScript 客户端 SDK，与 Unity (C#) 客户端功能对标，
可同时运行在浏览器与 Node.js 中。

## 特性

- 纯 JavaScript，无需 TypeScript 工具链
- ESM + CJS 双格式产物，浏览器和 Node.js 通用
- 三层消息管线：`jsonb`(MessagePack) + `zlib` + `crypto`(X25519 + ChaCha20-Poly1305)
- 与服务端 PyNaCl 实现底层一致（均基于 libsodium）
- 订阅 API 同时支持 EventEmitter 与回调风格，零依赖事件分发

## 安装

```bash
npm install @hetudb/client-sdk
# Node.js 环境额外安装 WebSocket 实现
npm install ws
```

## 快速开始

```js
import { HeTuClient } from "@hetudb/client-sdk";

const client = new HeTuClient();
await client.connect("ws://localhost:8080/hetu/mydb");

// RPC 调用
const result = await client.callSystem("login", "token123", 456);

// 单行订阅
const sub = await client.get("HP", "owner", userId);
sub.on("update", (data) => console.log("hp updated:", data));
sub.on("delete", () => console.log("hp deleted"));
console.log(sub.data);

// 范围订阅
const range = await client.range("HP", "owner", 0, 9999, { limit: 10 });
range.on("insert", (rowId, data) => {});
range.on("update", (rowId, data) => {});
range.on("delete", (rowId) => {});

// 关闭
sub.unsubscribe();
client.close();
```

## Component 数据

JS 客户端不需要、也不生成 Component 类型代码：服务端通过 MessagePack 推送的本来就是
plain object，直接用即可。例如订阅 `HP` 组件返回的就是
`{ id: 123, owner: 1, value: 100 }`。

```js
const sub = await client.get("HP", "owner", userId);
console.log(sub.data.value);  // 100

const m = await client.range("ChatMessage", "created_at_ms", 0, Date.now(), { limit: 50, desc: true });
for (const [rowId, msg] of m.rows) {
  console.log(msg.name, msg.text);
}
```

构造组件数据传给 `callSystem` 时，按服务端 `BaseComponent` 中定义的字段名写
plain object 即可：

```js
await client.callSystem("post_message", { text: "hello", kind: "chat" });
```

字段名、可订阅索引等元信息以服务端 `app.py` 里的 `@define_component`
定义为准。
