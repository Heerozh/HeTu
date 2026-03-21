# HeTu JavaScript 客户端 SDK 实现计划

## 概述

实现 HeTu 的 JavaScript 客户端 SDK，功能对标现有 Unity (C#) 客户端，支持浏览器和
Node.js 双平台运行。不包含 Unity Editor 相关工具（Inspector、PostStation、Setup）。

## 需求摘要

| 项目          | 决定                           |
|-------------|------------------------------|
| 运行环境        | 浏览器 + Node.js 通用             |
| 模块格式        | ESM 为主，兼容 CJS                |
| 语言          | 纯 JavaScript（不使用 TypeScript） |
| Pipeline 层级 | 全部三层：jsonb + zlib + crypto   |
| 订阅 API 风格   | 回调 + EventEmitter（零依赖）       |
| 代码生成        | 生成 JSDoc 注解的组件包装类            | 
| 发布方式        | 项目内子目录 `ClientSDK/js/`       |

## 依赖选型

| 依赖                        | 用途                              | 浏览器           | Node.js         |
|---------------------------|---------------------------------|---------------|-----------------|
| `msgpackr`                | MessagePack 序列化                 | 纯 JS fallback | native addon 加速 |
| `pako`                    | Zlib 压缩/解压                      | 纯 JS          | 纯 JS            |
| `libsodium-wrappers-sumo` | X25519 + ChaCha20-Poly1305-IETF | WASM          | WASM            |
| `ws` (peerDep)            | Node.js WebSocket               | 不需要           | peerDependency  |

选择 `libsodium` 的原因：服务端使用 PyNaCl（底层也是 libsodium），两端算法实现完全
一致，避免兼容性问题。

## 目录结构

```
ClientSDK/js/
├── package.json
├── rollup.config.js              # 打包：ESM + CJS
├── src/
│   ├── index.js                  # 公共 API 导出
│   ├── client.js                 # HeTuClient 主类
│   ├── pipeline/
│   │   ├── pipeline.js           # MessagePipeline 层级编排
│   │   ├── jsonb.js              # MessagePack 序列化层
│   │   ├── zlib.js               # Zlib 压缩层（流式 + preset dict）
│   │   └── crypto.js             # ECDH 密钥交换 + AEAD 加解密
│   ├── subscription.js           # RowSubscription / IndexSubscription
│   ├── response.js               # ResponseManager（FIFO 回调队列）
│   └── websocket.js              # WebSocket 跨平台适配器
├── gen/                          # codegen 输出（JSDoc 组件类）
└── tests/
    └── ...
```

### 与 Unity 客户端的模块映射

| Unity (C#)                         | JS                     | 说明                             |
|------------------------------------|------------------------|--------------------------------|
| `ClientBase.cs` + `UnityClient.cs` | `client.js`            | 合并，JS 无需 sync/async 双 API      |
| `Pipeline.cs`                      | `pipeline/pipeline.js` | 相同职责                           |
| `JsonbLayer.cs`                    | `pipeline/jsonb.js`    | msgpackr 替代 MessagePack-CSharp |
| `ZlibLayer.cs`                     | `pipeline/zlib.js`     | pako 替代 SharpZipLib            |
| `CryptoLayer.cs`                   | `pipeline/crypto.js`   | libsodium 替代 BouncyCastle      |
| `Subscription.cs`                  | `subscription.js`      | EventEmitter 替代 R3 Observable  |
| `Response.cs`                      | `response.js`          | 相同的 FIFO 模式                    |
| `InspectorTrace.cs`                | 不实现                    | Unity Editor 专属                |
| `Editor/`                          | 不实现                    | Unity Editor 专属                |

## 核心 API 设计

### HeTuClient

```js
const client = new HeTuClient();

// 连接（authKey 可选）
await client.connect("ws://localhost:8080/hetu/mydb", { authKey: "xxx" });

// RPC 调用
const result = await client.callSystem("login", "token123", 456);

// 单行订阅（Get）
const sub = await client.get("HP", "owner", userId);
sub.on("update", (data) => { /* row updated */ });
sub.on("delete", () => { /* row deleted */ });
console.log(sub.data);  // 当前行数据

// 范围订阅（Range）
const rangeSub = await client.range("HP", "owner", 0, 9999, { limit: 10, desc: false });
rangeSub.on("insert", (rowId, data) => { /* new row in range */ });
rangeSub.on("update", (rowId, data) => { /* row changed */ });
rangeSub.on("delete", (rowId) => { /* row removed */ });
console.log(rangeSub.rows);  // Map<rowId, data>

// 取消订阅
sub.unsubscribe();
rangeSub.unsubscribe();

// 连接事件
client.on("connected", () => {});
client.on("closed", (reason) => {});
client.on("error", (err) => {});

// 断开
client.close();
```

### 构造选项

```js
const client = new HeTuClient({
  autoReconnect: false,      // 可选：自动重连，默认关闭
  reconnectInterval: 3000,   // 重连间隔（ms）
});
```

### 与 Unity API 对比

| 功能   | Unity                                           | JS                                                  |
|------|-------------------------------------------------|-----------------------------------------------------|
| RPC  | `await CallSystem("name", args)` → `JsonObject` | `await callSystem("name", ...args)` → `Object`      |
| 单行订阅 | `await Get<T>("index", value)` 泛型强类型            | `await get(component, index, value)` → plain object |
| 范围订阅 | `await Range<T>(...)` + R3 Observable           | `await range(...)` + EventEmitter                   |
| 事件   | C# event delegate                               | `.on()` / `.off()`                                  |
| 取消订阅 | `sub.Dispose()`                                 | `sub.unsubscribe()`                                 |

## Pipeline 实现

### 层级架构

与服务端完全对齐的三层管线：

```
发送: payload → jsonb.encode → zlib.encode → crypto.encode → bytes
接收: bytes  → crypto.decode → zlib.decode → jsonb.decode → payload
```

每层统一接口：

```js
class PipelineLayer {
  get needsHandshake() { return false; }
  handshake(serverMsg) { return clientMsg; }
  encode(data, ctx) { return bytes; }
  decode(bytes, ctx) { return data; }
}
```

### 各层细节

**JsonbLayer**

- `msgpackr` 的 `pack()` / `unpack()`
- 无需握手

**ZlibLayer**

- `pako.Inflate` / `pako.Deflate` 流式实例
- 握手：接收服务端发来的 preset dictionary，设入压缩/解压上下文
- 使用 `Z_SYNC_FLUSH` 保持流式语义

**CryptoLayer**

- `libsodium-wrappers-sumo`
- 握手流程：
    1. 生成 X25519 临时密钥对
    2. 发送公钥（32 字节），或带 authKey 的签名 hello（H2A1 magic + 公钥 + 时间戳 + nonce +
       HMAC-SHA256，共 92 字节）
    3. 接收服务端公钥
    4. ECDH → 共享密钥 → blake2b 哈希 → session key
- 加解密：ChaCha20-Poly1305-IETF，12 字节 nonce 递增计数器
    - 客户端→服务端方向标志：`0x00`
    - 服务端→客户端方向标志：`0xff`

### 握手时序

```
Client                              Server
  │                                   │
  │  [crypto_hello]                   │   (32 或 92 字节)
  │ ──────────────────────────────►   │
  │                                   │
  │  [crypto_reply, zlib_dict]        │   (经 jsonb 编码)
  │ ◄──────────────────────────────   │
  │                                   │
  │  正常加密通信开始                    │
```

## 消息协议

与服务端完全一致，所有消息为 MessagePack 编码的数组：

### 客户端 → 服务端

```js
["rpc", "systemName", arg1, arg2, ...]          // RPC 调用
["sub", "Component", "get", "index", value]      // 单行订阅
["sub", "Component", "range", "index", l, r, limit, desc]  // 范围订阅
["unsub", subId]                                 // 取消订阅
["motd"]                                         // ping/调试
```

### 服务端 → 客户端

```js
["rsp", result]                                  // RPC 响应
["sub", subId, rowData]                          // 订阅初始数据
["updt", subId, { rowId: data, rowId2: null }]   // 订阅增量更新
```

### Subscription ID 格式

```
"{table}.{index}[{left}:{right}:{sign}][:{limit}]"
// 示例: "HP.owner[0:9999:1][:10]"
```

## WebSocket 跨平台适配

```js
// websocket.js
// 浏览器：使用全局 WebSocket（原生）
// Node.js：动态 import('ws')
// Deno/Bun：使用全局 WebSocket（原生）
```

Node.js 下 `ws` 作为 peerDependency，不打包进浏览器产物。

适配器封装：

- `connect(url)` → Promise
- `send(bytes)` → 发送 Uint8Array
- `onmessage` → 接收 Uint8Array
- `close()` / `onclose` / `onerror`

libsodium WASM 异步初始化在 `client.connect()` 内部处理：

```js
async connect(url, opts) {
  await sodium.ready;  // 确保 WASM 加载完毕
  // ... 建立 WebSocket，执行握手
}
```

## 错误处理

### 错误分类

| 类型        | 处理方式                                                             |
|-----------|------------------------------------------------------------------|
| 连接失败 / 断开 | `connect()` reject；触发 `client.on("closed")`；所有 pending 回调 reject |
| 握手失败      | `connect()` reject，附带错误信息                                        |
| RPC 服务端错误 | `callSystem()` reject，message 为服务端返回的错误文本                        |
| 订阅失败      | `get()` / `range()` reject（服务端返回 `["sub", "fail", ...]`）         |
| 消息解码异常    | 触发 `client.on("error")`，跳过该消息，不断开连接                              |

### 连接断开时的行为

- 所有 pending 的 `callSystem` / `get` / `range` 的 Promise 自动 reject
- 已有的 Subscription 对象触发 `close` 事件，标记为失效
- 离线队列中的消息保留（如果启用了 autoReconnect）

### 订阅生命周期

```
get()/range() → pending → 收到 ["sub", id, data] → active
                                                      │
                                          收到 ["updt"] → 触发事件
                                                      │
                                   unsubscribe() 或连接断开 → disposed
```

## 代码生成（sourcegen）

在 `hetu/sourcegen/` 下新增 `javascript.py`，与 `csharp.py` 平行。

### 生成产物示例

```js
/**
 * HP 组件
 * @typedef {Object} HP
 * @property {number} id
 * @property {number} owner
 * @property {number} value
 */

/**
 * 创建 HP 组件数据
 * @param {Partial<HP>} [data]
 * @returns {HP}
 */
export function createHP(data = {}) {
  return { id: 0, owner: 0, value: 0, ...data };
}

/** HP 组件的字段名常量 */
export const HPFields = {
  ID: "id",
  OWNER: "owner",
  VALUE: "value",
};
```

### 类型映射

| NumPy dtype                          | JS 类型     |
|--------------------------------------|-----------|
| `int8` / `int16` / `int32` / `int64` | `number`  |
| `float32` / `float64`                | `number`  |
| `bool`                               | `boolean` |
| `str*`                               | `string`  |

### CLI 集成

复用现有 `hetu build` 命令，增加 `--lang js` 选项：

```bash
hetu build --lang js --namespace MyGame
# 输出到 ClientSDK/js/gen/
```

## 实现阶段

### Phase 1：基础骨架

1. 初始化 `ClientSDK/js/` 项目结构、`package.json`、rollup 配置
2. 实现轻量 EventEmitter（浏览器/Node 通用）
3. 实现 WebSocket 跨平台适配器
4. 实现 `ResponseManager`（FIFO 回调队列）

### Phase 2：Pipeline 管线

5. 实现 `PipelineLayer` 基类和 `MessagePipeline` 编排
6. 实现 `JsonbLayer`（msgpackr）
7. 实现 `ZlibLayer`（pako，流式 + preset dictionary）
8. 实现 `CryptoLayer`（libsodium，ECDH + ChaCha20-Poly1305）
9. Pipeline 握手流程集成

### Phase 3：客户端核心

10. 实现 `HeTuClient` —— 连接、握手、消息收发
11. 实现 RPC 调用（`callSystem`）
12. 实现 `RowSubscription` 和 `IndexSubscription`
13. 实现订阅管理（get / range / unsubscribe / 更新分发）
14. 离线队列
15. 错误处理与连接断开清理

### Phase 4：代码生成

16. 实现 `hetu/sourcegen/javascript.py`
17. 集成到 `hetu build --lang js` CLI 命令

### Phase 5：打包与测试

18. rollup 打包配置（ESM + CJS 双格式输出）
19. 单元测试（Pipeline 各层编解码、握手）
20. 集成测试（连接真实 HeTu 服务端，RPC + 订阅完整流程）

## 不包含的功能

以下 Unity 客户端功能不在 JS 客户端范围内：

- `InspectorTrace` —— Unity Editor 调试工具
- `Editor/Inspector` —— Unity Inspector 窗口
- `Editor/PostStation` —— Unity 开发工具
- `Editor/Setup` —— Unity 设置向导
- R3 (Rx) Observable —— 用 EventEmitter 替代
- `PipelineBuffer` 零 GC 优化 —— JS 有 GC，无需手动管理
- Unity Awaitable / UniTask 桥接 —— JS 原生 async/await
