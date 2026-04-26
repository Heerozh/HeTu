import test from "node:test";
import assert from "node:assert/strict";

import {
  CryptoLayer,
  EventEmitter,
  IndexSubscription,
  JsonbLayer,
  MessagePipeline,
  ResponseManager,
  RowSubscription,
  SubscriptionRegistry,
  ZlibLayer,
} from "../src/index.js";

test("EventEmitter 基础 on/off/emit 行为", () => {
  const ev = new EventEmitter();
  const calls = [];
  const handler = (a, b) => calls.push([a, b]);
  ev.on("x", handler);
  ev.emit("x", 1, 2);
  ev.emit("x", 3, 4);
  ev.off("x", handler);
  ev.emit("x", 5, 6);
  assert.deepEqual(calls, [
    [1, 2],
    [3, 4],
  ]);
});

test("EventEmitter once 只触发一次", () => {
  const ev = new EventEmitter();
  let n = 0;
  ev.once("x", () => n++);
  ev.emit("x");
  ev.emit("x");
  assert.equal(n, 1);
});

test("EventEmitter 监听器中的异常发到 error 事件", () => {
  const ev = new EventEmitter();
  const errors = [];
  ev.on("error", (e) => errors.push(e));
  ev.on("x", () => {
    throw new Error("boom");
  });
  ev.emit("x");
  assert.equal(errors.length, 1);
  assert.equal(errors[0].message, "boom");
});

test("ResponseManager 按 FIFO 完成回调", () => {
  const rm = new ResponseManager();
  const log = [];
  rm.enqueue((r) => log.push(["a", r]));
  rm.enqueue((r) => log.push(["b", r]));
  rm.completeNext(["rsp", "1"]);
  rm.completeNext(["rsp", "2"]);
  assert.deepEqual(log, [
    ["a", ["rsp", "1"]],
    ["b", ["rsp", "2"]],
  ]);
});

test("ResponseManager cancelAll 取消所有等待", () => {
  const rm = new ResponseManager();
  let cancelCount = 0;
  rm.enqueue((_, cancel) => {
    if (cancel) cancelCount++;
  });
  rm.enqueue((_, cancel) => {
    if (cancel) cancelCount++;
  });
  rm.cancelAll("test");
  assert.equal(cancelCount, 2);
  assert.equal(rm.pending, 0);
});

test("JsonbLayer 编解码与 msgspec 兼容（数组与对象）", () => {
  const j = new JsonbLayer();
  const cases = [
    ["rsp", { a: 1, b: "x" }],
    ["sub", "HP.id[1:None:1][:1]", { id: 1, value: 99 }],
    ["updt", "Foo", { 5: { id: 5, name: "bar" }, 7: null }],
  ];
  for (const c of cases) {
    const enc = j.encode(c);
    assert.ok(enc instanceof Uint8Array);
    const dec = j.decode(enc);
    assert.deepEqual(dec, c);
  }
});

test("ZlibLayer 流式 deflate/inflate 往返（无字典）", () => {
  const enc = new ZlibLayer();
  const dec = new ZlibLayer();
  enc.handshake(new Uint8Array(0));
  dec.handshake(new Uint8Array(0));

  const td = new TextEncoder();
  const td2 = new TextDecoder();
  const messages = [
    "hello world",
    "second message with some repeating content content content",
    "final 漢字 测试",
  ];
  for (const m of messages) {
    const compressed = enc.encode(td.encode(m));
    const out = dec.decode(compressed);
    assert.equal(td2.decode(out), m);
  }
});

test("ZlibLayer 流式 deflate/inflate 带字典", () => {
  const enc = new ZlibLayer();
  const dec = new ZlibLayer();
  const dict = new TextEncoder().encode(
    "id\nname\nvalue\nowner\nupdt\nHP\nMP"
  );
  enc.handshake(dict);
  dec.handshake(dict);

  const td = new TextEncoder();
  const td2 = new TextDecoder();
  const m1 = JSON.stringify({ id: 1, name: "foo", value: 100, owner: 2 });
  const c1 = enc.encode(td.encode(m1));
  const out1 = dec.decode(c1);
  assert.equal(td2.decode(out1), m1);

  // 第二轮，验证流式上下文保持
  const m2 = JSON.stringify({ updt: "HP", id: 2, value: 200 });
  const c2 = enc.encode(td.encode(m2));
  const out2 = dec.decode(c2);
  assert.equal(td2.decode(out2), m2);
});

test("CryptoLayer 客户端/服务端握手 + 双向 AEAD", async () => {
  await CryptoLayer.ready();

  // 模拟服务端握手 - 直接用 sodium 实现一份
  const sodium = (await import("libsodium-wrappers-sumo")).default;
  await sodium.ready;

  const client = new CryptoLayer();
  const helloBytes = client.clientHello();
  assert.equal(helloBytes.length, 32);

  // server: 解析公钥，生成自己的 keypair，派生 session
  const serverKp = sodium.crypto_box_keypair();
  const serverShared = sodium.crypto_scalarmult(
    serverKp.privateKey,
    helloBytes
  );
  const serverSession = sodium.crypto_generichash(32, serverShared);

  // 把服务端公钥喂回 client.handshake
  client.handshake(serverKp.publicKey);

  // 双向加解密：客户端发 → 服务端解（counter=1, sign=0xFF）
  const td = new TextEncoder();
  const plaintext = td.encode("hello server");
  const cipher = client.encode(plaintext);

  // 服务端用 nonce sign=0xFF, counter=1 解
  const nonce1 = new Uint8Array(12);
  nonce1[0] = 0xff;
  nonce1[11] = 1;
  const decrypted = sodium.crypto_aead_chacha20poly1305_ietf_decrypt(
    null,
    cipher,
    null,
    nonce1,
    serverSession
  );
  assert.equal(new TextDecoder().decode(decrypted), "hello server");

  // 服务端发 → 客户端解（counter=1, sign=0x00）
  const reply = td.encode("hello client");
  const nonce2 = new Uint8Array(12);
  nonce2[0] = 0x00;
  nonce2[11] = 1;
  const replyCipher = sodium.crypto_aead_chacha20poly1305_ietf_encrypt(
    reply,
    null,
    null,
    nonce2,
    serverSession
  );
  const replyPlain = client.decode(replyCipher);
  assert.equal(new TextDecoder().decode(replyPlain), "hello client");
});

test("CryptoLayer authKey 触发 H2A1 签名 hello (92 字节)", async () => {
  await CryptoLayer.ready();
  const c = new CryptoLayer({ authKey: "my-secret" });
  const hello = c.clientHello();
  assert.equal(hello.length, 92);
  // magic = "H2A1"
  assert.deepEqual(Array.from(hello.slice(0, 4)), [0x48, 0x32, 0x41, 0x31]);
});

test("MessagePipeline 三层端到端往返", async () => {
  await CryptoLayer.ready();
  const sodium = (await import("libsodium-wrappers-sumo")).default;
  await sodium.ready;

  const clientPipe = new MessagePipeline();
  const clientCrypto = new CryptoLayer();
  const clientZlib = new ZlibLayer();
  clientPipe.setLayers([new JsonbLayer(), clientZlib, clientCrypto]);

  // 客户端 hello（jsonb + crypto 公钥；zlib 部分为空字节）
  const helloEncoded = clientPipe.clientHello();

  // 模拟服务端：jsonb 解出层级数组 -> [zlib, crypto]
  const serverJsonb = new JsonbLayer();
  const layerHellos = serverJsonb.decode(helloEncoded);
  assert.ok(Array.isArray(layerHellos));
  // [zlib_hello(0 bytes), crypto_hello(32 bytes)]
  assert.equal(layerHellos.length, 2);
  assert.equal(layerHellos[0].length, 0);
  assert.equal(layerHellos[1].length, 32);

  // 服务端构造握手回包：zlib 字典 + 公钥
  const dict = new TextEncoder().encode("id\nname\nvalue\nupdt\n");
  const serverKp = sodium.crypto_box_keypair();
  const serverShared = sodium.crypto_scalarmult(
    serverKp.privateKey,
    layerHellos[1]
  );
  const serverSession = sodium.crypto_generichash(32, serverShared);
  const handshakeReply = serverJsonb.encode([dict, serverKp.publicKey]);

  // 客户端处理握手响应
  clientPipe.handshake(handshakeReply);

  // 服务端 zlib 上下文初始化
  const serverZlib = new ZlibLayer();
  serverZlib.handshake(dict);

  // 客户端发 ["rpc", "login", "tok"] → 服务端解码
  const payload = ["rpc", "login", "tok"];
  const wire = clientPipe.encode(payload);

  // 模拟服务端：crypto 解（counter=1, sign=0xff）→ zlib inflate → jsonb decode
  const nonce1 = new Uint8Array(12);
  nonce1[0] = 0xff;
  nonce1[11] = 1;
  const afterCrypto = sodium.crypto_aead_chacha20poly1305_ietf_decrypt(
    null,
    wire,
    null,
    nonce1,
    serverSession
  );
  const afterZlib = serverZlib.decode(afterCrypto);
  const final = serverJsonb.decode(afterZlib);
  assert.deepEqual(final, payload);

  // 服务端响应 → 客户端解
  const reply = ["rsp", "ok"];
  const replyJsonb = serverJsonb.encode(reply);
  const replyZlib = serverZlib.encode(replyJsonb);
  const nonce2 = new Uint8Array(12);
  nonce2[0] = 0x00;
  nonce2[11] = 1;
  const replyCipher = sodium.crypto_aead_chacha20poly1305_ietf_encrypt(
    replyZlib,
    null,
    null,
    nonce2,
    serverSession
  );
  const decoded = clientPipe.decode(replyCipher);
  assert.deepEqual(decoded, reply);
});

test("RowSubscription 触发 update / delete 事件", () => {
  const fakeClient = { _unsubscribe: () => {} };
  const sub = new RowSubscription(
    "HP.id[1:None:1][:1]",
    "HP",
    { id: 1, value: 100 },
    fakeClient
  );
  const events = [];
  sub.on("update", (d) => events.push(["update", d]));
  sub.on("delete", () => events.push(["delete"]));

  sub._applyUpdate({ 1: { id: 1, value: 80 } });
  sub._applyUpdate({ 1: null });
  assert.deepEqual(events, [
    ["update", { id: 1, value: 80 }],
    ["delete"],
  ]);
  assert.equal(sub.data, null);
});

test("IndexSubscription 触发 insert/update/delete", () => {
  const fakeClient = { _unsubscribe: () => {} };
  const sub = new IndexSubscription(
    "HP.value[0:99:1][:5]",
    "HP",
    [{ id: 1, value: 10 }],
    fakeClient
  );
  const events = [];
  sub.on("insert", (id, d) => events.push(["insert", id, d]));
  sub.on("update", (id, d) => events.push(["update", id, d]));
  sub.on("delete", (id) => events.push(["delete", id]));

  // 注：JS 对象数字 key 按数字升序迭代，而非插入顺序。这里期望按 1,2,3 的顺序处理。
  sub._applyUpdate({
    2: { id: 2, value: 20 },
    1: { id: 1, value: 11 },
    3: { id: 3, value: 30 },
  });
  sub._applyUpdate({ 2: null });
  assert.deepEqual(events, [
    ["update", 1, { id: 1, value: 11 }],
    ["insert", 2, { id: 2, value: 20 }],
    ["insert", 3, { id: 3, value: 30 }],
    ["delete", 2],
  ]);
  assert.deepEqual([...sub.rows.keys()].sort(), [1, 3]);
});

test("SubscriptionRegistry 弱引用语义", () => {
  const reg = new SubscriptionRegistry();
  const fakeClient = { _unsubscribe: () => {} };
  const sub = new RowSubscription("X", "X", null, fakeClient);
  reg.set("X", sub);
  assert.equal(reg.get("X"), sub);
  reg.delete("X");
  assert.equal(reg.get("X"), null);
});
