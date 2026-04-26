// Verify that the signed hello matches what the Python server expects:
//   payload = "H2A1" + publicKey(32) + timestamp(8) + nonce(16)
//   signature = HMAC-SHA256(authKey, payload)
//
// This test reproduces the server-side validation logic
// (see hetu/server/pipeline/crypto.py CryptoLayer._parse_client_public_key).

import test from "node:test";
import assert from "node:assert/strict";
import sodium from "libsodium-wrappers-sumo";
import { CryptoLayer } from "../src/index.js";

await sodium.ready;
await CryptoLayer.ready();

test("H2A1 hello: 服务端验证逻辑成功", () => {
  const authKey = "shared-secret-1234";
  const c = new CryptoLayer({ authKey });
  const hello = c.clientHello();

  assert.equal(hello.length, 92);
  // Magic
  assert.deepEqual(Array.from(hello.slice(0, 4)), [0x48, 0x32, 0x41, 0x31]);

  // 服务端解析（mirror python 实现）
  const payload = hello.slice(0, hello.length - 32);
  const signature = hello.slice(hello.length - 32);
  const clientPublicKey = hello.slice(4, 36);

  const keyBytes = new TextEncoder().encode(authKey);
  const state = sodium.crypto_auth_hmacsha256_init(keyBytes);
  sodium.crypto_auth_hmacsha256_update(state, payload);
  const expected = sodium.crypto_auth_hmacsha256_final(state);

  assert.deepEqual(Array.from(signature), Array.from(expected));
  assert.equal(clientPublicKey.length, 32);
});

test("H2A1 hello: 错误的 authKey 服务端验证失败", () => {
  const c = new CryptoLayer({ authKey: "real-key" });
  const hello = c.clientHello();
  const payload = hello.slice(0, hello.length - 32);
  const signature = hello.slice(hello.length - 32);

  const wrongKey = new TextEncoder().encode("wrong-key");
  const state = sodium.crypto_auth_hmacsha256_init(wrongKey);
  sodium.crypto_auth_hmacsha256_update(state, payload);
  const expected = sodium.crypto_auth_hmacsha256_final(state);

  assert.notDeepEqual(Array.from(signature), Array.from(expected));
});
