import sodium from "libsodium-wrappers-sumo";
import { PipelineLayer } from "./pipeline.js";

const NONCE_SIZE = 12;
const SIGNED_HELLO_MAGIC = new Uint8Array([0x48, 0x32, 0x41, 0x31]); // "H2A1"
const LEGACY_HELLO_SIZE = 32;
const SIGNED_HELLO_SIZE = 92;

/**
 * 加密层。
 *
 * 使用 X25519 ECDH 协商共享密钥，再用 Blake2b-256 派生 session key，
 * 后续消息用 ChaCha20-Poly1305-IETF AEAD 加密。
 *
 * Nonce 设计：12 字节 = sign(1) + counter(11)。
 *  - 客户端→服务端方向 sign = 0xFF
 *  - 服务端→客户端方向 sign = 0x00
 *  - counter 从 1 开始递增（每次 encode/decode 前 +1），bigEndian 编码到末 11 字节。
 *
 * 与服务端 PyNaCl 实现完全等价。
 */
export class CryptoLayer extends PipelineLayer {
  /**
   * @param {object} [opts]
   * @param {string} [opts.authKey] 可选的预共享认证 key（启用 H2A1 握手）。
   */
  constructor(opts = {}) {
    super();
    /** @type {Uint8Array | null} */
    this._authKey = opts.authKey ? toUtf8(opts.authKey) : null;
    /** @type {Uint8Array | null} */
    this._privateKey = null;
    /** @type {Uint8Array | null} */
    this._sessionKey = null;
    this._sendNonce = 0;
    this._recvNonce = 0;
    this._ready = false;
  }

  /**
   * 等待 libsodium WASM 初始化完成。需在使用前调用。
   * @returns {Promise<void>}
   */
  static async ready() {
    await sodium.ready;
  }

  /**
   * 设置 / 清空预共享认证 key。
   * @param {string | null} authKey
   */
  setAuthKey(authKey) {
    this._authKey = authKey ? toUtf8(authKey) : null;
  }

  dispose() {
    this._privateKey = null;
    this._sessionKey = null;
  }

  /**
   * 生成 X25519 临时密钥对，返回客户端 hello（公钥或带签名的扩展 hello）。
   * @returns {Uint8Array}
   */
  clientHello() {
    if (!sodium.crypto_kx_keypair) {
      throw new Error(
        "[HeTu] libsodium 未初始化，请先 await CryptoLayer.ready()"
      );
    }
    const kp = sodium.crypto_box_keypair();
    this._privateKey = kp.privateKey;
    this._sessionKey = null;
    this._sendNonce = 0;
    this._recvNonce = 0;
    if (!this._authKey) {
      return kp.publicKey.slice();
    }
    return buildSignedHello(kp.publicKey, this._authKey);
  }

  /**
   * 接收服务端 32 字节公钥，派生 session key。
   * @param {Uint8Array} message
   */
  handshake(message) {
    if (!message || message.length !== 32) {
      throw new Error(
        `[HeTu] 加密握手失败：服务端公钥长度错误 (${message?.length ?? 0})`
      );
    }
    if (!this._privateKey) {
      throw new Error("[HeTu] 加密握手失败：本地私钥缺失");
    }

    // X25519 ECDH 共享点
    const shared = sodium.crypto_scalarmult(this._privateKey, message);
    // Blake2b-256 派生 session key
    const sessionKey = sodium.crypto_generichash(32, shared);

    this._sessionKey = sessionKey;
    this._sendNonce = 0;
    this._recvNonce = 0;
    this._privateKey = null;
    this._ready = true;
  }

  /**
   * 加密。
   * @param {Uint8Array} message
   * @returns {Uint8Array}
   */
  encode(message) {
    if (!this._sessionKey) return message;
    if (!(message instanceof Uint8Array)) {
      throw new TypeError("CryptoLayer.encode 输入必须是 Uint8Array");
    }
    this._sendNonce += 1;
    const nonce = buildNonce(0xff, this._sendNonce);
    return sodium.crypto_aead_chacha20poly1305_ietf_encrypt(
      message,
      null,
      null,
      nonce,
      this._sessionKey
    );
  }

  /**
   * 解密。
   * @param {Uint8Array} message
   * @returns {Uint8Array}
   */
  decode(message) {
    if (!this._sessionKey) return message;
    if (!(message instanceof Uint8Array)) {
      throw new TypeError("CryptoLayer.decode 输入必须是 Uint8Array");
    }
    if (message.length < 16) {
      throw new Error("[HeTu] 解密失败：数据长度不足");
    }
    this._recvNonce += 1;
    const nonce = buildNonce(0x00, this._recvNonce);
    try {
      return sodium.crypto_aead_chacha20poly1305_ietf_decrypt(
        null,
        message,
        null,
        nonce,
        this._sessionKey
      );
    } catch (e) {
      throw new Error(
        `[HeTu] 解密验证失败，可能密钥不匹配或数据被篡改: ${e?.message || e}`
      );
    }
  }
}

/**
 * 构建 12 字节 nonce：sign(1) + counter(11, big-endian)。
 *
 * @param {number} sign
 * @param {number} counter 递增计数器（JS 数字精度足够 2^53，此处取低 11 字节）。
 * @returns {Uint8Array}
 */
function buildNonce(sign, counter) {
  const out = new Uint8Array(NONCE_SIZE);
  out[0] = sign;
  // 从最低字节开始填充
  let c = counter;
  for (let i = NONCE_SIZE - 1; i >= 1; i--) {
    out[i] = c & 0xff;
    c = Math.floor(c / 256);
  }
  return out;
}

/**
 * 构建 H2A1 签名 hello。
 *
 *   payload = magic(4) + publicKey(32) + timestamp(8, 全零) + nonce(16, 随机)
 *   hello   = payload + HMAC-SHA256(authKey, payload)
 *
 * 时间戳目前固定为 8 个零字节，与 Unity 客户端保持一致。
 *
 * @param {Uint8Array} publicKey
 * @param {Uint8Array} authKey
 * @returns {Uint8Array}
 */
function buildSignedHello(publicKey, authKey) {
  const timestamp = new Uint8Array(8);
  const nonce = sodium.randombytes_buf(16);
  const payload = new Uint8Array(
    SIGNED_HELLO_MAGIC.length + publicKey.length + timestamp.length + nonce.length
  );
  let off = 0;
  payload.set(SIGNED_HELLO_MAGIC, off);
  off += SIGNED_HELLO_MAGIC.length;
  payload.set(publicKey, off);
  off += publicKey.length;
  payload.set(timestamp, off);
  off += timestamp.length;
  payload.set(nonce, off);

  const sig = hmacSha256(authKey, payload);
  const hello = new Uint8Array(payload.length + sig.length);
  hello.set(payload, 0);
  hello.set(sig, payload.length);
  if (hello.length !== SIGNED_HELLO_SIZE) {
    throw new Error("[HeTu] 签名 hello 长度异常");
  }
  return hello;
}

/**
 * HMAC-SHA256 实现。
 *
 * libsodium 的 `crypto_auth_hmacsha256` 单次 API 要求 key 必须是 32 字节，
 * 而 HMAC 标准支持任意长度 key（内部用 SHA256 哈希到 32 字节）。
 * 使用 init/update/final 增量 API 可以接受任意长度 key，与服务端
 * `hmac.new(authKey, msg, hashlib.sha256)` 完全等价。
 *
 * @param {Uint8Array} key
 * @param {Uint8Array} msg
 * @returns {Uint8Array}
 */
function hmacSha256(key, msg) {
  const state = sodium.crypto_auth_hmacsha256_init(key);
  sodium.crypto_auth_hmacsha256_update(state, msg);
  return sodium.crypto_auth_hmacsha256_final(state);
}

/**
 * @param {string} s
 * @returns {Uint8Array}
 */
function toUtf8(s) {
  return new TextEncoder().encode(s);
}
