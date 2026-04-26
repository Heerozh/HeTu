import { EventEmitter } from "./event-emitter.js";
import { CryptoLayer } from "./pipeline/crypto.js";
import { JsonbLayer } from "./pipeline/jsonb.js";
import { MessagePipeline } from "./pipeline/pipeline.js";
import { ZlibLayer } from "./pipeline/zlib.js";
import { ResponseManager } from "./response.js";
import {
  IndexSubscription,
  RowSubscription,
  SubscriptionRegistry,
} from "./subscription.js";
import { createWebSocket } from "./websocket.js";

const STATE_DISCONNECTED = "disconnected";
const STATE_CONNECTING = "connecting";
const STATE_CONNECTED = "connected";

const CMD_RPC = "rpc";
const CMD_SUB = "sub";
const CMD_UNSUB = "unsub";
const CMD_MOTD = "motd";
const QUERY_GET = "get";
const QUERY_RANGE = "range";
const MSG_RESPONSE = "rsp";
const MSG_SUBED = "sub";
const MSG_UPDATE = "updt";
const INDEX_ID = "id";

/**
 * HeTu 客户端。
 *
 * 事件：
 *   - `connected()`：握手完成
 *   - `closed(reason)`：连接关闭，reason 为 null 表示正常关闭
 *   - `error(err)`：连接异常或消息解码异常
 */
export class HeTuClient extends EventEmitter {
  /**
   * @param {object} [opts]
   * @param {boolean} [opts.autoReconnect=false] 预留，当前版本未实现自动重连
   * @param {number} [opts.reconnectInterval=3000]
   */
  constructor(opts = {}) {
    super();
    this._opts = {
      autoReconnect: opts.autoReconnect ?? false,
      reconnectInterval: opts.reconnectInterval ?? 3000,
    };
    this._state = STATE_DISCONNECTED;
    this._pipeline = new MessagePipeline();
    this._setupDefaultPipeline();

    this._responses = new ResponseManager();
    this._subscriptions = new SubscriptionRegistry();
    /** @type {Array<{payload: any[], cb: Function | null}>} */
    this._offlineQueue = [];
    /** @type {import("./websocket.js").WebSocketAdapter | null} */
    this._ws = null;
    this._handshakeDone = false;
  }

  get isConnected() {
    return this._state === STATE_CONNECTED;
  }

  /**
   * 替换 pipeline 层。需在 connect 前调用。
   * @param {import("./pipeline/pipeline.js").PipelineLayer[]} layers
   */
  setupPipeline(layers) {
    this._pipeline.setLayers(layers);
  }

  /** @returns {import("./pipeline/pipeline.js").MessagePipeline} */
  get pipeline() {
    return this._pipeline;
  }

  _setupDefaultPipeline() {
    this._pipeline.setLayers([
      new JsonbLayer(),
      new ZlibLayer(),
      new CryptoLayer(),
    ]);
  }

  _getCryptoLayer() {
    for (const layer of this._pipeline.layers) {
      if (layer instanceof CryptoLayer) return layer;
    }
    return null;
  }

  /**
   * 连接到 HeTu 服务端。
   *
   * @param {string} url WebSocket 地址，如 `ws://host:port/hetu/<dbName>`
   * @param {object} [opts]
   * @param {string} [opts.authKey] 加密层握手用的预共享 key
   * @returns {Promise<void>}
   */
  async connect(url, opts = {}) {
    if (this._state !== STATE_DISCONNECTED) {
      throw new Error("[HeTu] 当前已经在连接中，请先 close()");
    }
    this._state = STATE_CONNECTING;

    const cryptoLayer = this._getCryptoLayer();
    if (cryptoLayer) {
      if (opts.authKey !== undefined) cryptoLayer.setAuthKey(opts.authKey);
      // libsodium WASM 初始化
      await CryptoLayer.ready();
    }

    // 前置清理
    this._subscriptions.clear();
    this._responses.cancelAll("重新连接");
    this._handshakeDone = false;

    const ws = await createWebSocket(url);
    this._ws = ws;

    return new Promise((resolve, reject) => {
      let resolved = false;
      const settle = (err) => {
        if (resolved) return;
        resolved = true;
        if (err) {
          this._state = STATE_DISCONNECTED;
          try {
            ws.close();
          } catch {
            // ignore
          }
          reject(err);
        } else {
          resolve();
        }
      };

      ws.onopen = () => {
        try {
          const hello = this._pipeline.clientHello();
          ws.send(hello);
        } catch (e) {
          settle(e);
        }
      };

      ws.onmessage = (data) => {
        if (!this._handshakeDone) {
          try {
            this._pipeline.handshake(data);
            this._handshakeDone = true;
            this._state = STATE_CONNECTED;
            this.emit("connected");
            this._flushOfflineQueue();
            settle();
          } catch (e) {
            settle(new Error(`[HeTu] 握手失败: ${e?.message || e}`));
          }
          return;
        }
        this._handleMessage(data);
      };

      ws.onclose = (reason) => {
        const wasConnecting = this._state === STATE_CONNECTING;
        this._cleanupConnection(reason);
        if (wasConnecting) {
          settle(new Error(`[HeTu] 连接关闭: ${reason || "unknown"}`));
        }
      };

      ws.onerror = (err) => {
        if (this._state === STATE_CONNECTING) {
          settle(err instanceof Error ? err : new Error(String(err)));
        } else {
          this.emit("error", err);
        }
      };
    });
  }

  /**
   * 主动关闭连接。
   */
  close() {
    if (this._ws) {
      try {
        this._ws.close();
      } catch {
        // ignore
      }
    }
    this._cleanupConnection(null);
  }

  _cleanupConnection(reason) {
    if (this._state === STATE_DISCONNECTED) return;
    this._state = STATE_DISCONNECTED;
    this._handshakeDone = false;
    this._responses.cancelAll(reason ?? "连接关闭");
    this._subscriptions.markAllDisconnected(reason ?? "disconnected");
    if (!this._opts.autoReconnect) {
      this._offlineQueue = [];
    }
    this._ws = null;
    this.emit("closed", reason ?? null);
  }

  _flushOfflineQueue() {
    const queue = this._offlineQueue;
    this._offlineQueue = [];
    for (const item of queue) {
      this._sendOrQueue(item.payload, item.cb);
    }
  }

  _sendOrQueue(payload, cb) {
    if (this._state === STATE_CONNECTED && this._ws) {
      try {
        const bytes = this._pipeline.encode(payload);
        this._ws.send(bytes);
        if (cb) this._responses.enqueue(cb);
      } catch (e) {
        if (cb) cb(null, true);
        this.emit("error", e);
      }
    } else {
      this._offlineQueue.push({ payload, cb });
    }
  }

  _handleMessage(bytes) {
    let decoded;
    try {
      decoded = this._pipeline.decode(bytes);
    } catch (e) {
      this.emit("error", e);
      return;
    }
    if (!Array.isArray(decoded) || decoded.length === 0) return;

    const cmd = decoded[0];
    switch (cmd) {
      case MSG_RESPONSE:
      case MSG_SUBED:
        this._responses.completeNext(decoded);
        break;
      case MSG_UPDATE: {
        const subId = decoded[1];
        const sub = this._subscriptions.get(subId);
        if (sub) {
          try {
            sub._applyUpdate(decoded[2] ?? {});
          } catch (e) {
            this.emit("error", e);
          }
        }
        break;
      }
      default:
        // 未知命令静默忽略，便于服务端兼容扩展
        break;
    }
  }

  /**
   * 发起 System RPC 调用。
   *
   * @param {string} systemName
   * @param {...any} args
   * @returns {Promise<any>} 服务端返回的 payload；服务端返回 "ok" 时为 "ok"
   */
  callSystem(systemName, ...args) {
    return new Promise((resolve, reject) => {
      const payload = [CMD_RPC, systemName, ...args];
      this._sendOrQueue(payload, (response, cancel) => {
        if (cancel) {
          reject(new Error("[HeTu] 请求已取消"));
          return;
        }
        // ["rsp", payload]
        if (Array.isArray(response) && response.length > 1) {
          resolve(response[1]);
        } else {
          resolve(null);
        }
      });
    });
  }

  /**
   * 发起单行订阅。
   *
   * @template T
   * @param {string} componentName
   * @param {string} index 索引字段名
   * @param {any} value 索引值
   * @returns {Promise<RowSubscription | null>} 查不到行时返回 null
   */
  get(componentName, index, value) {
    return new Promise((resolve, reject) => {
      // 如果按 id 订阅，可以预先组合 subId 避免重复订阅
      if (index === INDEX_ID) {
        const predictId = makeSubId(componentName, INDEX_ID, value, null, 1, false);
        const existing = this._subscriptions.get(predictId);
        if (existing instanceof RowSubscription) {
          resolve(existing);
          return;
        }
      }

      const payload = [CMD_SUB, componentName, QUERY_GET, index, value];
      this._sendOrQueue(payload, (response, cancel) => {
        if (cancel) {
          reject(new Error("[HeTu] 订阅请求已取消"));
          return;
        }
        try {
          // 预期格式：["sub", subId, rowData] 或 ["sub", null, ...] / ["sub", "fail", ...]
          if (!Array.isArray(response)) {
            reject(new Error("[HeTu] 订阅响应格式错误"));
            return;
          }
          const subId = response[1];
          if (subId === null || subId === undefined) {
            resolve(null);
            return;
          }
          if (subId === "fail") {
            reject(new Error("[HeTu] 服务端拒绝订阅"));
            return;
          }
          const existing = this._subscriptions.get(subId);
          if (existing instanceof RowSubscription) {
            resolve(existing);
            return;
          }
          const data = response[2] ?? null;
          const sub = new RowSubscription(subId, componentName, data, this);
          this._subscriptions.set(subId, sub);
          resolve(sub);
        } catch (e) {
          reject(e);
        }
      });
    });
  }

  /**
   * 发起范围订阅。
   *
   * @template T
   * @param {string} componentName
   * @param {string} index
   * @param {any} left
   * @param {any} right
   * @param {object} [opts]
   * @param {number} [opts.limit=10]
   * @param {boolean} [opts.desc=false]
   * @param {boolean} [opts.force=true] 即使无数据也保持订阅
   * @returns {Promise<IndexSubscription | null>}
   */
  range(componentName, index, left, right, opts = {}) {
    const limit = opts.limit ?? 10;
    const desc = !!opts.desc;
    const force = opts.force ?? true;

    return new Promise((resolve, reject) => {
      const predictId = makeSubId(componentName, index, left, right, limit, desc);
      const existing = this._subscriptions.get(predictId);
      if (existing instanceof IndexSubscription) {
        resolve(existing);
        return;
      }

      const payload = [
        CMD_SUB,
        componentName,
        QUERY_RANGE,
        index,
        left,
        right,
        limit,
        desc,
        force,
      ];
      this._sendOrQueue(payload, (response, cancel) => {
        if (cancel) {
          reject(new Error("[HeTu] 范围订阅请求已取消"));
          return;
        }
        try {
          if (!Array.isArray(response)) {
            reject(new Error("[HeTu] 订阅响应格式错误"));
            return;
          }
          const subId = response[1];
          if (subId === null || subId === undefined) {
            resolve(null);
            return;
          }
          if (subId === "fail") {
            reject(new Error("[HeTu] 服务端拒绝订阅"));
            return;
          }
          const existing2 = this._subscriptions.get(subId);
          if (existing2 instanceof IndexSubscription) {
            resolve(existing2);
            return;
          }
          const rows = response[2];
          const initial = Array.isArray(rows) ? rows : [];
          const sub = new IndexSubscription(subId, componentName, initial, this);
          this._subscriptions.set(subId, sub);
          resolve(sub);
        } catch (e) {
          reject(e);
        }
      });
    });
  }

  /**
   * 发送 motd（welcome message of the day）。
   * 主要用于测试连通性。
   * @returns {Promise<any>}
   */
  motd() {
    return new Promise((resolve) => {
      this._sendOrQueue([CMD_MOTD], (response) => {
        resolve(response);
      });
    });
  }

  /**
   * 内部使用：取消订阅。被 BaseSubscription.unsubscribe() 调用。
   * @internal
   * @param {string} subId
   * @param {string} _from
   */
  _unsubscribe(subId, _from) {
    if (!this._subscriptions.has(subId)) return;
    this._subscriptions.delete(subId);
    if (this._state !== STATE_CONNECTED) return;
    const payload = [CMD_UNSUB, subId];
    this._sendOrQueue(payload, null);
  }
}

/**
 * 与服务端一致的订阅 ID 格式：
 *   "{table}.{index}[{left}:{right}:{sign}][:{limit}]"
 *
 * @param {string} table
 * @param {string} index
 * @param {any} left
 * @param {any} right
 * @param {number} limit
 * @param {boolean} desc
 * @returns {string}
 */
function makeSubId(table, index, left, right, limit, desc) {
  const sign = desc ? -1 : 1;
  const r = right == null ? "None" : right;
  return `${table}.${index}[${left}:${r}:${sign}][:${limit}]`;
}
