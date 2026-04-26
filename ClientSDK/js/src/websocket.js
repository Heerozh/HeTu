/**
 * 跨平台 WebSocket 适配器。
 *
 * - 浏览器/Deno/Bun：使用全局 `WebSocket`。
 * - Node.js：动态 import `ws`。`ws` 作为 peerDependency，浏览器打包不会包含。
 */

const isNode =
  typeof process !== "undefined" &&
  process.versions != null &&
  process.versions.node != null &&
  typeof globalThis.WebSocket === "undefined";

let _wsCtor = null;

async function getWebSocketCtor() {
  if (_wsCtor) return _wsCtor;
  if (typeof globalThis.WebSocket !== "undefined") {
    _wsCtor = globalThis.WebSocket;
    return _wsCtor;
  }
  if (isNode) {
    try {
      const mod = await import(/* @vite-ignore */ "ws");
      _wsCtor = mod.WebSocket || mod.default || mod;
      return _wsCtor;
    } catch (err) {
      throw new Error(
        "[HeTu] 在 Node.js 环境下无法加载 'ws' 模块，请安装：npm install ws"
      );
    }
  }
  throw new Error("[HeTu] 当前运行环境没有可用的 WebSocket 实现");
}

/**
 * 建立 WebSocket 连接，并返回统一接口的适配器。
 *
 * @param {string} url
 * @returns {Promise<WebSocketAdapter>}
 */
export async function createWebSocket(url) {
  const Ctor = await getWebSocketCtor();
  const ws = new Ctor(url);
  // 浏览器原生 WebSocket 默认 binaryType 是 "blob"，要切到 "arraybuffer"
  if ("binaryType" in ws) {
    ws.binaryType = "arraybuffer";
  }
  return new WebSocketAdapter(ws);
}

/**
 * WebSocket 适配器，对外暴露统一的事件回调与 send 接口。
 */
export class WebSocketAdapter {
  constructor(ws) {
    this._ws = ws;
    /** @type {(() => void) | null} */
    this.onopen = null;
    /** @type {((data: Uint8Array) => void) | null} */
    this.onmessage = null;
    /** @type {((reason: string | null) => void) | null} */
    this.onclose = null;
    /** @type {((err: Error) => void) | null} */
    this.onerror = null;

    ws.onopen = () => {
      this.onopen?.();
    };
    ws.onmessage = (ev) => {
      const data = ev.data;
      if (data == null) return;
      if (data instanceof ArrayBuffer) {
        this.onmessage?.(new Uint8Array(data));
        return;
      }
      // Node.js 的 ws 会给 Buffer
      if (typeof Buffer !== "undefined" && Buffer.isBuffer?.(data)) {
        this.onmessage?.(
          new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
        );
        return;
      }
      // Blob（旧浏览器）
      if (typeof Blob !== "undefined" && data instanceof Blob) {
        data
          .arrayBuffer()
          .then((buf) => this.onmessage?.(new Uint8Array(buf)))
          .catch((err) => this.onerror?.(err));
        return;
      }
      // 已经是 Uint8Array
      if (data instanceof Uint8Array) {
        this.onmessage?.(data);
        return;
      }
      // 字符串帧（非预期）
      if (typeof data === "string") {
        this.onmessage?.(new TextEncoder().encode(data));
        return;
      }
      this.onerror?.(new Error("Unknown message data type from WebSocket"));
    };
    ws.onclose = (ev) => {
      // 1000 正常，其它视为异常关闭
      const reason =
        ev?.code === 1000 || ev?.code === undefined
          ? null
          : ev.reason || `code=${ev.code}`;
      this.onclose?.(reason);
    };
    ws.onerror = (ev) => {
      const err =
        ev instanceof Error
          ? ev
          : new Error(ev?.message || "WebSocket error");
      this.onerror?.(err);
    };
  }

  /**
   * 当前连接是否已打开。
   * @returns {boolean}
   */
  get isOpen() {
    return this._ws.readyState === 1; // OPEN
  }

  /**
   * 发送二进制数据。
   * @param {Uint8Array} bytes
   */
  send(bytes) {
    this._ws.send(bytes);
  }

  /**
   * 主动关闭连接。
   * @param {number} [code]
   * @param {string} [reason]
   */
  close(code = 1000, reason) {
    try {
      this._ws.close(code, reason);
    } catch {
      // 忽略已关闭等情况
    }
  }
}
