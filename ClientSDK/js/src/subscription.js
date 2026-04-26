import { EventEmitter } from "./event-emitter.js";

/**
 * 订阅基类。
 *
 * @abstract
 */
export class BaseSubscription extends EventEmitter {
  /**
   * @param {string} subId 订阅 ID，由服务端生成
   * @param {string} componentName 组件名
   * @param {import("./client.js").HeTuClient} client 客户端引用
   */
  constructor(subId, componentName, client) {
    super();
    this.subId = subId;
    this.componentName = componentName;
    this._client = client;
    this._closed = false;
  }

  get isClosed() {
    return this._closed;
  }

  /**
   * 取消订阅。
   * 通知服务端、并触发 close 事件。
   */
  unsubscribe() {
    if (this._closed) return;
    this._closed = true;
    this._client._unsubscribe(this.subId, "unsubscribe");
    this.emit("close", null);
  }

  /**
   * 由 client.js 在收到 ["updt", subId, rows] 时调用。
   * @param {Object<string|number, any>} rows
   */
  _applyUpdate(rows) {
    throw new Error("_applyUpdate must be implemented by subclass");
  }

  /**
   * 由 client.js 在连接断开时调用。
   */
  _markDisconnected(reason) {
    if (this._closed) return;
    this._closed = true;
    this.emit("close", reason ?? "disconnected");
  }
}

/**
 * 单行订阅（Get 结果）。
 *
 * 事件：
 *   - `update(data)`: 行数据更新
 *   - `delete()`: 行被删除（data 变为 null）
 *   - `close(reason)`: 取消订阅或连接断开
 */
export class RowSubscription extends BaseSubscription {
  /**
   * @param {string} subId
   * @param {string} componentName
   * @param {any} initialData
   * @param {import("./client.js").HeTuClient} client
   */
  constructor(subId, componentName, initialData, client) {
    super(subId, componentName, client);
    this.data = initialData;
    this.lastRowId = initialData ? toRowId(initialData.id) : null;
  }

  _applyUpdate(rows) {
    // rows 是 { rowId: data | null }
    for (const [rowIdRaw, rowData] of Object.entries(rows)) {
      const rowId = toRowId(rowIdRaw);
      if (rowData == null) {
        this.data = null;
        this.emit("delete", rowId);
      } else {
        this.data = rowData;
        this.lastRowId = toRowId(rowData.id ?? rowId);
        this.emit("update", rowData);
      }
    }
  }
}

/**
 * 范围订阅（Range 结果）。
 *
 * 事件：
 *   - `insert(rowId, data)`: 范围内有新行
 *   - `update(rowId, data)`: 范围内行有变更
 *   - `delete(rowId)`: 范围内行被删除/移出
 *   - `close(reason)`: 取消订阅或连接断开
 */
export class IndexSubscription extends BaseSubscription {
  /**
   * @param {string} subId
   * @param {string} componentName
   * @param {Array<any>} initialRows
   * @param {import("./client.js").HeTuClient} client
   */
  constructor(subId, componentName, initialRows, client) {
    super(subId, componentName, client);
    /** @type {Map<number|string, any>} */
    this.rows = new Map();
    if (Array.isArray(initialRows)) {
      for (const row of initialRows) {
        if (row == null) continue;
        const id = toRowId(row.id);
        this.rows.set(id, row);
      }
    }
  }

  _applyUpdate(rows) {
    for (const [rowIdRaw, rowData] of Object.entries(rows)) {
      const rowId = toRowId(rowIdRaw);
      const exists = this.rows.has(rowId);
      if (rowData == null) {
        if (!exists) continue;
        this.rows.delete(rowId);
        this.emit("delete", rowId);
      } else {
        this.rows.set(rowId, rowData);
        if (exists) {
          this.emit("update", rowId, rowData);
        } else {
          this.emit("insert", rowId, rowData);
        }
      }
    }
  }
}

/**
 * 把字符串/数字 row id 统一规范化为 number（如果可解析为安全整数）或保留字符串。
 * @param {string | number} value
 * @returns {number | string}
 */
function toRowId(value) {
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    const n = Number(value);
    if (Number.isFinite(n) && Number.isSafeInteger(n)) return n;
    return value;
  }
  return value;
}

/**
 * 订阅注册表，弱引用语义。
 *
 * 浏览器/Node 中 WeakRef 已经普及（Node 14+, 所有现代浏览器），可用来实现
 * 与 Unity 客户端等价的“订阅对象 GC 后自动失效”。
 */
export class SubscriptionRegistry {
  constructor() {
    /** @type {Map<string, WeakRef<BaseSubscription>>} */
    this._refs = new Map();
  }

  /**
   * @param {string} subId
   * @returns {BaseSubscription | null}
   */
  get(subId) {
    const ref = this._refs.get(subId);
    if (!ref) return null;
    const sub = ref.deref();
    if (!sub) {
      this._refs.delete(subId);
      return null;
    }
    return sub;
  }

  /**
   * @param {string} subId
   * @param {BaseSubscription} sub
   */
  set(subId, sub) {
    this._refs.set(subId, new WeakRef(sub));
  }

  /**
   * @param {string} subId
   */
  delete(subId) {
    this._refs.delete(subId);
  }

  has(subId) {
    return this.get(subId) !== null;
  }

  clear() {
    this._refs.clear();
  }

  /**
   * 当连接断开时，遍历所有有效订阅并调用 _markDisconnected。
   * @param {string} reason
   */
  markAllDisconnected(reason) {
    for (const ref of this._refs.values()) {
      const sub = ref.deref();
      if (sub) {
        try {
          sub._markDisconnected(reason);
        } catch {
          // ignore
        }
      }
    }
    this._refs.clear();
  }
}
