/**
 * 轻量 EventEmitter，浏览器与 Node.js 通用，零依赖。
 */
export class EventEmitter {
  constructor() {
    /** @type {Map<string, Set<Function>>} */
    this._listeners = new Map();
  }

  /**
   * 注册事件回调。
   * @param {string} event
   * @param {Function} listener
   * @returns {this}
   */
  on(event, listener) {
    if (typeof listener !== "function") {
      throw new TypeError("listener must be a function");
    }
    let bucket = this._listeners.get(event);
    if (!bucket) {
      bucket = new Set();
      this._listeners.set(event, bucket);
    }
    bucket.add(listener);
    return this;
  }

  /**
   * 注册只触发一次的事件回调。
   * @param {string} event
   * @param {Function} listener
   * @returns {this}
   */
  once(event, listener) {
    const wrapped = (...args) => {
      this.off(event, wrapped);
      listener(...args);
    };
    return this.on(event, wrapped);
  }

  /**
   * 移除事件回调。listener 为空时移除该事件下的所有回调。
   * @param {string} event
   * @param {Function} [listener]
   * @returns {this}
   */
  off(event, listener) {
    const bucket = this._listeners.get(event);
    if (!bucket) return this;
    if (listener === undefined) {
      this._listeners.delete(event);
      return this;
    }
    bucket.delete(listener);
    if (bucket.size === 0) this._listeners.delete(event);
    return this;
  }

  /**
   * 同步触发事件。回调中的异常不会中断剩余监听器。
   * @param {string} event
   * @param  {...any} args
   * @returns {boolean} 是否有监听器被触发
   */
  emit(event, ...args) {
    const bucket = this._listeners.get(event);
    if (!bucket || bucket.size === 0) return false;
    for (const listener of [...bucket]) {
      try {
        listener(...args);
      } catch (err) {
        if (event === "error") {
          // 避免错误事件中再抛出错误造成无限递归
          if (typeof console !== "undefined" && console.error) {
            console.error("[HeTu] error listener threw:", err);
          }
        } else {
          this.emit("error", err);
        }
      }
    }
    return true;
  }

  /**
   * 移除全部监听器。
   * @param {string} [event]
   */
  removeAllListeners(event) {
    if (event === undefined) {
      this._listeners.clear();
    } else {
      this._listeners.delete(event);
    }
    return this;
  }

  /**
   * 当前事件的监听器数量。
   * @param {string} event
   */
  listenerCount(event) {
    const bucket = this._listeners.get(event);
    return bucket ? bucket.size : 0;
  }
}
