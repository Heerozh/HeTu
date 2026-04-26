/**
 * @typedef {(response: any[] | null, cancel?: boolean) => void} ResponseCallback
 */

/**
 * 管理请求-响应回调队列。
 *
 * 每次发送需要等待服务端返回的请求时，都会按发送顺序入队一个回调；
 * 收到响应后按 FIFO 顺序触发对应回调。
 */
export class ResponseManager {
  constructor() {
    /** @type {ResponseCallback[]} */
    this._queue = [];
  }

  /**
   * 入队一个等待响应的回调。
   * @param {ResponseCallback} callback
   */
  enqueue(callback) {
    this._queue.push(callback);
  }

  /**
   * 完成队列中的下一个请求回调。
   * @param {any[]} response
   */
  completeNext(response) {
    const cb = this._queue.shift();
    if (cb) cb(response, false);
  }

  /**
   * 取消所有等待中的请求。
   * @param {string} [_reason] 用于日志提示。
   */
  cancelAll(_reason) {
    if (this._queue.length === 0) return;
    const queue = this._queue;
    this._queue = [];
    for (const cb of queue) {
      try {
        cb(null, true);
      } catch {
        // 取消阶段忽略回调内的异常
      }
    }
  }

  get pending() {
    return this._queue.length;
  }
}
