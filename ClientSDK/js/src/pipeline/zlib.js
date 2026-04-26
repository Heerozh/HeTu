import pako from "pako";
import { PipelineLayer } from "./pipeline.js";

/**
 * Zlib 流式压缩层。与服务端 `zlib.compressobj/decompressobj` + Z_SYNC_FLUSH 对齐。
 *
 * 握手时服务端会发回预置字典（preset dictionary），客户端用同一字典初始化双向流。
 *
 * 注：pako.Inflate 默认只在输出缓冲被填满或流结束时才触发 onData，
 * 而 Z_SYNC_FLUSH 模式下二者皆不会发生，需要在每次 push 后手动从内部
 * stream 把已解压字节取出。这里通过自定义子类 `SyncFlushInflate` 实现。
 */
export class ZlibLayer extends PipelineLayer {
  /**
   * @param {object} [opts]
   * @param {number} [opts.level=1] zlib 压缩级别。
   */
  constructor(opts = {}) {
    super();
    this._level = opts.level ?? 1;
    /** @type {Uint8Array} */
    this._dict = new Uint8Array(0);
    /** @type {pako.Deflate | null} */
    this._deflater = null;
    /** @type {SyncFlushInflate | null} */
    this._inflater = null;
    /** @type {Uint8Array[]} */
    this._encChunks = [];
    /** @type {Uint8Array[]} */
    this._decChunks = [];
  }

  dispose() {
    this._deflater = null;
    this._inflater = null;
    this._encChunks = [];
    this._decChunks = [];
  }

  /**
   * @param {Uint8Array} message - 服务端发送的预置字典字节
   */
  handshake(message) {
    const dict = message && message.length > 0 ? message : new Uint8Array(0);
    this._dict = dict;

    /** @type {pako.DeflateOptions} */
    const deflateOpts = { level: this._level };
    if (dict.length > 0) deflateOpts.dictionary = dict;
    this._deflater = new pako.Deflate(deflateOpts);
    this._deflater.onData = (chunk) => {
      this._encChunks.push(chunk);
    };

    /** @type {pako.InflateOptions} */
    const inflateOpts = {};
    if (dict.length > 0) inflateOpts.dictionary = dict;
    this._inflater = new SyncFlushInflate(inflateOpts);
    this._inflater.onData = (chunk) => {
      this._decChunks.push(chunk);
    };
  }

  /**
   * @param {Uint8Array} message
   * @returns {Uint8Array}
   */
  encode(message) {
    if (!this._deflater) return message;
    if (!(message instanceof Uint8Array)) {
      throw new TypeError("ZlibLayer.encode 输入必须是 Uint8Array");
    }
    this._encChunks = [];
    const ok = this._deflater.push(message, pako.constants.Z_SYNC_FLUSH);
    if (!ok) {
      throw new Error(
        `[HeTu] zlib deflate 失败: ${this._deflater.msg || "unknown"}`
      );
    }
    return concatChunks(this._encChunks);
  }

  /**
   * @param {Uint8Array} message
   * @returns {Uint8Array}
   */
  decode(message) {
    if (!this._inflater) return message;
    if (!(message instanceof Uint8Array)) {
      throw new TypeError("ZlibLayer.decode 输入必须是 Uint8Array");
    }
    this._decChunks = [];
    const ok = this._inflater.push(message, pako.constants.Z_SYNC_FLUSH);
    if (!ok) {
      throw new Error(
        `[HeTu] zlib inflate 失败: ${this._inflater.msg || "unknown"}`
      );
    }
    return concatChunks(this._decChunks);
  }
}

/**
 * pako.Inflate 的子类，每次 push 之后都强制把内部输出缓冲冲刷到 onData，
 * 实现 Z_SYNC_FLUSH 流式语义。
 */
class SyncFlushInflate extends pako.Inflate {
  push(data, mode) {
    const result = super.push(data, mode);
    const strm = this.strm;
    if (strm && strm.next_out > 0) {
      const chunk = strm.output.slice(0, strm.next_out);
      this.onData(chunk);
      strm.next_out = 0;
      strm.avail_out = strm.output.length;
    }
    return result;
  }
}

/**
 * 拼接 Uint8Array 数组为单个 Uint8Array。
 * @param {Uint8Array[]} chunks
 * @returns {Uint8Array}
 */
function concatChunks(chunks) {
  if (chunks.length === 0) return new Uint8Array(0);
  if (chunks.length === 1) return chunks[0];
  let total = 0;
  for (const c of chunks) total += c.length;
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.length;
  }
  return out;
}
