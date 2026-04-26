import { Packr, Unpackr } from "msgpackr";
import { PipelineLayer } from "./pipeline.js";

/**
 * MessagePack 编解码层。与服务端 msgspec.msgpack 完全兼容。
 *
 * - encode：list/dict → MessagePack bytes
 * - decode：MessagePack bytes → list/dict
 *
 * 对于服务器标准消息（rsp/sub/updt），上层 client.js 拿到的就是普通数组。
 */
export class JsonbLayer extends PipelineLayer {
  constructor() {
    super();
    // structuredClone:false 避免 msgpackr 试图保留无关元信息
    // useRecords:false 让 dict 严格按 map 编码，与服务端一致
    // mapsAsObjects:true 让 map 解码为普通 object，符合 JS 习惯
    this._packr = new Packr({
      useRecords: false,
      mapsAsObjects: true,
      bundleStrings: false,
      // 保留 BigInt：服务端 long 大整数可正确表示
      useBigIntExtension: false,
      int64AsType: "number",
    });
    this._unpackr = new Unpackr({
      useRecords: false,
      mapsAsObjects: true,
      bundleStrings: false,
      useBigIntExtension: false,
      int64AsType: "number",
    });
  }

  /** @returns {boolean} */
  get needsHandshake() {
    return false;
  }

  /**
   * @param {any} message
   * @returns {Uint8Array}
   */
  encode(message) {
    return this._packr.pack(message);
  }

  /**
   * @param {Uint8Array} bytes
   * @returns {any}
   */
  decode(bytes) {
    if (!(bytes instanceof Uint8Array)) {
      throw new TypeError("JsonbLayer.decode 输入必须是 Uint8Array");
    }
    return this._unpackr.unpack(bytes);
  }
}
