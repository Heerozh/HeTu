/**
 * 管道层基类。每层包装握手与编解码逻辑。
 *
 * 接口契约：
 * - `needsHandshake`：是否参与握手流程。
 * - `clientHello()`：返回客户端握手消息字节（Uint8Array）。
 * - `handshake(serverMsg)`：处理服务端握手响应。
 * - `encode(data)`：发送方向，将上层数据编码为下层数据。
 * - `decode(data)`：接收方向，将下层数据解码为上层数据。
 */
export class PipelineLayer {
  /** @returns {boolean} */
  get needsHandshake() {
    return true;
  }

  /**
   * 客户端先发送 hello 消息，再等服务端发送握手回复。
   * @returns {Uint8Array}
   */
  clientHello() {
    return new Uint8Array(0);
  }

  /**
   * 处理对端握手消息。
   * @param {Uint8Array} _serverMsg
   */
  handshake(_serverMsg) {
    // 默认无握手
  }

  /**
   * 释放资源。
   */
  dispose() {}

  /**
   * 正向处理（发送方向）。
   * @param {any} data
   * @returns {any}
   */
  encode(data) {
    return data;
  }

  /**
   * 逆向处理（接收方向）。
   * @param {any} data
   * @returns {any}
   */
  decode(data) {
    return data;
  }
}

/**
 * 多层消息流水线。
 *
 * 发送：layer[0].encode → layer[1].encode → ... → bytes
 * 接收：bytes → layer[N-1].decode → ... → layer[0].decode → object
 */
export class MessagePipeline {
  constructor() {
    /** @type {PipelineLayer[]} */
    this._layers = [];
  }

  /**
   * 添加一层。
   * @param {PipelineLayer} layer
   */
  addLayer(layer) {
    this._layers.push(layer);
  }

  /**
   * 替换全部层。
   * @param {PipelineLayer[]} layers
   */
  setLayers(layers) {
    this.dispose();
    this._layers = [];
    for (const l of layers) this.addLayer(l);
  }

  /** @returns {ReadonlyArray<PipelineLayer>} */
  get layers() {
    return this._layers;
  }

  get numLayers() {
    return this._layers.length;
  }

  /**
   * 释放所有层资源。
   */
  dispose() {
    for (const layer of this._layers) {
      try {
        layer.dispose();
      } catch {
        // ignore
      }
    }
  }

  /**
   * 客户端先发的握手消息：把每层的 clientHello() 收集到一个数组里，
   * 整体经过 jsonb（layer 0）编码后发送给服务端。
   * @returns {Uint8Array}
   */
  clientHello() {
    /** @type {Uint8Array[]} */
    const replies = [];
    for (const layer of this._layers) {
      if (!layer.needsHandshake) continue;
      const msg = layer.clientHello();
      replies.push(msg ?? new Uint8Array(0));
    }
    // 用 jsonb 把数组编码为 bytes（仅经过第 0 层）
    const out = this._layers[0].encode(replies);
    if (!(out instanceof Uint8Array)) {
      throw new Error("[HeTu] jsonb 层 encode 必须返回 Uint8Array");
    }
    return out;
  }

  /**
   * 处理服务端握手响应。先用 jsonb 解出层级数组，再依次给每层 handshake。
   * @param {Uint8Array} serverBytes
   */
  handshake(serverBytes) {
    const layerMsgs = this._layers[0].decode(serverBytes);
    if (!Array.isArray(layerMsgs)) {
      throw new Error("[HeTu] 握手响应格式错误，需要数组");
    }
    let j = 0;
    for (const layer of this._layers) {
      if (!layer.needsHandshake) continue;
      const raw = j < layerMsgs.length ? layerMsgs[j] : new Uint8Array(0);
      const bytes =
        raw instanceof Uint8Array ? raw : new Uint8Array(raw ?? []);
      layer.handshake(bytes);
      j++;
    }
  }

  /**
   * 正向处理。第一层接受 list/dict，最后一层产出 bytes。
   * @param {any} message
   * @returns {Uint8Array}
   */
  encode(message) {
    let cur = message;
    for (const layer of this._layers) {
      cur = layer.encode(cur);
    }
    if (!(cur instanceof Uint8Array)) {
      throw new Error("[HeTu] 最终编码结果必须是 Uint8Array");
    }
    return cur;
  }

  /**
   * 逆向处理。
   * @param {Uint8Array} bytes
   * @returns {any}
   */
  decode(bytes) {
    let cur = bytes;
    for (let i = this._layers.length - 1; i >= 0; i--) {
      cur = this._layers[i].decode(cur);
    }
    return cur;
  }
}
