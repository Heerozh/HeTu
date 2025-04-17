import pako from 'pako';

export interface IProtocol {
    compress(data: Uint8Array): Uint8Array;
    decompress(data: Uint8Array): Uint8Array;
    crypt?(data: Uint8Array): Uint8Array;
    decrypt?(data: Uint8Array): Uint8Array;
}

/**
 * 默认的消息压缩协议
 */
export class ZlibProtocol implements IProtocol {
    /**
     * 压缩数据
     * @param data 要压缩的字节数组
     * @returns 压缩后的字节数组
     */
    public compress(data: Uint8Array): Uint8Array {
        try {
            return pako.deflate(data);
        } catch (error) {
            console.error('压缩数据失败:', error);
            return data; // 出错时返回原始数据
        }
    }

    /**
     * 解压缩数据
     * @param data 要解压缩的字节数组
     * @returns 解压缩后的字节数组
     */
    public decompress(data: Uint8Array): Uint8Array {
        try {
            return pako.inflate(data);
        } catch (error) {
            console.error('解压缩数据失败:', error);
            return data; // 出错时返回原始数据
        }
    }
}