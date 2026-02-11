// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity消息压缩</summary>

using System;
using System.Buffers;
using System.IO;
using Unity.SharpZipLib.Zip.Compression;
using Unity.SharpZipLib.Zip.Compression.Streams;

namespace HeTu
{
    /// <summary>
    ///     使用 zlib 进行消息的流式压缩和解压缩。
    /// </summary>
    public sealed class ZlibLayer : MessageProcessLayer
    {
        private readonly int _level;
        private readonly byte[] _presetDict;
        private MemoryStream _deflateBuffer;

        private Deflater _deflater;
        private DeflaterOutputStream _deflateStream;
        private byte[] _dict;
        private Inflater _inflater;

        public ZlibLayer(int level = 1, byte[] presetDictionary = null)
        {
            _level = level;
            _presetDict = presetDictionary ?? Array.Empty<byte>();
        }

        public override void Dispose()
        {
            _deflateStream?.Dispose();
            _deflateBuffer?.Dispose();
            _deflateStream = null;
            _deflateBuffer = null;
            _deflater = null;
            _inflater = null;
        }

        public override byte[] ClientHello()
        {
            Dispose();
            return Array.Empty<byte>();
        }


        public override void Handshake(byte[] message)
        {
            Dispose();
            _dict = message is { Length: > 0 } ? message : _presetDict;

            var deflater = new Deflater(_level, false);
            var inflater = new Inflater(false);

            if (_dict.Length > 0)
            {
                deflater.SetDictionary(_dict);
            }

            var deflateBuffer = new MemoryStream();
            var deflateStream = new DeflaterOutputStream(deflateBuffer, deflater, 4096)
            {
                IsStreamOwner = false
            };

            _deflater = deflater;
            _inflater = inflater;
            _deflateBuffer = deflateBuffer;
            _deflateStream = deflateStream;
        }

        public override object Encode(object message)
        {
            if (_deflater == null) return message;
            if (message is not byte[] bytes)
                throw new InvalidOperationException("ZlibLayer 只能压缩 byte[] 类型的消息");

            var chunk = Deflate(bytes);

            return chunk;
        }

        public override object Decode(object message)
        {
            if (_inflater == null) return message;
            if (message is not byte[] bytes)
                throw new InvalidOperationException("ZlibLayer 只能解压 byte[] 类型的消息");

            return Inflate(bytes);
        }

        private byte[] Deflate(byte[] input)
        {
            _deflateBuffer.SetLength(0);
            _deflateBuffer.Position = 0;
            _deflateStream.Write(input, 0, input.Length);
            _deflateStream.Flush();
            // todo 0gc
            return _deflateBuffer.ToArray();
        }

        private byte[] Inflate(byte[] input)
        {
            _inflater.SetInput(input);

            // 预估一个大小，避免频繁扩容
            using var outputStream = new MemoryStream(input.Length * 2);
            var buffer = ArrayPool<byte>.Shared.Rent(4096); // 临时缓存

            while (!_inflater.IsFinished)
            {
                // 执行解压
                var count = _inflater.Inflate(buffer);

                // 解压出数据了，写入输出流
                if (count > 0)
                {
                    outputStream.Write(buffer, 0, count);
                }
                else
                {
                    // 没解压出数据，检查是否需要字典
                    if (_inflater.IsNeedingDictionary)
                    {
                        if (_dict == null)
                            throw new Exception("Need dictionary but none set!");
                        _inflater.SetDictionary(_dict);
                        // 设置完字典，无需其他操作，直接进入下一次循环再次 Inflate 即可
                        continue;
                    }

                    // 既没数据，也不需要字典，那可能是数据读完了或者出错了
                    if (_inflater.IsNeedingInput)
                    {
                        // 输入数据(input)已经被完全消耗完了，
                        // 且 Z_SYNC_FLUSH 保证了当前数据段已完整输出。
                        // 我们可以安全退出，等待服务器发下一个包。
                        break;
                    }

                    // 理论上不应该走到这里，除非数据损坏
                    throw new Exception("Unknown inflater state");
                }
            }

            ArrayPool<byte>.Shared.Return(buffer);
            // todo 0gc
            return outputStream.ToArray();
        }
    }
}
