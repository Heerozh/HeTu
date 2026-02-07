// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity消息压缩</summary>

using System;
using System.IO;
using Unity.SharpZipLib.Zip;
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
        }

        public override byte[] ClientHello() => Array.Empty<byte>();


        public override void Handshake(byte[] message)
        {
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
            _deflateBuffer?.Dispose();
            _deflateBuffer = deflateBuffer;
            _deflateStream?.Dispose();
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
            return _deflateBuffer.ToArray();
        }

        private byte[] Inflate(byte[] input)
        {
            using var inputStream = new MemoryStream(input);
            using var inflateStream =
                new InflaterInputStream(inputStream, _inflater, 4096)
                {
                    IsStreamOwner = false
                };
            using var output = new MemoryStream();

            // 准备一个缓冲区
            var buffer = new byte[4096];

            while (true)
            {
                int count;
                try
                {
                    // 尝试从解压流读取
                    count = inflateStream.Read(buffer, 0, buffer.Length);
                }
                catch (ZipException)
                {
                    // SharpZipLib 在读到 header 发现需要字典时，通常会抛出 ZipException
                    // 或者是 Read 返回 0 但 IsNeedingDictionary 为 true
                    if (!_inflater.IsNeedingDictionary) throw;
                    _inflater.SetDictionary(_dict);
                    // 此时解压器已经有了字典，就能继续解压后面的数据了
                    continue;
                }

                // 处理 Read 返回 0 的情况（有些版本可能不报错只返回 0）
                if (count == 0)
                {
                    if (_inflater.IsNeedingDictionary)
                    {
                        _inflater.SetDictionary(_dict);
                        continue;
                    }

                    // 真的读完了（EOF）
                    break;
                }

                // 将读到的解压数据写入输出流
                output.Write(buffer, 0, count);
            }

            return output.ToArray();
        }
    }
}
