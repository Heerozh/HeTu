// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity消息压缩</summary>

using System;
using System.IO;
using Unity.SharpZipLib.Zip.Compression;
using Unity.SharpZipLib.Zip.Compression.Streams;

namespace HeTu
{
    /// <summary>
    ///     使用 zlib 进行消息的流式压缩和解压缩。
    /// </summary>
    public class ZlibLayer : MessageProcessLayer
    {
        private readonly int _level;
        private readonly byte[] _presetDict;
        private MemoryStream _deflateBuffer;

        private Deflater _deflater;
        private DeflaterOutputStream _deflateStream;
        private Inflater _inflater;

        public ZlibLayer(int level = 1, byte[] presetDictionary = null)
        {
            _level = level;
            _presetDict = presetDictionary ?? Array.Empty<byte>();
        }


        public override byte[] Handshake(byte[] message)
        {
            var dict = message is { Length: > 0 } ? message : _presetDict;

            var deflater = new Deflater(_level, false);
            var inflater = new Inflater(false);

            if (dict.Length > 0)
            {
                deflater.SetDictionary(dict);
                inflater.SetDictionary(dict);
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
            return Array.Empty<byte>();
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
            inflateStream.CopyTo(output);
            return output.ToArray();
        }
    }
}
