// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity消息管道</summary>
using System;
using Unity.SharpZipLib.Zip.Compression;
using Unity.SharpZipLib.Zip.Compression.Streams;
using System.IO;

namespace HeTu
{
    /// <summary>
    /// 使用 zlib 进行消息的流式压缩和解压缩。
    /// </summary>
    public class ZlibLayer : MessageProcessLayer
    {
        class ZlibContext
        {
            public Deflater Deflater;
            public Inflater Inflater;
            public MemoryStream DeflateBuffer;
            public DeflaterOutputStream DeflateStream;
            public byte[] Dict;
        }

        readonly int _level;
        readonly byte[] _presetDict;

        public ZlibLayer(int level = 1, byte[] presetDictionary = null)
        {
            _level = level;
            _presetDict = presetDictionary ?? Array.Empty<byte>();
        }


        public override LayerHandshakeResult Handshake(byte[] message)
        {
            var dict = (message != null && message.Length > 0) ? message : _presetDict;

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

            var ctx = new ZlibContext
            {
                Deflater = deflater,
                Inflater = inflater,
                DeflateBuffer = deflateBuffer,
                DeflateStream = deflateStream,
                Dict = dict
            };

            return new LayerHandshakeResult(ctx, Array.Empty<byte>());
        }

        public override object Encode(object layerCtx, object message)
        {
            if (layerCtx == null) return message;
            if (message is not byte[] bytes)
                throw new InvalidOperationException("ZlibLayer 只能压缩 byte[] 类型的消息");

            var ctx = (ZlibContext)layerCtx;
            var chunk = Deflate(ctx, bytes);

            return chunk;
        }

        public override object Decode(object layerCtx, object message)
        {
            if (layerCtx == null) return message;
            if (message is not byte[] bytes)
                throw new InvalidOperationException("ZlibLayer 只能解压 byte[] 类型的消息");

            var ctx = (ZlibContext)layerCtx;
            try
            {
                return Inflate(ctx, bytes);
            }
            catch (Exception)
            {
                throw;
            }
        }

        static byte[] Deflate(ZlibContext ctx, byte[] input)
        {
            ctx.DeflateBuffer.SetLength(0);
            ctx.DeflateBuffer.Position = 0;
            ctx.DeflateStream.Write(input, 0, input.Length);
            ctx.DeflateStream.Flush();
            return ctx.DeflateBuffer.ToArray();
        }

        static byte[] Inflate(ZlibContext ctx, byte[] input)
        {
            using var inputStream = new MemoryStream(input);
            using var inflateStream = new InflaterInputStream(inputStream, ctx.Inflater, 4096)
            {
                IsStreamOwner = false
            };
            using var output = new MemoryStream();
            inflateStream.CopyTo(output);
            return output.ToArray();
        }
    }
}
