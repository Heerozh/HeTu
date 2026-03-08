// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity消息压缩</summary>

using System;
using System.Buffers;
using System.IO;
using Unity.SharpZipLib.Zip.Compression;
using Unity.SharpZipLib.Zip.Compression.Streams;
using UnityEngine;

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

        /// <summary>
        ///     创建 zlib 压缩层。
        /// </summary>
        /// <param name="level">压缩级别（1-9）。</param>
        /// <param name="presetDictionary">预置字典，可选。</param>
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

        /// <summary>
        ///     初始化压缩/解压状态并返回握手消息。
        /// </summary>
        public override byte[] ClientHello()
        {
            Dispose();
            return Array.Empty<byte>();
        }


        /// <summary>
        ///     根据服务端握手结果初始化压缩字典与状态机。
        /// </summary>
        /// <param name="message">服务端返回的压缩字典。</param>
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

            ReadOnlySpan<byte> inputSpan = message switch
            {
                byte[] bytes => bytes,
                PipelineBuffer buf => buf.Segment.AsSpan(),
                _ => throw new InvalidOperationException(
                    "ZlibLayer 只能压缩 byte[] 或 PipelineBuffer 类型的消息")
            };

            var chunk = Deflate(inputSpan);

            return chunk;
        }

        public override object Decode(object message)
        {
            if (_inflater == null) return message;

            ReadOnlySpan<byte> inputSpan = message switch
            {
                byte[] bytes => bytes,
                PipelineBuffer buf => buf.Segment.AsSpan(),
                _ => throw new InvalidOperationException(
                    "ZlibLayer 只能解压 byte[] 或 PipelineBuffer 类型的消息")
            };

            return
                Inflate(inputSpan
                    .ToArray()); // TODO: Inflater requires byte[], we may need to allocate or pass rented array if SharpZipLib supports it.
        }

        private PipelineBuffer Deflate(ReadOnlySpan<byte> inputSpan)
        {
            _deflateBuffer.SetLength(0);
            _deflateBuffer.Position = 0;

            var tempArray = ArrayPool<byte>.Shared.Rent(inputSpan.Length);
            inputSpan.CopyTo(tempArray);

            _deflateStream.Write(tempArray, 0, inputSpan.Length);
            _deflateStream.Flush();

            ArrayPool<byte>.Shared.Return(tempArray);

            // 必须从 ArrayPool 借出新数组，严禁直接使用 MemoryStream 后端数组包装进 PipelineBuffer
            // 否则在后续层 Dispose() 时，会把 MemoryStream 私有数组错误归还进全局线程池造成内存毁损。
            var count = (int)_deflateBuffer.Length;
            var rentedOutput = ArrayPool<byte>.Shared.Rent(count);

            if (_deflateBuffer.TryGetBuffer(out var bufferSegment))
            {
                Debug.Assert(bufferSegment.Array != null);
                Array.Copy(bufferSegment.Array, bufferSegment.Offset, rentedOutput, 0,
                    count);
            }
            else
            {
                var prevPos = _deflateBuffer.Position;
                _deflateBuffer.Position = 0;
                var read = _deflateBuffer.Read(rentedOutput, 0, count);
                _deflateBuffer.Position = prevPos;
                Debug.Assert(read == count);
            }

            return PipelineBuffer.CreateFromRented(rentedOutput, 0, count);
        }

        private PipelineBuffer
            Inflate(byte[] input) // SharpZipLib Inflater requires byte[]
        {
            _inflater.SetInput(input);

            // 预估一个大小，避免频繁扩容
            var rentedOutput = ArrayPool<byte>.Shared.Rent(input.Length * 2);
            var outputOffset = 0;
            var buffer = ArrayPool<byte>.Shared.Rent(4096); // 临时缓存

            while (!_inflater.IsFinished)
            {
                // 执行解压
                var count = _inflater.Inflate(buffer);

                // 解压出数据了，写入输出流
                if (count > 0)
                {
                    // Ensure space
                    if (outputOffset + count > rentedOutput.Length)
                    {
                        var newRented =
                            ArrayPool<byte>.Shared.Rent(rentedOutput.Length * 2 + count);
                        Array.Copy(rentedOutput, 0, newRented, 0, outputOffset);
                        ArrayPool<byte>.Shared.Return(rentedOutput);
                        rentedOutput = newRented;
                    }

                    Array.Copy(buffer, 0, rentedOutput, outputOffset, count);
                    outputOffset += count;
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

            // 0分配，直接使用 rentedOutput 组装
            return PipelineBuffer.CreateFromRented(rentedOutput, 0, outputOffset);
        }
    }
}
