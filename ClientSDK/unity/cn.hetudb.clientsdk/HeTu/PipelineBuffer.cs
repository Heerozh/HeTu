// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的零GC内部缓冲传递载体</summary>

using System;
using System.Buffers;

namespace HeTu
{
    /// <summary>
    ///     提供一种零 GC 分配的缓冲区容器，实现消息管道在层与层之间的低GC中转。
    ///     使用完毕必须调用 <see cref="Dispose" /> 将内部缓冲返还进 <see cref="ArrayPool{T}" />。
    /// </summary>
    public sealed class PipelineBuffer : IDisposable
    {
        private byte[] _rentedArray;

        private PipelineBuffer(byte[] rentedArray, int offset, int length)
        {
            _rentedArray = rentedArray;
            Memory = new ReadOnlyMemory<byte>(rentedArray, offset, length);
            Segment = new ArraySegment<byte>(rentedArray, offset, length);
        }

        /// <summary>
        ///     有效数据的只读切片。
        /// </summary>
        public ReadOnlyMemory<byte> Memory { get; private set; }

        /// <summary>
        ///     有效数据切片。
        /// </summary>
        public ArraySegment<byte> Segment { get; private set; }

        public void Dispose()
        {
            if (_rentedArray == null) return;
            ArrayPool<byte>.Shared.Return(_rentedArray);
            _rentedArray = null;
            Memory = default;
            Segment = default;
        }

        /// <summary>
        ///     从 ArrayPool 中租用并创建一个基于池数组的 PipelineBuffer 容器。
        ///     包含被申请缓冲区的有效载荷范围(offset与length)。
        /// </summary>
        public static PipelineBuffer Rent(int minLength)
        {
            var array = ArrayPool<byte>.Shared.Rent(minLength);
            return new PipelineBuffer(array, 0, minLength);
        }

        /// <summary>
        ///     根据已有的租用数组借出创建 PipelineBuffer 容器。
        ///     通常在由加密层或解压层写入完成后提取。
        /// </summary>
        public static PipelineBuffer CreateFromRented(byte[] rentedArray, int offset,
            int length) => new(rentedArray, offset, length);
    }
}
