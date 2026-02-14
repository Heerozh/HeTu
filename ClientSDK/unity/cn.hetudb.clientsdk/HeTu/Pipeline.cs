// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity消息管道</summary>

using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;

namespace HeTu
{
    /// <summary>
    ///     管道编解码尺寸指标。
    /// </summary>
    public sealed class PipelineMetrics
    {
        /// <summary>
        ///     源数据尺寸（通常为业务层原始字节）。
        /// </summary>
        public int SourceSizeBytes { get; set; } = -1;

        /// <summary>
        ///     传输尺寸（实际网络收发字节）。
        /// </summary>
        public int TransportSizeBytes { get; set; } = -1;
    }

    [MustDisposeResource]
    public abstract class MessageProcessLayer : IDisposable
    {
        /// <summary>
        ///     当前层在管道中的索引。
        /// </summary>
        protected int LayerIndex;

        /// <summary>
        ///     所属的消息管道。
        /// </summary>
        protected MessagePipeline Parent;

        /// <summary>
        ///     释放层内资源。
        /// </summary>
        public abstract void Dispose();

        /// <summary>
        ///     附加到消息管道时回调。
        /// </summary>
        /// <param name="parent">所属管道。</param>
        /// <param name="layerIdx">当前层索引。</param>
        public virtual void OnAttach(MessagePipeline parent, int layerIdx)
        {
            Parent = parent;
            LayerIndex = layerIdx;
        }

        /// <summary>
        ///     是否需要握手流程。
        /// </summary>
        public virtual bool IsHandshakeRequired() => true;

        /// <summary>
        ///     客户端先发送hello消息，然后服务器才发送握手消息
        /// </summary>
        public abstract byte[] ClientHello();

        /// <summary>
        ///     连接前握手工作，例如协商参数等。
        ///     返回的Context会保存在连接中，贯穿之后的Encode/Decode调用。
        ///     Reply将发送给对端。
        /// </summary>
        public abstract void Handshake(byte[] message);

        /// <summary>对消息进行正向处理</summary>
        public abstract object Encode(object message);

        /// <summary>对消息进行逆向处理</summary>
        public abstract object Decode(object message);
    }

    /// <summary>
    ///     消息流层叠处理类。
    /// </summary>
    [MustDisposeResource]
    public sealed class MessagePipeline : IDisposable
    {
        private readonly List<bool> _disabled = new();
        private readonly List<MessageProcessLayer> _layers = new();

        /// <summary>
        ///     当前已注册的处理层数量。
        /// </summary>
        public int NumLayers => _layers.Count;

        public void Dispose()
        {
            foreach (var layer in _layers)
                layer.Dispose();
        }

        /// <summary>
        ///     添加一层消息处理层。
        /// </summary>
        /// <param name="layer">处理层实例。</param>
        public void AddLayer(MessageProcessLayer layer)
        {
            _layers.Add(layer);
            _disabled.Add(false);
            layer.OnAttach(this, _layers.Count - 1);
        }

        /// <summary>
        ///     禁用指定索引的处理层。
        /// </summary>
        /// <param name="idx">层索引。</param>
        public void DisableLayer(int idx) => _disabled[idx] = true;

        /// <summary>
        ///     清空管道并释放所有层资源。
        /// </summary>
        public void Clean()
        {
            Dispose();

            _layers.Clear();
            _disabled.Clear();
        }

        /// <summary>
        ///     客户端先发送hello消息，然后服务器才发送握手消息
        /// </summary>
        public byte[] ClientHello()
        {
            var replyMessages = new List<byte[]>(_layers.Count);

            for (var i = 0; i < _layers.Count; i++)
            {
                if (_disabled[i] || !_layers[i].IsHandshakeRequired())
                    continue;

                var result = _layers[i].ClientHello();
                replyMessages.Add(result ?? Array.Empty<byte>());
            }

            var reply = Encode(replyMessages);
            return reply ?? Array.Empty<byte>();
        }

        /// <summary>
        ///     通过对端发来的握手消息，完成所有层的握手工作。
        ///     返回握手后的上下文；以及要发送给对端的握手消息。
        /// </summary>
        public void Handshake(byte[][] peerMessages)
        {
            var j = 0;
            for (var i = 0; i < _layers.Count; i++)
            {
                if (_disabled[i] || !_layers[i].IsHandshakeRequired())
                    continue;

                var msg = peerMessages != null && j < peerMessages.Length
                    ? peerMessages[j]
                    : Array.Empty<byte>();

                _layers[i].Handshake(msg);
                j++;
            }

            Logger.Instance.Info(
                $"客户端握手完成，握手消息总长度：{peerMessages?.Sum(m => m.Length) ?? 0}字节");
        }

        /// <summary>
        ///     对消息进行正向处理，可以传入until参数表示只处理到哪层
        /// </summary>
        public byte[] Encode(object message, int until = -1)
        {
            var encoded = Encode(message, out _, until);
            return encoded;
        }

        /// <summary>
        ///     对消息进行正向处理并采集尺寸指标。
        /// </summary>
        public byte[] Encode(object message, out PipelineMetrics metrics,
            int until = -1)
        {
            metrics = new PipelineMetrics();
            var encoded = message;
            for (var i = 0; i < _layers.Count; i++)
            {
                if (_disabled[i]) continue;
                if (0 < until && until < i) break;

                encoded = _layers[i].Encode(encoded);
                if (metrics.SourceSizeBytes < 0 && encoded is byte[] sourceBytes)
                    metrics.SourceSizeBytes = sourceBytes.Length;
            }

            var transport = encoded as byte[];
            if (transport != null)
                metrics.TransportSizeBytes = transport.Length;
            return transport;
        }

        /// <summary>
        ///     对消息进行逆向处理
        /// </summary>
        public object Decode(byte[] message)
        {
            return Decode(message, out _);
        }

        /// <summary>
        ///     对消息进行逆向处理并采集尺寸指标。
        /// </summary>
        public object Decode(byte[] message, out PipelineMetrics metrics)
        {
            metrics = new PipelineMetrics
            {
                TransportSizeBytes = message?.Length ?? -1
            };

            object decoded = message;
            for (var i = 0; i < _layers.Count; i++)
            {
                var originalIndex = _layers.Count - 1 - i;
                if (_disabled[originalIndex]) continue;

                decoded = _layers[originalIndex].Decode(decoded);

                if (decoded is byte[] sourceBytes)
                    metrics.SourceSizeBytes = sourceBytes.Length;
            }

            return decoded;
        }
    }
}
