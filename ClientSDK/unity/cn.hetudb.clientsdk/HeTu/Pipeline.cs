// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity消息管道</summary>

using System;
using System.Collections.Generic;


namespace HeTu
{

    public abstract class MessageProcessLayer
    {
        protected MessagePipeline Parent;
        protected int LayerIndex;

        public virtual void OnAttach(MessagePipeline parent, int layerIdx)
        {
            Parent = parent;
            LayerIndex = layerIdx;
        }

        /// <summary>
        /// 连接前握手工作，例如协商参数等。
        /// 返回的Context会保存在连接中，贯穿之后的Encode/Decode调用。
        /// Reply将发送给对端。
        /// </summary>
        public abstract byte[] Handshake(byte[] message);

        /// <summary>对消息进行正向处理</summary>
        public abstract object Encode(object message);

        /// <summary>对消息进行逆向处理</summary>
        public abstract object Decode(object message);
    }

    /// <summary>
    /// 消息流层叠处理类。
    /// </summary>
    public class MessagePipeline
    {
        readonly List<MessageProcessLayer> _layers = new();
        readonly List<bool> _disabled = new();

        public void AddLayer(MessageProcessLayer layer)
        {
            _layers.Add(layer);
            _disabled.Add(false);
            layer.OnAttach(this, _layers.Count - 1);
        }

        public void DisableLayer(int idx)
        {
            _disabled[idx] = true;
        }

        public void Clean()
        {
            _layers.Clear();
            _disabled.Clear();
        }

        public int NumLayers => _layers.Count;

        /// <summary>
        /// 通过对端发来的握手消息，完成所有层的握手工作。
        /// 返回握手后的上下文；以及要发送给对端的握手消息。
        /// </summary>
        public byte[] Handshake(IList<byte[]> peerMessages)
        {
            var replyMessages = new List<byte[]>(_layers.Count);

            for (var i = 0; i < _layers.Count; i++)
            {
                if (_disabled[i])
                {
                    replyMessages.Add(Array.Empty<byte>());
                    continue;
                }

                var msg = (peerMessages != null && i < peerMessages.Count)
                    ? peerMessages[i]
                    : Array.Empty<byte>();

                var result = _layers[i].Handshake(msg);
                replyMessages.Add(result ?? Array.Empty<byte>());
            }

            var reply = Encode(replyMessages);
            return reply as byte[] ?? Array.Empty<byte>();
        }

        /// <summary>
        /// 对消息进行正向处理，可以传入until参数表示只处理到哪层
        /// </summary>
        public object Encode(object message, int until = -1)
        {
            var encoded = message;
            for (var i = 0; i < _layers.Count; i++)
            {
                if (_disabled[i]) continue;
                if (0 < until && until < i) break;

                encoded = _layers[i].Encode(encoded);
            }

            return encoded;
        }

        /// <summary>
        /// 对消息进行逆向处理
        /// </summary>
        public object Decode(object message)
        {
            var decoded = message;
            for (var i = 0; i < _layers.Count; i++)
            {
                var originalIndex = _layers.Count - 1 - i;
                if (_disabled[originalIndex]) continue;

                decoded = _layers[originalIndex].Decode(decoded);
            }

            return decoded;
        }
    }


}
