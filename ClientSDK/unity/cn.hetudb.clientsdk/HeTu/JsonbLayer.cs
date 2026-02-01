// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity序列化层</summary>

using System;
using System.Collections.Generic;
using MessagePack;
using MessagePack.Resolvers;

namespace HeTu
{
    public class JsonObject
    {
        private readonly byte[] _rawData;

        public JsonObject(MessagePackReader reader, byte[] bytes)
        {
            // 记录当前 Data 开始的字节位置
            var startOffset = reader.Consumed;

            // 跳过当前的数据块（不进行反序列化）
            reader.Skip();

            // 记录结束位置
            var endOffset = reader.Consumed;

            // 计算长度并切片
            var dataLength = (int)(endOffset - startOffset);
            _rawData = new byte[dataLength];

            // 将这部分字节从原始数组中拷贝出来
            Array.Copy(bytes, startOffset, _rawData, 0, dataLength);
        }

        /// <summary>
        ///     转换为c#变量。
        /// - 如果值是Hetu的组件，建议使用“hetu build”命令生成的客户端强类型Struct，比如To<Item>()。
        ///   如果没有生成强类型，可使用通用的DictComponent类型。
        /// - 如果是列表类型，可以使用To<List<DictComponent>>()，或使用简写ToList<DictComponent>()。
        /// - 如果是Map类型，服务器端传过来的键必须为同一类型，然后可以使用
        ///   To<Dictionary<string, DictComponent>>()，或是用简写：ToDict<string, DictComponent>()。
        /// - 如果是混合类型，使用To<object>()可转换为动态类型，之后通过强制转换为具体类型。
        /// </summary>
        public T To<T>()
        {
            if (_rawData == null || _rawData.Length == 0) return default;
            return MessagePackSerializer.Deserialize<T>(_rawData);
        }

        /// <summary>
        ///     转换为Dict
        /// </summary>
        public Dictionary<T1, T2> ToDict<T1, T2>()
        {
            if (_rawData == null || _rawData.Length == 0) return null;
            return MessagePackSerializer.Deserialize<Dictionary<T1, T2>>(_rawData);
        }

        /// <summary>
        ///     转换为列表
        /// </summary>
        public List<T> ToList<T>()
        {
            if (_rawData == null || _rawData.Length == 0) return null;
            return MessagePackSerializer.Deserialize<List<T>>(_rawData);
        }

    }

    public class JsonbLayer : MessageProcessLayer
    {
        public override byte[] Handshake(byte[] message) => Array.Empty<byte>();

        public override object Encode(object message)
        {
            if (message is not byte[] bytes)
                throw new InvalidOperationException("CryptoLayer只能加密 byte[] 类型数据");
            // todo，应该写到decode里
            var reader = new MessagePackReader(new ReadOnlyMemory<byte>(bytes));
            // 1. 读取数组长度 (对应 Python 的 list 长度)
            var count = reader.ReadArrayHeader();
            if (count < 2) throw new Exception("数据包格式错误，长度不足2");
            // 2. 读取 Cmd (int)
            var cmd = reader.ReadString();
            // 现在服务器反馈的消息只有rsp，sub, updt
            // ["rsp", json_data]
            // ["sub", sub_id, struct_data | list[struct_data]]
            // ["updt", sub_id, dict[id, struct_data]]
            switch (cmd)
            {
                case "rsp": // list[Any] | dict[Any, Any]
                    {
                        var jsonData = new JsonObject(reader, bytes);
                        return new object[] { cmd, jsonData };
                    }
                case "sub": // dict[str, Any] | list[dict[str, Any]]
                case "updt":// dict[str, dict[str, Any]]
                    {
                        var subId = reader.ReadString();
                        var jsonData = new JsonObject(reader, bytes);
                        return new object[] { cmd, subId, jsonData };
                    }
                default:
                    throw new Exception($"未知的命令类型: {cmd}");
            }
        }

        public override object Decode(object message) =>
            // todo
            null;
    }
}
