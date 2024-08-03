// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的C#库</summary>

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace HeTu
{
    public class BaseSubscription
    {
        public readonly string ComponentName;
        public readonly int SubscriptID;

        protected BaseSubscription(int subscriptID, string componentName)
        {
            SubscriptID = subscriptID;
            ComponentName = componentName;
        }

        public event Action<int> OnUpdate;
        public event Action<int> OnDelete;

        ~BaseSubscription()
        {
            // 取消订阅
        }
    }

    public class RowSubscription : BaseSubscription
    {
        public readonly int RowID;

        public RowSubscription(int subscriptID, string componentName, int rowID) : base(subscriptID,
            componentName)
        {
            RowID = rowID;
        }
    }

    public class IndexSubscription : BaseSubscription
    {
        public readonly List<int> RowIDs;

        public IndexSubscription(int subscriptID, string componentName, List<int> rowIDs) : base(
            subscriptID,
            componentName)
        {
            RowIDs = rowIDs;
        }

        public event Action<int> OnInsert;
    }

    /// <summary>
    ///     自定义压缩/加密方法，用来处理封包数据。
    ///     压缩部分可以用内置的ZlibProtocol。
    /// </summary>
    public interface IProtocol
    {
        public byte[] Compress(byte[] data);
        public byte[] Decompress(byte[] data);

        public byte[] Crypt(byte[] data)
        {
            return data;
        }

        public byte[] Decrypt(byte[] data)
        {
            return data;
        }
    }

    /// <summary>
    ///     C#河图Client类
    /// </summary>
    public class HeTuClient
    {
        public delegate void LogFunction(object message);


        // ------------Private定义------------
        static readonly Lazy<HeTuClient> Lazy = new(() => new HeTuClient());
        readonly ConcurrentQueue<byte[]> _sendingQueue = new();
        readonly ClientWebSocket _socket;
        int _buffSize = 0x200000;
        LogFunction _logError;
        LogFunction _logInfo;
        IProtocol _protocol;
        byte[] _rxBuff;

        Task _sendingTask;
        // -----------------------------------

        public HeTuClient()
        {
            _socket = new ClientWebSocket();
        }

        public static HeTuClient Instance => Lazy.Value;

        // 连接成功时的回调
        public event Action OnConnected;

        // 收到System返回的`ResponseToClient`时的回调 todo 可能要返回json好的数据
        public event Action<string> OnResponse;

        // 设置日志函数，info为信息日志，err为错误日志。可以直接传入Unity的Debug.Log和Debug.LogError
        public void SetLogger(LogFunction info, LogFunction err)
        {
            _logInfo = info;
            _logError = err;
        }

        // 设置接收缓冲区大小，决定能接受的消息最大长度，单位字节，默认2MB。请在连接前调用，不然无效。
        public void SetReceiveBuffSize(int size)
        {
            _buffSize = size;
        }

        // 设置封包的编码/解码协议，封包可以进行压缩和加密。默认不加密，使用zlib压缩。
        // 协议要和你的河图服务器中的配置一致
        public void SetProtocol(IProtocol protocol)
        {
            _protocol = protocol;
        }

        /// <summary>
        ///     连接到河图url，url格式为"wss://host:port/hetu"
        ///     此方法会异步堵塞，在连接断开前不会结束。
        /// </summary>
        /// <returns>
        ///     返回连接断开的异常，如果正常断开则返回null，或者编辑器停止Play会抛出OperationCanceledException。
        /// </returns>
        /// <remarks>
        ///     UnityEngine使用示例：
        ///     public class YourNetworkManager : MonoBehaviour {
        ///         async void Start() {
        ///             HeTuClient.Instance.SetLogger(Debug.Log, Debug.LogError);
        ///             // 服务器端默认是启动zlib的，但客户端不是，如何启用zlib请看ZlibProtocol.cs
        ///             HeTuClient.Instance.SetProtocol(new ZlibProtocol());
        ///             HeTuClient.Instance.OnConnected += () => {
        ///                 HeTuClient.Instance.CallSystem("login", "userToken");
        ///             };
        ///             // 手游可以放入while循环，实现断线自动重连
        ///             while (true) {
        ///                 var e = await HeTuClient.Instance.Connect("wss://host:port/hetu",
        ///                                 UnityEngine.Application.exitCancellationToken);
        ///                 // 断线处理...是否重连等等
        ///                 if (e is null || e is OperationCanceledException) {
        ///                 break;
        ///             }else{
        ///                 Debug.LogError("连接断开, 将继续重连：" + e.Message);
        ///             await Task.Delay(1000);
        ///     }}}}
        /// </remarks>
        public async Task<Exception> Connect(string url, CancellationToken token)
        {
            // 连接websocket
            try
            {
                _logInfo?.Invoke($"[HeTuClient] 正在连接到：{url}...");
                _rxBuff = new byte[_buffSize];
                await _socket.ConnectAsync(new Uri(url), CancellationToken.None);
                lock (_sendingQueue)
                {
                    _sendingQueue.Clear();
                }

                _logInfo?.Invoke("[HeTuClient] 连接成功。");
                OnConnected?.Invoke();
            }
            catch (Exception e)
            {
                _logError?.Invoke($"[HeTuClient] 连接失败: {e}");
                return e;
            }

            // 循环接受消息
            while (_socket.State == WebSocketState.Open)
                try
                {
                    var received = await _socket.ReceiveAsync(
                        new ArraySegment<byte>(_rxBuff), token);
                    if (received.MessageType == WebSocketMessageType.Close)
                    {
                        if (_socket.State != WebSocketState.Closed)
                            await _socket.CloseAsync(
                                WebSocketCloseStatus.NormalClosure, 
                                "收到关闭消息",
                                CancellationToken.None);
                        _logInfo?.Invoke("[HeTuClient] 连接断开，收到了服务器Close消息。");
                        return null;
                    }

                    // 如果消息不完整，补全
                    var cur = received.Count;
                    while (!received.EndOfMessage)
                    {
                        if (cur >= _rxBuff.Length)
                        {
                            await _socket.CloseAsync(
                                WebSocketCloseStatus.MessageTooBig, 
                                "接收缓冲区溢出",
                                CancellationToken.None);
                            var errMsg = "[HeTuClient] 接受数据超出缓冲区容量(" +
                                         _rxBuff.Length / 0x100000 +
                                         "MB)，可用SetReceiveBuffSize修改";
                            _logError?.Invoke(errMsg);
                            return new WebSocketException(errMsg);
                        }

                        received = await _socket.ReceiveAsync(
                            new ArraySegment<byte>(_rxBuff, cur, _rxBuff.Length - cur),
                            token);
                        cur += received.Count;
                    }

                    // todo 测试是否copy了消息
                    OnReceived(cur);
                }
                catch (Exception e)
                {
                    _logError?.Invoke($"[HeTuClient] 接受消息时发生异常: {e}");
                    return e;
                }

            return null;
        }

        // 关闭河图连接
        public async Task Close()
        {
            _logInfo?.Invoke("[HeTuClient] 连接断开，因为主动调用了Close");
            await _socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "主动调用close",
                CancellationToken.None);
        }

        // 发送System调用，立即返回，后台执行
        public void CallSystem(string method, params object[] args)
        {
            var payload = new object[] { "sys", method }.Concat(args);
            _Send(payload);
        }

        // 订阅组件的一行数据，异步执行，返回订阅对象，可以对订阅对象注册事件
        public async Task<RowSubscription> Select(string componentName, object value,
            string where = "id")
        {
            const string callType = "sub";
            const string subType = "select";
            var buffer = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(
                new { callType, component_name = componentName, subType, value, where }));
            Send(buffer);
            //这里要想一下怎么弄，怎么wait
            await Task.Delay(0);
            return new RowSubscription(1, componentName, 1);
        }

        // 订阅组件的索引数据，异步执行，返回订阅对象，可以对订阅对象注册事件
        // left和right为索引范围，limit为返回数量，desc为是否降序，force为未查询到数据时是否也强制订阅
        public async Task<IndexSubscription> Query(string componentName, string index,
            object left, object right, int limit, bool desc, bool force)
        {
            var callType = "sub";
            var subType = "query";
            var buffer = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(
                new
                {
                    callType, component_name = componentName, subType, left, right, limit, desc,
                    force
                }));
            Send(buffer);
            return new IndexSubscription(1, componentName, new List<int> { 1, 2 });
        }

        // --------------以下为内部方法----------------

        void _Send(object payload)
        {
            var buffer = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(payload));
            buffer = _protocol?.Compress(buffer) ?? buffer;
            buffer = _protocol?.Crypt(buffer) ?? buffer;
            SendRaw(buffer);
        }

        void SendRaw(byte[] buffer)
        {
            lock (_sendingQueue)
            {
                _sendingQueue.Enqueue(buffer);
            }

            if (_sendingTask == null || _sendingTask.IsCompleted)
                _sendingTask = Task.Run(async () => { await _SendingThread(); });
        }

        async Task _SendingThread()
        {
            while (true)
            {
                byte[] data;
                lock (_sendingQueue)
                {
                    if (!_sendingQueue.TryDequeue(out data))
                        return;
                }

                try
                {
                    await _socket.SendAsync(
                        new ArraySegment<byte>(data),
                        WebSocketMessageType.Text,
                        true,
                        CancellationToken.None);
                }
                catch (Exception e)
                {
                    _logError?.Invoke($"[HeTuClient] 发送消息时发生异常: {e}");
                    return;
                }
            }
        }

        void OnReceived(int length)
        {
            // 解码消息
            var buffer = new byte[length];
            Array.Copy(_rxBuff, buffer, length);
            buffer = _protocol?.Decrypt(buffer) ?? buffer;
            buffer = _protocol?.Decompress(buffer) ?? buffer;
            var msg = Encoding.UTF8.GetString(buffer);
            // 处理消息
            var obj = JsonConvert.DeserializeObject<object[]>(msg);
        }
    }
}