// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity库</summary>

#if UNITY_6000_0_OR_NEWER
using System.Threading.Tasks;
#else
using Cysharp.Threading.Tasks;
#endif
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using UnityEngine;
using UnityWebSocket;

namespace HeTu
{
    /// <summary>
    ///     Unity河图Client类，把Callback风格的Websocket封装为async/await风格。
    /// </summary>
    public class HeTuUnityClient
    {
        // ------------Private定义------------

        private readonly MessagePipeline _messagePipeline;
        private readonly RequestManager _requestManager;
        private readonly ConcurrentQueue<byte[]> _sendingQueue = new();
        private readonly Subscriptions _subscriptions;
        private IWebSocket _socket;

        // -----------------------------------

        // 连接成功时的回调
        public event Action OnConnected;

        //


        /// <summary>
        ///     连接到河图url，url格式为"wss://host:port/hetu"
        ///     此方法为async/await异步堵塞，在连接断开前不会结束。
        /// </summary>
        /// <returns>
        ///     返回异常（而不是抛出异常）。
        ///     - 连接异常断开返回Exception；
        ///     - 正常断开返回null。
        ///     - 如果CancellationToken触发，则返回OperationCanceledException。
        /// </returns>
        /// <code>
        ///     //UnityEngine使用示例：
        ///     public class YourNetworkManager : MonoBehaviour {
        ///         async void Start() {
        ///             HeTuClient.Instance.SetLogger(Debug.Log, Debug.LogError);
        ///             // 服务器端默认是使用zlib的压缩消息
        ///             HeTuClient.Instance.SetProtocol(new ZlibProtocol());
        ///             HeTuClient.Instance.OnConnected += () => {
        ///                 HeTuClient.Instance.CallSystem("login", "userToken");
        ///             };
        ///             // 手游可以放入while循环，实现断线自动重连
        ///             while (true) {
        ///                 var e = await HeTuClient.Instance.Connect("wss://host:port/hetu",
        ///                                 UnityEngine.Application.exitCancellationToken);
        ///                 // 断线处理...是否重连等等
        ///                 if (e is null || e is OperationCanceledException)
        ///                     break;
        ///                 else
        ///                     Debug.LogError("连接断开, 将继续重连：" + e.Message);
        ///                 await Task.Delay(1000);
        ///     }}}
        /// </code>
#if UNITY_6000_0_OR_NEWER
        public async Awaitable<Exception> Connect(string url, CancellationToken? token)
#else
        public async UniTask<Exception> Connect(string url, CancellationToken? token)
#endif
        {
            // 设置Logger
            if (!Logger.Instance.IsSetup)
                Logger.Instance.SetLogger(Debug.Log, Debug.LogError);

            // 检查连接状态(应该不会遇到）
            var state = _socket?.ReadyState ?? WebSocketState.Closed;
            if (state != WebSocketState.Closed)
            {
                Logger.Instance.Error("[HeTuClient] Connect前请先Close Socket。");
                return null;
            }

            // 前置清理
            Logger.Instance.Info($"[HeTuClient] 正在连接到：{url}...");
            _subscriptions.Clean();

            // 初始化返回值
            var ret = RequestManager.CreateTcs<Exception>();

            // 初始化WebSocket以及事件
            var lastState = "ReadyForConnect";
            _socket = new WebSocket(url);
            _socket.OnOpen += (sender, e) =>
            {
                Logger.Instance.Info("[HeTuClient] 连接成功。");
                lastState = "Connected";
                OnConnected?.Invoke();
                foreach (var data in _sendingQueue)
                    _socket.SendAsync(data);
                _sendingQueue.Clear();
            };
            _socket.OnMessage += (sender, e) => { _OnReceived(e.RawData); };
            _socket.OnClose += (sender, e) =>
            {
                _requestManager.CancelAll("连接断开");
                switch (e.StatusCode)
                {
                    case CloseStatusCode.Normal:
                        Logger.Instance.Info("[HeTuClient] 连接断开，收到了服务器Close消息。");
                        ret.TrySetResult(null);
                        break;
                    case CloseStatusCode.Unknown:
                    case CloseStatusCode.Away:
                    case CloseStatusCode.ProtocolError:
                    case CloseStatusCode.UnsupportedData:
                    case CloseStatusCode.Undefined:
                    case CloseStatusCode.NoStatus:
                    case CloseStatusCode.Abnormal:
                    case CloseStatusCode.InvalidData:
                    case CloseStatusCode.PolicyViolation:
                    case CloseStatusCode.TooBig:
                    case CloseStatusCode.MandatoryExtension:
                    case CloseStatusCode.ServerError:
                    case CloseStatusCode.TlsHandshakeFailure:
                    default:
                        ret.TrySetResult(new Exception(e.Reason));
                        break;
                }
            };
            _socket.OnError += (sender, e) =>
            {
                switch (lastState)
                {
                    case "ReadyForConnect":
                        Logger.Instance.Error($"[HeTuClient] 连接失败: {e.Message}");
                        break;
                    case "Connected":
                        Logger.Instance.Error($"[HeTuClient] 接受消息时发生异常: {e.Message}");
                        break;
                }
            };
            // 开始连接
            _socket.ConnectAsync();

            // token可取消等待
            token?.Register(() =>
            {
                Logger.Instance.Info("[HeTuClient] 连接断开，收到了CancellationToken取消请求.");
                _socket.CloseAsync();
                _requestManager.CancelAll("手动断线");
                ret.TrySetResult(new OperationCanceledException());
            });

            // 等待连接断开
            return await ret.Task;
        }

        // 关闭河图连接
        public void Close()
        {
            Logger.Instance.Info("[HeTuClient] 主动调用了Close");
            _requestManager.CancelAll("主动调用了Close");
            _socket.CloseAsync();
        }

        private void _Send(object payload)
        {
            var buffer = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(payload));
            buffer = _protocol?.Compress(buffer) ?? buffer;
            buffer = _protocol?.Crypt(buffer) ?? buffer;

            if (_socket.ReadyState == WebSocketState.Open)
            {
                _socket.SendAsync(buffer);
            }
            else
            {
                Logger.Instance.Info("尝试发送数据但连接未建立，将加入队列在建立后发送。");
                _sendingQueue.Enqueue(buffer);
            }
        }

        private void _OnReceived(byte[] buffer)
        {
            // 解码消息
            buffer = _protocol?.Decrypt(buffer) ?? buffer;
            buffer = _protocol?.Decompress(buffer) ?? buffer;
            var decoded = Encoding.UTF8.GetString(buffer);
            // 处理消息
            // Logger.Instance.Info($"[HeTuClient] 收到消息: {decoded}");
            var structuredMsg = JsonConvert.DeserializeObject<List<object>>(decoded);
            if (structuredMsg is null) return;
            switch (structuredMsg[0])
            {
                case "rsp":
                    OnResponse?.Invoke((JObject)structuredMsg[1]);
                    break;
                case "sub":
                    if (!_waitingSubTasks.TryDequeue(out var tcs))
                        break;
                    tcs.TrySetResult(structuredMsg);
                    break;
                case "updt":
                    var subID = (string)structuredMsg[1];
                    if (!_subscriptions.TryGetValue(subID, out var pSubscribed))
                        break;
                    if (pSubscribed.Target is not BaseSubscription subscribed)
                        break;
                    var rows = ((JObject)structuredMsg[2])
                        .ToObject<Dictionary<long, JObject>>();
                    foreach (var (rowID, data) in rows)
                        subscribed.Update(rowID, data);
                    break;
            }
        }
    }
}
