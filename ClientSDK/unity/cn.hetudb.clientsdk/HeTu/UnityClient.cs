// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity库</summary>

#if UNITY_6000_0_OR_NEWER
using System.Threading.Tasks;
using UnityEngine;
#else
using Cysharp.Threading.Tasks;
#endif
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using UnityWebSocket;

namespace HeTu
{
    /// <summary>
    ///     河图Unity专用Client类，把ClientBase封装成Unity友好的异步接口。
    /// </summary>
    public class HeTuUnityClient : HeTuClientBase
    {
        private static readonly Lazy<HeTuUnityClient> Lazy = new(() =>
            new HeTuUnityClient());

        public static HeTuUnityClient Instance => Lazy.Value;

#if UNITY_6000_0_OR_NEWER
        readonly ConcurrentQueue<TaskCompletionSource<List<object>>> _waitingSubTasks =
 new();
#else
        private readonly ConcurrentQueue<UniTaskCompletionSource<List<object>>>
            _waitingSubTasks = new();
#endif
        private IWebSocket _socket;

        // 实际Websocket连接方法
        protected override void _connect(string url, Action onConnected,
            Action<byte[]> onMessage, Action onClose, Action<string> onError)
        {
            _socket = new WebSocket(url);
            _socket.OnOpen += (sender, e) => { onConnected(); };

            _socket.OnMessage += (sender, e) => { onMessage(e.RawData); };
            _socket.OnClose += (sender, e) =>
            {
                _StopAllTcs();
                switch (e.StatusCode)
                {
                    case CloseStatusCode.Normal:
                        _logInfo?.Invoke("[HeTuClient] 连接断开，收到了服务器Close消息。");
                        tcs.TrySetResult(null);
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
                        tcs.TrySetResult(new Exception(e.Reason));
                        break;
                }
            };
            _socket.OnError += (sender, e) =>
            {
                switch (lastState)
                {
                    case "ReadyForConnect":
                        _logError?.Invoke($"[HeTuClient] 连接失败: {e.Message}");
                        break;
                    case "Connected":
                        _logError?.Invoke($"[HeTuClient] 接受消息时发生异常: {e.Message}");
                        break;
                }
            };
            _socket.ConnectAsync();
        }

        // 实际关闭ws连接的方法
        protected override void _close()
        {
        }

        // 实际往ws发送数据的方法
        protected override void _send(byte[] data)
        {
        }

        private void _StopAllTcs()
        {
            _logInfo?.Invoke("[HeTuClient] 取消所有等待任务...");
            foreach (var tcs in _waitingSubTasks)
                tcs.TrySetCanceled();
            _waitingSubTasks.Clear();
        }
        // -----------------------------------


        // 连接成功时的回调

        // 收到System返回的`ResponseToClient`时的回调，根据你服务器发送的是什么数据类型来转换
        // 比如服务器发送的是字典，可以用JObject.ToObject<Dictionary<string, object>>();
        public event Action<JObject> OnResponse;


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
            // 检查连接状态(应该不会遇到）
            var state = _socket?.ReadyState ?? WebSocketState.Closed;
            if (state != WebSocketState.Closed)
            {
                _logError?.Invoke("[HeTuClient] Connect前请先Close Socket。");
                return null;
            }

            // 前置清理
            _logInfo?.Invoke($"[HeTuClient] 正在连接到：{url}...");
            _subscriptions = new Dictionary<string, WeakReference>();

            // 连接并等待
#if UNITY_6000_0_OR_NEWER
            var tcs = new TaskCompletionSource<Exception>();
#else
            var tcs = new UniTaskCompletionSource<Exception>();
#endif
            var lastState = "ReadyForConnect";
            ConnectSync(url);

            // token可取消等待
            token?.Register(() =>
            {
                _logInfo?.Invoke("[HeTuClient] 连接断开，收到了CancellationToken取消请求.");
                _socket.CloseAsync();
                _StopAllTcs();
                tcs.TrySetResult(new OperationCanceledException());
            });

            // 等待连接断开
            return await tcs.Task;
        }

        // 关闭河图连接
        public void Close()
        {
            _logInfo?.Invoke("[HeTuClient] 主动调用了Close");
            _StopAllTcs();
            _socket.CloseAsync();
        }

        /// <summary>
        ///     后台发送System调用，此方法立即返回。
        ///     可通过`HeTuClient.Instance.SystemCallbacks["system_name"] = (args) => {}`
        ///     注册客户端调用回调（非服务器端回调）。
        /// </summary>
        public void CallSystem(string systemName, params object[] args)
        {
            var payload = new object[] { "sys", systemName }.Concat(args);
            _Send(payload); // 后台线程发送，这里无论成功立即返回
            SystemCallbacks.TryGetValue(systemName, out var callbacks);
            callbacks?.Invoke(args);
        }

        // todo 异步堵塞的CallSystem，会等待并返回服务器回应，外加CancellationToken，不要一直等

        /// <summary>
        ///     订阅组件的行数据。订阅`where`属性值==`value`的第一行数据。
        ///     `Select`只对“单行”订阅，如果没有查询到行，会返回`null`。
        ///     如果想要订阅不存在的行，请用`Query`订阅索引。
        /// </summary>
        /// <returns>
        ///     返回`null`如果没查询到行，否则返回`RowSubscription`对象。
        ///     可通过`RowSubscription.Data`获取数据。
        ///     可以注册`RowSubscription.OnUpdate`和`OnDelete`事件处理数据更新。
        /// </returns>
        /// <remarks>
        ///     可使用`T`模板参数定义数据类型，不写就是默认`Dictionary{string, string}`类型。
        ///     使用`T`模板时，对象定义要和服务器定义一致，可使用服务器端工具自动生成c#定义。
        ///     使用默认的Dictionary更自由灵活，但都是字符串类型需要自行转换。
        /// </remarks>
        /// <code>
        /// // 使用示例
        /// // 假设HP组件有owner属性，表示属于哪个玩家，value属性表示hp值。
        /// var subscription = await HeTuClient.Instance.Select("HP", user_id, "owner");
        /// Debug.log("My HP:" + int.Parse(subscription.Data["value"]));
        /// subscription.OnUpdate += (sender, rowID) => {
        ///     Debug.log("My New HP:" + int.Parse(sender.Data["value"]));
        /// }
        /// // --------或者--------
        /// Class HP : IBaseComponent {  // Class名必须和服务器一致
        ///     public long id {get; set;}  // 就id这一项必须定义get set
        ///     public long owner;
        ///     public int value;
        /// }
        /// var subscription = await HeTuClient.Instance.Select{HP}(user_id, "owner");
        /// Debug.log("My HP:" + subscription.Data.value);
        /// </code>
#if UNITY_6000_0_OR_NEWER
        public async Awaitable<RowSubscription<T>> Select<T>(
#else
        public async UniTask<RowSubscription<T>> Select<T>(
#endif
            object value, string where = "id", string componentName = null)
            where T : IBaseComponent
        {
            componentName ??= typeof(T).Name;
            // 如果where是id，我们可以事先判断是否已经订阅过
            if (where == "id")
            {
                var predictID = _makeSubID(
                    componentName, "id", value, null, 1, false);
                if (_subscriptions.TryGetValue(predictID, out var subscribed))
                    if (subscribed.Target is RowSubscription<T> casted)
                        return casted;
                    else
                        throw new InvalidCastException(
                            $"[HeTuClient] 已订阅该数据，但之前订阅使用的是{typeof(T)}类型");
            }

            // 向服务器订阅
            var payload = new[] { "sub", componentName, "select", value, where };
            _Send(payload);
            _logDebug?.Invoke(
                $"[HeTuClient] 发送Select订阅: {componentName}.{where}[{value}:]");

            // 等待服务器结果
#if UNITY_6000_0_OR_NEWER
            var tcs = new TaskCompletionSource<List<object>>();
#else
            var tcs = new UniTaskCompletionSource<List<object>>();
#endif
            _waitingSubTasks.Enqueue(tcs);
            List<object> subMsg;
            try
            {
                // await UniTask会调用task.GetResult()，如果cancel了，会抛出异常
                subMsg = await tcs.Task;
            }
            catch (OperationCanceledException e)
            {
                // NUnit不把取消信号视为错误，所以这里要LogError一下让测试不通过
                _logError?.Invoke($"[HeTuClient] 订阅数据过程中遇到取消信号: {e}");
                throw;
            }

            var subID = (string)subMsg[1];
            // 如果没有查询到值
            if (subID is null) return null;
            // 如果依然是重复订阅，直接返回副本
            if (_subscriptions.TryGetValue(subID, out var stillSubscribed))
                if (stillSubscribed.Target is RowSubscription<T> casted)
                    return casted;
                else
                    throw new InvalidCastException(
                        $"[HeTuClient] 已订阅该数据，但之前订阅使用的是{typeof(T)}类型");

            var data = ((JObject)subMsg[2]).ToObject<T>();
            var newSub = new RowSubscription<T>(subID, componentName, data);
            _subscriptions[subID] = new WeakReference(newSub, false);
            _logInfo?.Invoke($"[HeTuClient] 成功订阅了 {subID}");
            return newSub;
        }

#if UNITY_6000_0_OR_NEWER
        public async Awaitable<RowSubscription<DictComponent>> Select(
#else
        public async UniTask<RowSubscription<DictComponent>> Select(
#endif
            string componentName, object value, string where = "id")
        {
            return await Select<DictComponent>(value, where, componentName);
        }

        /// <summary>
        ///     订阅组件的索引数据。`index`是开启了索引的属性名，`left`和`right`为索引范围，
        ///     `limit`为返回数量，`desc`为是否降序，`force`为未查询到数据时是否也强制订阅。
        /// </summary>
        /// <returns>
        ///     返回`IndexSubscription`对象。
        ///     可通过`IndexSubscription.Rows`获取数据。
        ///     并可以注册`IndexSubscription.OnInsert`和`OnUpdate`，`OnDelete`数据事件。
        /// </returns>
        /// <remarks>
        ///     可使用`T`模板参数定义数据类型，不写就是默认`Dictionary{string, string}`类型。
        ///     使用`T`模板时，对象定义要和服务器定义一致，可使用服务器端工具自动生成c#定义。
        ///     使用默认的Dictionary更自由灵活，但都是字符串类型需要自行转换。
        ///     如果目标组件权限为Owner，则只能查询到`owner`属性==自己的行。
        /// </remarks>
        /// <code>
        /// //使用示例
        /// var subscription = await HeTuClient.Instance.Query("HP", "owner", 0, 9999, 10);
        /// foreach (var row in subscription.Rows)
        ///     Debug.log($"HP: {row.Key}:{row.Value["value"]}");
        /// subscription.OnUpdate += (sender, rowID) => {
        ///     Debug.log($"New HP: {rowID}:{sender.Rows[rowID]["value"]}");
        /// }
        /// subscription.OnDelete += (sender, rowID) => {
        ///     Debug.log($"Delete row: {rowID}，之前的数据：{sender.Rows[rowID]["value"]}");
        /// }
        /// </code>
#if UNITY_6000_0_OR_NEWER
        public async Awaitable<IndexSubscription<T>> Query<T>(
#else
        public async UniTask<IndexSubscription<T>> Query<T>(
#endif
            string index, object left, object right, int limit,
            bool desc = false, bool force = true, string componentName = null)
            where T : IBaseComponent
        {
            componentName ??= typeof(T).Name;

            // 先要组合sub_id看看是否已订阅过
            var predictID = _makeSubID(
                componentName, index, left, right, limit, desc);
            if (_subscriptions.TryGetValue(predictID, out var subscribed))
                if (subscribed.Target is IndexSubscription<T> casted)
                    return casted;
                else
                    throw new InvalidCastException(
                        $"[HeTuClient] 已订阅该数据，但之前订阅使用的是{typeof(T)}类型");

            // 发送订阅请求
            var payload = new[]
            {
                "sub", componentName, "query", index, left, right, limit, desc, force
            };
            _Send(payload);
            _logDebug?.Invoke($"[HeTuClient] 发送Query订阅: {predictID}");

            // 等待服务器结果
#if UNITY_6000_0_OR_NEWER
            var tcs = new TaskCompletionSource<List<object>>();
#else
            var tcs = new UniTaskCompletionSource<List<object>>();
#endif
            _waitingSubTasks.Enqueue(tcs);
            List<object> subMsg;
            try
            {
                subMsg = await tcs.Task;
            }
            catch (OperationCanceledException e)
            {
                // NUnit不把取消信号视为错误，所以这里要LogError一下让测试不通过
                _logError?.Invoke($"[HeTuClient] 订阅数据过程中遇到取消信号: {e}");
                throw;
            }

            var subID = (string)subMsg[1];
            // 如果没有查询到值
            if (subID is null) return null;
            // 如果依然是重复订阅，直接返回副本
            if (_subscriptions.TryGetValue(subID, out var stillSubscribed))
                if (stillSubscribed.Target is IndexSubscription<T> casted)
                    return casted;
                else
                    throw new InvalidCastException(
                        $"[HeTuClient] 已订阅该数据，但之前订阅使用的是{typeof(T)}类型");

            var rows = ((JArray)subMsg[2]).ToObject<List<T>>();
            var newSub = new IndexSubscription<T>(subID, componentName, rows);
            _subscriptions[subID] = new WeakReference(newSub, false);
            _logInfo?.Invoke($"[HeTuClient] 成功订阅了 {subID}");
            return newSub;
        }

#if UNITY_6000_0_OR_NEWER
        public async Awaitable<IndexSubscription<DictComponent>> Query(
#else
        public async UniTask<IndexSubscription<DictComponent>> Query(
#endif
            string componentName, string index, object left, object right, int limit,
            bool desc = false, bool force = true)
        {
            return await Query<DictComponent>(index, left, right, limit, desc, force,
                componentName);
        }
    }
}
