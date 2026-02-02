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
    public class HeTuClient : HeTuClientBase
    {
        private static readonly Lazy<HeTuClient> Lazy = new(() =>
            new HeTuClient());

        public static HeTuClient Instance => Lazy.Value;

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
                switch (e.StatusCode)
                {
                    case CloseStatusCode.Normal:
                        onClose();
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
                        onError(e.Reason);
                        onClose();
                        break;
                }
            };
            _socket.OnError += (sender, e) =>
            {
                onError(e.Message);
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

        // -----------------------------------


        // 连接成功时的回调




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
        ///                 await Awaitable.WaitForSecondsAsync(1); // Unity 6000+
        ///                 // Unity 2022+: await UniTask.Delay(1000);
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
                Logger.Instance.Error("[HeTuClient] Connect前请先Close Socket。");
                return null;
            }

            // 连接并等待
#if UNITY_6000_0_OR_NEWER
            var tcs = new AwaitableCompletionSource<Exception>();
#else
            var tcs = new UniTaskCompletionSource<Exception>();
#endif

            ConnectSync(url);

            OnClosed += (errMsg) =>
            {
                if (errMsg is null)
                    tcs.TrySetResult(null);
                else
                    tcs.TrySetResult(new Exception(errMsg));
            };

            // token可取消等待
            token?.Register(() =>
            {
                Logger.Instance.Info("[HeTuClient] 连接断开，收到了CancellationToken取消请求.");
                ResponseQueue.CancelAll("收到了CancellationToken取消请求");
                _close();
                tcs.TrySetResult(new OperationCanceledException());
            });

            // 等待连接断开
            return await tcs.Task;
        }


        /// <summary>
        ///     执行System调用。
        ///     如果不await此方法，调用会在后台异步发送，立即返回。
        ///     如果await此方法，调用会等待服务器回应，默认返回"ok"，除非有使用ResponseToClient。
        ///
        ///     另可通过`HeTuClient.Instance.SystemLocalCallbacks["system_name"] = (args) => {}`
        ///     注册客户端对应逻辑，每次CallSystem调用时也都会先执行这些回调，这样一些本地逻辑可以放在客户端回调里。
        /// </summary>
#if UNITY_6000_0_OR_NEWER
        public async Awaitable<JsonObject> CallSystem(string systemName, params object[] args)
#else
        public async UniTask<JsonObject> CallSystem(string systemName, params object[] args)
#endif
        {
#if UNITY_6000_0_OR_NEWER
            var tcs = new AwaitableCompletionSource<JsonObject>();
#else
            var tcs = new UniTaskCompletionSource<JsonObject>();
#endif

            CallSystemSync(systemName, args, (response, cancel) =>
            {
                if (cancel)
                {
                    Logger.Instance.Error($"[HeTuClient] CallSystem过程中遇到取消信号");
                    tcs.TrySetCanceled();
                }
                else
                    tcs.TrySetResult(response);
            });

            return await tcs.Task;
        }

        /// <summary>
        ///     订阅组件的行数据。订阅`index`属性值==`value`的第一行数据。
        ///     `Get`只对“单行”订阅，如果没有查询到行，会返回`null`。
        ///     如果想要订阅不存在的行，请用`Range`订阅索引。
        /// </summary>
        /// <returns>
        ///     返回`null`如果没查询到行，否则返回`RowSubscription`对象。
        ///     可通过`RowSubscription.Data`获取数据。
        ///     可以注册`RowSubscription.OnUpdate`和`OnDelete`事件处理数据更新。
        ///     但建议通过响应式`RowSubscription.ToObserveable()`来处理数据变化，更加方便。
        /// </returns>
        /// <remarks>
        ///     可使用`T`模板参数定义数据类型，不写就是默认`Dictionary{string, object}`类型。
        ///     使用`T`模板时，对象定义要和服务器定义一致，可使用服务器端工具自动生成c#定义。
        ///     使用默认的Dictionary更自由灵活，但类型需要自行转换。
        /// </remarks>
        /// <code>
        /// // 使用示例
        /// // 假设HP组件有owner属性，表示属于哪个玩家，value属性表示hp值。
        /// var subscription = await HeTuClient.Instance.Get("HP", "owner", user_id);
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
        /// var subscription = await HeTuClient.Instance.Get<HP>("owner", user_id);
        /// Debug.log("My HP:" + subscription.Data.value);
        /// </code>
#if UNITY_6000_0_OR_NEWER
        public async Awaitable<RowSubscription<T>> Get<T>(
#else
        public async UniTask<RowSubscription<T>> Get<T>(
#endif
            string index, object value, string componentName = null)
            where T : IBaseComponent
        {
            componentName ??= typeof(T).Name;

#if UNITY_6000_0_OR_NEWER
            var tcs = new AwaitableCompletionSource<RowSubscription<T>>();
#else
            var tcs = new UniTaskCompletionSource<RowSubscription<T>>();
#endif

            // 如果index是id，我们可以事先判断是否已经订阅过
            GetSync<T>(index, value, (rowSub, cancel) =>
            {
                if (cancel)
                {
                    Logger.Instance.Error($"[HeTuClient] 订阅数据过程中遇到取消信号");
                    tcs.TrySetCanceled();
                }
                else
                    tcs.TrySetResult(rowSub);
            }, componentName);

            return await tcs.Task;
        }

#if UNITY_6000_0_OR_NEWER
        public async Awaitable<RowSubscription<DictComponent>> Get(
#else
        public async UniTask<RowSubscription<DictComponent>> Get(
#endif
            string componentName, string index, object value)
        {
            return await Get<DictComponent>(index, value, componentName);
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
        ///     可使用`T`模板参数定义数据类型，不写就是默认`Dictionary{string, object}`类型。
        ///     使用`T`模板时，对象定义要和服务器定义一致，可使用服务器端工具自动生成c#定义。
        ///     使用默认的Dictionary更自由灵活，但类型需要自行转换。
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
        public async Awaitable<IndexSubscription<T>> Range<T>(
#else
        public async UniTask<IndexSubscription<T>> Range<T>(
#endif
            string index, object left, object right, int limit,
            bool desc = false, bool force = true, string componentName = null)
            where T : IBaseComponent
        {
            componentName ??= typeof(T).Name;

#if UNITY_6000_0_OR_NEWER
            var tcs = new AwaitableCompletionSource<IndexSubscription<T>>();
#else
            var tcs = new UniTaskCompletionSource<IndexSubscription<T>>();
#endif

            RangeSync<T>(
                index, left, right, limit,
                (idxSub, cancel) =>
                {
                    if (cancel)
                    {
                        Logger.Instance.Error($"[HeTuClient] 订阅数据过程中遇到取消信号");
                        tcs.TrySetCanceled();
                    }
                    else
                        tcs.TrySetResult(idxSub);
                }, desc, force, componentName);
            return await tcs.Task;
        }

#if UNITY_6000_0_OR_NEWER
        public async Awaitable<IndexSubscription<DictComponent>> Range(
#else
        public async UniTask<IndexSubscription<DictComponent>> Range(
#endif
            string componentName, string index, object left, object right, int limit,
            bool desc = false, bool force = true)
        {
            return await Range<DictComponent>(index, left, right, limit, desc, force,
                componentName);
        }
    }
}
