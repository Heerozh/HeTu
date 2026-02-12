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
using System.Threading;
using JetBrains.Annotations;
using UnityEngine;
using UnityWebSocket;

namespace HeTu
{
    /// <summary>
    ///     河图Unity专用Client类，把ClientBase封装成Unity友好的异步接口。
    /// </summary>
    public sealed class HeTuClient : HeTuClientBase
    {
        private static readonly Lazy<HeTuClient> s_lazy = new(() =>
        {
            Logger.Instance.SetLogger(Debug.Log, Debug.LogError, Debug.Log);
            return new HeTuClient();
        });

#if UNITY_6000_0_OR_NEWER
        private static AwaitableCompletionSource<T> NewCompletionSource<T>() => new();
        private static Awaitable<T> AwaitFrom<T>(AwaitableCompletionSource<T> tcs) => tcs.Awaitable;
#else
        private static UniTaskCompletionSource<T> NewCompletionSource<T>() => new();
        private static UniTask<T> AwaitFrom<T>(UniTaskCompletionSource<T> tcs) => tcs.Task;
#endif

        private CancellationTokenSource _connectionCancelSource;

        private IWebSocket _socket;

        /// <summary>
        ///     全局单例客户端实例。
        /// </summary>
        public static HeTuClient Instance => s_lazy.Value;

        /// <summary>
        ///     对于全局连接(HeTuClient.Instance)，可以不做Dispose。
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            _socket?.CloseAsync();
            _socket = null;
            _connectionCancelSource?.Cancel();
            _connectionCancelSource?.Dispose();
            _connectionCancelSource = null;
        }

        // 实际Websocket连接方法
        protected override void ConnectCore(string url, Action onConnected,
            Action<byte[]> onMessage, Action<string> onClose, Action<string> onError)
        {
            _socket = new WebSocket(url);
            _socket.OnOpen += (_, _) => { onConnected(); };

            _socket.OnMessage += (_, e) => { onMessage(e.RawData); };
            _socket.OnClose += (_, e) =>
            {
                switch (e.StatusCode)
                {
                    case CloseStatusCode.Normal:
                        onClose(null);
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
                        onClose(e.Reason);
                        break;
                }
            };
            _socket.OnError += (_, e) => { onError(e.Message); };
            _socket.ConnectAsync();
        }

        // 实际关闭ws连接的方法
        protected override void CloseCore()
        {
            _socket?.CloseAsync(); // 并不一定会激发onclose事件。
            // 如果场景没挂WebsocketManager，会导致close不掉socket的task
            // 这是正常的，如果运行test套件，确保不在editor mode里，而是在player mode里运行
            _socket = null;
            _connectionCancelSource?.Cancel();
            _connectionCancelSource?.Dispose();
            _connectionCancelSource = null;
            State = ConnectionState.Disconnected;
        }

        // 实际往ws发送数据的方法
        protected override void SendCore(byte[] data) => _socket.SendAsync(data);

        // -----------------------------------


        /// <summary>
        ///     连接到河图url，url格式为"wss://host:port/hetu"
        ///     此方法为async/await异步堵塞，在连接断开前不会结束。
        /// </summary>
        /// <returns>
        ///     返回null或错误信息。
        ///     - 正常断开返回null或"Canceled"。其中Canceled在游戏退出，或手动调用Close时返回。
        ///     - 连接异常断开返回错误信息；
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
        ///                 var e = await HeTuClient.Instance.Connect("wss://host:port/hetu");
        ///                 // 断线处理...是否重连等等
        ///                 if (e is null || e == "Canceled")
        ///                     break;
        ///                 else
        ///                     Debug.LogError("连接断开, 将继续重连：" + e);
        ///                 await Awaitable.WaitForSecondsAsync(1); // Unity 6000+
        ///                 // Unity 2022+: await UniTask.Delay(1000);
        ///     }}}
        /// </code>
#if UNITY_6000_0_OR_NEWER
        public async Awaitable<string> Connect(string url)
#else
        public async UniTask<string> Connect(string url)
#endif
        {
            // 检查连接状态(应该不会遇到，但ReadyState经常为Closing状态）
            var state = _socket?.ReadyState ?? WebSocketState.Closed;
            if (state != WebSocketState.Closed)
            {
                Logger.Instance.Error(
                    $"[HeTuClient] Connect前请先Close Socket, socket state:{_socket?.ReadyState}");
                return null;
            }

            // 连接并等待
            var tcs = NewCompletionSource<string>();

            ConnectSync(url);

            Action<string> onClose = errMsg =>
            {
                if (errMsg is null)
                {
                    Logger.Instance.Info("[HeTuClient] 连接已断开.");
                    tcs.TrySetResult(null);
                }
                else
                {
                    Logger.Instance.Info($"[HeTuClient] 连接断开，{errMsg}.");
                    tcs.TrySetResult(errMsg);
                }

                CloseCore();
            };
            OnClosed += onClose;

            // 必须在退出时保证cancel, 不然会卡死unity
            _connectionCancelSource?.Cancel();
            _connectionCancelSource?.Dispose();
            _connectionCancelSource = new CancellationTokenSource();
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                _connectionCancelSource.Token,
                Application.exitCancellationToken
            );

            // token可取消等待
            await using var reg = linkedCts.Token.Register(() =>
            {
                Logger.Instance.Info("[HeTuClient] 连接断开，收到了Cancel取消请求.");
                ResponseQueue.CancelAll("收到了Cancel取消请求");
                tcs.TrySetResult("Canceled");
                CloseCore();
            });

            // 等待连接断开
            var result = await AwaitFrom(tcs);
            OnClosed -= onClose;
            return result;
        }


        /// <summary>
        ///     执行System调用。
        ///     如果不CallSystem().Forget()此方法，调用会在后台异步发送，立即返回。
        ///     如果await CallSystem()此方法，调用会等待服务器回应，默认返回"ok"，除非有使用ResponseToClient。
        ///     另可通过`HeTuClient.Instance.SystemLocalCallbacks["system_name"] = (args) => {}`
        ///     注册客户端对应逻辑，每次CallSystem调用时也都会先执行这些回调，这样一些本地逻辑可以放在客户端回调里。
        /// </summary>
#if UNITY_6000_0_OR_NEWER
        public async Awaitable<JsonObject> CallSystem(string systemName, params object[] args)
#else
        public async UniTask<JsonObject> CallSystem(string systemName,
            params object[] args)
#endif
        {
            if (_connectionCancelSource == null)
            {
                throw new InvalidOperationException(
                    "CallSystem前请先Connect Socket");
            }

            var tcs = NewCompletionSource<JsonObject>();

            CallSystemSync(systemName, args, (response, cancel) =>
            {
                if (cancel)
                {
                    Logger.Instance.Error("[HeTuClient] CallSystem过程中遇到取消信号");
                    tcs.TrySetCanceled();
                }
                else
                    tcs.TrySetResult(response);
            });

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                _connectionCancelSource.Token,
                Application.exitCancellationToken
            );
            await using var reg = linkedCts.Token.Register(() =>
            {
                tcs.TrySetCanceled();
                CloseCore();
            });

            return await AwaitFrom(tcs);
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
        /// subscription.Dispose(); // 反订阅
        /// // --------或者--------
        /// <![CDATA[
        /// Class HP : IBaseComponent {  // Class名必须和服务器一致
        ///     public long id {get; set;}  // 就id这一项必须定义get set
        ///     public long owner;
        ///     public int value;
        /// }
        /// var subscription = await HeTuClient.Instance.Get<HP>("owner", user_id);
        /// Debug.log("My HP:" + subscription.Data.value);
        /// subscription.Dispose(); // 反订阅
        /// // 或使用AddTo和gameObject生命周期绑定
        /// // subscription.AddTo(gameObject);
        /// ]]>
        /// </code>
        [MustDisposeResource]
#if UNITY_6000_0_OR_NEWER
        public async Awaitable<RowSubscription<T>> Get<T>(
#else
        public async UniTask<RowSubscription<T>> Get<T>(
#endif
            string index, object value, string componentName = null)
            where T : IBaseComponent
        {
            componentName ??= typeof(T).Name;
            if (_connectionCancelSource == null)
            {
                throw new InvalidOperationException(
                    "CallSystem前请先Connect Socket");
            }

            var tcs = NewCompletionSource<RowSubscription<T>>();
            // todo 做一个network inspector，记录每次订阅的index和value，方便调试
            // todo 做一个postman类似的工具驿栈，直接发送请求，查看服务器响应，也可以新建订阅，方便调试
            GetSync<T>(index, value, (rowSub, cancel, ex) =>
            {
                if (cancel)
                {
                    Logger.Instance.Error("[HeTuClient] 订阅数据过程中遇到取消信号");
                    tcs.TrySetCanceled();
                }
                else if (ex != null)
                    tcs.TrySetException(ex);
                else
                    tcs.TrySetResult(rowSub);
            }, componentName);

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                _connectionCancelSource.Token,
                Application.exitCancellationToken
            );
            await using var reg = linkedCts.Token.Register(() =>
            {
                tcs.TrySetCanceled();
                CloseCore();
            });

            return await AwaitFrom(tcs);
        }

        /// <summary>
        ///     订阅单行数据（字典版本）。
        /// </summary>
        /// <param name="componentName">组件名。</param>
        /// <param name="index">索引字段名。</param>
        /// <param name="value">索引值。</param>
        /// <returns>查询到时返回行订阅；未命中时返回 <see langword="null"/>。</returns>
        [MustDisposeResource]
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
        ///     但建议通过响应式`IndexSubscription.ToObservableDictionary()`来处理数据变化，更加方便。
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
        /// subscription.Dispose(); // 反订阅
        /// </code>
        [MustDisposeResource]
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
            if (_connectionCancelSource == null)
            {
                throw new InvalidOperationException(
                    "CallSystem前请先Connect Socket");
            }

            var tcs = NewCompletionSource<IndexSubscription<T>>();

            RangeSync<T>(
                index, left, right, limit,
                (idxSub, cancel, ex) =>
                {
                    if (cancel)
                    {
                        Logger.Instance.Error("[HeTuClient] 订阅数据过程中遇到取消信号");
                        tcs.TrySetCanceled();
                    }
                    else if (ex != null)
                        tcs.TrySetException(ex);
                    else
                        tcs.TrySetResult(idxSub);
                }, desc, force, componentName
            );

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                _connectionCancelSource.Token,
                Application.exitCancellationToken
            );
            await using var reg = linkedCts.Token.Register(() =>
            {
                tcs.TrySetCanceled();
                CloseCore();
            });
            return await AwaitFrom(tcs);
        }

        /// <summary>
        ///     订阅索引范围数据（字典版本）。
        /// </summary>
        /// <param name="componentName">组件名。</param>
        /// <param name="index">索引字段名。</param>
        /// <param name="left">范围左边界。</param>
        /// <param name="right">范围右边界。</param>
        /// <param name="limit">返回条数上限。</param>
        /// <param name="desc">是否降序。</param>
        /// <param name="force">未命中时是否保持订阅。</param>
        /// <returns>范围订阅对象。</returns>
        [MustDisposeResource]
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
