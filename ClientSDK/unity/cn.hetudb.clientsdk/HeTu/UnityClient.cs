// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity库</summary>


#if !UNITY_6000_0_OR_NEWER
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
        private static Awaitable<T> AwaitFrom<T>(AwaitableCompletionSource<T> tcs) =>
            tcs.Awaitable;
#else
        private static UniTaskCompletionSource<T> NewCompletionSource<T>() => new();
        private static UniTask<T> AwaitFrom<T>(UniTaskCompletionSource<T> tcs) =>
            tcs.Task;
#endif

        private CancellationTokenSource _connectionCancelSource;
#if UNITY_6000_0_OR_NEWER
        private AwaitableCompletionSource<bool> _connectCompletion;
        private AwaitableCompletionSource<string> _closeCompletion;
#else
        private UniTaskCompletionSource<bool> _connectCompletion;
        private UniTaskCompletionSource<string> _closeCompletion;
#endif

        private IWebSocket _socket;

        /// <summary>
        ///     全局单例客户端实例。
        /// </summary>
        public static HeTuClient Instance => s_lazy.Value;

        /// <summary>
        ///     底层 WebSocket 是否仍处于 Open。区别于 <see cref="HeTuClientBase.IsConnected" />
        ///     （读缓存的握手状态,断线事件没派发时会 stale,典型如 Editor 关 Domain
        ///     Reload 后停 Play）,本属性直读底层 socket 的真实 <c>ReadyState</c>,
        ///     可靠反映连接是否还活着,可用来判断现有连接能否直接复用。
        /// </summary>
        public bool IsConnectionAlive =>
            _socket?.ReadyState == WebSocketState.Open;

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
            _closeCompletion?.TrySetResult("Canceled");
            State = ConnectionState.Disconnected;
        }

        // 实际往ws发送数据的方法
        protected override void SendCore(byte[] data) => _socket.SendAsync(data);

        // -----------------------------------


        /// <summary>
        ///     连接到河图 url，并在握手完成后返回。url格式为"wss://host:port/hetu/<instance_name>"
        /// </summary>
#if UNITY_6000_0_OR_NEWER
        public async Awaitable Connect(string url, string authKey = null,
#else
        public async UniTask Connect(string url, string authKey = null,
#endif
            CancellationToken cancellationToken = default)
        {
            var state = _socket?.ReadyState ?? WebSocketState.Closed;
            if (state != WebSocketState.Closed)
                throw new InvalidOperationException(
                    $"Connect前请先Close Socket, socket state:{_socket?.ReadyState}");

            if (authKey != null)
                ConfigureCryptoAuthKey(authKey);

            _connectionCancelSource?.Cancel();
            _connectionCancelSource?.Dispose();
            _connectionCancelSource = new CancellationTokenSource();

            _connectCompletion = NewCompletionSource<bool>();
            _closeCompletion = NewCompletionSource<string>();

            Action onConnected = null;
            onConnected = () => _connectCompletion.TrySetResult(true);
            OnConnected += onConnected;
            OnClosed += HandleConnectionClosed;

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                _connectionCancelSource.Token,
                Application.exitCancellationToken,
                cancellationToken
            );
            using var reg = linkedCts.Token.Register(() =>
            {
                _connectCompletion.TrySetCanceled();
                _closeCompletion.TrySetResult("Canceled");
                CloseCore();
            });

            ConnectSync(url);
            try
            {
                await AwaitFrom(_connectCompletion);
            }
            finally
            {
                OnConnected -= onConnected;
            }
        }

        /// <summary>
        ///     等待当前连接关闭。
        /// </summary>
#if UNITY_6000_0_OR_NEWER
        public Awaitable<string> WaitClosedAsync()
#else
        public UniTask<string> WaitClosedAsync()
#endif
        {
            return _closeCompletion == null
                ? CompletedAwaitable<string>(null)
                : WaitClosedCoreAsync();
        }

#if UNITY_6000_0_OR_NEWER
        private async Awaitable<string> WaitClosedCoreAsync()
#else
        private async UniTask<string> WaitClosedCoreAsync()
#endif
        {
            try
            {
                return await AwaitFrom(_closeCompletion);
            }
            finally
            {
                OnClosed -= HandleConnectionClosed;
            }
        }

        private void HandleConnectionClosed(string errMsg)
        {
            _closeCompletion?.TrySetResult(errMsg);
            CloseCore();
        }

#if UNITY_6000_0_OR_NEWER
        private static Awaitable<T> CompletedAwaitable<T>(T value)
        {
            var tcs = NewCompletionSource<T>();
            tcs.SetResult(value);
            return AwaitFrom(tcs);
        }
#else
        private static UniTask<T> CompletedAwaitable<T>(T value) =>
            UniTask.FromResult(value);
#endif

        /// <summary>
        ///     执行System调用。
        ///     如果不await，用`CallSystem().Forget()`或直接`_ = CallSystem()`这种射后不管模式，
        ///     调用会立即返回，并且后台发送依然会按顺序进行。
        ///     如果await此方法，调用会等待服务器回应，默认返回"ok"，除非有使用ResponseToClient。
        ///     另可通过`HeTuClient.Instance.SystemLocalCallbacks["system_name"] = (args) => {}`
        ///     注册客户端对应逻辑，每次CallSystem调用时也都会先执行这些回调，这样一些本地逻辑可以放在客户端回调里。
        /// </summary>
#if UNITY_6000_0_OR_NEWER
        public async Awaitable<JsonObject> CallSystem(string systemName,
            params object[] args)
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

            CallSystemSync(systemName, args, (response, outcome, code) =>
            {
                switch (outcome)
                {
                    case CallOutcome.Canceled:
                        Logger.Instance.Error("CallSystem过程中遇到取消信号");
                        tcs.TrySetCanceled();
                        break;
                    case CallOutcome.Rejected:
                        tcs.TrySetException(
                            new HeTuCallRejectedException(systemName, code));
                        break;
                    default:
                        tcs.TrySetResult(response);
                        break;
                }
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
        ///     订阅组件的行数据。查找`index`属性值==`value`的第一行数据，然后按该行RowID订阅。
        ///     如果没有查询到行，会返回`null`。按RowID订阅的快速模式，不处理索引值的变化。
        ///     如果想要订阅不存在的行，请用`WatchRange`订阅索引。
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
        /// var subscription = await HeTuClient.Instance.WatchRow("HP", "owner", user_id);
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
        /// var subscription = await HeTuClient.Instance.WatchRow<HP>("owner", user_id);
        /// Debug.log("My HP:" + subscription.Data.value);
        /// subscription.Dispose(); // 反订阅
        /// // 或使用AddTo和gameObject生命周期绑定
        /// // subscription.AddTo(gameObject);
        /// ]]>
        /// </code>
        [MustDisposeResource]
#if UNITY_6000_0_OR_NEWER
        public async Awaitable<RowSubscription<T>> WatchRow<T>(
#else
        public async UniTask<RowSubscription<T>> WatchRow<T>(
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
            WatchRowSync<T>(index, value, (rowSub, cancel, ex) =>
            {
                if (cancel)
                {
                    Logger.Instance.Error("订阅数据过程中遇到取消信号");
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
        ///     同 WatchRow<T>，但使用默认字典类型。
        /// </summary>
        /// <param name="componentName">组件名。</param>
        /// <param name="index">索引字段名。</param>
        /// <param name="value">索引值。</param>
        /// <returns>查询到时返回行订阅；未命中时返回 <see langword="null" />。</returns>
        [MustDisposeResource]
#if UNITY_6000_0_OR_NEWER
        public async Awaitable<RowSubscription<DictComponent>> WatchRow(
#else
        public async UniTask<RowSubscription<DictComponent>> WatchRow(
#endif
            string componentName, string index, object value)
        {
            return await WatchRow<DictComponent>(index, value, componentName);
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
        public async Awaitable<IndexSubscription<T>> WatchRange<T>(
#else
        public async UniTask<IndexSubscription<T>> WatchRange<T>(
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

            WatchRangeSync<T>(
                index, left, right, limit,
                (idxSub, cancel, ex) =>
                {
                    if (cancel)
                    {
                        Logger.Instance.Error("订阅数据过程中遇到取消信号");
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
        public async Awaitable<IndexSubscription<DictComponent>> WatchRange(
#else
        public async UniTask<IndexSubscription<DictComponent>> WatchRange(
#endif
            string componentName, string index, object left, object right, int limit,
            bool desc = false, bool force = true)
        {
            return await WatchRange<DictComponent>(index, left, right, limit, desc, force,
                componentName);
        }
    }
}
