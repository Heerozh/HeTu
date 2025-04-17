// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Unity库</summary>

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityWebSocket;
using System.Threading;
using Cysharp.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using UnityEngine.Scripting;

namespace HeTu
{
    public interface IBaseComponent
    {
        public long id { get; }
    }

    [Preserve]
    public class DictComponent : Dictionary<string, string>, IBaseComponent
    {
        [Preserve] // strip会导致Unable to find a default constructor to use for type [0].id
        public long id => long.Parse(this["id"]);
    }

    public abstract class BaseSubscription
    {
        public readonly string ComponentName;
        readonly string _subscriptID;

        protected BaseSubscription(string subscriptID, string componentName)
        {
            _subscriptID = subscriptID;
            ComponentName = componentName;
        }

        public abstract void Update(long rowID, JObject data);
        
        /// <summary>
        /// 销毁远端订阅对象。
        /// Dispose应该明确调用，虽然gc回收时会调用，但时间不确定，这会导致服务器端该对象销毁不及时。
        /// </summary>
        public void Dispose()
        {
            HeTuClient.Instance._unsubscribe(_subscriptID, "Dispose");
        }

        ~BaseSubscription()
        {
            HeTuClient.Instance._unsubscribe(_subscriptID, "析构");
        }
    }

    /// Select结果的订阅对象
    public class RowSubscription<T> : BaseSubscription where T : IBaseComponent
    {
        public T Data { get; private set; }

        public RowSubscription(string subscriptID, string componentName, T row) :
            base(subscriptID, componentName)
        {
            Data = row;
        }

        public event Action<RowSubscription<T>> OnUpdate;
        public event Action<RowSubscription<T>> OnDelete;

        public override void Update(long rowID, JObject data)
        {
            if (data is null)
            {
                OnDelete?.Invoke(this);
                Data = default;
            }
            else
            {
                Data = data.ToObject<T>();
                OnUpdate?.Invoke(this);
            }
        }
    }

    /// Query结果的订阅对象
    public class IndexSubscription<T> : BaseSubscription where T : IBaseComponent
    {
        public Dictionary<long, T> Rows { get; private set; }

        public IndexSubscription(string subscriptID, string componentName, List<T> rows) :
            base(subscriptID, componentName)
        {
            Rows = rows.ToDictionary(row => row.id);
        }

        public event Action<IndexSubscription<T>, long> OnUpdate;
        public event Action<IndexSubscription<T>, long> OnDelete;
        public event Action<IndexSubscription<T>, long> OnInsert;

        public override void Update(long rowID, JObject data)
        {
            var exist = Rows.ContainsKey(rowID);
            var delete = data is null;

            if (delete)
            {
                if (!exist) return;
                OnDelete?.Invoke(this, rowID);
                Rows.Remove(rowID);
            }
            else
            {
                var tData = data.ToObject<T>();
                Rows[rowID] = tData;
                if (exist)
                    OnUpdate?.Invoke(this, rowID);
                else
                    OnInsert?.Invoke(this, rowID);
            }
        }
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
        readonly ConcurrentQueue<UniTaskCompletionSource<List<object>>> _waitingSubTasks = new();
        Dictionary<string, WeakReference> _subscriptions = new();
        IWebSocket _socket;
        LogFunction _logError;
        LogFunction _logInfo;
        LogFunction _logDebug;
        IProtocol _protocol = null;

        void _StopAllTcs()
        {
            _logInfo?.Invoke("[HeTuClient] 取消所有等待任务...");
            foreach (var tcs in _waitingSubTasks)
                tcs.TrySetCanceled();
            _waitingSubTasks.Clear();
        }
        // -----------------------------------

        public static HeTuClient Instance => Lazy.Value;

        // 连接成功时的回调
        public event Action OnConnected;

        // 收到System返回的`ResponseToClient`时的回调，根据你服务器发送的是什么数据类型来转换
        // 比如服务器发送的是字典，可以用JObject.ToObject<Dictionary<string, object>>();
        public event Action<JObject> OnResponse;

        // 本地调用System时的回调。调用时立即就会回调，是否成功调用未知。
        public Dictionary<string, Action<object[]>> SystemCallbacks = new();

        // 设置日志函数，info为信息日志，err为错误日志。可以直接传入Unity的Debug.Log和Debug.LogError
        public void SetLogger(LogFunction info, LogFunction err, LogFunction dbg = null)
        {
            _logInfo = info;
            _logError = err;
            _logDebug = dbg;
        }

        // 设置封包的编码/解码协议，封包可以进行压缩和加密。默认不加密，使用zlib压缩。
        // 协议要和你的河图服务器中的配置一致
        public void SetProtocol(IProtocol protocol)
        {
            _protocol = protocol;
        }

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
        public async UniTask<Exception> Connect(string url, CancellationToken? token)
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
            var tcs = new UniTaskCompletionSource<Exception>();
            var lastState = "ReadyForConnect";
            _socket = new WebSocket(url);
            _socket.OnOpen += (object sender, OpenEventArgs e) =>
            {
                _logInfo?.Invoke("[HeTuClient] 连接成功。");
                lastState = "Connected";
                OnConnected?.Invoke();
                foreach (var data in _sendingQueue)
                    _socket.SendAsync(data);
                _sendingQueue.Clear();
            };
            _socket.OnMessage += (object sender, MessageEventArgs e) => { _OnReceived(e.RawData); };
            _socket.OnClose += (object sender, CloseEventArgs e) =>
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
            _socket.OnError += (object sender, ErrorEventArgs e) =>
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

            // token可取消等待
            token?.Register(() =>
            {
                _logInfo?.Invoke($"[HeTuClient] 连接断开，收到了CancellationToken取消请求.");
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
        /// 后台发送System调用，此方法立即返回。
        /// 可通过`HeTuClient.Instance.SystemCallbacks["system_name"] = (args) => {}`
        /// 注册客户端调用回调（非服务器端回调）。
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
        /// 订阅组件的行数据。订阅`where`属性值==`value`的第一行数据。
        /// `Select`只对“单行”订阅，如果没有查询到行，会返回`null`。
        /// 如果想要订阅不存在的行，请用`Query`订阅索引。
        /// </summary>
        /// <returns>
        /// 返回`null`如果没查询到行，否则返回`RowSubscription`对象。
        /// 可通过`RowSubscription.Data`获取数据。
        /// 可以注册`RowSubscription.OnUpdate`和`OnDelete`事件处理数据更新。
        /// </returns>
        /// <remarks>
        /// 可使用`T`模板参数定义数据类型，不写就是默认`Dictionary{string, string}`类型。
        /// 使用`T`模板时，对象定义要和服务器定义一致，可使用服务器端工具自动生成c#定义。
        /// 使用默认的Dictionary更自由灵活，但都是字符串类型需要自行转换。
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
        public async UniTask<RowSubscription<T>> Select<T>(
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
            _logDebug?.Invoke($"[HeTuClient] 发送Select订阅: {componentName}.{where}[{value}:]");

            // 等待服务器结果
            var tcs = new UniTaskCompletionSource<List<object>>();
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

        public UniTask<RowSubscription<DictComponent>> Select(
            string componentName, object value, string where = "id")
        {
            return Select<DictComponent>(value, where, componentName);
        }

        /// <summary>
        /// 订阅组件的索引数据。`index`是开启了索引的属性名，`left`和`right`为索引范围，
        /// `limit`为返回数量，`desc`为是否降序，`force`为未查询到数据时是否也强制订阅。
        /// </summary>
        /// <returns>
        /// 返回`IndexSubscription`对象。
        /// 可通过`IndexSubscription.Rows`获取数据。
        /// 并可以注册`IndexSubscription.OnInsert`和`OnUpdate`，`OnDelete`数据事件。
        /// </returns>
        /// <remarks>
        /// 可使用`T`模板参数定义数据类型，不写就是默认`Dictionary{string, string}`类型。
        /// 使用`T`模板时，对象定义要和服务器定义一致，可使用服务器端工具自动生成c#定义。
        /// 使用默认的Dictionary更自由灵活，但都是字符串类型需要自行转换。
        ///
        /// 如果目标组件权限为Owner，则只能查询到`owner`属性==自己的行。
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
        public async UniTask<IndexSubscription<T>> Query<T>(
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
            var tcs = new UniTaskCompletionSource<List<object>>();
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

        public UniTask<IndexSubscription<DictComponent>> Query(
            string componentName, string index, object left, object right, int limit,
            bool desc = false, bool force = true)
        {
            return Query<DictComponent>(index, left, right, limit, desc, force, componentName);
        }

        // --------------以下为内部方法----------------

        internal void _unsubscribe(string subID, string from)
        {
            if (!_subscriptions.ContainsKey(subID)) return;
            _subscriptions.Remove(subID);
            var payload = new object[] { "unsub", subID };
            _Send(payload);
            _logInfo?.Invoke($"[HeTuClient] 因BaseSubscription {from}，已取消订阅 {subID}");
        }

        static string _makeSubID(string table, string index, object left, object right,
            int limit, bool desc)
        {
            return $"{table}.{index}[{left}:{right ?? "None"}:{(desc ? -1 : 1)}][:{limit}]";
        }

        void _Send(object payload)
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
                _logInfo?.Invoke("尝试发送数据但连接未建立，将加入队列在建立后发送。");
                _sendingQueue.Enqueue(buffer);
            }
        }

        void _OnReceived(byte[] buffer)
        {
            // 解码消息
            buffer = _protocol?.Decrypt(buffer) ?? buffer;
            buffer = _protocol?.Decompress(buffer) ?? buffer;
            var decoded = Encoding.UTF8.GetString(buffer);
            // 处理消息
            // _logInfo?.Invoke($"[HeTuClient] 收到消息: {decoded}");
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
                    var rows = ((JObject)structuredMsg[2]).ToObject<Dictionary<long, JObject>>();
                    foreach (var (rowID, data) in rows)
                        subscribed.Update(rowID, data);
                    break;
            }
        }
    }
}
