// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的订阅库</summary>


using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using JetBrains.Annotations;
using R3;
#if UNITY_2022_3_OR_NEWER
using UnityEngine;
using UnityEngine.Scripting;
#endif

namespace HeTu
{
    /// <summary>
    ///     组件数据的基础接口。
    /// </summary>
    public interface IBaseComponent
    {
        /// <summary>
        ///     行主键 ID。
        /// </summary>
        public long ID { get; }
    }

    /// <summary>
    ///     动态字典组件类型。
    /// </summary>
    public class DictComponent : Dictionary<string, object>, IBaseComponent
    {
#if UNITY_2022_3_OR_NEWER
        [Preserve] // strip会导致Unable to find a default constructor to use for type [0].id
#endif
        public long ID => Convert.ToInt64(this["id"]);
    }

    internal interface IRestorableSubscription
    {
        void Suspend();

        void Restore(
            IHeTuSessionTransport transport,
            Action<bool> onCompleted,
            Action<Exception> onFailed);
    }

    /// <summary>
    ///     订阅句柄基类，封装反订阅与资源释放逻辑。
    ///     每次 Watch 都拿到一个自己的句柄；同一个订阅（同一行、同一范围）的所有句柄共用一份
    ///     服务端订阅和数据，各自 Dispose 只退掉自己，最后一个句柄 Dispose 时才去服务器反订阅。
    ///     Every Watch call returns its own handle. Handles of the same subscription
    ///     share one server subscription and its data; disposing a handle only releases
    ///     that handle, and the server subscription is dropped with the last one.
    /// </summary>
    /// <remarks>
    ///     订阅推送是尽力而为的最终一致：正常负载下约 99% 的情况能收到最新数据（通常在
    ///     变更后 100~200ms 内）；服务端 Redis 压力过大（副本复制延迟超过约 100ms）时，
    ///     可能残留旧数据，直到该行下次变更。订阅数据适合用来显示，扣钱、发奖、校验这类
    ///     需要强一致的判断请交给服务端 System。
    ///     Subscriptions are best-effort and eventually consistent: under normal load
    ///     you get the latest data about 99% of the time (usually within 100-200 ms of
    ///     the change); when the server's Redis is overloaded (replica lag above
    ///     ~100 ms), stale data may remain until the row changes again. Use subscription
    ///     data for display and leave strongly consistent decisions (spending currency,
    ///     granting rewards, validation) to a server System.
    /// </remarks>
    [MustDisposeResource]
    public abstract class BaseSubscription : IDisposable
    {
        /// <summary>
        ///     对应的组件名。
        /// </summary>
        public readonly string ComponentName;

        /// <summary>
        ///     该订阅关联的 R3 资源袋。
        /// </summary>
        public DisposableBag DisposeBag;

        private protected BaseSubscription(ManagedSubscriptionCore core,
            StackTrace creationTrace)
        {
            Core = core;
            ComponentName = core.ComponentName;
            CreationTrace = creationTrace;
        }

        // 同一订阅的所有句柄共用的那一份
        internal ManagedSubscriptionCore Core { get; }

        internal StackTrace CreationTrace { get; }

        /// <summary>
        ///     服务器侧 canonical 订阅 ID。
        /// </summary>
        public string SubId => Core.SubId;

        /// <summary>
        ///     当前对象是否已脱离活跃连接，等待 Session 重连恢复。Dispose 后恒为 false，
        ///     生命周期判断请用 IsDisposed。
        /// </summary>
        public bool IsStale => !IsDisposed && Core.IsStale;

        internal bool IsDisposed { get; private set; }

        /// <summary>
        ///     释放本句柄。Dispose应该明确调用。同一订阅的其他句柄不受影响；最后一个句柄
        ///     Dispose 时才销毁远端订阅对象。
        /// </summary>
        public virtual void Dispose()
        {
            if (IsDisposed)
                return;

            IsDisposed = true;
            DisposeBag.Dispose();
            Core.Release(this);
            GC.SuppressFinalize(this);
        }

#if R3_UNITY_INSTALLED
        /// <summary>
        ///     把HeTu数据订阅的生命周期和GameObject绑定，在GameObject.Destroy时自动对数据订阅Dispose。
        ///     Dispose负责去HeTu服务器反订阅，并清理后续的所有R3响应式Subscribe。
        ///     注：任意IDisposable对象都可以使用AddTo方法。本方法只是为了告知Rider，
        ///     使用后不再需要警告未Dispose的资源泄漏问题。
        /// </summary>
        [HandlesResourceDisposal]
        public BaseSubscription AddTo(GameObject gameObject) =>
            MonoBehaviourExtensions.AddTo(this, gameObject);
#endif

        /// <summary>
        ///     应用服务器推送的行更新（同一订阅的所有句柄都会收到）。
        /// </summary>
        /// <param name="data">更新数据。</param>
        public abstract void UpdateRows(JsonObject data);

        ~BaseSubscription() =>
            Logger.Instance.Error(
                "检测到资源泄漏！订阅被 GC 回收但未调用 .Dispose() 方法！订阅ID：" + SubId +
                "\n创建时的堆栈：\n" + CreationTrace);
    }

    /// <summary>
    ///     单行订阅句柄（Get 结果）。
    /// </summary>
    /// <typeparam name="T">组件类型。</typeparam>
    public class RowSubscription<T> : BaseSubscription where T : IBaseComponent
    {
        private readonly ManagedRowCore<T> _core;
        private readonly Subject<T> _subject;

        public RowSubscription(string subscriptID, string componentName, T row,
            HeTuClientBase client, StackTrace creationTrace = null) :
            this(new ManagedRowCore<T>(subscriptID, componentName, row, client,
                creationTrace), creationTrace)
        {
        }

        internal RowSubscription(ManagedRowCore<T> core, StackTrace creationTrace) :
            base(core, creationTrace)
        {
            _core = core;
            _subject = new Subject<T>();
            DisposeBag.Add(_subject);
            core.Attach(this);
        }

        /// <summary>
        ///     最近一次收到的行 ID（删除后保留删除前的值）。
        /// </summary>
        public long LastRowID => _core.LastRowID;

        /// <summary>
        ///     当前缓存的行数据。
        /// </summary>
        public T Data => _core.Data;

        /// <summary>
        ///     该订阅最终绑定的行 ID。WatchRow 解析成功后，重连恢复按此 ID 进行。
        /// </summary>
        public long BoundRowID => _core.BoundRowID;

        /// <summary>
        ///     获得 RowSubscription 的 Subject 响应式热源，自动处理 OnUpdate 和 OnDelete 事件。
        ///     用法：
        ///     // HeTu数据订阅
        ///     <![CDATA[
        ///     RowSubscription<HP> hpSub = client.WatchRow<HP>("owner", 123);
        ///     // 在gameObject销毁时反订阅HeTu数据，或手动调用hpSub.Dispose()，不然服务器永远发送订阅消息。
        ///     hpSub.AddTo(gameObject);
        ///     ]]>
        ///     // 逻辑：当 hp 变化 -> 转换成字符串 -> 赋值给 Text 组件
        ///     hpSub.Subject.Select(x => x.ID != 0 ? $"HP: {x.value}" : "Dead")
        ///     .SubscribeToText(textBox) // R3 特有的 Unity 扩展，自动处理赋值
        ///     .AddTo(ref hpSub.DisposeBag); // .Subscribe的生命周期和订阅源头绑定
        ///     // 虽然.Subscribe返回的Observer对象都有AutoDisposeOnCompleted标记
        ///     // 当热源hpSub Dispose时，会调用OnCompleted，自动Dispose所有订阅Node
        ///     // 但.Subscribe后自己负责Dispose是最佳实践
        /// </summary>
        public Observable<T> Subject => _subject.Prepend(Data);

        /// <summary>
        ///     行更新事件。
        /// </summary>
        public event Action<RowSubscription<T>> OnUpdate;

        /// <summary>
        ///     行删除事件。
        /// </summary>
        public event Action<RowSubscription<T>> OnDelete;

        /// <summary>
        ///     Session 重连后完成重新同步时触发。
        /// </summary>
        public event Action OnResynced;

        /// <summary>
        ///     更新当前行数据并触发相关事件（同一订阅的所有句柄都会收到）。
        /// </summary>
        /// <param name="data">新数据；为 <see langword="null" /> 表示删除。</param>
        public void Update(T data) => _core.Update(data);

        public override void UpdateRows(JsonObject data) => _core.UpdateRows(data);

        internal void Rebind(
            string subscriptID,
            T row,
            HeTuClientBase client) =>
            _core.Rebind(subscriptID, row, client);

        // 以下由 ManagedRowCore 对每个还活着的句柄调用
        internal void RaiseUpdate() => OnUpdate?.Invoke(this);

        internal void RaiseDelete() => OnDelete?.Invoke(this);

        internal void PushToSubject(T row) => _subject.OnNext(row);

        internal void RaiseResynced() => OnResynced?.Invoke();
    }

    /// <summary>
    ///     范围订阅句柄（Range 结果）。
    /// </summary>
    /// <typeparam name="T">组件类型。</typeparam>
    public class IndexSubscription<T> : BaseSubscription where T : IBaseComponent
    {
        private readonly Subject<T> _addSubject;
        private readonly ManagedIndexCore<T> _core;
        private readonly Subject<long> _removeSubject;
        private readonly Dictionary<long, Subject<T>> _replaceSubjects;

        public IndexSubscription(string subscriptID, string componentName, List<T> rows,
            HeTuClientBase client, StackTrace creationTrace = null) :
            this(new ManagedIndexCore<T>(subscriptID, componentName, rows, client,
                creationTrace), creationTrace)
        {
        }

        internal IndexSubscription(ManagedIndexCore<T> core, StackTrace creationTrace) :
            base(core, creationTrace)
        {
            _core = core;
            _addSubject = new Subject<T>();
            _removeSubject = new Subject<long>();
            _replaceSubjects = new Dictionary<long, Subject<T>>();
            DisposeBag.Add(_addSubject);
            DisposeBag.Add(_removeSubject);
            // per-row subject 不进 DisposeBag——R3 的 DisposableBag 只能追加，
            // 删行的 churn 会让 bag 单调增长。改成跟着 _replaceSubjects 字典走，
            // 删除时显式 Dispose，整体 Dispose 在 override 里兜底。
            foreach (var id in core.Rows.Keys)
                _replaceSubjects[id] = new Subject<T>();
            core.Attach(this);
        }

        /// <summary>
        ///     当前订阅范围内的行集合（key 为行 ID）。同一订阅的所有句柄共用这一份。
        /// </summary>
        public Dictionary<long, T> Rows => _core.Rows;

        /// <summary>
        ///     为 true 表示这是 WatchTable 建立的整表订阅，断线重连时重发 table 而非 range。
        /// </summary>
        internal bool RestoreAsTable => _core.RestoreAsTable;

        public override void Dispose()
        {
            if (IsDisposed) return;
            foreach (var subject in _replaceSubjects.Values)
                subject.Dispose();
            _replaceSubjects.Clear();
            base.Dispose();
        }

        /// <summary>
        ///     行更新事件。
        /// </summary>
        public event Action<IndexSubscription<T>, long> OnUpdate;

        /// <summary>
        ///     行删除事件。
        /// </summary>
        public event Action<IndexSubscription<T>, long> OnDelete;

        /// <summary>
        ///     行新增事件。
        /// </summary>
        public event Action<IndexSubscription<T>, long> OnInsert;

        /// <summary>
        ///     Session 重连后完成重新同步时触发。
        /// </summary>
        public event Action OnResynced;

        /// <summary>
        ///     对指定行应用新增/更新/删除变更（同一订阅的所有句柄都会收到）。
        /// </summary>
        /// <param name="rowID">目标行 ID。</param>
        /// <param name="data">新数据；为 <see langword="null" /> 表示删除。</param>
        public void Update(long rowID, T data) => _core.Update(rowID, data);

        public override void UpdateRows(JsonObject data) => _core.UpdateRows(data);

        /// <summary>
        ///     获得范围订阅的新增热源，初始行也会先依次发出。
        ///     你需要在本ObserveAdd事件中，通过ObserveRow订阅新增数据，不然只能收到1次初始值。
        ///     推荐和 <see cref="ObserveRemove" /> 配对使用。
        ///     用法：
        ///     <![CDATA[
        ///     var uiItems = new Dictionary<long, GameObject>();
        ///     IndexSubscription<HP> indexSub = client.WatchRange<HP>(...);
        ///     indexSub.AddTo(gameObject);
        ///
        ///     // 创建新UI对象
        ///     Action<long> createUI = rowID => {
        ///         var go = new GameObject($"ID: {rowID}");
        ///         uiItems[rowID] = go;
        ///
        ///         var hpText = go.AddComponent<TMP_Text>();
        ///         // bind 更新
        ///         indexSub.ObserveRow(rowID)
        ///             .Select(x => $"{x.value}")
        ///             .Subscribe(
        ///                 value => hpText.text = value // OnNext事件
        ///             )
        ///             //.AddTo(go);  // IndexSubscription自己会负责
        ///             // 注意，规范上来说，AddTo应该能加就加，不管别人是否会处理，重复添加一般
        ///             // 无副作用。但这里不是，这里是ObserveAdd事件，导致Bag会持续增长，内存溢出。
        ///     };
        ///
        ///     // 统一处理新增（包含初始行）
        ///     indexSub.ObserveAdd()
        ///         .Subscribe(added => { createUI(added.ID); })
        ///         .AddTo(ref indexSub.DisposeBag);
        ///
        ///     // 统一处理删除
        ///     indexSub.ObserveRemove()
        ///         .Subscribe(removedID => {
        ///             if (!uiItems.TryGetValue(removedID, out var go)) return;
        ///             Object.Destroy(go);
        ///             uiItems.Remove(removedID);
        ///         })
        ///         .AddTo(ref indexSub.DisposeBag);
        ///     ]]>
        /// </summary>
        public Observable<T> ObserveAdd() =>
            // Concat会把前一个订阅源中的OnCompleted事件屏蔽然后自动切换下一个订阅
            Observable.Defer(() => Rows.Values.ToObservable().Concat(_addSubject));

        /// <summary>
        ///     监听范围内行删除事件，返回被删除的行 ID。
        /// </summary>
        public Observable<long> ObserveRemove() => _removeSubject;

        /// <summary>
        ///     监听指定行的数据更新流。
        /// </summary>
        /// <param name="rowID">目标行 ID。</param>
        /// <returns>该行的更新流；删除时会完成（OnCompleted）。</returns>
        public Observable<T> ObserveRow(long rowID) => _replaceSubjects[rowID];

        internal void Rebind(
            string subscriptID,
            List<T> rows,
            HeTuClientBase client) =>
            _core.Rebind(subscriptID, rows, client);

        // 以下由 ManagedIndexCore 对每个还活着的句柄调用
        internal void RaiseInsert(long rowID, T data)
        {
            // 先建好这行的 subject，OnInsert 里就能 ObserveRow
            if (_replaceSubjects.Remove(rowID, out var stale))
                stale.Dispose();
            _replaceSubjects[rowID] = new Subject<T>();
            OnInsert?.Invoke(this, rowID);
            _addSubject.OnNext(data);
        }

        internal void RaiseUpdate(long rowID, T data)
        {
            OnUpdate?.Invoke(this, rowID);
            if (_replaceSubjects.TryGetValue(rowID, out var subject))
                subject.OnNext(data);
        }

        // 删除分两步：这一步行还在 Rows 里；之后核心删掉行，再 PushRemove。
        // OnDelete 里还能 ObserveRow 这一行，之后它的流才完成
        internal void RaiseDelete(long rowID)
        {
            try
            {
                OnDelete?.Invoke(this, rowID);
            }
            finally
            {
                if (_replaceSubjects.Remove(rowID, out var subject))
                {
                    subject.OnCompleted();
                    subject.Dispose();
                }
            }
        }

        internal void PushRemove(long rowID) => _removeSubject.OnNext(rowID);

        internal void RaiseResynced() => OnResynced?.Invoke();
    }

    /// <summary>
    ///     同一个订阅（sub_id）的所有句柄共用的那一份：服务端订阅、数据、断线恢复状态。
    ///     每次 Watch 从这里发一个句柄（<see cref="BaseSubscription" />），句柄 Dispose 时
    ///     交回来；最后一个句柄交回时才去服务器反订阅。服务端对同一连接的重复订阅回同一个
    ///     sub_id、不计数，一条 unsub 就删，所以计数只能在客户端做。
    /// </summary>
    internal abstract class ManagedSubscriptionCore : IRestorableSubscription
    {
        private readonly List<BaseSubscription> _handles = new();
        private HeTuClientBase _parentClient;

        protected ManagedSubscriptionCore(string subscriptID, string componentName,
            HeTuClientBase client, StackTrace creationTrace)
        {
            SubId = subscriptID;
            ComponentName = componentName;
            _parentClient = client;
            CreationTrace = creationTrace;
        }

        public string SubId { get; }
        public string ComponentName { get; }

        // 第一次订阅时的调用栈，Inspector 用它标注推送
        public StackTrace CreationTrace { get; }

        public bool IsStale { get; private set; }

        // 远端订阅已退（最后一个句柄已 Dispose，或会话关闭）
        public bool IsClosed { get; private set; }

        public int HandleCount => _handles.Count;

        // 远端订阅已退（Close 末尾触发），Session 据此注销
        internal event Action<ManagedSubscriptionCore> Closed;

        public abstract void UpdateRows(JsonObject data);

        // 发一个新句柄（类型由具体核心决定）
        internal abstract BaseSubscription NewHandle(StackTrace creationTrace);

        internal void Attach(BaseSubscription handle) => _handles.Add(handle);

        internal void Release(BaseSubscription handle)
        {
            if (!_handles.Remove(handle) || _handles.Count > 0)
                return;
            Close();
        }

        // 退掉远端订阅并 Dispose 所有句柄。最后一个句柄交回时调用；会话关闭时也直接调。
        internal void Close()
        {
            if (IsClosed)
                return;

            IsClosed = true;
            // Stale 表示底层连接已经断了（Session 在 MarkConnectionLost 里挨个
            // Suspend），此时再向 _parentClient 发 unsub 没意义：底层
            // HeTuClient 通常已经 Subscriptions.Clean()，能早返；即使顺序变了
            // 守住这里也不会戳到死 socket。
            if (!IsStale)
                _parentClient?.Unsubscribe(this);
            IsStale = false;
            foreach (var handle in _handles.ToArray())
                handle.Dispose();
            _handles.Clear();
            Closed?.Invoke(this);
        }

        public void Suspend() => IsStale = true;

        // 会话重连后按原来的订阅意图重新订阅，回复绑回本核心（句柄不变）
        public abstract void Restore(
            IHeTuSessionTransport transport,
            Action<bool> onCompleted,
            Action<Exception> onFailed);

        protected void RebindRemote(string subscriptID, HeTuClientBase client)
        {
            if (SubId != subscriptID)
                throw new Exception("错误，Rebind的subscriptID不应该有变化，" +
                                    $"旧的：{SubId}, 新的：{subscriptID}");
            _parentClient = client;
            IsStale = false;
        }

        // 对每个还活着的句柄通知一次。某个句柄的用户回调抛异常只记日志，不能挡住
        // 别的句柄（它们可能属于完全无关的界面）。
        protected void ForEachHandle<THandle>(Action<THandle> notify)
            where THandle : BaseSubscription
        {
            foreach (var handle in _handles.ToArray())
            {
                if (handle.IsDisposed)
                    continue;
                try
                {
                    notify((THandle)handle);
                }
                catch (Exception ex)
                {
                    Logger.Instance.Error($"订阅 {SubId} 的回调抛出异常：{ex}");
                }
            }
        }
    }

    /// <summary>
    ///     单行订阅的共享核心，句柄是 <see cref="RowSubscription{T}" />。
    /// </summary>
    internal sealed class ManagedRowCore<T> : ManagedSubscriptionCore
        where T : IBaseComponent
    {
        public ManagedRowCore(string subscriptID, string componentName, T row,
            HeTuClientBase client, StackTrace creationTrace) :
            base(subscriptID, componentName, client, creationTrace)
        {
            Data = row;
            BoundRowID = row.ID;
            LastRowID = row.ID;
        }

        public T Data { get; private set; }
        public long BoundRowID { get; private set; }
        public long LastRowID { get; private set; }

        public RowSubscription<T> CreateHandle(StackTrace creationTrace) =>
            new(this, creationTrace);

        internal override BaseSubscription NewHandle(StackTrace creationTrace) =>
            CreateHandle(creationTrace);

        public void Update(T data)
        {
            // 防御 Restore 异步回调晚于 Dispose 到达："僵尸"更新会戳到已 Dispose
            // 的 subject，R3 直接抛 ObjectDisposedException。
            if (IsClosed) return;
            if (data is null)
            {
                // OnDelete 里还能读到删除前的 Data
                ForEachHandle<RowSubscription<T>>(h => h.RaiseDelete());
                Data = default;
                // 不应该_subject.OnCompleted，如果订阅的查询条件不是RowId，那么数据可能会重新出现
                // 如果是订阅RowId，那么数据不会重新出现，所以可以OnCompleted
                // 但为了统一，我们始终不调用OnCompleted
            }
            else
            {
                Data = data;
                LastRowID = data.ID;
                ForEachHandle<RowSubscription<T>>(h => h.RaiseUpdate());
            }

            var current = Data;
            ForEachHandle<RowSubscription<T>>(h => h.PushToSubject(current));
        }

        public override void UpdateRows(JsonObject data)
        {
            var rows = data.ToDict<long, T>();
            foreach (var (_, rowData) in rows)
            {
                Update(rowData);
            }
        }

        public void Rebind(
            string subscriptID,
            T row,
            HeTuClientBase client)
        {
            // 已关闭直接返回：避免 RebindRemote 把对象表面上"激活"
            //（IsStale = false, _parentClient = 新 client），以及避免后续 Update
            //  戳到已被 DisposeBag 销毁的 subject。
            if (IsClosed) return;
            RebindRemote(subscriptID, client);
            BoundRowID = row.ID;
            // 重连后 always-fire：T 通常是用户类没实现 Equals，引用比较恒不等；
            // 与其要求实现 Equals 或按字节比较，不如统一回放给响应式链路，
            // 保证断线期间的变化不会漏给 Subject/OnUpdate 订阅方。
            Update(row);
            ForEachHandle<RowSubscription<T>>(h => h.RaiseResynced());
        }

        public override void Restore(
            IHeTuSessionTransport transport,
            Action<bool> onCompleted,
            Action<Exception> onFailed) =>
            transport.WatchRow(
                HeTuClientBase.IndexId,
                BoundRowID,
                (core, canceled, exception) =>
                {
                    // canceled 代表底层 ResponseQueue.CancelAll 把这条迟到的回调冲掉了，
                    // 一般发生在新一轮 Connect 的 ConnectSync 内部。Session 已通过
                    // transport.Closed 事件知道断线并自行调度了重连，再走 onFailed 会
                    // 反过来触发 HandleRecoverableFailure → MarkConnectionLost，把刚
                    // 建出来的新 transport 当场 Dispose 形成死循环。直接吞掉。
                    if (canceled)
                        return;

                    if (exception != null)
                    {
                        onFailed(exception);
                        return;
                    }

                    if (core == null)
                    {
                        Update(default);
                        onCompleted(false);
                        return;
                    }

                    onCompleted(true);
                },
                ComponentName,
                this);
    }

    /// <summary>
    ///     范围 / 整表订阅的共享核心，句柄是 <see cref="IndexSubscription{T}" />。
    /// </summary>
    internal sealed class ManagedIndexCore<T> : ManagedSubscriptionCore
        where T : IBaseComponent
    {
        public ManagedIndexCore(string subscriptID, string componentName, List<T> rows,
            HeTuClientBase client, StackTrace creationTrace) :
            base(subscriptID, componentName, client, creationTrace) =>
            Rows = rows.ToDictionary(row => row.ID);

        public Dictionary<long, T> Rows { get; }

        internal string RestoreIndex { get; private set; }
        internal object RestoreLeft { get; private set; }
        internal object RestoreRight { get; private set; }
        internal int RestoreLimit { get; private set; }
        internal bool RestoreDesc { get; private set; }
        internal bool RestoreForce { get; private set; } = true;

        // 为 true 表示这是 WatchTable 建立的整表订阅，断线重连时重发 table 而非 range
        internal bool RestoreAsTable { get; private set; }

        public IndexSubscription<T> CreateHandle(StackTrace creationTrace) =>
            new(this, creationTrace);

        internal override BaseSubscription NewHandle(StackTrace creationTrace) =>
            CreateHandle(creationTrace);

        public void Update(long rowID, T data)
        {
            // 防御 Restore 异步回调晚于 Dispose 到达
            if (IsClosed) return;
            var exist = Rows.ContainsKey(rowID);

            if (data is null)
            {
                if (!exist) return;
                ForEachHandle<IndexSubscription<T>>(h => h.RaiseDelete(rowID));
                Rows.Remove(rowID);
                ForEachHandle<IndexSubscription<T>>(h => h.PushRemove(rowID));
            }
            else
            {
                Rows[rowID] = data;
                if (exist)
                    ForEachHandle<IndexSubscription<T>>(h => h.RaiseUpdate(rowID, data));
                else
                    ForEachHandle<IndexSubscription<T>>(h => h.RaiseInsert(rowID, data));
            }
        }

        public override void UpdateRows(JsonObject data)
        {
            var rows = data.ToDict<long, T>();
            foreach (var (rowID, rowData) in rows)
            {
                Update(rowID, rowData);
            }
        }

        internal void ConfigureRestoreQuery(
            string index,
            object left,
            object right,
            int limit,
            bool desc,
            bool force)
        {
            RestoreIndex = index;
            RestoreLeft = left;
            RestoreRight = right;
            RestoreLimit = limit;
            RestoreDesc = desc;
            RestoreForce = force;
            RestoreAsTable = false;
        }

        internal void ConfigureRestoreTable()
        {
            RestoreAsTable = true;
        }

        public void Rebind(
            string subscriptID,
            List<T> rows,
            HeTuClientBase client)
        {
            // 同 ManagedRowCore.Rebind：关闭后任何延迟到达的 Restore 回调
            // 都 noop，避免对象被"复活"或戳到已清空的 per-row subject。
            if (IsClosed) return;
            RebindRemote(subscriptID, client);
            ReplaceSnapshot(rows);
            ForEachHandle<IndexSubscription<T>>(h => h.RaiseResynced());
        }

        public override void Restore(
            IHeTuSessionTransport transport,
            Action<bool> onCompleted,
            Action<Exception> onFailed)
        {
            void OnRestored(ManagedIndexCore<T> core, bool canceled,
                Exception exception)
            {
                // 同 ManagedRowCore.Restore：迟到的 cancel 不能再调 onFailed，
                // 否则会级联到 HandleRecoverableFailure 把新 transport 又 Dispose。
                if (canceled)
                    return;

                if (exception != null)
                {
                    onFailed(exception);
                    return;
                }

                onCompleted(core != null);
            }

            if (RestoreAsTable)
            {
                // 整表订阅：重发 table，而不是 range
                transport.WatchTable<T>(OnRestored, ComponentName, this);
                return;
            }

            transport.WatchRange<T>(
                RestoreIndex,
                RestoreLeft,
                RestoreRight,
                RestoreLimit,
                OnRestored,
                RestoreDesc,
                RestoreForce,
                ComponentName,
                this);
        }

        private void ReplaceSnapshot(List<T> rows)
        {
            // 把 diff 全部交给 Update：删除、新增、变更都走同一条事件路径，
            // 重连后 R3 链路（ObserveAdd/ObserveRemove/ObserveRow 以及
            // OnInsert/OnDelete/OnUpdate）就能像断线期间一直在线那样收到完整变更。
            // 已存在的行其 per-row subject 会被复用，订阅方不会断流。
            var nextRows = rows.ToDictionary(row => row.ID);

            foreach (var rowId in Rows.Keys.ToArray())
            {
                if (!nextRows.ContainsKey(rowId))
                    Update(rowId, default);
            }

            foreach (var (rowId, row) in nextRows)
                Update(rowId, row);
        }
    }

    /// <summary>
    ///     订阅强引用管理器（按 sub_id 管理共享核心）。
    /// </summary>
    public class SubscriptionManager
    {
        private readonly Dictionary<string, ManagedSubscriptionCore> _subscriptions =
            new();

        /// <summary>
        ///     清空缓存。
        /// </summary>
        public void Clean() => _subscriptions.Clear();

        internal bool TryGet(string subID, out ManagedSubscriptionCore subscription) =>
            _subscriptions.TryGetValue(subID, out subscription);

        internal void Add(string subID, ManagedSubscriptionCore subscription) =>
            _subscriptions[subID] = subscription;

        /// <summary>
        ///     移除订阅引用。
        /// </summary>
        /// <param name="subID">订阅 ID。</param>
        public void Remove(string subID) => _subscriptions.Remove(subID);

        /// <summary>
        ///     判断是否包含指定订阅。
        /// </summary>
        /// <param name="subID">订阅 ID。</param>
        public bool Contains(string subID) => _subscriptions.ContainsKey(subID);
    }
}
