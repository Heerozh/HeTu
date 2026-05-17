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
    ///     订阅基类，封装反订阅与资源释放逻辑。
    /// </summary>
    [MustDisposeResource]
    public abstract class BaseSubscription : IDisposable, IRestorableSubscription
    {
        private readonly StackTrace _creationTrace;
        private HeTuClientBase _parentClient;
        private readonly string _subscriptID;
        private bool _disposed;

        /// <summary>
        ///     对应的组件名。
        /// </summary>
        public readonly string ComponentName;

        /// <summary>
        ///     该订阅关联的 R3 资源袋。
        /// </summary>
        public DisposableBag DisposeBag;

        internal StackTrace CreationTrace => _creationTrace;

        /// <summary>
        ///     服务器侧 canonical 订阅 ID。
        /// </summary>
        public string SubId => _subscriptID;

        /// <summary>
        ///     当前对象是否已脱离活跃连接，等待 Session 重连恢复。
        /// </summary>
        public bool IsStale { get; private set; }

        internal bool IsDisposed => _disposed;

        internal event Action<BaseSubscription> Disposed;

        protected BaseSubscription(string subscriptID, string componentName,
            HeTuClientBase client, StackTrace creationTrace = null)
        {
            _subscriptID = subscriptID;
            ComponentName = componentName;
            _parentClient = client;
            _creationTrace = creationTrace;
        }

        /// <summary>
        ///     销毁远端订阅对象。Dispose应该明确调用。
        /// </summary>
        public virtual void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            // Stale 表示底层连接已经断了（Session 在 MarkConnectionLost 里挨个
            // Suspend），此时再向 _parentClient 发 unsub 没意义：底层
            // HeTuClient 通常已经 Subscriptions.Clean()，能早返；即使顺序变了
            // 守住这里也不会戳到死 socket。
            if (!IsStale)
                _parentClient?.Unsubscribe(_subscriptID, "Dispose");
            // 复位 IsStale：Dispose 后对象不再属于"等重连"状态，外部只看
            // IsStale 不应被误导。生命周期判断请用 IsDisposed。
            IsStale = false;
            DisposeBag.Dispose();
            Disposed?.Invoke(this);
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
        ///     应用服务器推送的行更新。
        /// </summary>
        /// <param name="data">更新数据。</param>
        public abstract void UpdateRows(JsonObject data);

        void IRestorableSubscription.Suspend() => Suspend();

        void IRestorableSubscription.Restore(
            IHeTuSessionTransport transport,
            Action<bool> onCompleted,
            Action<Exception> onFailed) =>
            Restore(transport, onCompleted, onFailed);

        internal void RebindRemote(string subscriptID, HeTuClientBase client)
        {
            if (_subscriptID != subscriptID)
                throw new Exception("错误，Rebind的subscriptID不应该有变化，" +
                                    $"旧的：{_subscriptID}, 新的：{subscriptID}");
            _parentClient = client;
            IsStale = false;
        }

        internal void Suspend() => IsStale = true;

        internal virtual void Restore(
            IHeTuSessionTransport transport,
            Action<bool> onCompleted,
            Action<Exception> onFailed) =>
            onFailed(new NotSupportedException(
                $"Subscription type '{GetType()}' cannot be restored by a session."));

        ~BaseSubscription() =>
            Logger.Instance.Error(
                "检测到资源泄漏！订阅被 GC 回收但未调用 .Dispose() 方法！订阅ID：" + _subscriptID +
                "\n创建时的堆栈：\n" + _creationTrace);
    }

    /// <summary>
    ///     单行订阅对象（Get 结果）。
    /// </summary>
    /// <typeparam name="T">组件类型。</typeparam>
    public class RowSubscription<T> : BaseSubscription where T : IBaseComponent
    {
        private readonly Subject<T> _subject;

        public long LastRowID;

        public RowSubscription(string subscriptID, string componentName, T row,
            HeTuClientBase client, StackTrace creationTrace = null) :
            base(subscriptID, componentName, client, creationTrace)
        {
            Data = row;
            BoundRowID = row.ID;
            LastRowID = row.ID;
            _subject = new Subject<T>();
            DisposeBag.Add(_subject);
        }

        /// <summary>
        ///     当前缓存的行数据。
        /// </summary>
        public T Data { get; private set; }

        /// <summary>
        ///     该订阅最终绑定的行 ID。WatchRow 解析成功后，重连恢复按此 ID 进行。
        /// </summary>
        public long BoundRowID { get; private set; }

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
        ///     更新当前行数据并触发相关事件。
        /// </summary>
        /// <param name="data">新数据；为 <see langword="null" /> 表示删除。</param>
        public void Update(T data)
        {
            // 防御 Restore 异步回调晚于 Dispose 到达："僵尸"更新会戳到已 Dispose
            // 的 _subject，R3 直接抛 ObjectDisposedException。
            if (IsDisposed) return;
            if (data is null)
            {
                OnDelete?.Invoke(this);
                Data = default;
                // 不应该_subject.OnCompleted，如果订阅的查询条件不是RowId，那么数据可能会重新出现
                // 如果是订阅RowId，那么数据不会重新出现，所以可以OnCompleted
                // 但为了统一，我们始终不调用OnCompleted
            }
            else
            {
                Data = data;
                LastRowID = data.ID;
                OnUpdate?.Invoke(this);
            }

            _subject.OnNext(Data);
        }

        public override void UpdateRows(JsonObject data)
        {
            var rows = data.ToDict<long, T>();
            foreach (var (_, rowData) in rows)
            {
                Update(rowData);
            }
        }

        internal void Rebind(
            string subscriptID,
            T row,
            HeTuClientBase client)
        {
            // 已 Dispose 直接返回：避免 RebindRemote 把对象表面上"激活"
            //（IsStale = false, _parentClient = 新 client），以及避免后续 Update
            //  戳到已被 DisposeBag 销毁的 _subject。
            if (IsDisposed) return;
            RebindRemote(subscriptID, client);
            BoundRowID = row.ID;
            // 重连后 always-fire：T 通常是用户类没实现 Equals，引用比较恒不等；
            // 与其要求实现 Equals 或按字节比较，不如统一回放给响应式链路，
            // 保证断线期间的变化不会漏给 Subject/OnUpdate 订阅方。
            Update(row);
            OnResynced?.Invoke();
        }

        internal override void Restore(
            IHeTuSessionTransport transport,
            Action<bool> onCompleted,
            Action<Exception> onFailed)
        {
            transport.WatchRow(
                HeTuClientBase.IndexId,
                BoundRowID,
                (subscription, canceled, exception) =>
                {
                    if (canceled)
                    {
                        onFailed(new OperationCanceledException(
                            $"WatchRow '{SubId}' was canceled."));
                        return;
                    }

                    if (exception != null)
                    {
                        onFailed(exception);
                        return;
                    }

                    if (subscription == null)
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
    }

    /// <summary>
    ///     范围订阅对象（Range 结果）。
    /// </summary>
    /// <typeparam name="T">组件类型。</typeparam>
    public class IndexSubscription<T> : BaseSubscription where T : IBaseComponent
    {
        private readonly Subject<T> _addSubject;
        private readonly Subject<long> _removeSubject;
        private readonly Dictionary<long, Subject<T>> _replaceSubjects;

        public IndexSubscription(string subscriptID, string componentName, List<T> rows,
            HeTuClientBase client, StackTrace creationTrace = null) :
            base(subscriptID, componentName, client, creationTrace)
        {
            Rows = rows.ToDictionary(row => row.ID);
            _addSubject = new Subject<T>();
            _removeSubject = new Subject<long>();
            _replaceSubjects = new Dictionary<long, Subject<T>>();
            DisposeBag.Add(_addSubject);
            DisposeBag.Add(_removeSubject);
            // per-row subject 不进 DisposeBag——R3 的 DisposableBag 只能追加，
            // 删行的 churn 会让 bag 单调增长。改成跟着 _replaceSubjects 字典走，
            // 删除时显式 Dispose，整体 Dispose 在 override 里兜底。
            foreach (var (id, _) in Rows)
                _replaceSubjects[id] = new Subject<T>();
        }

        public override void Dispose()
        {
            if (IsDisposed) return;
            foreach (var subject in _replaceSubjects.Values)
                subject.Dispose();
            _replaceSubjects.Clear();
            base.Dispose();
        }

        /// <summary>
        ///     当前订阅范围内的行集合（key 为行 ID）。
        /// </summary>
        public Dictionary<long, T> Rows { get; }

        internal string RestoreIndex { get; private set; }
        internal object RestoreLeft { get; private set; }
        internal object RestoreRight { get; private set; }
        internal int RestoreLimit { get; private set; }
        internal bool RestoreDesc { get; private set; }
        internal bool RestoreForce { get; private set; } = true;

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
        ///     对指定行应用新增/更新/删除变更。
        /// </summary>
        /// <param name="rowID">目标行 ID。</param>
        /// <param name="data">新数据；为 <see langword="null" /> 表示删除。</param>
        public void Update(long rowID, T data)
        {
            // 防御 Restore 异步回调晚于 Dispose 到达：_replaceSubjects 已被
            // override Dispose 清空，再戳进来会 KeyNotFoundException。
            if (IsDisposed) return;
            var exist = Rows.ContainsKey(rowID);
            var delete = data is null;

            if (delete)
            {
                if (!exist) return;
                OnDelete?.Invoke(this, rowID);
                var subject = _replaceSubjects[rowID];
                subject.OnCompleted();
                subject.Dispose();
                Rows.Remove(rowID);
                _replaceSubjects.Remove(rowID);
                _removeSubject.OnNext(rowID);
            }
            else
            {
                Rows[rowID] = data;
                if (exist)
                {
                    OnUpdate?.Invoke(this, rowID);
                    _replaceSubjects[rowID].OnNext(data);
                }
                else
                {
                    OnInsert?.Invoke(this, rowID);
                    _replaceSubjects[rowID] = new Subject<T>();
                    _addSubject.OnNext(data);
                }
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

        /// <summary>
        ///     获得范围订阅的新增热源，初始行也会先依次发出。
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
        ///             .AddTo(go);
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
        }

        internal void Rebind(
            string subscriptID,
            List<T> rows,
            HeTuClientBase client)
        {
            // 同 RowSubscription.Rebind：Dispose 后任何延迟到达的 Restore 回调
            // 都 noop，避免对象被"复活"或戳到已清空的 _replaceSubjects。
            if (IsDisposed) return;
            RebindRemote(subscriptID, client);
            ReplaceSnapshot(rows);
            OnResynced?.Invoke();
        }

        internal override void Restore(
            IHeTuSessionTransport transport,
            Action<bool> onCompleted,
            Action<Exception> onFailed)
        {
            transport.WatchRange(
                RestoreIndex,
                RestoreLeft,
                RestoreRight,
                RestoreLimit,
                (subscription, canceled, exception) =>
                {
                    if (canceled)
                    {
                        onFailed(new OperationCanceledException(
                            $"WatchRange '{SubId}' was canceled."));
                        return;
                    }

                    if (exception != null)
                    {
                        onFailed(exception);
                        return;
                    }

                    onCompleted(subscription != null);
                },
                RestoreDesc,
                RestoreForce,
                ComponentName,
                this);
        }

        private void ReplaceSnapshot(List<T> rows)
        {
            // 把 diff 全部交给 Update：删除、新增、变更都走同一条事件路径，
            // 重连后 R3 链路（_addSubject/_removeSubject/_replaceSubjects 以及
            // OnInsert/OnDelete/OnUpdate）就能像断线期间一直在线那样收到完整变更。
            // 已存在的行其 _replaceSubjects 条目会被 Update 复用，订阅方不会断流。
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
    ///     订阅对象强引用管理器。
    /// </summary>
    public class SubscriptionManager
    {
        private readonly Dictionary<string, BaseSubscription> _subscriptions = new();

        /// <summary>
        ///     清空缓存。
        /// </summary>
        public void Clean() => _subscriptions.Clear();

        /// <summary>
        ///     按订阅 ID 获取订阅对象。
        /// </summary>
        /// <param name="subID">订阅 ID。</param>
        /// <param name="subscription">输出订阅对象。</param>
        /// <returns>是否获取成功。</returns>
        public bool TryGet(string subID, out BaseSubscription subscription) =>
            _subscriptions.TryGetValue(subID, out subscription);

        /// <summary>
        ///     添加或覆盖订阅引用。
        /// </summary>
        /// <param name="subID">订阅 ID。</param>
        /// <param name="subscription">订阅对象。</param>
        public void Add(string subID, BaseSubscription subscription) =>
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
