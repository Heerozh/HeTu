// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的订阅库</summary>


using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using R3;
#if UNITY_2022_3_OR_NEWER
using UnityEngine;
using UnityEngine.Scripting;
#endif

namespace HeTu
{
    public interface IBaseComponent
    {
        public long ID { get; }
    }

    public class DictComponent : Dictionary<string, object>, IBaseComponent
    {
#if UNITY_2022_3_OR_NEWER
        [Preserve] // strip会导致Unable to find a default constructor to use for type [0].id
#endif
        public long ID => Convert.ToInt64(this["id"]);
    }

    [MustDisposeResource]
    public abstract class BaseSubscription : IDisposable
    {
        private readonly string _creationStack;
        private readonly HeTuClientBase _parentClient;
        private readonly string _subscriptID;
        public readonly string ComponentName;
        public DisposableBag DisposeBag;

        protected BaseSubscription(string subscriptID, string componentName,
            HeTuClientBase client, string creationStack = null)
        {
            _subscriptID = subscriptID;
            ComponentName = componentName;
            _parentClient = client;
            _creationStack = creationStack;
        }

        /// <summary>
        ///     销毁远端订阅对象。Dispose应该明确调用。
        /// </summary>
        public virtual void Dispose()
        {
            _parentClient.Unsubscribe(_subscriptID, "Dispose");
            DisposeBag.Dispose();
            GC.SuppressFinalize(this);
        }

#if UNITY_2022_3_OR_NEWER
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

        public abstract void UpdateRows(JsonObject data);

        ~BaseSubscription() =>
            Logger.Instance.Error(
                "检测到资源泄漏！订阅被 GC 回收但未调用 .Dispose() 方法！订阅ID：" + _subscriptID +
                "\n创建时的堆栈：\n" + _creationStack);
    }

    /// Select结果的订阅对象
    public class RowSubscription<T> : BaseSubscription where T : IBaseComponent
    {
        private readonly Subject<T> _subject;
        public long LastRowID;

        public RowSubscription(string subscriptID, string componentName, T row,
            HeTuClientBase client, string creationStack = null) :
            base(subscriptID, componentName, client, creationStack)
        {
            Data = row;
            LastRowID = row.ID;
            _subject = new Subject<T>();
            DisposeBag.Add(_subject);
        }

        public T Data { get; private set; }

        /// <summary>
        ///     获得 RowSubscription 的 Subject 响应式热源，自动处理 OnUpdate 和 OnDelete 事件。
        ///     用法：
        ///     // HeTu数据订阅
        ///     <![CDATA[
        ///     RowSubscription<HP> hpSub = client.Get<HP>("owner", 123);
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

        public event Action<RowSubscription<T>> OnUpdate;
        public event Action<RowSubscription<T>> OnDelete;

        public void Update(T data)
        {
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
            foreach (var (rowID, rowData) in rows)
            {
                Update(rowData);
            }
        }
    }

    /// Query结果的订阅对象
    public class IndexSubscription<T> : BaseSubscription where T : IBaseComponent
    {
        private readonly Subject<T> _addSubject;
        private readonly Dictionary<long, Subject<T>> _replaceSubjects;

        public IndexSubscription(string subscriptID, string componentName, List<T> rows,
            HeTuClientBase client, string creationStack = null) :
            base(subscriptID, componentName, client, creationStack)
        {
            Rows = rows.ToDictionary(row => row.ID);
            _addSubject = new Subject<T>();
            _replaceSubjects = new Dictionary<long, Subject<T>>();
            DisposeBag.Add(_addSubject);
            foreach (var (id, row) in Rows)
                DisposeBag.Add(_replaceSubjects[id] = new Subject<T>());
        }

        public Dictionary<long, T> Rows { get; }

        public event Action<IndexSubscription<T>, long> OnUpdate;
        public event Action<IndexSubscription<T>, long> OnDelete;
        public event Action<IndexSubscription<T>, long> OnInsert;

        public void Update(long rowID, T data)
        {
            var exist = Rows.ContainsKey(rowID);
            var delete = data is null;

            if (delete)
            {
                if (!exist) return;
                OnDelete?.Invoke(this, rowID);
                _replaceSubjects[rowID].OnCompleted();
                Rows.Remove(rowID);
                _replaceSubjects.Remove(rowID);
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
                    DisposeBag.Add(_replaceSubjects[rowID] = new Subject<T>());
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
        ///     获得范围订阅的响应式热源，自动处理 OnInsert、OnUpdate 和 OnDelete 事件。
        ///     用法：
        ///     <![CDATA[
        ///     IndexSubscription<HP> indexSub = client.Range<HP>(...);
        ///     indexSub.AddTo(gameObject);
        ///
        ///     // 订阅所有数据，初始数据也会触发此事件
        ///     indexSub.ObserveAdd()
        ///         .Subscribe(added => {
        ///             var go = new GameObject($"ID: {added.id}");
        ///             var hpText = go.AddComponent<TMP_Text>();
        ///
        ///             // 监听更新，和删除
        ///             indexSub.ObserveReplace(added.id)
        ///                 .Select(x => $"{x.value}")
        ///                 .Subscribe(
        ///                     value => hpText.text = value, // OnNext事件
        ///                     result => Object.Destroy(go)  // OnCompleted事件
        ///                 )
        ///                 .AddTo(go);
        ///         })
        ///         .AddTo(ref indexSub.DisposeBag);
        ///     ]]>
        /// </summary>
        public Observable<T> ObserveAdd() =>
            // Concat会把前一个订阅源中的OnCompleted事件屏蔽然后自动切换下一个订阅
            Observable.Defer(() => Rows.Values.ToObservable().Concat(_addSubject));

        public Observable<T> ObserveReplace(long rowID) => _replaceSubjects[rowID];
    }

    public class SubscriptionManager
    {
        private readonly Dictionary<string, WeakReference> _subscriptions = new();

        public void Clean() => _subscriptions.Clear();

        public bool TryGet(string subID, out BaseSubscription subscription)
        {
            subscription = null;
            if (!_subscriptions.TryGetValue(subID, out var weakRef))
                return false;
            if (weakRef.Target is not BaseSubscription casted) return false;
            subscription = casted;
            return true;
        }

        public void Add(string subID, WeakReference subscription) =>
            _subscriptions[subID] = subscription;

        public void Remove(string subID) => _subscriptions.Remove(subID);

        public bool Contains(string subID) => _subscriptions.ContainsKey(subID);
    }
}
