// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的订阅库</summary>


using System;
using System.Collections.Generic;
using System.Linq;
using UnityEngine.Scripting;

namespace HeTu
{
    public interface IBaseComponent
    {
        public long id { get; }
    }

    [Preserve]
    public class DictComponent : Dictionary<string, object>, IBaseComponent
    {
        [Preserve] // strip会导致Unable to find a default constructor to use for type [0].id
        public long id => (int)this["id"];
    }

    public abstract class BaseSubscription
    {
        private readonly string _subscriptID;
        public readonly string ComponentName;

        protected BaseSubscription(string subscriptID, string componentName)
        {
            _subscriptID = subscriptID;
            ComponentName = componentName;
        }

        public abstract void Update(long rowID, JsonObject data);

        /// <summary>
        ///     销毁远端订阅对象。
        ///     Dispose应该明确调用，虽然gc回收时会调用，但时间不确定，这会导致服务器端该对象销毁不及时。
        /// </summary>
        public void Dispose() =>
            HeTuClient.Instance._unsubscribe(_subscriptID, "Dispose");

        ~BaseSubscription() => HeTuClient.Instance._unsubscribe(_subscriptID, "析构");
    }

    /// Select结果的订阅对象
    public class RowSubscription<T> : BaseSubscription where T : IBaseComponent
    {
        public RowSubscription(string subscriptID, string componentName, T row) :
            base(subscriptID, componentName) =>
            Data = row;

        public T Data { get; private set; }

        public event Action<RowSubscription<T>> OnUpdate;
        public event Action<RowSubscription<T>> OnDelete;

        public override void Update(long rowID, JsonObject data)
        {
            if (data is null)
            {
                OnDelete?.Invoke(this);
                Data = default;
            }
            else
            {
                Data = data.To<T>();
                OnUpdate?.Invoke(this);
            }
        }
    }

    /// Query结果的订阅对象
    public class IndexSubscription<T> : BaseSubscription where T : IBaseComponent
    {
        public IndexSubscription(string subscriptID, string componentName, List<T> rows) :
            base(subscriptID, componentName) =>
            Rows = rows.ToDictionary(row => row.id);

        public Dictionary<long, T> Rows { get; }

        public event Action<IndexSubscription<T>, long> OnUpdate;
        public event Action<IndexSubscription<T>, long> OnDelete;
        public event Action<IndexSubscription<T>, long> OnInsert;

        public override void Update(long rowID, JsonObject data)
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
                var tData = data.To<T>();
                Rows[rowID] = tData;
                if (exist)
                    OnUpdate?.Invoke(this, rowID);
                else
                    OnInsert?.Invoke(this, rowID);
            }
        }
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
