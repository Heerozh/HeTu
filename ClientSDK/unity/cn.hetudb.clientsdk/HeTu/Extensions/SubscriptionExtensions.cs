// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的订阅响应式库</summary>

#if R3_INSTALLED

using System;
using System.Collections;
using System.Collections.Generic;
using ObservableCollections;
using R3;

namespace HeTu.Extensions
{
    public static class SubscriptionExtensions
    {
        // ========================================================================
        // RowSubscription 扩展
        // ========================================================================

        /// <summary>
        ///     将 RowSubscription 转换为 R3 的 ReadOnlyReactiveProperty。
        ///     自动处理 OnUpdate 和 OnDelete 事件。
        ///     用法：
        ///     // 原始对象
        ///     <![CDATA[
        /// RowSubscription<HP> hpSub = client.Get<HP>("owner", 123);
        /// ]]>
        ///     // 转换成 ReactiveProperty
        ///     // 只有当 hpSub 被回收时，或者你手动 dispose rp 时，才会停止监听
        ///     var hpRp = hpSub.ToReactiveProperty();
        ///     // 逻辑：当 hpRp 变化 -> 转换成字符串 -> 赋值给 Text 组件
        ///     hpRp.Select(hp => hp != null ? $"HP: {hp.value}" : "Dead")
        ///     .SubscribeToText(hpText) // R3 特有的 Unity 扩展，自动处理赋值
        ///     .AddTo(this);        // 放入垃圾袋，随组件销毁而销毁
        /// </summary>
        public static ReadOnlyReactiveProperty<T> ToReactiveProperty<T>(
            this RowSubscription<T> subscription)
            where T : class, IBaseComponent
        {
            // 获取初始值
            var initialValue = subscription.Data;

            // 创建一个 Observable 来监听源对象的变化
            var observable = Observable.Create<T>(observer =>
            {
                // 定义事件处理函数
                void onUpdate(RowSubscription<T> sub) =>
                    // 当 Update 发生时，发射新数据
                    observer.OnNext(sub.Data);

                void onDelete(RowSubscription<T> sub) =>
                    // 当 Delete 发生时，发射 null (或 default)
                    observer.OnNext(null);

                // 订阅原始事件
                subscription.OnUpdate += onUpdate;
                subscription.OnDelete += onDelete;

                // 当 Observable 被 Dispose 时，取消订阅原始事件
                return Disposable.Create(() =>
                {
                    subscription.OnUpdate -= onUpdate;
                    subscription.OnDelete -= onDelete;
                });
            });

            // 转换为 ReactiveProperty，这样它就有了 "当前值" 的概念，并且线程安全
            return observable.ToReadOnlyReactiveProperty(initialValue);
        }

        // ========================================================================
        // IndexSubscription 扩展 (基础事件流)
        // ========================================================================

        /// <summary>
        ///     监听新增事件 (返回 id 和数据)
        ///     用法：
        ///     <![CDATA[
        ///     IndexSubscription<HP> indexSub = client.Range<HP>(...);
        ///
        ///     // 监听插入
        ///     indexSub.ObserveInsert()
        ///         .Subscribe(x => Console.WriteLine($"插入 ID: {x.id}, Val: {x.data.value}"));
        ///         .AddTo(this);        // 放入垃圾袋，随组件销毁而销毁
        ///
        ///     // 监听更新 (过滤特定条件)
        ///     indexSub.ObserveUpdate()
        ///         .Where(x => x.data.value < 50) // 只关心血量低于50的更新
        ///         .Subscribe(x => Console.WriteLine($"ID {x.id} 血量危急!"));
        ///         .AddTo(this);
        ///
        ///     // 监听删除
        ///     indexSub.ObserveDelete()
        ///         .Subscribe(id => Console.WriteLine($"ID {id} 已移除"));
        ///         .AddTo(this);
        ///     ]]>
        /// </summary>
        public static Observable<(long id, T data)> ObserveInsert<T>(
            this IndexSubscription<T> subscription)
            where T : IBaseComponent =>
            Observable.Create<(long, T)>(observer =>
            {
                void handler(IndexSubscription<T> sub, long id)
                {
                    // 从 Rows 中获取刚插入的数据
                    if (sub.Rows.TryGetValue(id, out var data))
                    {
                        observer.OnNext((id, data));
                    }
                }

                subscription.OnInsert += handler;
                return Disposable.Create(() => subscription.OnInsert -= handler);
            });

        /// <summary>
        ///     监听更新事件 (返回 id 和数据)
        /// </summary>
        public static Observable<(long id, T data)> ObserveUpdate<T>(
            this IndexSubscription<T> subscription)
            where T : IBaseComponent =>
            Observable.Create<(long, T)>(observer =>
            {
                void handler(IndexSubscription<T> sub, long id)
                {
                    if (sub.Rows.TryGetValue(id, out var data))
                    {
                        observer.OnNext((id, data));
                    }
                }

                subscription.OnUpdate += handler;
                return Disposable.Create(() => subscription.OnUpdate -= handler);
            });

        /// <summary>
        ///     监听删除事件 (只返回 id，因为数据可能已经没了或不重要)
        /// </summary>
        public static Observable<long> ObserveDelete<T>(
            this IndexSubscription<T> subscription)
            where T : IBaseComponent =>
            Observable.Create<long>(observer =>
            {
                void handler(IndexSubscription<T> sub, long id) => observer.OnNext(id);
                subscription.OnDelete += handler;
                return Disposable.Create(() => subscription.OnDelete -= handler);
            });

        // ========================================================================
        // IndexSubscription 扩展 (高级：同步到 ObservableDictionary)
        // ========================================================================

        /// <summary>
        ///     创建一个与 Subscription 保持同步的 ObservableDictionary。
        ///     注意：返回的对象需要 Dispose 以停止同步。
        ///     用法：
        ///     <![CDATA[
        ///     IndexSubscription<HP> indexSub = client.Range<HP>(...);
        ///     ]]>
        ///     // 创建同步字典
        ///     // 注意：这个 syncDict 需要被 Dispose，否则会一直监听 indexSub 的事件
        ///     using var syncDict = indexSub.ToObservableDictionary();
        ///     // 现在你拥有了完整的 ObservableCollection 能力
        ///     syncDict.Collection.ObserveCountChanged()
        ///     .Subscribe(c => Console.WriteLine($"总数: {c}"));
        ///     .AddTo(this);
        /// </summary>
        public static SynchronizedObservableDictionary<T> ToObservableDictionary<T>(
            this IndexSubscription<T> subscription)
            where T : IBaseComponent =>
            new(subscription);

        // 辅助类：用于管理 ObservableDictionary 的同步生命周期
        public class SynchronizedObservableDictionary<T> : IDisposable,
            IEnumerable<KeyValuePair<long, T>>
            where T : IBaseComponent
        {
            private readonly IDisposable _eventSubscription;
            private readonly IndexSubscription<T> _source;

            public SynchronizedObservableDictionary(IndexSubscription<T> source)
            {
                _source = source;
                // 初始化集合
                Collection = new ObservableDictionary<long, T>(source.Rows);

                // 组合所有事件订阅
                var d1 = source.ObserveInsert().Subscribe(x => Collection[x.id] = x.data);
                var d2 = source.ObserveUpdate().Subscribe(x => Collection[x.id] = x.data);
                var d3 = source.ObserveDelete().Subscribe(id => Collection.Remove(id));

                _eventSubscription = Disposable.Combine(d1, d2, d3);
            }

            // 公开的 R3 集合
            public ObservableDictionary<long, T> Collection { get; }

            public virtual void Dispose() => _eventSubscription.Dispose(); // 停止监听源事件

            // 实现 IEnumerable 方便直接遍历
            public IEnumerator<KeyValuePair<long, T>> GetEnumerator() =>
                Collection.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => Collection.GetEnumerator();
        }
    }
}
#endif
