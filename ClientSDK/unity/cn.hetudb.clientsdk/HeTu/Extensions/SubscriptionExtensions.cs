// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的订阅响应式库</summary>

#if R3_INSTALLED

using System;
using System.Collections;
using System.Collections.Generic;
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
        ///     RowSubscription<HP> hpSub = client.Get<HP>("owner", 123);
        ///     // 在gameObject销毁时反订阅，以及Dispose所有R3相关对象。
        ///     // 或手动调用hpSub.Dispose()，不然服务器永远发送订阅消息。
        ///     hpSub.AddTo(gameObject);
        ///     ]]>
        ///     // 转换成 ReactiveProperty
        ///     var hp = hpSub.ToReactiveProperty();
        ///     // 逻辑：当 hp 变化 -> 转换成字符串 -> 赋值给 Text 组件
        ///     hp.Select(x => x != null ? $"HP: {x.value}" : "Dead")
        ///         .SubscribeToText(textBox) // R3 特有的 Unity 扩展，自动处理赋值
        ///     // 通常R3的订阅.Subscribe后需要跟.AddTo一个垃圾袋负责销毁，
        ///     // 但这里源头的hpSub会负责销毁所有子订阅
        /// </summary>
        public static ReadOnlyReactiveProperty<T> ToReactiveProperty<T>(
            this RowSubscription<T> subscription)
            where T : class, IBaseComponent
        {
            // 获取初始值
            var initialValue = subscription.Data;

            // 创建一个 Observable 来监听源对象的变化, 第一个参数是SubscribeCore回调
            // 每次 observable.Subscribe 时都会调用这个回调
            var observable = Observable.Create<T>(observer =>
            {
                // 订阅原始事件
                subscription.OnUpdate += OnUpdate;
                subscription.OnDelete += OnDelete;

                // 当 Observable 被 Dispose 时，取消订阅原始事件
                return Disposable.Create(() =>
                {
                    subscription.OnUpdate -= OnUpdate;
                    subscription.OnDelete -= OnDelete;
                }).AddTo(ref subscription.DisposeBag);

                void OnDelete(RowSubscription<T> sub) =>
                    // 当 Delete 发生时，发射 null (或 default)
                    observer.OnNext(null);

                // 定义事件处理函数
                void OnUpdate(RowSubscription<T> sub) =>
                    // 当 Update 发生时，发射新数据
                    observer.OnNext(sub.Data);
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
        ///     indexSub.AddTo(gameObject);
        ///
        ///     // 订阅所有数据，初始数据也会触发此事件
        ///     indexSub.ObserveAdd()
        ///         .Subscribe(added => {
        ///             Console.WriteLine($"插入 ID: {added.id}, Val: {added.value}");
        ///             var rowId = added.id;
        ///
        ///             // 监听更新，数据删除时不会触发，值永远不为null
        ///             added.ObserveReplace()
        ///                 .Subscribe(x => Console.WriteLine($"ID {rowId} 血量: { x.value }"));
        ///
        ///             // 监听删除
        ///             added.ObserveRemove()
        ///                 .Subscribe(id => Console.WriteLine($"ID {id} 已移除"));
        ///         });
        ///     ]]>
        /// </summary>
        public static Observable<(long id, T data)> ObserveAdd<T>(
            this IndexSubscription<T> subscription)
            where T : IBaseComponent =>
            Observable.Create<(long, T)>(observer =>
            {
                subscription.OnInsert += Handler;
                return Disposable
                    .Create(() =>
                    {
                        // raise latest value on subscribe(before add observer to list)
                        // observer.OnNext(initialValue);
                        subscription.OnInsert -= Handler;
                    })
                    .AddTo(ref subscription.DisposeBag);

                void Handler(IndexSubscription<T> sub, long id)
                {
                    // 从 Rows 中获取刚插入的数据
                    if (sub.Rows.TryGetValue(id, out var data))
                    {
                        observer.OnNext((id, data));
                    }
                }
            });

        /// <summary>
        ///     监听更新事件 (返回 id 和数据)
        /// </summary>
        public static Observable<(long id, T data)> ObserveReplace<T>(
            this IndexSubscription<T> subscription)
            where T : IBaseComponent =>
            Observable.Create<(long, T)>(observer =>
            {
                subscription.OnUpdate += Handler;
                return Disposable
                    .Create(() => subscription.OnUpdate -= Handler)
                    .AddTo(ref subscription.DisposeBag);

                void Handler(IndexSubscription<T> sub, long id)
                {
                    if (sub.Rows.TryGetValue(id, out var data))
                    {
                        observer.OnNext((id, data));
                    }
                }
            });

        /// <summary>
        ///     监听删除事件 (只返回 id，因为数据可能已经没了或不重要)
        /// </summary>
        public static Observable<long> ObserveRemove<T>(
            this IndexSubscription<T> subscription)
            where T : IBaseComponent =>
            Observable.Create<long>(observer =>
            {
                subscription.OnDelete += Handler;
                return Disposable
                    .Create(() => subscription.OnDelete -= Handler)
                    .AddTo(ref subscription.DisposeBag);

                void Handler(IndexSubscription<T> sub, long id) => observer.OnNext(id);
            });


    }
}
#endif
