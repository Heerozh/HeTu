using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using HeTu;
using NUnit.Framework;
using R3;

namespace Tests.HeTu
{
    [TestFixture]
    public sealed class SubscriptionTest
    {
        // 测试里没 Dispose 的订阅在这里就回收：终结器的泄漏日志落在本测试里（先静默 logger），
        // 不会在之后别的测试（如 LoggerTest 接住日志断言时）冒出来
        [TearDown]
        public void CollectUndisposedSubscriptions()
        {
            Logger.Instance.SetLogger(_ => { }, _ => { }, _ => { });
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        [Test]
        public void RowSubscription_Rebind_FiresOnUpdateWithNewData()
        {
            var client = new StubClient();
            var initial = new TestRow { ID = 7, Value = 10 };
            var sub = new RowSubscription<TestRow>("sub-1", "Test", initial, client);

            var updates = new List<TestRow>();
            sub.OnUpdate += s => updates.Add(s.Data);

            sub.Rebind("sub-1", new TestRow { ID = 7, Value = 20 }, client);

            Assert.AreEqual(1, updates.Count);
            Assert.AreEqual(20, updates[0].Value);
            Assert.AreEqual(20, sub.Data.Value);
        }

        [Test]
        public void RowSubscription_Rebind_PushesNewValueToSubject()
        {
            var client = new StubClient();
            var initial = new TestRow { ID = 7, Value = 10 };
            var sub = new RowSubscription<TestRow>("sub-1", "Test", initial, client);

            var received = new List<int>();
            sub.Subject.Subscribe(row => received.Add(row?.Value ?? -1));

            sub.Rebind("sub-1", new TestRow { ID = 7, Value = 20 }, client);

            // Prepend gives the initial value 10, Rebind pushes 20.
            CollectionAssert.AreEqual(new[] { 10, 20 }, received);
        }

        [Test]
        public void RowSubscription_Rebind_FiresOnUpdateBeforeOnResynced()
        {
            var client = new StubClient();
            var initial = new TestRow { ID = 7, Value = 10 };
            var sub = new RowSubscription<TestRow>("sub-1", "Test", initial, client);

            var order = new List<string>();
            sub.OnUpdate += _ => order.Add("update");
            sub.OnResynced += () => order.Add("resynced");

            sub.Rebind("sub-1", new TestRow { ID = 7, Value = 20 }, client);

            CollectionAssert.AreEqual(new[] { "update", "resynced" }, order);
        }

        [Test]
        public void IndexSubscription_Rebind_NotifiesRemovedRows()
        {
            var client = new StubClient();
            var sub = new IndexSubscription<TestRow>("sub-1", "Test",
                new List<TestRow>
                {
                    new() { ID = 1, Value = 1 },
                    new() { ID = 2, Value = 2 }
                },
                client);

            var deleteEvents = new List<long>();
            sub.OnDelete += (_, id) => deleteEvents.Add(id);

            var removeFromSubject = new List<long>();
            sub.ObserveRemove().Subscribe(id => removeFromSubject.Add(id));

            sub.Rebind("sub-1",
                new List<TestRow> { new() { ID = 1, Value = 1 } }, client);

            CollectionAssert.AreEqual(new long[] { 2 }, deleteEvents);
            CollectionAssert.AreEqual(new long[] { 2 }, removeFromSubject);
        }

        [Test]
        public void IndexSubscription_Rebind_NotifiesAddedRows()
        {
            var client = new StubClient();
            var sub = new IndexSubscription<TestRow>("sub-1", "Test",
                new List<TestRow> { new() { ID = 1, Value = 1 } },
                client);

            var insertEvents = new List<long>();
            sub.OnInsert += (_, id) => insertEvents.Add(id);

            var addFromSubject = new List<long>();
            sub.ObserveAdd().Subscribe(row => addFromSubject.Add(row.ID));

            sub.Rebind("sub-1",
                new List<TestRow>
                {
                    new() { ID = 1, Value = 1 },
                    new() { ID = 2, Value = 2 }
                },
                client);

            CollectionAssert.AreEqual(new long[] { 2 }, insertEvents);
            // ObserveAdd 先发初始行 1，Rebind 再发 2。
            CollectionAssert.AreEqual(new long[] { 1, 2 }, addFromSubject);
        }

        [Test]
        public void IndexSubscription_Rebind_NotifiesPersistingRows()
        {
            var client = new StubClient();
            var sub = new IndexSubscription<TestRow>("sub-1", "Test",
                new List<TestRow> { new() { ID = 1, Value = 1 } },
                client);

            var updateEvents = new List<long>();
            sub.OnUpdate += (_, id) => updateEvents.Add(id);

            var rowValues = new List<int>();
            sub.ObserveRow(1).Subscribe(row => rowValues.Add(row.Value));

            sub.Rebind("sub-1",
                new List<TestRow> { new() { ID = 1, Value = 10 } }, client);

            CollectionAssert.AreEqual(new long[] { 1 }, updateEvents);
            CollectionAssert.AreEqual(new[] { 10 }, rowValues);
        }

        [Test]
        public void IndexSubscription_Rebind_FiresOnResyncedAfterDiff()
        {
            var client = new StubClient();
            var sub = new IndexSubscription<TestRow>("sub-1", "Test",
                new List<TestRow>
                {
                    new() { ID = 1, Value = 1 },
                    new() { ID = 3, Value = 3 }
                },
                client);

            var order = new List<string>();
            sub.OnInsert += (_, id) => order.Add($"insert {id}");
            sub.OnDelete += (_, id) => order.Add($"delete {id}");
            sub.OnUpdate += (_, id) => order.Add($"update {id}");
            sub.OnResynced += () => order.Add("resynced");

            sub.Rebind("sub-1",
                new List<TestRow>
                {
                    new() { ID = 1, Value = 10 },
                    new() { ID = 2, Value = 2 }
                },
                client);

            Assert.AreEqual("resynced", order[^1]);
            CollectionAssert.Contains(order, "delete 3");
            CollectionAssert.Contains(order, "insert 2");
            CollectionAssert.Contains(order, "update 1");
        }

        [Test]
        public void IndexSubscription_RowChurn_DoesNotGrowDisposeBag()
        {
            var client = new StubClient();
            var sub = new IndexSubscription<TestRow>("sub-1", "Test",
                new List<TestRow>(),
                client);

            var initialCount = GetDisposeBagCount(sub);

            for (long id = 1; id <= 50; id++)
            {
                sub.Update(id, new TestRow { ID = id, Value = (int)id });
                sub.Update(id, null);
            }

            Assert.AreEqual(initialCount, GetDisposeBagCount(sub),
                "DisposeBag should not accumulate references through " +
                "insert+delete churn of per-row subjects");
        }

        [Test]
        public void IndexSubscription_RowDelete_DisposesPerRowSubject()
        {
            var client = new StubClient();
            var sub = new IndexSubscription<TestRow>("sub-1", "Test",
                new List<TestRow> { new() { ID = 1, Value = 1 } },
                client);

            sub.ObserveRow(1);
            var subjectForRow1 = GetReplaceSubject(sub, 1L);

            sub.Update(1, null);

            Assert.IsTrue(IsSubjectDisposed(subjectForRow1),
                "Per-row Subject should be fully disposed (not just " +
                "completed) when its row is deleted");
        }

        [Test]
        public void UpdateFanOut_DoesNotAllocate()
        {
            // 推送热路径：分发给各句柄时不能每次复制句柄表、建闭包
            var client = new StubClient();
            var rowSub = new RowSubscription<TestRow>("sub-1", "Test",
                new TestRow { ID = 1, Value = 1 }, client);
            var rowOther = ((ManagedRowCore<TestRow>)rowSub.Core).CreateHandle(null);
            var idxSub = new IndexSubscription<TestRow>("sub-2", "Test",
                new List<TestRow> { new() { ID = 1, Value = 1 } }, client);
            var idxOther = ((ManagedIndexCore<TestRow>)idxSub.Core).CreateHandle(null);
            var row = new TestRow { ID = 1, Value = 2 };
            for (var i = 0; i < 100; i++)
            {
                rowSub.Update(row);
                idxSub.Update(1, row);
            }

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < 1000; i++)
            {
                rowSub.Update(row);
                idxSub.Update(1, row);
            }

            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Less(allocated, 1000, $"2000 次更新分配了 {allocated} 字节");
            rowOther.Dispose();
            rowSub.Dispose();
            idxOther.Dispose();
            idxSub.Dispose();
        }

        [Test]
        public void IndexSubscription_PerRowSubjects_CreatedOnlyWhenObserved()
        {
            // 大表 × 多个句柄时不能每行每句柄都预建 subject：只给 ObserveRow 过的行建
            var client = new StubClient();
            var rows = new List<TestRow>();
            for (long id = 1; id <= 100; id++)
                rows.Add(new TestRow { ID = id, Value = (int)id });
            var sub = new IndexSubscription<TestRow>("sub-1", "Test", rows, client);
            sub.Update(101, new TestRow { ID = 101, Value = 101 });
            Assert.AreEqual(0, GetReplaceSubjectCount(sub));

            var values = new List<int>();
            sub.ObserveRow(5).Subscribe(row => values.Add(row.Value));
            sub.Update(5, new TestRow { ID = 5, Value = 50 });

            Assert.AreEqual(1, GetReplaceSubjectCount(sub));
            CollectionAssert.AreEqual(new[] { 50 }, values);
            Assert.Throws<KeyNotFoundException>(() => sub.ObserveRow(999),
                "不在范围里的行照旧抛 KeyNotFound");
        }

        [Test]
        public void IndexSubscription_OnDelete_CanStillObserveRow()
        {
            // OnDelete 触发时这行还在：回调里 ObserveRow 能拿到它的流，随后流会完成
            var client = new StubClient();
            var sub = new IndexSubscription<TestRow>("sub-1", "Test",
                new List<TestRow> { new() { ID = 1, Value = 1 } },
                client);
            var completed = false;
            sub.OnDelete += (s, id) =>
                s.ObserveRow(id).Subscribe(_ => { }, _ => completed = true);

            sub.Update(1, null);

            Assert.IsTrue(completed);
        }

        [Test]
        public void IndexSubscription_Dispose_DisposesAllRemainingPerRowSubjects()
        {
            var client = new StubClient();
            var sub = new IndexSubscription<TestRow>("sub-1", "Test",
                new List<TestRow>
                {
                    new() { ID = 1, Value = 1 },
                    new() { ID = 2, Value = 2 }
                },
                client);

            sub.ObserveRow(1);
            sub.ObserveRow(2);
            var s1 = GetReplaceSubject(sub, 1L);
            var s2 = GetReplaceSubject(sub, 2L);

            sub.Dispose();

            Assert.IsTrue(IsSubjectDisposed(s1));
            Assert.IsTrue(IsSubjectDisposed(s2));
        }

        [Test]
        public void RowSubscription_Rebind_AfterDispose_IsNoop()
        {
            var client = new StubClient();
            var sub = new RowSubscription<TestRow>("sub-1", "Test",
                new TestRow { ID = 7, Value = 10 }, client);

            var updates = 0;
            sub.OnUpdate += _ => updates++;
            var resynced = 0;
            sub.OnResynced += () => resynced++;

            sub.Dispose();

            // 模拟 Restore 回调延迟回到已 Dispose 的订阅上。
            Assert.DoesNotThrow(() => sub.Rebind(
                "sub-1", new TestRow { ID = 7, Value = 20 }, client));

            Assert.AreEqual(0, updates);
            Assert.AreEqual(0, resynced);
        }

        [Test]
        public void IndexSubscription_Rebind_AfterDispose_IsNoop()
        {
            var client = new StubClient();
            var sub = new IndexSubscription<TestRow>("sub-1", "Test",
                new List<TestRow> { new() { ID = 1, Value = 1 } },
                client);

            var inserts = 0;
            sub.OnInsert += (_, _) => inserts++;
            var deletes = 0;
            sub.OnDelete += (_, _) => deletes++;
            var updates = 0;
            sub.OnUpdate += (_, _) => updates++;
            var resynced = 0;
            sub.OnResynced += () => resynced++;

            sub.Dispose();

            Assert.DoesNotThrow(() => sub.Rebind(
                "sub-1",
                new List<TestRow> { new() { ID = 2, Value = 2 } },
                client));

            Assert.AreEqual(0, inserts);
            Assert.AreEqual(0, deletes);
            Assert.AreEqual(0, updates);
            Assert.AreEqual(0, resynced);
        }

        private static int GetDisposeBagCount(BaseSubscription sub)
        {
            var bagField = typeof(BaseSubscription).GetField("DisposeBag");
            var bag = bagField!.GetValue(sub);
            var countField = bag!.GetType().GetField("count",
                BindingFlags.NonPublic | BindingFlags.Instance);
            return (int)countField!.GetValue(bag)!;
        }

        private static object GetReplaceSubject(
            IndexSubscription<TestRow> sub, long rowId)
        {
            var field = typeof(IndexSubscription<TestRow>).GetField(
                "_replaceSubjects",
                BindingFlags.NonPublic | BindingFlags.Instance);
            var dict = field!.GetValue(sub);
            var indexer = dict!.GetType().GetProperty("Item");
            return indexer!.GetValue(dict, new object[] { rowId })!;
        }

        private static int GetReplaceSubjectCount(IndexSubscription<TestRow> sub)
        {
            var field = typeof(IndexSubscription<TestRow>).GetField(
                "_replaceSubjects",
                BindingFlags.NonPublic | BindingFlags.Instance);
            return ((ICollection)field!.GetValue(sub)!).Count;
        }

        private static bool IsSubjectDisposed(object subject) =>
            (bool)subject.GetType().GetProperty("IsDisposed")!.GetValue(subject)!;

        private sealed class TestRow : IBaseComponent
        {
            public long ID { get; set; }
            public int Value { get; set; }
        }

        private sealed class StubClient : HeTuClientBase
        {
            protected override void ConnectCore(string url, Action onConnected,
                Action<byte[]> onMessage, Action<int, string> onClose,
                Action<string> onError)
            {
            }

            protected override void CloseCore()
            {
            }

            protected override void SendCore(byte[] data)
            {
            }
        }
    }
}
