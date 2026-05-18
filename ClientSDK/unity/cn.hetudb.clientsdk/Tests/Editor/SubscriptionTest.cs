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

            var subjectForRow1 = GetReplaceSubject(sub, 1L);

            sub.Update(1, null);

            Assert.IsTrue(IsSubjectDisposed(subjectForRow1),
                "Per-row Subject should be fully disposed (not just " +
                "completed) when its row is deleted");
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
                Action<byte[]> onMessage, Action<string> onClose,
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
