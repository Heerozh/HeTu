using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using HeTu;
using MessagePack;
using NUnit.Framework;
using R3;
using UnityEngine;
using UnityEngine.TestTools;
using Object = UnityEngine.Object;
#if !UNITY_6000_0_OR_NEWER
using Cysharp.Threading.Tasks;
#endif

namespace Tests.HeTu
{
    [TestFixture]
    public class HeTuClientTest
    {
        [OneTimeSetUp]
        public void Init()
        {
            Debug.Log("测试前请启动河图服务器的tests/app.py");
            HeTuClient.Instance.ConfigureInspector(true);
            HeTuClient.Instance.AddInspectorDispatcher(
                new LoggerInspectorTraceDispatcher());
            _ = HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu/pytest", "pytest");
        }

        [OneTimeTearDown]
        public void DeInit()
        {
            Debug.Log("测试结束");
            HeTuClient.Instance.Close();
        }

        [MessagePackObject(true)]
        public class RLSComp : IBaseComponent
        {
            public long owner;
            public int value;
            public long ID { get; set; }
        }

        [MessagePackObject]
        public class IndexComp1 : IBaseComponent
        {
            [Key("owner")] public long Owner;

            [Key("value")] public double Value;

            [Key("id")] public long ID { get; set; }
        }

        private IEnumerator RunTask(Task task)
        {
            while (!task.IsCompleted)
            {
                yield return null;
            }

            if (task.Exception != null) throw task.Exception;
        }

#if UNITY_6000_0_OR_NEWER
        private static async Awaitable Sleep(int seconds) =>
            await Awaitable.WaitForSecondsAsync(seconds);
#else
        private static async UniTask Sleep(int seconds) =>
            await UniTask.Delay(seconds * 1000);
#endif

        [UnityTest]
        [Order(1)] // 必须未调用过login测试才会通过
        public IEnumerator TestRowSubscribe()
        {
            Debug.Log("test RowSubscribe开始");
            yield return RunTask(TestRowSubscribeAsync());
        }

        private async Task TestRowSubscribeAsync()
        {
            // 测试订阅失败
            var sub = await HeTuClient.Instance.Get(
                "RLSComp", "owner", 123);
            Assert.AreEqual(sub, null);

            // 测试订阅
            _ = HeTuClient.Instance.CallSystem("login", 123, true);
            _ = HeTuClient.Instance.CallSystem("add_rls_comp_value", 1);
            sub = await HeTuClient.Instance.Get(
                "RLSComp", "owner", 123);
            var lastValue = Convert.ToInt32(sub.Data["value"]);

            // 测试订阅事件
            int? newValue = null;
            sub.OnUpdate += sender =>
            {
                Debug.Log("收到了更新...");
                newValue = Convert.ToInt32(sender.Data["value"]);
            };
            _ = HeTuClient.Instance.CallSystem("add_rls_comp_value", -2);

            await Sleep(1);
            Assert.AreEqual(lastValue - 2, newValue);

            // 测试重复订阅，但换一个类型，应该报错
            _ = HeTuClient.Instance.CallSystem("add_rls_comp_value", -1);
            // Assert.ThrowsAsync 用的是当前协程Wait，会卡死
            var success = false;
            try
            {
                using var _ = await HeTuClient.Instance.Get<RLSComp>("owner", 123);
                success = true;
            }
            catch (InvalidCastException)
            {
                Debug.Log("正常收到InvalidCastException异常");
            }

            Assert.False(success, "没有抛出InvalidCastException");

            // unity delay后第二次gc才会回收
            sub.Dispose();

            // 测试回收自动反订阅，顺带测试Class类型
            using var typedSub = await HeTuClient.Instance.Get<RLSComp>(
                "owner", 123);
            Assert.AreEqual(lastValue - 3, typedSub.Data.value);

            Debug.Log("TestRowSubscribe结束");
        }

        [UnityTest]
        [Order(2)]
        public IEnumerator TestSystemCall()
        {
            Debug.Log("TestSystemCall开始");
            yield return RunTask(TestSystemCallAsync());
        }

        private async Task TestSystemCallAsync()
        {
            var callbackCalled = false;
            HeTuClient.Instance.SystemLocalCallbacks["login"] = _ =>
            {
                callbackCalled = true;
            };

            var resp = await HeTuClient.Instance.CallSystem("login", 123, true);
            var data = resp.ToDict<string, object>();
            var responseID = Convert.ToInt64(data["id"]);

            Assert.AreEqual(123, responseID);

            Assert.True(callbackCalled);

            Debug.Log("TestSystemCall结束");
        }

        [UnityTest]
        public IEnumerator TestIndexSubscribeOnUpdate()
        {
            Debug.Log("TestIndexSubscribeOnUpdate开始");
            yield return RunTask(TestIndexSubscribeOnUpdateAsync());
        }

        private async Task TestIndexSubscribeOnUpdateAsync()
        {
            // 测试订阅
            _ = HeTuClient.Instance.CallSystem("login", 123, true);
            _ = HeTuClient.Instance.CallSystem("add_rls_comp_value", -1);
            using var sub = await HeTuClient.Instance.Range(
                "RLSComp", "owner", 0, 300, 100);
            // 这是Owner权限表，应该只能取到自己的数据
            Assert.AreEqual(1, sub.Rows.Count);
            var lastValue = Convert.ToInt32(sub.Rows.Values.First()["value"]);

            // 测试订阅事件
            int? newValue = null;
            sub.OnUpdate += (sender, rowID) =>
            {
                newValue = Convert.ToInt32(sender.Rows[rowID]["value"]);
            };
            _ = HeTuClient.Instance.CallSystem("add_rls_comp_value", -2);

            await Sleep(1);
            Assert.AreEqual(newValue, lastValue - 2);

            Debug.Log("TestIndexSubscribeOnUpdate结束");
        }

        [UnityTest]
        public IEnumerator TestIndexSubscribeOnInsert()
        {
            Debug.Log("TestIndexSubscribeOnInsert开始");
            yield return RunTask(TestIndexSubscribeOnInsertAsync());
        }

        private async Task TestIndexSubscribeOnInsertAsync()
        {
            _ = HeTuClient.Instance.CallSystem("login", 123, true);
            _ = HeTuClient.Instance.CallSystem("client_index_upsert_test", 123, -10);
            _ = HeTuClient.Instance.CallSystem("client_index_upsert_test", 234, 0);
            _ = HeTuClient.Instance.CallSystem("client_index_upsert_test", 345, 10);

            // 测试OnInsert, OnDelete
            using var sub = await HeTuClient.Instance.Range<IndexComp1>(
                "value", 0, 10, 100);

            long? newPlayer = null;
            sub.OnInsert += (sender, rowID) => { newPlayer = sender.Rows[rowID].Owner; };
            _ = HeTuClient.Instance.CallSystem("client_index_upsert_test", 123, 2);

            await Sleep(1);
            Assert.AreEqual(newPlayer, 123);

            // OnDelete
            long? removedPlayer = null;
            sub.OnDelete += (sender, rowID) =>
            {
                removedPlayer = sender.Rows[rowID].Owner;
            };
            _ = HeTuClient.Instance.CallSystem("client_index_upsert_test", 123, 11);

            await Sleep(1);
            Assert.AreEqual(removedPlayer, 123);

            Assert.False(sub.Rows.ContainsKey(123));
            Debug.Log("TestIndexSubscribeOnInsert结束");
        }

        //todo unity6测试
        //todo R3响应式测试

        [UnityTest]
        public IEnumerator TestRowSubscribeR3()
        {
            Debug.Log("TestIndexSubscribeR3开始");
            yield return RunTask(TestRowSubscribeR3Async());
        }

        private async Task TestRowSubscribeR3Async()
        {
            var go = new GameObject("TestRowSubscribeR3");

            // 初始化数据
            _ = HeTuClient.Instance.CallSystem("login", 123, true);
            _ = HeTuClient.Instance.CallSystem("add_rls_comp_value", 1);
            // 订阅
            var sub = await HeTuClient.Instance.Get<RLSComp>(
                "owner", 123);
            Assert.IsNotNull(sub);
            sub.AddTo(go);
            var initValue = sub.Data.value;

            // 订阅响应
            List<int> receivedValues = new();
            sub.Subject
                .Subscribe(x => receivedValues.Add(x.value));

            // 测试是否已经加入到了DisposeBag
            var countField = typeof(DisposableBag)
                .GetField("count", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.IsNotNull(countField);
            Assert.True((int)countField.GetValue(sub.DisposeBag) == 1);

            // 发送更新，等待变更
            _ = HeTuClient.Instance.CallSystem("add_rls_comp_value", 2);
            await Sleep(1);
            _ = HeTuClient.Instance.CallSystem("add_rls_comp_value", -3);
            await Sleep(1);
            _ = HeTuClient.Instance.CallSystem("add_rls_comp_value", 1);
            await Sleep(1);

            // 检查收到的值
            Assert.AreEqual(
                new List<int> { initValue, initValue + 2, initValue - 1, initValue },
                receivedValues);

            // 检查DisposeBag是否已Dispose
            Object.Destroy(go);
            await Sleep(1);

            var isDisposed = typeof(DisposableBag)
                .GetField("isDisposed", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.IsNotNull(isDisposed);
            Assert.IsTrue((bool)isDisposed.GetValue(sub.DisposeBag));
            Debug.Log("TestRowSubscribeR3结束");
        }

        [UnityTest]
        public IEnumerator TestIndexSubscribeR3()
        {
            Debug.Log("TestIndexSubscribeR3开始");
            yield return RunTask(TestIndexSubscribeR3Async());
        }

        private async Task TestIndexSubscribeR3Async()
        {
            var go = new GameObject("TestRowSubscribeR3");

            // 初始化数据
            _ = HeTuClient.Instance.CallSystem("login", 123, true);
            _ = HeTuClient.Instance.CallSystem("client_index_upsert_test", 123, -10);
            _ = HeTuClient.Instance.CallSystem("client_index_upsert_test", 234, 0);
            _ = HeTuClient.Instance.CallSystem("client_index_upsert_test", 345, 10);

            // 订阅
            var sub = await HeTuClient.Instance.Range<IndexComp1>(
                "value", 0, 10, 100);
            sub.AddTo(go);
            // 应该查询到2个值
            var initValues = sub.Rows.Values.Select(x => x.Value).ToList();
            var initIDs = sub.Rows.Keys.ToList();
            Assert.AreEqual(2, initValues.Count);

            // 订阅响应
            List<double> receivedValues = new();
            sub.ObserveAdd()
                .Subscribe(added =>
                    {
                        receivedValues.Add(added.Value);

                        sub.ObserveReplace(added.ID)
                            .Subscribe(
                                replaced => { receivedValues.Add(replaced.Value); },
                                result => receivedValues.Add(added.ID)
                            ).AddTo(ref sub.DisposeBag);
                    }
                ).AddTo(ref sub.DisposeBag);

            // 测试是否已经加入到了DisposeBag
            var countField = typeof(DisposableBag)
                .GetField("count", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.IsNotNull(countField);
            // 1 add subject， 2查询到的值到replaceSubject, 1 sub自身, 在加上2查询到的值.Subscribe,和1 add.Subscribe
            Assert.True((int)countField.GetValue(sub.DisposeBag) == 6);

            // 发送更新，等待变更
            _ = HeTuClient.Instance.CallSystem("client_index_upsert_test", 123, 1);
            await Sleep(1);
            _ = HeTuClient.Instance.CallSystem("client_index_upsert_test", 234, 2);
            await Sleep(1);
            _ = HeTuClient.Instance.CallSystem("client_index_upsert_test", 234, -1);
            await Sleep(1);

            // 检查收到的值
            Assert.AreEqual(
                new List<double>
                {
                    initValues[0],
                    initValues[1],
                    1,
                    2,
                    initIDs[0]
                },
                receivedValues);

            // 检查DisposeBag是否已Dispose
            Object.Destroy(go);
            await Sleep(1);

            var isDisposed = typeof(DisposableBag)
                .GetField("isDisposed", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.IsNotNull(isDisposed);
            Assert.IsTrue((bool)isDisposed.GetValue(sub.DisposeBag));
            Debug.Log("TestIndexSubscribeR3结束");
        }
    }
}
