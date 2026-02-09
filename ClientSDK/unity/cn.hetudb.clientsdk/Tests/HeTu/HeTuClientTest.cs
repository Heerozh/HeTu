using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using HeTu;
using HeTu.Extensions;
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
            HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu/pytest").Forget();
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
            public long id { get; set; }
        }

        [MessagePackObject]
        public class IndexComp1 : IBaseComponent
        {
            [Key("owner")] public long Owner;

            [Key("value")] public double Value;

            [Key("id")] public long id { get; set; }
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
        private static async Awaitable Sleep(int seconds)
        {
            await Awaitable.WaitForSecondsAsync(seconds);
        }
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
            HeTuClient.Instance.CallSystem("login", 123, true).Forget();
            HeTuClient.Instance.CallSystem("add_rls_comp_value", 1).Forget();
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
            HeTuClient.Instance.CallSystem("add_rls_comp_value", -2).Forget();

            await Sleep(1);
            Assert.AreEqual(lastValue - 2, newValue);

            // 测试重复订阅，但换一个类型，应该报错
            HeTuClient.Instance.CallSystem("add_rls_comp_value", -1).Forget();
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
            HeTuClient.Instance.CallSystem("login", 234, true).Forget();
            HeTuClient.Instance.CallSystem("add_rls_comp_value", -1).Forget();
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
            HeTuClient.Instance.CallSystem("add_rls_comp_value", -2).Forget();

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
            HeTuClient.Instance.CallSystem("login", 345, true).Forget();
            HeTuClient.Instance.CallSystem("client_index_upsert_test", 123, -10).Forget();
            HeTuClient.Instance.CallSystem("client_index_upsert_test", 234, 0).Forget();
            HeTuClient.Instance.CallSystem("client_index_upsert_test", 345, 10).Forget();

            // 测试OnInsert, OnDelete
            using var sub = await HeTuClient.Instance.Range<IndexComp1>(
                "value", 0, 10, 100);

            long? newPlayer = null;
            sub.OnInsert += (sender, rowID) => { newPlayer = sender.Rows[rowID].Owner; };
            HeTuClient.Instance.CallSystem("client_index_upsert_test", 123, 2).Forget();

            await Sleep(1);
            Assert.AreEqual(newPlayer, 123);

            // OnDelete
            long? removedPlayer = null;
            sub.OnDelete += (sender, rowID) =>
            {
                removedPlayer = sender.Rows[rowID].Owner;
            };
            HeTuClient.Instance.CallSystem("client_index_upsert_test", 123, 11)
                .Forget();

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

            HeTuClient.Instance.CallSystem("login", 456, true).Forget();
            HeTuClient.Instance.CallSystem("add_rls_comp_value", 1).Forget();
            var sub = await HeTuClient.Instance.Get<RLSComp>(
                "owner", 456);
            sub.AddTo(go);
            var initValue = sub.Data.value;

            List<int> receivedValues = new();
            var observer = sub.ToReactiveProperty()
                .Subscribe(x => receivedValues.Add(x.value));
            // todo 考虑是否让SubscribeCore的订阅，全部关注到sub里，不需要外面dispose两个
            // Disposable.Combine(sub, observer).AddTo(go);


            HeTuClient.Instance.CallSystem("add_rls_comp_value", 2).Forget();
            await Sleep(1);
            HeTuClient.Instance.CallSystem("add_rls_comp_value", -3).Forget();
            await Sleep(1);
            HeTuClient.Instance.CallSystem("add_rls_comp_value", 1).Forget();
            await Sleep(1);

            Assert.AreEqual(
                new List<int> { initValue, initValue + 2, initValue - 1, initValue },
                receivedValues);

            Object.Destroy(go);
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
            HeTuClient.Instance.CallSystem("login", 456, true).Forget();
            HeTuClient.Instance.CallSystem("client_index_upsert_test", 456, 5).Forget();

            using var sub = await HeTuClient.Instance.Range<IndexComp1>(
                "value", 0, 10, 100);
        }
    }
}
