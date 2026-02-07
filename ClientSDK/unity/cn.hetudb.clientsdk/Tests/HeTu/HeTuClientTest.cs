using System;
using System.Collections;
using System.Linq;
using System.Threading.Tasks;
using HeTu;
using NUnit.Framework;
using UnityEngine;
using UnityEngine.TestTools;
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

        private class RLSComp : IBaseComponent
        {
            public long owner;
            public int value;
            public long id { get; set; }
        }

        private class Position : IBaseComponent
        {
            public long owner;
            public float x;
            public float y;
            public long id { get; set; }
        }

        private IEnumerator RunTask(Task task)
        {
            while (!task.IsCompleted)
            {
                yield return null;
            }

            if (task.IsFaulted)
            {
                throw task.Exception;
            }
        }

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
            HeTuClient.Instance.CallSystem("login", 123, true);
            HeTuClient.Instance.CallSystem("add_rls_comp_value", 1);
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
            HeTuClient.Instance.CallSystem("add_rls_comp_value", -2);
#if UNITY_6000_0_OR_NEWER
            await Awaitable.WaitForSecondsAsync(1);
#else
            await UniTask.Delay(1000);
#endif
            Assert.AreEqual(lastValue - 2, newValue);

            // 测试重复订阅，但换一个类型，应该报错
            HeTuClient.Instance.CallSystem("add_rls_comp_value", -1);
            // Assert.ThrowsAsync 用的是当前协程Wait，会卡死
            var success = false;
            try
            {
                await HeTuClient.Instance.Get<RLSComp>("owner", 123);
                success = true;
            }
            catch (InvalidCastException)
            {
            }

            Assert.False(success, "没有抛出InvalidCastException");

            sub = null;
            // unity delay后第二次gc才会回收
            for (var i = 0; i < 10; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
#if UNITY_6000_0_OR_NEWER
                await Awaitable.WaitForSecondsAsync(1);
#else
                await UniTask.Delay(1);
#endif
            }

            // 测试回收自动反订阅，顺带测试Class类型
            var typedSub = await HeTuClient.Instance.Get<RLSComp>(
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
            var a = HeTuClient.Instance.SystemLocalCallbacks["login"] =
                args => { callbackCalled = true; };

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
            HeTuClient.Instance.CallSystem("login", 234, true);
            HeTuClient.Instance.CallSystem("add_rls_comp_value", -1);
            var sub = await HeTuClient.Instance.Range(
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
            HeTuClient.Instance.CallSystem("add_rls_comp_value", -2);
#if UNITY_6000_0_OR_NEWER
            await Awaitable.WaitForSecondsAsync(1);
#else
            await UniTask.Delay(1000);
#endif
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
            HeTuClient.Instance.CallSystem("login", 345, true);
            HeTuClient.Instance.CallSystem("move_user", 123, -10, -10);
            HeTuClient.Instance.CallSystem("move_user", 234, 0, 0);
            HeTuClient.Instance.CallSystem("move_user", 345, 10, 10);

            // 测试OnInsert, OnDelete
            var sub = await HeTuClient.Instance.Range<Position>(
                "x", 0, 10, 100);

            long? newPlayer = null;
            sub.OnInsert += (sender, rowID) => { newPlayer = sender.Rows[rowID].owner; };
            HeTuClient.Instance.CallSystem("move_user", 123, 2, -10);
#if UNITY_6000_0_OR_NEWER
            await Awaitable.WaitForSecondsAsync(1);
#else
            await UniTask.Delay(1000);
#endif
            Assert.AreEqual(newPlayer, 123);

            // OnDelete
            long? removedPlayer = null;
            sub.OnDelete += (sender, rowID) =>
            {
                removedPlayer = sender.Rows[rowID].owner;
            };
            HeTuClient.Instance.CallSystem("move_user", 123, 11, -10);
#if UNITY_6000_0_OR_NEWER
            await Awaitable.WaitForSecondsAsync(1);
#else
            await UniTask.Delay(1000);
#endif
            Assert.AreEqual(removedPlayer, 123);

            Assert.False(sub.Rows.ContainsKey(123));
            Debug.Log("TestIndexSubscribeOnInsert结束");
        }
    }
}
