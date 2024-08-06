using System.Collections.Generic;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using HeTu;
using UnityEngine;
using NUnit.Framework;

namespace Tests.HeTu
{
    [TestFixture]
    [TestOf(typeof(HeTuClient))]
    public class HeTuClientTest
    {
        class HP : IBaseComponent
        {
            public long id { get; set; }
            public long owner;
            public int value;
        }
        
        class Position: IBaseComponent
        {
            public long id { get; set; }
            public long owner;
            public float x;
            public float y;
        }
        
        [OneTimeSetUp]
        public void Init()
        {
            Debug.Log("测试前请启动河图服务器的tests/app.py");
            // var connected = false;
            // HeTuClient.Instance.OnConnected += () =>
            // {
            //     connected = true;
            // };
            
            HeTuClient.Instance.SetLogger(Debug.Log, Debug.LogError);
            
            Task.Run(async () =>
            {
                await HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu",
                    Application.exitCancellationToken);
                Debug.Log("连接断开");
            });
            // Assert.IsTrue(connected);
        }
        
        [Test, Order(1)]  // 必须未调用过login测试才会通过
        public async Task TestRowSubscribe()
        {
            // 测试订阅失败
            var sub = await HeTuClient.Instance.Select(
                "HP", 123, "owner");
            Assert.AreEqual(sub, null);
            
            // 测试订阅
            HeTuClient.Instance.CallSystem("login", 123, true);
            HeTuClient.Instance.CallSystem("use_hp", 1);
            sub = await HeTuClient.Instance.Select(
                "HP", 123, "owner");
            var lastValue = int.Parse(sub.Data["value"]);
 
            // 测试订阅事件
            int? newValue = null;
            sub.OnUpdate += (sender) =>
            {
                Debug.Log("收到了更新...");
                newValue = int.Parse(sender.Data["value"]);
            };
            HeTuClient.Instance.CallSystem("use_hp", 2);
            await Task.Delay(1000);
            Assert.AreEqual(lastValue - 2, newValue);
            
            // 测试重复订阅，但换一个类型，应该报错
            HeTuClient.Instance.CallSystem("use_hp", 1);
            // Assert.ThrowsAsync用的是当前协程Wait，会卡死
            Assert.Throws<AggregateException>(() => Task.Run(async () => 
                await HeTuClient.Instance.Select<HP>(123, "owner")).Wait());

            sub = null;
            // unity delay后第二次gc才会回收
            for (int i = 0; i < 10; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                await Task.Delay(1);
            }
            
            // 测试回收自动反订阅，顺带测试Class类型
            var typedSub = await HeTuClient.Instance.Select<HP>(
                123, "owner");
            Assert.AreEqual(lastValue - 3, typedSub.Data.value);
            
            Debug.Log("TestRowSubscribe结束");
        }
        
        [Test, Order(2)]
        public async Task TestSystemCall()
        {
            long responseID = 0;
            HeTuClient.Instance.OnResponse += (jsonData) =>
            {
                var data = jsonData.ToObject<Dictionary<string, object>>();
                responseID = (long)data["id"];
            };
            
            bool callbackCalled = false;
            var a = HeTuClient.Instance.SystemCallbacks["login"] =
            (args) =>
            {
                callbackCalled = true;
            };
            
            HeTuClient.Instance.CallSystem("login", 123, true);
            await Task.Delay(300);
            Assert.AreEqual(123, responseID);
            
            Assert.True(callbackCalled);
            
            Debug.Log("TestSystemCall结束");
        }
        
        [Test]
        public async Task TestIndexSubscribeOnUpdate()
        {
            // 测试订阅
            HeTuClient.Instance.CallSystem("login", 234, true);
            HeTuClient.Instance.CallSystem("use_hp", 1);
            var sub = await HeTuClient.Instance.Query(
                "HP", "owner", 0, 300, 100);
            // 这是Owner权限表，应该只能取到自己的数据
            Assert.AreEqual(1, sub.Rows.Count);
            var lastValue = int.Parse(sub.Rows.Values.First()["value"]);
 
            // 测试订阅事件
            int? newValue = null;
            sub.OnUpdate += (sender, rowID) =>
            {
                newValue = int.Parse(sender.Rows[rowID]["value"]);
            };
            HeTuClient.Instance.CallSystem("use_hp", 2);
            await Task.Delay(1000);
            Assert.AreEqual(newValue, lastValue - 2);
            
            Debug.Log("TestIndexSubscribeOnUpdate结束");
        }
        
        [Test]
        public async Task TestIndexSubscribeOnInsert()
        {
            HeTuClient.Instance.CallSystem("login", 345, true);
            HeTuClient.Instance.CallSystem("move_user", 123, -10, -10);
            HeTuClient.Instance.CallSystem("move_user", 234, 0, 0);
            HeTuClient.Instance.CallSystem("move_user", 345, 10, 10);
            
            // 测试OnInsert, OnDelete
            var sub = await HeTuClient.Instance.Query<Position>(
                "x", 0, 10, 100);

            long? newPlayer = null;
            sub.OnInsert += (sender, rowID) =>
            {
                newPlayer = sender.Rows[rowID].owner;
            };
            HeTuClient.Instance.CallSystem("move_user", 123, 2, -10);
            await Task.Delay(1000);
            Assert.AreEqual(newPlayer, 123);
            
            // OnDelete
            long? removedPlayer = null;
            sub.OnDelete += (sender, rowID) =>
            {
                removedPlayer = sender.Rows[rowID].owner;
            };
            HeTuClient.Instance.CallSystem("move_user", 123, 11, -10);
            await Task.Delay(1000);
            Assert.AreEqual(removedPlayer, 123);
            
            Assert.False(sub.Rows.ContainsKey(123));
            Debug.Log("TestIndexSubscribeOnInsert结束");
        }
    }
}