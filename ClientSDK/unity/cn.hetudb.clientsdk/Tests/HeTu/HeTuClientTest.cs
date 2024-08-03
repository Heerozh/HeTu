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

        [SetUp]
        public void Init()
        {
            Debug.Log("测试前请启动河图服务器的tests/app.py");
            var connected = false;
            HeTuClient.Instance.OnConnected += () =>
            {
                connected = true;
                HeTuClient.Instance.Close().Wait();
            };
            
            HeTuClient.Instance.SetLogger(Debug.Log, Debug.LogError);
            
#if HETU_CLIENT_USING_ZLIB
            HeTuClient.Instance.SetProtocol(new ZlibProtocol());
#endif
            Task.Run(async () =>
            {
                
                await HeTuClient.Instance.Connect("ws://127.0.0.1:2466/hetu", Application.exitCancellationToken); 
            }).Wait();
            Debug.Log("连接断开");
            Assert.IsTrue(connected);

        }
        
        
        [Test]
        public async Task TestSystemCall()
        {
            var response = "";
            HeTuClient.Instance.OnResponse += (msg) =>
            {
                response = msg;
            };
            HeTuClient.Instance.CallSystem("login", 1, true);
            await Task.Delay(1000);
            Assert.AreEqual("test", response);
        }
    }
}