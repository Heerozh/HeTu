using System;
using System.Threading.Tasks;
using HeTu;
using NUnit.Framework;

namespace HeTu.Client.Tests
{
    [Explicit("需要运行中的 HeTu 服务端；设置 HETU_URL / HETU_AUTHKEY 后运行")]
    public class IntegrationSmokeTests
    {
        [Test]
        public async Task ConnectAndCallSystem()
        {
            var url = Environment.GetEnvironmentVariable("HETU_URL");      // ws://127.0.0.1:2466/hetu/<inst>
            var authKey = Environment.GetEnvironmentVariable("HETU_AUTHKEY");
            if (string.IsNullOrEmpty(url)) Assert.Ignore("未设置 HETU_URL");

            using var c = new HeadlessHeTuClient();
            await c.Connect(url, authKey).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.That(c.IsConnected, Is.True, "握手后应为已连接");

            // 调一个无需登录的 System 或 echo；具体名以目标 app 为准。
            // 这里以最小可达性为目标：连接 + 握手成功即视为冒烟通过。
        }
    }
}
