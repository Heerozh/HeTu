using System;
using System.Threading.Tasks;
using HeTu;
using NUnit.Framework;

namespace HeTu.Client.Tests
{
    public class HeadlessHeTuClientTests
    {
        // 验收 #2：无单例，一个进程可构造多条独立连接实例并各自释放。
        [Test]
        public void MultipleInstances_NoSingleton_ConstructAndDispose()
        {
            Assert.DoesNotThrow(() =>
            {
                for (var i = 0; i < 3; i++)
                {
                    using var c = new HeadlessHeTuClient();
                }
            });
        }

        // 端到端（无需服务端）验证 传输→泵→基类 OnClosed→Task 外观 的失败路径：
        // 连到拒绝连接的端口 → WebSocketTransport 报错 → 基类 HandleClosed → OnClosed → Connect 的 Task 异常完成。
        [Test]
        public async Task Connect_ToRefusedPort_TaskFaults()
        {
            using var c = new HeadlessHeTuClient();
            var connect = c.Connect("ws://127.0.0.1:1/nope"); // 拒绝连接的端口
            try
            {
                await connect.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Fail("连接到拒绝端口应使 Connect 的 Task 异常完成");
            }
            catch (Exception ex)
            {
                Assert.That(ex, Is.Not.InstanceOf<TimeoutException>(),
                    "应为连接失败异常而非 5 秒超时（超时即说明失败未传播到 Task）");
            }
            Assert.That(c.IsConnected, Is.False, "失败后应为未连接");
        }

        // WaitClosedAsync 在未连接（断开态）时应立即完成而非永久挂起。
        [Test]
        public async Task WaitClosedAsync_WhenNotConnected_CompletesImmediately()
        {
            using var c = new HeadlessHeTuClient();
            var reason = await c.WaitClosedAsync().WaitAsync(TimeSpan.FromSeconds(2));
            Assert.That(reason, Is.Null, "断开态视为正常关闭，原因为 null");
        }

        // Close() 路由到泵线程；对未连接实例调用应安全不抛。
        [Test]
        public void Close_OnFreshClient_DoesNotThrow()
        {
            using var c = new HeadlessHeTuClient();
            Assert.DoesNotThrow(() => c.Close());
        }
    }
}
