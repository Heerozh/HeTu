using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using HeTu;
using MessagePack;
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

        // 参数没法序列化：编码在泵线程上抛出，Task 要以这个异常结束，不能一直挂着（泵只记日志）。
        [Test]
        public void CallSystem_EncodeFailure_FaultsTask()
        {
            using var c = new HeadlessHeTuClient();
            typeof(HeTuClientBase)
                .GetField("State", BindingFlags.NonPublic | BindingFlags.Instance)!
                .SetValue(c, ConnectionState.Connected);
            Action unserializable = () => { };

            foreach (var call in new[]
                     {
                         c.CallSystem("buy", unserializable),
                         c.CallSystemAwaitPush("buy", unserializable)
                     })
            {
                Assert.That(async () => await call.WaitAsync(TimeSpan.FromSeconds(2)),
                    Throws.Exception.Not.InstanceOf<TimeoutException>()
                        .And.Not.InstanceOf<OperationCanceledException>());
            }
        }

        // 等推送的调用同 CallSystem 走泵线程：未连接时以取消结束，不会一直挂着。
        [Test]
        public void CallSystemAwaitPush_WhenNotConnected_IsCanceled()
        {
            using var c = new HeadlessHeTuClient();
            var call = c.CallSystemAwaitPush("buy", 1);
            Assert.That(async () => await call.WaitAsync(TimeSpan.FromSeconds(2)),
                Throws.InstanceOf<OperationCanceledException>());
        }

        // 订阅句柄的 Dispose 常在调用方线程上调用（WatchXxx 的续体跑在线程池），但共享核心、
        // 订阅表、挂起的 unsub 只能在泵线程上动：Dispose 要投到泵线程执行。
        [Test]
        public void SubscriptionDispose_FromCallerThread_RunsOnPump()
        {
            using var c = new HeadlessHeTuClient();
            var sub = new RowSubscription<DictComponent>("Item.id[1:None:1][:1]", "Item",
                new DictComponent { ["id"] = 1L }, c);
            var probe = new ThreadProbe();
            sub.DisposeBag.Add(probe);

            sub.Dispose();

            Assert.That(probe.Disposed.Wait(TimeSpan.FromSeconds(5)), Is.True);
            Assert.That(probe.ThreadName, Is.EqualTo("HeTuClientPump"));
        }

        // 复合索引的按组订阅：group 不合法时在调用方线程上当场抛，不投到泵线程（泵上抛出只会记日志，
        // Task 永远挂着）
        [Test]
        public void WatchWithInvalidGroup_ThrowsOnCallerThread()
        {
            using var c = new HeadlessHeTuClient();
            Assert.Throws<ArgumentException>(() => c.WatchRange<DictComponent>(
                "level", 10L, 20L, 100, componentName: "Item",
                group: new Dictionary<string, object> { ["zone"] = 1.5 }));
            Assert.Throws<ArgumentException>(() => c.WatchRow<DictComponent>(
                "name", "abc", "Item",
                new Dictionary<string, object> { ["zone"] = null }));
            Assert.Throws<ArgumentException>(() => c.WatchRow<DictComponent>(
                "id", 7L, "Item", new Dictionary<string, object> { ["zone"] = 3 }));
        }

        // group 在调用方线程上就拷贝好：动作投到泵线程之后，调用方马上改字典也不影响发出的请求和
        // 重连恢复用的参数
        [Test]
        public async Task WatchRange_WithGroup_CopiedBeforePostingToPump()
        {
            const string serverSubId = "Item.level[10:20:1][:100]{zone=3}";
            using var c = PayloadCapturingClient(out var payloads);
            using var gate = new ManualResetEventSlim();
            c.RunSerialized(() => gate.Wait(TimeSpan.FromSeconds(5))); // 先堵住泵

            var group = new Dictionary<string, object> { ["zone"] = 3 };
            var watch = c.WatchRange<DictComponent>("level", 10L, 20L, 100,
                componentName: "Item", group: group);
            group["zone"] = 4;
            gate.Set();
            Receive(c, new object[] { "sub", serverSubId, Array.Empty<object>() });

            using var sub = await watch.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.That(sub.SubId, Is.EqualTo(serverSubId));
            var restoreGroup = ((ManagedIndexCore<DictComponent>)sub.Core).RestoreGroup;
            Assert.That(restoreGroup["zone"], Is.EqualTo(3));
            Assert.That(payloads, Has.Some.EqualTo(
                "[\"sub\", \"Item\", \"range\", \"level\", 10, 20, 100, False, True, " +
                "{\"zone\": 3}]"));
        }

        [Test]
        public async Task WatchRow_WithGroup_CopiedBeforePostingToPump()
        {
            using var c = PayloadCapturingClient(out var payloads);
            using var gate = new ManualResetEventSlim();
            c.RunSerialized(() => gate.Wait(TimeSpan.FromSeconds(5)));

            var group = new Dictionary<string, object> { ["zone"] = 3 };
            var watch = c.WatchRow<DictComponent>("name", "abc", "Item", group);
            group["zone"] = 4;
            gate.Set();
            var row = new Dictionary<string, object> { ["id"] = 7L };
            Receive(c, new object[] { "sub", "Item.id[7:None:1][:1]", row });

            using var sub = await watch.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.That(sub.Data.ID, Is.EqualTo(7));
            Assert.That(payloads, Has.Some.EqualTo(
                "[\"sub\", \"Item\", \"get\", \"name\", \"abc\", {\"zone\": 3}]"));
        }

        // 已连接（只有 JsonbLayer，发送是空操作）、Inspector 记下每个请求的 payload 的客户端
        private static HeadlessHeTuClient PayloadCapturingClient(
            out ConcurrentQueue<string> payloads)
        {
            var c = new HeadlessHeTuClient();
            c.SetupPipeline(new List<MessageProcessLayer> { new JsonbLayer() });
            typeof(HeTuClientBase)
                .GetField("State", BindingFlags.NonPublic | BindingFlags.Instance)!
                .SetValue(c, ConnectionState.Connected);
            var capture = new PayloadCapture();
            c.ConfigureInspector(true);
            c.AddInspectorDispatcher(capture);
            payloads = capture.Payloads;
            return c;
        }

        // 在泵线程上把一帧当作服务端发来的消息处理
        private static void Receive(HeadlessHeTuClient c, object[] frame)
        {
            var bytes = MessagePackSerializer.Serialize(frame);
            var onReceived = typeof(HeTuClientBase).GetMethod("OnReceived",
                BindingFlags.NonPublic | BindingFlags.Instance)!;
            c.RunSerialized(() => onReceived.Invoke(c, new object[] { bytes }));
        }

        private sealed class PayloadCapture : IInspectorTraceDispatcher
        {
            public readonly ConcurrentQueue<string> Payloads = new();

            // 同一个请求会分发好几次（发出、更新尺寸、完成），payload 相同，记一次就够
            public void Dispatch(InspectorTraceEvent traceEvent)
            {
                var payload = traceEvent.Payload;
                if (traceEvent.Status == "pending" && !Payloads.Contains(payload))
                    Payloads.Enqueue(payload);
            }
        }

        private sealed class ThreadProbe : IDisposable
        {
            public readonly ManualResetEventSlim Disposed = new();
            public string ThreadName;

            public void Dispose()
            {
                ThreadName = Thread.CurrentThread.Name;
                Disposed.Set();
            }
        }
    }
}
