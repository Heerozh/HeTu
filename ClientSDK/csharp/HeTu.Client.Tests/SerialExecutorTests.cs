using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using HeTu;
using NUnit.Framework;

namespace HeTu.Client.Tests
{
    public class SerialExecutorTests
    {
        [Test]
        public void AllActionsRunOnOneThreadAndComplete()
        {
            using var exec = new SerialExecutor();
            var threadIds = new ConcurrentBag<int>();
            var done = new CountdownEvent(200);

            // 从多个线程并发 Post，验证它们都在同一根线程串行执行
            Parallel.For(0, 200, i =>
                exec.Post(() =>
                {
                    threadIds.Add(Thread.CurrentThread.ManagedThreadId);
                    done.Signal();
                }));

            Assert.That(done.Wait(5000), Is.True, "200 个动作应全部执行");
            Assert.That(threadIds, Has.Count.EqualTo(200));
            Assert.That(new HashSet<int>(threadIds), Has.Count.EqualTo(1),
                "所有动作必须在同一根泵线程上执行（串行）");
        }

        [Test]
        public void PostAfterDispose_DoesNotThrow()
        {
            var exec = new SerialExecutor();
            exec.Dispose();
            // 契约：Dispose 后 Post 必须安全（静默丢弃），不得向调用方抛异常
            Assert.DoesNotThrow(() => exec.Post(() => { }));
        }
    }
}
