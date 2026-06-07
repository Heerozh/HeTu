using System;
using System.Collections.Concurrent;
using System.Threading;

namespace HeTu
{
    /// <summary>每连接一个：把所有动作排到单线程串行执行，匹配 SDK 的单线程假设
    /// （CryptoLayer/Pipeline 非线程安全，收发须串行）。</summary>
    internal sealed class SerialExecutor : IDisposable
    {
        private readonly BlockingCollection<Action> _q = new();
        private readonly Thread _thread;

        public SerialExecutor()
        {
            _thread = new Thread(Run) { IsBackground = true, Name = "HeTuClientPump" };
            _thread.Start();
        }

        public void Post(Action action)
        {
            if (action == null || _q.IsAddingCompleted) return;
            // IsAddingCompleted 只能缩小竞态窗口：Dispose() 仍可能在上面的检查与
            // 下面的 Add 之间调用 CompleteAdding()，此时 Add 会抛 InvalidOperationException。
            // 断连时调用方线程并发 Post + Dispose 会命中该窗口，捕获并丢弃即可（动作本就该停）。
            try { _q.Add(action); }
            catch (InvalidOperationException) { /* CompleteAdding 抢先：丢弃 */ }
        }

        private void Run()
        {
            foreach (var a in _q.GetConsumingEnumerable())
            {
                try { a(); }
                catch (Exception ex) { Logger.Instance.Error($"pump action threw: {ex}"); }
            }
        }

        /// <summary>停止接收新动作；已入队的动作会在泵线程上执行完毕，随后泵线程退出。
        /// 返回前会（有上限地）等待泵线程结束，从而调用方可在 Dispose 后安全释放下游资源
        /// （Pipeline / socket）而不与排空中的动作竞争。</summary>
        public void Dispose()
        {
            // CompleteAdding 通知泵排空后退出；不在此 Dispose _q：泵线程仍在
            // GetConsumingEnumerable 上迭代，立即 Dispose 会与之竞争（use-after-dispose）。
            try { _q.CompleteAdding(); } catch { /* 已释放 */ }
            // 等待泵把已入队动作排空并退出，使 teardown 确定化。
            // 自我 join 守卫：若从泵线程内部调用 Dispose，跳过 join 以免死锁。
            // 超时（动作阻塞）时不静默继续而是记一条错误日志，便于诊断病态动作。
            if (Thread.CurrentThread != _thread && !_thread.Join(TimeSpan.FromSeconds(5)))
                Logger.Instance.Error("SerialExecutor: 泵线程 5s 内未退出，继续 teardown（可能有动作阻塞）");
        }
    }
}
