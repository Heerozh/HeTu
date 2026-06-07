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

        /// <summary>停止接收新动作；已入队的动作仍会在泵线程上执行完毕。
        /// 若调用方需要在释放下游资源前确保清空，应在 Dispose 前 Post 一个收尾动作。</summary>
        public void Dispose()
        {
            // 仅 CompleteAdding 通知泵退出；不在此 Dispose _q：泵线程仍在
            // GetConsumingEnumerable 上迭代，立即 Dispose 会与之竞争（use-after-dispose）。
            // 泵线程 IsBackground=true + 每连接生命周期，进程退出时自然回收。
            try { _q.CompleteAdding(); } catch { /* 已释放 */ }
        }
    }
}
