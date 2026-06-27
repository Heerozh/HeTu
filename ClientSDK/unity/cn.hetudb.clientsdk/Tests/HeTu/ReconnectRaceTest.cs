using System;
using System.Collections;
using System.Linq;
using System.Reflection;
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
    /// <summary>
    ///     物理连接生命周期竞态的回归测试。
    ///     <para>
    ///         运行前需启动河图服务器 tests/app.py(监听
    ///         ws://127.0.0.1:2466/hetu/pytest),且必须在 <b>Player mode</b> 下跑:
    ///         Editor mode 没有 Unity 主循环驱动 UnityWebSocket 的关闭 task,close
    ///         路径会卡住(见 HeTuClient.CloseCore 注释)。
    ///     </para>
    /// </summary>
    [TestFixture]
    public class ReconnectRaceTest
    {
        private const string Url = "ws://127.0.0.1:2466/hetu/pytest";
        private const string AuthKey = "pytest";

        [OneTimeTearDown]
        public void DeInit()
        {
            HeTuClient.Instance.Close();
        }

        // 把 async Task 跑在 PlayMode 协程里,异常如实抛出让测试失败。
        private static IEnumerator RunTask(Task task)
        {
            while (!task.IsCompleted)
                yield return null;
            if (task.Exception != null)
                throw task.Exception;
        }

        /// <summary>
        ///     回归:Close() 之后不做任何等待立刻 Connect(),应当握手成功。
        ///     <para>
        ///         修复前——UnityWebSocket 的 OnClose 隔若干帧(还要等与服务器的
        ///         close 握手往返)才在主线程派发;被取代的旧 socket 这条迟到的 close
        ///         会路由到共享单例 HeTuClient 的 OnClosed,把刚建立的新连接取消,抛
        ///         OperationCanceledException("The operation was canceled.")。
        ///         修复后——ConnectCore 的"代次守卫"丢弃旧 socket 的迟到事件,这次
        ///         Connect 正常完成。
        ///     </para>
        /// </summary>
        [UnityTest]
        public IEnumerator TestReconnectImmediatelyAfterClose()
        {
            // 防御:从干净的断开态开始(避免上一个 fixture 残留的连接),并给旧
            // socket 留几帧把 close 走完。这段等待不是被测对象。
            if (HeTuClient.Instance.IsConnectionAlive)
            {
                HeTuClient.Instance.Close();
                for (var i = 0; i < 60; i++)
                    yield return null;
            }

            yield return RunTask(ReconnectImmediatelyAfterCloseAsync());
        }

        private static async Task ReconnectImmediatelyAfterCloseAsync()
        {
            // 1. 首连,等握手完成。
            await HeTuClient.Instance.Connect(Url, AuthKey);
            Assert.IsTrue(HeTuClient.Instance.IsConnectionAlive,
                "首次连接应握手成功");

            // 2. 主动关闭。CloseAsync 只是把关闭排队,旧 socket 的 OnClose 要隔
            //    若干帧才在主线程派发——此刻它还在路上。
            HeTuClient.Instance.Close();

            // 3. 零等待立刻重连。修复前这一步必然被旧 socket 迟到的 close 取消;
            //    修复后应正常返回、不抛异常。
            await HeTuClient.Instance.Connect(Url, AuthKey);
            Assert.IsTrue(HeTuClient.Instance.IsConnectionAlive,
                "Close 后零等待重连应握手成功");

            // 4.(可选,端到端确认连接确实可用)跑一次 RPC 往返。若担心与其它
            //    fixture 的服务器状态耦合,可删掉这三行,仅靠上面的握手断言即可
            //    证明 Connect 没被取消。
            var resp = await HeTuClient.Instance.CallSystem("login", 123, true);
            Assert.IsNotNull(resp, "重连后应能正常调用 System");
        }

        /// <summary>
        ///     回归:多次 Close→Connect 不应让 HeTuClient 自带的
        ///     HandleConnectionClosed 在进程级单例 OnClosed 上无限累加。
        ///     <para>
        ///         事件 += 不去重,若每次 Connect 都无脑 += 就会跨重连累加;修复后
        ///         Connect 改为幂等订阅(先 -= 再 +=),订阅数恒为 1。
        ///     </para>
        /// </summary>
        [UnityTest]
        public IEnumerator TestReconnectDoesNotLeakClosedHandler()
        {
            yield return RunTask(ReconnectDoesNotLeakClosedHandlerAsync());
        }

        private static async Task ReconnectDoesNotLeakClosedHandlerAsync()
        {
            const int cycles = 3;
            for (var i = 0; i < cycles; i++)
            {
                await HeTuClient.Instance.Connect(Url, AuthKey);
                Assert.IsTrue(HeTuClient.Instance.IsConnectionAlive,
                    $"第 {i + 1} 次连接应握手成功");
                HeTuClient.Instance.Close();
                await Settle();
            }

            // 跑了 cycles 次 Connect。幂等订阅下,无论本进程之前(本 fixture 或别的
            // fixture)连过多少次,单例 OnClosed 上 HandleConnectionClosed 都只该有
            // 一个(最后一次 Connect 挂上、Close 不解绑、故恒留 1);修复前则会随每次
            // Connect 线性累加。
            var count = CountOnClosedHandlers("HandleConnectionClosed");
            Assert.AreEqual(1, count,
                $"HandleConnectionClosed 订阅数应恒为 1,实际 {count}");
        }

#if UNITY_6000_0_OR_NEWER
        private static async Awaitable Settle() =>
            await Awaitable.WaitForSecondsAsync(0.2f);
#else
        private static async UniTask Settle() =>
            await UniTask.Delay(200);
#endif

        // 反射数一下单例 OnClosed(field-like event)上名为 methodName 的订阅数。
        // 没有公开 API 能枚举事件订阅,只能读编译器生成的 backing field——本仓测试
        // 已有读私有字段的先例(见 HeTuClientTest 对 DisposableBag 的反射)。
        private static int CountOnClosedHandlers(string methodName)
        {
            var field = typeof(HeTuClientBase).GetField("OnClosed",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.IsNotNull(field,
                "找不到 OnClosed 的 backing field(field-like event 实现变了?)");
            var del = field.GetValue(HeTuClient.Instance) as Delegate;
            if (del == null)
                return 0;
            return del.GetInvocationList()
                .Count(d => d.Method.Name == methodName);
        }
    }
}
