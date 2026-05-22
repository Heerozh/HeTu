using HeTu;
using NUnit.Framework;

namespace Tests.HeTu
{
    [TestFixture]
    public class LoggerTest
    {
        [Test]
        public void Info_PrependsColoredHeTuClientTag()
        {
            object captured = null;
            Logger.Instance.SetLogger(m => captured = m, _ => { }, _ => { });

            Logger.Instance.Info("连接成功。");

            Assert.AreEqual("[<color=#19aaff>HeTuClient</color>] 连接成功。",
                captured);
        }

        [Test]
        public void Error_PrependsColoredHeTuClientTag()
        {
            object captured = null;
            Logger.Instance.SetLogger(_ => { }, m => captured = m, _ => { });

            Logger.Instance.Error("连接失败。");

            Assert.AreEqual("[<color=#19aaff>HeTuClient</color>] 连接失败。",
                captured);
        }

        [Test]
        public void Debug_PrependsColoredHeTuClientTag()
        {
            object captured = null;
            Logger.Instance.SetLogger(_ => { }, _ => { }, m => captured = m);

            Logger.Instance.Debug("调试信息。");

            Assert.AreEqual("[<color=#19aaff>HeTuClient</color>] 调试信息。",
                captured);
        }
    }
}
