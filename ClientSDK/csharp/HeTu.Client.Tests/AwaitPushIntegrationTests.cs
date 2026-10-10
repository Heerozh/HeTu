using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using HeTu;
using NUnit.Framework;

namespace HeTu.Client.Tests
{
    // 验收：等推送的调用（CallSystemAwaitPush，设计稿
    // docs/superpowers/specs/2026-10-10-rpcs-sync-design.md）返回时，这次调用改到的本连接
    // WatchRow / WatchRange 行已推到，即订阅事件（updt）先于调用返回（rsp）。覆盖行进出区间、
    // 共享订阅（组件不按 RLS 判定可见时，同一查询在 worker 内共用一个订阅对象）两种情况。
    //
    // 连真服务器跑（HeTu 仓库根目录，Redis 用自己的；--workers=1 让两条连接落在同一个 worker，
    // 共享订阅才有两个成员）：
    //   uv run hetu start --app-file=tests/app.py --namespace=pytest --instance=pytest
    //       --port=2496 --db=redis://127.0.0.1:6391/0 --workers=1
    //   HETU_URL=ws://127.0.0.1:2496/hetu/pytest dotnet test HeTu.Client.Tests
    //       --filter FullyQualifiedName~AwaitPushIntegrationTests
    // 每个用例用随机的玩家 id 与 value 区间，库里的旧数据不影响结果。
    [Explicit("需要运行中的 HeTu 服务端（tests/app.py）；设置 HETU_URL / HETU_AUTHKEY 后运行")]
    [NonParallelizable]
    public class AwaitPushIntegrationTests
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
        private static readonly Random Rng = new();

        private readonly List<IDisposable> _owned = new();
        private string _url;
        private string _authKey;

        [SetUp]
        public void SetUp()
        {
            _url = Environment.GetEnvironmentVariable("HETU_URL");
            _authKey = Environment.GetEnvironmentVariable("HETU_AUTHKEY");
            if (string.IsNullOrEmpty(_url)) Assert.Ignore("未设置 HETU_URL");
            Logger.Instance.SetLogger(_ => { }, TestContext.Progress.WriteLine,
                TestContext.Progress.WriteLine);
        }

        [TearDown]
        public void TearDown()
        {
            // 后建的先放：订阅先于它的连接
            for (var i = _owned.Count - 1; i >= 0; i--)
                _owned[i].Dispose();
            _owned.Clear();
        }

        // ---- WatchRow：改写订阅着的那一行 ----

        [Test]
        public async Task WatchRow_Private_UpdateBeforeReturn()
        {
            var me = NewId();
            var c = await ConnectAs(me);
            await Call(c, "add_rls_comp_value", 1); // 建行；RLSComp 按 owner 做 RLS：私有订阅
            var sub = Own(await c.WatchRow<DictComponent>("owner", me, "RLSComp")
                .WaitAsync(Timeout));
            Assert.That(sub, Is.Not.Null);
            var expected = Num(Convert.ToInt64(sub.Data["value"]) + 5);
            var probe = new Probe();
            sub.OnUpdate += s => probe.Add("update " + Num(s.Data["value"]));

            var seen = await SeenOnReturn(c, probe, "add_rls_comp_value", 5);

            Assert.That(seen, Does.Contain("update " + expected));
        }

        [Test]
        public async Task WatchRow_Shared_UpdateBeforeReturn()
        {
            var me = NewId();
            var c = await ConnectAs(me);
            await Call(c, "client_index_upsert_test", me, (double)me); // IndexComp1 不做 RLS：共享
            var sub = Own(await c.WatchRow<DictComponent>("owner", me, "IndexComp1")
                .WaitAsync(Timeout));
            Assert.That(sub, Is.Not.Null);
            var probe = new Probe();
            sub.OnUpdate += s => probe.Add("update " + Num(s.Data["value"]));

            var seen = await SeenOnReturn(c, probe, "client_index_upsert_test", me, me + 0.5);

            Assert.That(seen, Does.Contain("update " + Num(me + 0.5)));
        }

        // ---- WatchRange：行进出区间 ----

        [Test]
        public async Task WatchRange_Private_NewRowEntersBeforeReturn()
        {
            var me = NewId();
            var c = await ConnectAs(me);
            // 还没有行：force 订单键区间，第一次写入建行即进区间（懒建的自有行）
            var sub = Own(await c.WatchRange<DictComponent>("owner", me, me, 10,
                componentName: "RLSComp").WaitAsync(Timeout));
            Assert.That(sub.Rows, Is.Empty);
            var probe = new Probe();
            sub.OnInsert += (s, id) => probe.Add("insert " + Num(s.Rows[id]["owner"]));

            var seen = await SeenOnReturn(c, probe, "add_rls_comp_value", 1);

            Assert.That(seen, Does.Contain("insert " + Num(me)));
        }

        [Test]
        public async Task WatchRange_Shared_NewRowEntersBeforeReturn()
        {
            var me = NewId();
            var c = await ConnectAs(me);
            var sub = Own(await WatchBand(c, me));
            Assert.That(sub.Rows, Is.Empty);
            var probe = new Probe();
            sub.OnInsert += (s, id) => probe.Add("insert " + Num(s.Rows[id]["owner"]));

            var seen = await SeenOnReturn(c, probe, "client_index_upsert_test", me, (double)me);

            Assert.That(seen, Does.Contain("insert " + Num(me)));
        }

        [Test]
        public async Task WatchRange_Shared_RowLeavesAndReentersBeforeReturn()
        {
            var me = NewId();
            var c = await ConnectAs(me);
            await Call(c, "client_index_upsert_test", me, (double)me);
            var sub = Own(await WatchBand(c, me));
            var row = sub.Rows.Keys.Single();
            var probe = new Probe();
            sub.OnDelete += (_, id) => probe.Add("delete " + id);
            sub.OnInsert += (_, id) => probe.Add("insert " + id);

            var left = await SeenOnReturn(c, probe, "client_index_upsert_test", me, me + 5.0);
            var back = await SeenOnReturn(c, probe, "client_index_upsert_test", me, me + 0.5);

            Assert.That(left, Does.Contain("delete " + row), "改出区间");
            Assert.That(back, Does.Contain("insert " + row), "改回区间");
        }

        // ---- 共享订阅：别的连接也订着同一查询 ----

        [Test]
        public async Task WatchRange_SharedWithAnotherConnection_ChangesBeforeReturn()
        {
            var me = NewId();
            var other = await ConnectAs(NewId());
            var c = await ConnectAs(me);
            await Call(c, "client_index_upsert_test", me, (double)me);
            // 别的连接先订、本连接后订同一查询：同一个 worker 里本连接加入它的共享订阅
            Own(await WatchBand(other, me));
            var sub = Own(await WatchBand(c, me));
            var row = sub.Rows.Keys.Single();
            var probe = new Probe();
            sub.OnUpdate += (s, id) => probe.Add("update " + Num(s.Rows[id]["value"]));
            sub.OnDelete += (_, id) => probe.Add("delete " + id);

            var moved = await SeenOnReturn(c, probe, "client_index_upsert_test", me, me + 0.5);
            var left = await SeenOnReturn(c, probe, "client_index_upsert_test", me, me + 5.0);

            Assert.That(moved, Does.Contain("update " + Num(me + 0.5)), "区间内改值");
            Assert.That(left, Does.Contain("delete " + row), "改出区间");
        }

        // ---- 工具 ----

        // 调用返回那一刻已经发生的订阅事件。事件在泵线程上随 updt 同步触发；调用在泵线程处理
        // sync 时完成，续体排在那之后，所以快照里有它 = updt 先于调用返回
        private static async Task<string[]> SeenOnReturn(HeadlessHeTuClient c, Probe probe,
            string system, params object[] args)
        {
            probe.Clear();
            await c.CallSystemAwaitPush(system, args).WaitAsync(Timeout);
            return probe.Snapshot();
        }

        // 准备数据也等推送：它的推送不会晚到、混进下一次调用的快照
        private static Task Call(HeadlessHeTuClient c, string system, params object[] args) =>
            c.CallSystemAwaitPush(system, args).WaitAsync(Timeout);

        // IndexComp1 按 value 的区间 [id, id + 1]：用例各用各的区间
        private static Task<IndexSubscription<DictComponent>> WatchBand(HeadlessHeTuClient c,
            long id) =>
            c.WatchRange<DictComponent>("value", (double)id, id + 1.0, 10,
                componentName: "IndexComp1").WaitAsync(Timeout);

        private async Task<HeadlessHeTuClient> ConnectAs(long userId)
        {
            var c = Own(new HeadlessHeTuClient());
            await c.Connect(_url, _authKey).WaitAsync(Timeout);
            await c.CallSystem("login", userId).WaitAsync(Timeout);
            return c;
        }

        private T Own<T>(T disposable) where T : IDisposable
        {
            if (disposable != null) _owned.Add(disposable);
            return disposable;
        }

        private static long NewId()
        {
            lock (Rng) return Rng.NextInt64(1_000_000_000L, 4_000_000_000L);
        }

        private static string Num(object value) =>
            Convert.ToDouble(value, CultureInfo.InvariantCulture)
                .ToString("R", CultureInfo.InvariantCulture);

        // 泵线程写、测试线程读
        private sealed class Probe
        {
            private readonly List<string> _events = new();

            public void Add(string e)
            {
                lock (_events) _events.Add(e);
            }

            public void Clear()
            {
                lock (_events) _events.Clear();
            }

            public string[] Snapshot()
            {
                lock (_events) return _events.ToArray();
            }
        }
    }
}
