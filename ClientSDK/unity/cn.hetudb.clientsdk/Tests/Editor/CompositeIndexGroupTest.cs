using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using HeTu;
using NUnit.Framework;

namespace Tests.HeTu
{
    // 复合索引（服务端 property_field(groupby=...)）的按组订阅：WatchRow / WatchRange 带 group
    // （{分组列: 值}），见设计稿 docs/superpowers/specs/2026-11-08-composite-index-design.md §7。
    // 发出的帧经真实的 JsonbLayer 编码，再按 MessagePack 原样解回来检查（见 FrameClient）。
    [TestFixture]
    public class CompositeIndexGroupTest
    {
        private const string SingleColumnKey = "Item.level[10:20:1][:100]";

        // 服务端对带 group 的查询回的 sub_id：单列格式后面跟 {键=repr(值),...}，键按名字排序
        private const string ServerSubId = SingleColumnKey + "{zone=3}";

        [SetUp]
        public void SilenceLogger() =>
            Logger.Instance.SetLogger(_ => { }, _ => { }, _ => { });

        // 测试里没 Dispose 的订阅在这里就回收，泄漏日志不会落到之后别的测试里
        [TearDown]
        public void CollectUndisposedSubscriptions()
        {
            Logger.Instance.SetLogger(_ => { }, _ => { }, _ => { });
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        // ---------- 查询键（MakeSubId） ----------

        [Test]
        public void MakeSubId_NoGroup_KeepsSingleColumnFormat()
        {
            Assert.AreEqual(SingleColumnKey,
                HeTuClientBase.MakeSubId("Item", "level", 10L, 20L, 100, false));
            Assert.AreEqual(SingleColumnKey,
                HeTuClientBase.MakeSubId("Item", "level", 10L, 20L, 100, false, null));
            Assert.AreEqual(SingleColumnKey, Key(new Dictionary<string, object>()),
                "空字典当作没给 group");
            Assert.AreEqual("Item.id[7:None:1][:1]",
                HeTuClientBase.MakeSubId("Item", "id", 7L, null, 1, false));
            Assert.AreEqual("Item.owner[1:1:-1][:10]",
                HeTuClientBase.MakeSubId("Item", "owner", 1L, 1L, 10, true));
        }

        [Test]
        public void MakeSubId_WithGroup_StartsWithMarkerThenSingleColumnFormat()
        {
            // 字符串断言一律按 Ordinal：按区域设置比较时 \u0001 这类控制字符会被忽略
            var key = Key(Group(3));
            var expectedStart = "\u0001" + SingleColumnKey;
            Assert.AreEqual('\u0001', key[0]);
            Assert.IsTrue(key.StartsWith(expectedStart, StringComparison.Ordinal), key);
            Assert.AreNotEqual(ServerSubId, key, "客户端键不能撞上服务端 sub_id");
        }

        [Test]
        public void MakeSubId_WithGroup_FormatIsPinned()
        {
            // 键按 Ordinal 排（'B' < 'a' < 'zone'）；整数 n:，字符串 s:<UTF-8 字节数>:，
            // bytes b:<小写 hex>
            var key = Key(new Dictionary<string, object>
            {
                ["zone"] = -3,
                ["a"] = "中x",
                ["B"] = new byte[] { 0x0f, 0xA0 }
            });
            Assert.AreEqual(
                "\u0001" + SingleColumnKey + "{B=b:0fa0,a=s:4:中x,zone=n:-3}", key);
        }

        [Test]
        public void MakeSubId_StringVsBytes_Differ()
        {
            var bytes = new byte[] { 0x41, 0x42 };
            Assert.AreNotEqual(
                Key(new Dictionary<string, object> { ["b"] = "4142" }),
                Key(new Dictionary<string, object> { ["b"] = bytes }));
        }

        [Test]
        public void MakeSubId_SeparatorsInsideStringValues_Differ()
        {
            Assert.AreNotEqual(
                Key(new Dictionary<string, object> { ["a"] = "x,b=y", ["b"] = "z" }),
                Key(new Dictionary<string, object> { ["a"] = "x", ["b"] = "y,b=z" }));
            // 只加类型标记、不带长度的格式在这一对上照样会撞
            Assert.AreNotEqual(
                Key(new Dictionary<string, object> { ["a"] = "x,b=s:y", ["b"] = "z" }),
                Key(new Dictionary<string, object> { ["a"] = "x", ["b"] = "y,b=s:z" }));
        }

        [Test]
        public void MakeSubId_ValuesOfDifferentTypes_NeverCollide()
        {
            // 服务端把这些都当作不同的分组值（或不同列类型下的值），客户端键也必须两两不同
            var values = new object[]
            {
                3, "3", new byte[] { 3 }, new byte[] { 0x33 }, "33", 33,
                "4142", new byte[] { 0x41, 0x42 }, 4142, "AB",
                "n:3", "b:33", "s:1:3", "", Array.Empty<byte>(), 0, -3, 30, true, "True"
            };
            var keys = values
                .Select(v => Key(new Dictionary<string, object> { ["b"] = v }))
                .ToList();
            Assert.AreEqual(keys.Count, keys.Distinct().Count(), string.Join("\n", keys));
        }

        [Test]
        public void MakeSubId_DifferentKeySetsOrPairings_NeverCollide()
        {
            var groups = new[]
            {
                new Dictionary<string, object> { ["a"] = 1 },
                new Dictionary<string, object> { ["b"] = 1 },
                new Dictionary<string, object> { ["a"] = 1, ["b"] = 1 },
                new Dictionary<string, object> { ["a"] = 1, ["b"] = 2 },
                new Dictionary<string, object> { ["a"] = 2, ["b"] = 1 },
                new Dictionary<string, object> { ["a,b"] = 1 },
                new Dictionary<string, object> { ["a"] = "1,b=n:1" },
                new Dictionary<string, object> { ["a"] = "n:1,b=n:1" },
                new Dictionary<string, object> { ["a"] = "x}", ["b"] = "y" },
                new Dictionary<string, object> { ["a"] = "x", ["b"] = "}y" }
            };
            var keys = groups.Select(Key).ToList();
            Assert.AreEqual(keys.Count, keys.Distinct().Count(), string.Join("\n", keys));
        }

        [Test]
        public void MakeSubId_SameValueInDifferentNumericTypes_SameKey()
        {
            var values = new object[]
            {
                3, 3L, (short)3, (sbyte)3, (byte)3, (ushort)3, 3u, 3UL, 3.0, 3.0f
            };
            var keys = values
                .Select(v => Key(new Dictionary<string, object> { ["zone"] = v }))
                .Distinct()
                .ToList();
            Assert.AreEqual(1, keys.Count, string.Join("\n", keys));

            // 服务端把 True 和 1 当作同一组
            Assert.AreEqual(Key(Group(1)), Key(Group(true)));
            Assert.AreEqual(Key(Group(0L)), Key(Group(false)));
            Assert.AreEqual(Key(Group(0)), Key(Group(-0.0)));
            Assert.AreEqual(Key(Group(9223372036854775808UL)),
                Key(Group(9223372036854775808.0)));
            Assert.AreEqual(Key(Group(long.MinValue)),
                Key(Group(-9223372036854775808.0)));
        }

        [Test]
        public void MakeSubId_InsertionOrder_DoesNotMatter()
        {
            Assert.AreEqual(
                Key(new Dictionary<string, object> { ["zone"] = 3, ["guild"] = "x" }),
                Key(new Dictionary<string, object> { ["guild"] = "x", ["zone"] = 3 }));
        }

        [Test]
        [SetCulture("sv-SE")]
        public void MakeSubId_GroupNumbers_UseInvariantCulture()
        {
            // 有的区域设置负号不是 '-'（sv-SE 用 U+2212）
            foreach (var (value, tail) in new (object, string)[]
                     {
                         (-3, "{zone=n:-3}"),
                         (-3.0, "{zone=n:-3}"),
                         (long.MinValue, "{zone=n:-9223372036854775808}")
                     })
            {
                var key = Key(Group(value));
                Assert.IsTrue(key.EndsWith(tail, StringComparison.Ordinal), key);
            }
        }

        // ---------- 入口检查 ----------

        private static IEnumerable<TestCaseData> InvalidGroups()
        {
            TestCaseData Value(string label, object value) =>
                new(label, new Dictionary<string, object> { ["zone"] = value });

            yield return Value("null", null);
            yield return Value("非整数 double", 3.5);
            yield return Value("非整数 float", 0.5f);
            yield return Value("NaN", double.NaN);
            yield return Value("正无穷", double.PositiveInfinity);
            yield return Value("负无穷", float.NegativeInfinity);
            yield return Value("超出 64 位整数的浮点", 1e20);
            yield return Value("2^64", 18446744073709551616.0);
            yield return Value("低于 -2^63", -9.3e18);
            yield return Value("decimal", 3m);
            yield return Value("char", 'a');
            yield return Value("枚举", DayOfWeek.Monday);
            yield return Value("列表", new List<object> { 1 });
            yield return Value("字典", new Dictionary<string, object> { ["x"] = 1 });
            yield return Value("DateTime", new DateTime(2026, 1, 1));
            yield return new TestCaseData("空键",
                new Dictionary<string, object> { [""] = 1 });
            yield return new TestCaseData("键含 =",
                new Dictionary<string, object> { ["zone=n:1"] = 1 });
        }

        [TestCaseSource(nameof(InvalidGroups))]
        public void WatchRange_InvalidGroup_ThrowsWithoutSending(string label,
            Dictionary<string, object> group)
        {
            var client = FrameClient.Connected();
            Assert.Throws<ArgumentException>(() => client.WatchRangeSync<DictComponent>(
                "level", 10L, 20L, 100, (_, _, _) => Assert.Fail("不该回调"),
                componentName: "Item", group: group), label);
            Assert.AreEqual(0, client.CountSent("sub"));
        }

        [TestCaseSource(nameof(InvalidGroups))]
        public void WatchRow_InvalidGroup_ThrowsWithoutSending(string label,
            Dictionary<string, object> group)
        {
            var client = FrameClient.Connected();
            Assert.Throws<ArgumentException>(() => client.WatchRowSync<DictComponent>(
                "name", "abc", (_, _, _) => Assert.Fail("不该回调"), "Item", group), label);
            Assert.AreEqual(0, client.CountSent("sub"));
        }

        [Test]
        public void IdIndexWithGroup_Throws()
        {
            // WatchRow 在 id 上走预测键，group 会被丢掉；id 也不可能是复合索引
            var client = FrameClient.Connected();
            Assert.Throws<ArgumentException>(() => client.WatchRowSync<DictComponent>(
                "id", 7L, (_, _, _) => { }, "Item", Group(3)));
            Assert.Throws<ArgumentException>(() => client.WatchRangeSync<DictComponent>(
                "id", 1L, 9L, 10, (_, _, _) => { }, componentName: "Item",
                group: Group(3)));
            Assert.AreEqual(0, client.CountSent("sub"));
        }

        [Test]
        public void IdIndexWithEmptyGroup_IsAPlainIdWatch()
        {
            var client = FrameClient.Connected();
            client.WatchRowSync<DictComponent>("id", 7L, (_, _, _) => { }, "Item",
                new Dictionary<string, object>());
            Assert.AreEqual(new object[] { "sub", "Item", "get", "id", 7L },
                client.LastSent);
        }

        [Test]
        public void WatchRange_WhitelistedValueTypes_AreSentAsGiven()
        {
            var client = FrameClient.Connected();
            var group = new Dictionary<string, object>
            {
                ["i"] = 1,
                ["l"] = -2L,
                ["s"] = (short)3,
                ["sb"] = (sbyte)-4,
                ["b"] = (byte)5,
                ["us"] = (ushort)6,
                ["ui"] = 7u,
                ["ul"] = ulong.MaxValue,
                ["t"] = true,
                ["d"] = 8.0,
                ["f"] = 9.0f,
                ["big"] = 9223372036854775808.0,
                ["min"] = -9223372036854775808.0,
                ["str"] = "x",
                ["empty"] = "",
                ["raw"] = new byte[] { 1, 255 },
                ["noraw"] = Array.Empty<byte>()
            };

            client.WatchRangeSync<DictComponent>("level", 10L, 20L, 100,
                (_, _, _) => { }, componentName: "Item", group: group);

            var sent = (IDictionary)client.LastSent[9];
            Assert.AreEqual(group.Count, sent.Count);
            Assert.AreEqual(1L, Convert.ToInt64(sent["i"]));
            Assert.AreEqual(-2L, Convert.ToInt64(sent["l"]));
            Assert.AreEqual(-4L, Convert.ToInt64(sent["sb"]));
            Assert.AreEqual(ulong.MaxValue, Convert.ToUInt64(sent["ul"]));
            Assert.AreEqual(true, sent["t"]);
            Assert.AreEqual(8.0, Convert.ToDouble(sent["d"]));
            Assert.AreEqual(9223372036854775808.0, Convert.ToDouble(sent["big"]));
            Assert.AreEqual("x", sent["str"]);
            Assert.AreEqual("", sent["empty"]);
            CollectionAssert.AreEqual(new byte[] { 1, 255 }, (byte[])sent["raw"]);
            CollectionAssert.IsEmpty((byte[])sent["noraw"]);
        }

        // ---------- payload ----------

        [Test]
        public void WatchRange_Payload_AppendsGroupOnlyWhenGiven()
        {
            var client = FrameClient.Connected();
            client.WatchRangeSync<DictComponent>("owner", 1L, 1L, 10, (_, _, _) => { },
                componentName: "Item");
            Assert.AreEqual(
                new object[]
                    { "sub", "Item", "range", "owner", 1L, 1L, 10L, false, true },
                client.LastSent);
            client.WatchRangeSync<DictComponent>("owner", 1L, 1L, 10, (_, _, _) => { },
                componentName: "Item", group: new Dictionary<string, object>());
            Assert.AreEqual(9, client.LastSent.Length, "空字典当作没给 group");

            client.WatchRangeSync<DictComponent>("level", 10L, 20L, 100, (_, _, _) => { },
                true, false, "Item",
                new Dictionary<string, object> { ["zone"] = 3, ["name"] = "abc" });

            var frame = client.LastSent;
            Assert.AreEqual(10, frame.Length);
            Assert.AreEqual(
                new object[]
                    { "sub", "Item", "range", "level", 10L, 20L, 100L, true, false },
                frame.Take(9).ToArray());
            var group = (IDictionary)frame[9];
            Assert.AreEqual(2, group.Count);
            Assert.AreEqual(3L, Convert.ToInt64(group["zone"]));
            Assert.AreEqual("abc", group["name"]);
        }

        [Test]
        public void WatchRow_Payload_AppendsGroupOnlyWhenGiven()
        {
            var client = FrameClient.Connected();
            client.WatchRowSync<DictComponent>("name", "abc", (_, _, _) => { }, "Item");
            Assert.AreEqual(new object[] { "sub", "Item", "get", "name", "abc" },
                client.LastSent);
            client.WatchRowSync<DictComponent>("name", "abc", (_, _, _) => { }, "Item",
                new Dictionary<string, object>());
            Assert.AreEqual(5, client.LastSent.Length, "空字典当作没给 group");

            client.WatchRowSync<DictComponent>("name", "abc", (_, _, _) => { }, "Item",
                Group(3));

            var frame = client.LastSent;
            Assert.AreEqual(6, frame.Length);
            Assert.AreEqual(new object[] { "sub", "Item", "get", "name", "abc" },
                frame.Take(5).ToArray());
            Assert.AreEqual(3L, Convert.ToInt64(((IDictionary)frame[5])["zone"]));
        }

        [Test]
        public void DictOverloads_ForwardGroup()
        {
            var client = FrameClient.Connected();
            client.WatchRowSync("name", "abc", (_, _, _) => { }, "Item", Group(3));
            Assert.AreEqual(6, client.LastSent.Length);
            client.WatchRangeSync("Item", "level", 10L, 20L, 100, (_, _, _) => { },
                group: Group(3));
            Assert.AreEqual(10, client.LastSent.Length);
            var sent = (IDictionary)client.LastSent[9];
            Assert.AreEqual(3L, Convert.ToInt64(sent["zone"]));
        }

        // ---------- 查重、共享、退订 ----------

        [Test]
        public void WatchRange_SameGroup_SharesCoreThroughAlias()
        {
            // 客户端键和服务端 sub_id 对不上：用服务端回的 sub_id 登记核心，客户端键记成别名
            var client = FrameClient.Connected();
            var a = client.WatchGroupRange(Group(3), ServerSubId, Row(7, 10));
            Assert.AreEqual(ServerSubId, a.SubId);

            var b = client.WatchGroupRange(Group(3));
            var c = client.WatchGroupRange(Group(3.0));
            Assert.AreEqual(1, client.CountSent("sub"), "同一组再订不再发 sub");
            Assert.AreSame(a.Core, b.Core);
            Assert.AreSame(a.Core, c.Core);

            client.Receive(Update(ServerSubId, 7, Row(7, 11)));
            Assert.AreEqual(11, Value(c.Rows[7]));

            var other = client.WatchGroupRange(Group(4), SingleColumnKey + "{zone=4}");
            Assert.AreEqual(2, client.CountSent("sub"));
            Assert.AreNotSame(a.Core, other.Core);

            a.Dispose();
            b.Dispose();
            Assert.AreEqual(0, client.CountSent("unsub"));
            c.Dispose();
            Assert.AreEqual(new object[] { "unsub", ServerSubId }, client.LastSent);
            other.Dispose();
            Assert.AreEqual(2, client.CountSent("unsub"));
            Assert.AreEqual(0, client.KeyMappingCount);
        }

        [Test]
        public void WatchRange_GroupAndSingleColumnQueries_DoNotShare()
        {
            var client = FrameClient.Connected();
            var grouped = client.WatchGroupRange(Group(3), ServerSubId);
            IndexSubscription<DictComponent> single = null;
            client.WatchRangeSync<DictComponent>("level", 10L, 20L, 100,
                (s, _, _) => single = s, componentName: "Item");
            Assert.AreEqual(2, client.CountSent("sub"));
            client.Receive(
                new object[] { "sub", SingleColumnKey, Array.Empty<object>() });
            Assert.AreNotSame(grouped.Core, single.Core);
        }

        // ---------- 调用方事后改字典 ----------

        [Test]
        public void WatchRange_CallerMutatesGroupAfterCall_SubscriptionKeepsOriginal()
        {
            var client = FrameClient.Connected();
            var raw = new byte[] { 1, 2 };
            var group = new Dictionary<string, object> { ["zone"] = 3, ["raw"] = raw };
            IndexSubscription<DictComponent> sub = null;
            client.WatchRangeSync<DictComponent>("level", 10L, 20L, 100,
                (s, _, _) => sub = s, componentName: "Item", group: group);
            var sent = (IDictionary)client.LastSent[9];

            // 回复到达之前改：恢复参数是在回复里记下的
            group["zone"] = 4;
            group["extra"] = 1;
            raw[0] = 9;
            client.Receive(new object[] { "sub", ServerSubId, Array.Empty<object>() });

            Assert.AreEqual(3L, Convert.ToInt64(sent["zone"]));
            CollectionAssert.AreEqual(new byte[] { 1, 2 }, (byte[])sent["raw"]);
            var restore = ((ManagedIndexCore<DictComponent>)sub.Core).RestoreGroup;
            Assert.AreEqual(2, restore.Count);
            Assert.AreEqual(3, restore["zone"]);
            CollectionAssert.AreEqual(new byte[] { 1, 2 }, (byte[])restore["raw"]);
            Assert.AreNotSame(group, restore);

            // 查询键也是按原值记的：原来的组命中已有订阅，改过的组另发
            client.WatchRangeSync<DictComponent>("level", 10L, 20L, 100, (_, _, _) => { },
                componentName: "Item",
                group: new Dictionary<string, object>
                {
                    ["raw"] = new byte[] { 1, 2 }, ["zone"] = 3
                });
            Assert.AreEqual(1, client.CountSent("sub"));
            client.WatchRangeSync<DictComponent>("level", 10L, 20L, 100, (_, _, _) => { },
                componentName: "Item", group: group);
            Assert.AreEqual(2, client.CountSent("sub"));
        }

        // ---------- 会话层（经真实的 HeTuClientBase） ----------

        [Test]
        public void Session_GroupRange_SharesThroughAliasAndRestoresWithGroup()
        {
            var client = new FrameClient();
            var scheduler = new ManualScheduler();
            using var session = CreateSession(client, scheduler);
            session.Start();
            Assert.AreEqual(HeTuSessionState.Ready, session.State);

            var group = Group(3);
            IndexSubscription<DictComponent> a = null, b = null;
            session.WatchRange<DictComponent>("level", 10L, 20L, 100, "Item",
                s => a = s, ex => Assert.Fail(ex.ToString()), group: group);
            client.Receive(new object[] { "sub", ServerSubId, new[] { Row(7, 10) } });
            Assert.NotNull(a);
            group["zone"] = 4;

            // 会话的快路径按服务端 sub_id 查不到客户端键，落到客户端，由别名命中，不再发 sub
            session.WatchRange<DictComponent>("level", 10L, 20L, 100, "Item",
                s => b = s, ex => Assert.Fail(ex.ToString()), group: Group(3));
            Assert.NotNull(b);
            Assert.AreSame(a.Core, b.Core);
            Assert.AreEqual(1, client.CountSent("sub"));

            var resynced = 0;
            b.OnResynced += () => resynced++;
            client.RaiseClosed();
            Assert.IsTrue(a.IsStale);
            scheduler.RunNext(); // 重连，恢复订阅：按原来的意图重发，group 是订阅时的值

            Assert.AreEqual(HeTuSessionState.RestoringSubscriptions, session.State);
            Assert.AreEqual(2, client.CountSent("sub"));
            var restore = client.LastSent;
            Assert.AreEqual(10, restore.Length);
            Assert.AreEqual(new object[] { "sub", "Item", "range", "level", 10L, 20L },
                restore.Take(6).ToArray());
            var restoreGroup = (IDictionary)restore[9];
            Assert.AreEqual(1, restoreGroup.Count);
            Assert.AreEqual(3L, Convert.ToInt64(restoreGroup["zone"]));

            client.Receive(new object[]
                { "sub", ServerSubId, new[] { Row(7, 20), Row(8, 1) } });
            Assert.AreEqual(HeTuSessionState.Ready, session.State);
            Assert.IsFalse(a.IsStale);
            Assert.AreEqual(1, resynced);
            Assert.AreEqual(20, Value(a.Rows[7]));
            client.Receive(Update(ServerSubId, 8, Row(8, 2)));
            Assert.AreEqual(2, Value(b.Rows[8]));

            a.Dispose();
            b.Dispose();
            Assert.AreEqual(1, client.CountSent("unsub"));
        }

        [Test]
        public void Session_GroupWatchBeforeReady_SendsGroupAsOfTheCall()
        {
            var client = new FrameClient();
            using var session = CreateSession(client, new ManualScheduler());
            var rangeGroup = Group(3);
            var rowGroup = Group(5);
            // 不回复这两个订阅：测试结束 Dispose 会话时它们以"会话已关闭"失败，属预期
            session.WatchRange<DictComponent>("level", 10L, 20L, 100, "Item", _ => { },
                _ => { }, group: rangeGroup);
            session.WatchRow<DictComponent>("name", "abc", "Item", _ => { }, _ => { },
                rowGroup);
            rangeGroup["zone"] = 4;
            rowGroup["zone"] = 6;

            session.Start(); // Ready 时才派发排队的订阅

            var sent = client.Sent.Where(f => (string)f[0] == "sub").ToList();
            Assert.AreEqual(2, sent.Count);
            Assert.AreEqual(3L, Convert.ToInt64(((IDictionary)sent[0][9])["zone"]));
            Assert.AreEqual(5L, Convert.ToInt64(((IDictionary)sent[1][5])["zone"]));
        }

        [Test]
        public void Session_GroupRow_RestoresByIdWithoutGroup()
        {
            var client = new FrameClient();
            var scheduler = new ManualScheduler();
            using var session = CreateSession(client, scheduler);
            session.Start();
            RowSubscription<DictComponent> row = null;
            session.WatchRow<DictComponent>("name", "abc", "Item", s => row = s,
                ex => Assert.Fail(ex.ToString()), Group(3));
            Assert.AreEqual(6, client.LastSent.Length);
            client.Receive(new object[] { "sub", "Item.id[7:None:1][:1]", Row(7, 10) });
            Assert.NotNull(row);

            client.RaiseClosed();
            scheduler.RunNext();

            // 行订阅解析出行之后按 id 恢复，不再带 group
            Assert.AreEqual(new object[] { "sub", "Item", "get", "id", 7L },
                client.LastSent);
            client.Receive(new object[] { "sub", "Item.id[7:None:1][:1]", Row(7, 20) });
            Assert.AreEqual(HeTuSessionState.Ready, session.State);
            Assert.AreEqual(20, Value(row.Data));
            row.Dispose();
        }

        [Test]
        public void Session_InvalidGroup_ThrowsWithoutQueueing()
        {
            var client = new FrameClient();
            using var session = CreateSession(client, new ManualScheduler());
            Assert.Throws<ArgumentException>(() => session.WatchRange<DictComponent>(
                "level", 10L, 20L, 100, "Item", _ => { }, _ => { },
                group: new Dictionary<string, object> { ["zone"] = 1.5 }));
            Assert.Throws<ArgumentException>(() => session.WatchRow<DictComponent>(
                "id", 7L, "Item", _ => { }, _ => { }, Group(3)));

            session.Start();
            Assert.AreEqual(HeTuSessionState.Ready, session.State);
            Assert.AreEqual(0, client.CountSent("sub"));
        }

        // ---------- helpers ----------

        // 会话层经真实的 HeTuClientBase 收发（重连时新建 transport，底下还是同一个客户端）
        private static HeTuSessionClientBase CreateSession(FrameClient client,
            ManualScheduler scheduler) =>
            new(() => new ClientTransport(client), scheduler, null, TimeSpan.Zero);

        private static Dictionary<string, object> Group(object zone) =>
            new() { ["zone"] = zone };

        private static string Key(IReadOnlyDictionary<string, object> group) =>
            HeTuClientBase.MakeSubId("Item", "level", 10L, 20L, 100, false, group);

        private static Dictionary<string, object> Row(long id, int value) =>
            new() { ["id"] = id, ["value"] = value };

        private static int Value(DictComponent row) => Convert.ToInt32(row["value"]);

        private static object[] Update(string subId, long rowId,
            Dictionary<string, object> row) =>
            new object[]
            {
                "updt", subId, new Dictionary<long, object> { [rowId] = row }
            };

        // 只有 JsonbLayer 的客户端：走真实的连接入口和握手（JsonbLayer 不需要握手数据），发出的帧
        // 包成 ["rsp", 帧] 交给 JsonbLayer 解码，再按 MessagePack 原样展开（直接解码的话，客户端
        // 会把 ["sub", ...] 当作服务端的订阅回复来解析）
        private sealed class FrameClient : HeTuClientBase
        {
            private static readonly byte[] RspPrefix =
                { 0x92, 0xA3, (byte)'r', (byte)'s', (byte)'p' };

            private Action _onConnected;
            private Action<byte[]> _onMessage;
            private Action<int, string> _onClose;

            public FrameClient() =>
                SetupPipeline(new List<MessageProcessLayer> { new JsonbLayer() });

            public List<object[]> Sent { get; } = new();

            public object[] LastSent => Sent[^1];

            // SubscriptionManager 里"客户端键 → 服务端 sub_id"表的条目数
            public int KeyMappingCount
            {
                get
                {
                    var aliases = typeof(SubscriptionManager).GetField("_aliases",
                        System.Reflection.BindingFlags.NonPublic |
                        System.Reflection.BindingFlags.Instance);
                    return ((ICollection)aliases!.GetValue(Subscriptions)!).Count;
                }
            }

            public static FrameClient Connected()
            {
                var client = new FrameClient();
                client.Open();
                return client;
            }

            public void Open()
            {
                ConnectSync("ws://test");
                _onConnected();
                _onMessage(Pipeline.Encode(Array.Empty<object>(), out _));
            }

            public void RaiseClosed() => _onClose(HeTuCloseCode.Abnormal, "network lost");

            public int CountSent(string command) =>
                Sent.Count(frame => (string)frame[0] == command);

            // 订阅 Item 的 level=[10,20] 范围（limit 100）；reply 不为空时用它当服务端回的 sub_id
            public IndexSubscription<DictComponent> WatchGroupRange(
                IReadOnlyDictionary<string, object> group, string reply = null,
                params Dictionary<string, object>[] rows)
            {
                IndexSubscription<DictComponent> result = null;
                WatchRangeSync<DictComponent>("level", 10L, 20L, 100,
                    (s, _, _) => result = s, componentName: "Item", group: group);
                if (reply != null)
                    Receive(new object[] { "sub", reply, rows });
                return result;
            }

            public void Receive(object[] frame) =>
                OnReceived(Pipeline.Encode(frame, out _));

            protected override void ConnectCore(string url, Action onConnected,
                Action<byte[]> onMessage, Action<int, string> onClose,
                Action<string> onError)
            {
                _onConnected = onConnected;
                _onMessage = onMessage;
                _onClose = onClose;
            }

            protected override void CloseCore()
            {
            }

            protected override void SendCore(byte[] data)
            {
                var wrapped = RspPrefix.Concat(data).ToArray();
                var rsp = (object[])Pipeline.Decode(wrapped);
                if (((JsonObject)rsp[1]).ToUntyped() is object[] { Length: > 0 } frame)
                    Sent.Add(frame);
            }
        }

        // 会话层的 transport，直接套在 FrameClient 上（同 Unity 的 UnityHeTuSessionTransport：
        // 重连时新建 transport、底下还是同一个客户端）
        private sealed class ClientTransport : IHeTuSessionTransport
        {
            private readonly FrameClient _client;

            public ClientTransport(FrameClient client)
            {
                _client = client;
                _client.OnConnected += HandleConnected;
                _client.OnClosed += HandleClosed;
            }

            public event Action Connected;
            public event Action<int, string> Closed;

            public bool IsConnected => _client.IsConnected;

            public void Connect() => _client.Open();

            public void Close() => _client.Close();

            public void CallSystem(string systemName, object[] args,
                Action<JsonObject, CallOutcome, string> onResponse,
                bool awaitPush = false,
                Action<JsonObject> onAnswered = null) =>
                _client.CallSystemSync(systemName, args, onResponse, awaitPush,
                    onAnswered);

            public void WatchRow<T>(string index, object value,
                Action<ManagedRowCore<T>, bool, Exception> onResponse,
                string componentName = null,
                ManagedRowCore<T> reusable = null,
                StackTrace creationTrace = null,
                IReadOnlyDictionary<string, object> group = null)
                where T : IBaseComponent =>
                _client.WatchRowCoreSync(index, value, onResponse, componentName,
                    reusable, creationTrace, group);

            public void WatchRange<T>(string index, object left, object right, int limit,
                Action<ManagedIndexCore<T>, bool, Exception> onResponse,
                bool desc = false,
                bool force = true,
                string componentName = null,
                ManagedIndexCore<T> reusable = null,
                StackTrace creationTrace = null,
                IReadOnlyDictionary<string, object> group = null)
                where T : IBaseComponent =>
                _client.WatchRangeCoreSync(index, left, right, limit, onResponse, desc,
                    force, componentName, reusable, creationTrace, group);

            public void WatchTable<T>(
                Action<ManagedIndexCore<T>, bool, Exception> onResponse,
                string componentName = null,
                ManagedIndexCore<T> reusable = null,
                StackTrace creationTrace = null)
                where T : IBaseComponent =>
                _client.WatchTableCoreSync(onResponse, componentName, reusable,
                    creationTrace);

            public void Dispose()
            {
                _client.OnConnected -= HandleConnected;
                _client.OnClosed -= HandleClosed;
            }

            private void HandleConnected() => Connected?.Invoke();

            private void HandleClosed(string reason) =>
                Closed?.Invoke(_client.LastCloseCode, reason);
        }

        private sealed class ManualScheduler : IHeTuSessionScheduler
        {
            private readonly Queue<Scheduled> _scheduled = new();

            public IDisposable Schedule(TimeSpan delay, Action action)
            {
                var scheduled = new Scheduled(action);
                _scheduled.Enqueue(scheduled);
                return scheduled;
            }

            public void RunNext()
            {
                while (_scheduled.Count > 0)
                {
                    var scheduled = _scheduled.Dequeue();
                    if (scheduled.IsDisposed)
                        continue;
                    scheduled.Run();
                    return;
                }
            }

            private sealed class Scheduled : IDisposable
            {
                private readonly Action _action;

                public Scheduled(Action action) => _action = action;

                public bool IsDisposed { get; private set; }

                public void Run() => _action();

                public void Dispose() => IsDisposed = true;
            }
        }
    }
}
