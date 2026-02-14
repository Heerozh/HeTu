using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using UnityEditor;
using UnityEngine;
#if !UNITY_6000_0_OR_NEWER
using Cysharp.Threading.Tasks;
#endif

namespace HeTu.Editor
{
    public class HeTuPostStationWindow : EditorWindow
    {
        private string _callSystemArgsJson = "[]";

        private bool _isRequesting;

        private string _rangeComponentName = string.Empty;
        private bool _rangeDesc;
        private string _rangeIndex = "id";
        private string _rangeLeft = "0";
        private string _rangeLimit = "10";
        private string _rangeRight = "null";
        private Vector2 _resultScroll;
        private string _resultText = "等待请求...";
        private string _systemName = string.Empty;

        private void OnGUI()
        {
            DrawHeader();
            DrawConnectionWarning();

            using (new EditorGUI.DisabledScope(_isRequesting))
            {
                DrawCallSystemSection();
                GUILayout.Space(8);
                DrawRangeSection();
            }

            GUILayout.Space(8);
            DrawResultSection();
        }

        [MenuItem("HeTu/驿栈 (API Testing)...")]
        public static void Open()
        {
            var window = GetWindow<HeTuPostStationWindow>("HeTu 驿栈");
            window.minSize = new Vector2(760, 520);
            window.Show();
        }

        private void DrawConnectionWarning()
        {
            if (EditorApplication.isPlaying)
                return;

            if (HeTuClient.Instance.IsConnected)
                return;

            EditorGUILayout.HelpBox(
                "当前不在 Play 模式且尚未 Connect。请先进入 Play 模式并建立连接后再发起请求。",
                MessageType.Warning);
        }

        private void DrawHeader()
        {
            using (new EditorGUILayout.HorizontalScope(EditorStyles.toolbar))
            {
                GUILayout.Label("HeTu 驿栈 (API Testing)", EditorStyles.boldLabel);
                GUILayout.FlexibleSpace();
                GUILayout.Label(_isRequesting ? "请求中..." : "Ready",
                    EditorStyles.miniLabel);
            }
        }

        private void DrawCallSystemSection()
        {
            using (new EditorGUILayout.VerticalScope("box"))
            {
                EditorGUILayout.LabelField("CallSystem", EditorStyles.boldLabel);

                _systemName = EditorGUILayout.TextField("System", _systemName);
                EditorGUILayout.LabelField("Args (JSON)");
                _callSystemArgsJson = EditorGUILayout.TextArea(_callSystemArgsJson,
                    GUILayout.MinHeight(64));

                using (new EditorGUILayout.HorizontalScope())
                {
                    GUILayout.FlexibleSpace();
                    if (GUILayout.Button("发送 CallSystem", GUILayout.Width(140)))
                        SendCallSystem();
                }
            }
        }

        private void DrawRangeSection()
        {
            using (new EditorGUILayout.VerticalScope("box"))
            {
                EditorGUILayout.LabelField("数据查询 (Range)", EditorStyles.boldLabel);

                _rangeComponentName =
                    EditorGUILayout.TextField("Component", _rangeComponentName);
                _rangeIndex = EditorGUILayout.TextField("Index", _rangeIndex);
                _rangeLeft = EditorGUILayout.TextField("Left(JSON)", _rangeLeft);
                _rangeRight = EditorGUILayout.TextField("Right(JSON)", _rangeRight);
                _rangeLimit = EditorGUILayout.TextField("Limit", _rangeLimit);
                _rangeDesc = EditorGUILayout.Toggle("Desc", _rangeDesc);

                using (new EditorGUILayout.HorizontalScope())
                {
                    GUILayout.FlexibleSpace();
                    if (GUILayout.Button("发送 Range 查询", GUILayout.Width(140)))
                        SendRange();
                }
            }
        }

        private void DrawResultSection()
        {
            using (new EditorGUILayout.VerticalScope("box", GUILayout.ExpandHeight(true)))
            {
                using (new EditorGUILayout.HorizontalScope())
                {
                    EditorGUILayout.LabelField("结果", EditorStyles.boldLabel);
                    GUILayout.FlexibleSpace();
                    using (new EditorGUI.DisabledScope(_isRequesting))
                    {
                        if (GUILayout.Button("清空", EditorStyles.miniButton,
                                GUILayout.Width(52)))
                            _resultText = "等待请求...";
                    }
                }

                _resultScroll = EditorGUILayout.BeginScrollView(_resultScroll,
                    GUILayout.ExpandHeight(true));
                EditorGUILayout.TextArea(_resultText, EditorStyles.textArea,
                    GUILayout.ExpandHeight(true));
                EditorGUILayout.EndScrollView();
            }
        }

        private async void SendCallSystem()
        {
            if (_isRequesting) return;
            if (string.IsNullOrWhiteSpace(_systemName))
            {
                _resultText = "System 名不能为空";
                return;
            }

            _resultText = "请求中...";
            Repaint();
            _isRequesting = true;
            try
            {
                var args = ParseArgsJson(_callSystemArgsJson);
#if UNITY_6000_0_OR_NEWER
                var rsp = await HeTuClient.Instance.CallSystem(_systemName, args);
#else
                var rsp = await HeTuClient.Instance.CallSystem(_systemName, args);
#endif
                var value = rsp?.ToUntyped();
                _resultText = MiniJson.PrettyPrint(value);
            }
            catch (Exception ex)
            {
                _resultText = $"请求失败: {ex.Message}";
            }
            finally
            {
                _isRequesting = false;
                Repaint();
            }
        }

        private async void SendRange()
        {
            if (_isRequesting) return;
            if (string.IsNullOrWhiteSpace(_rangeComponentName))
            {
                _resultText = "Component 名不能为空";
                return;
            }

            _resultText = "请求中...";
            Repaint();
            _isRequesting = true;
            IndexSubscription<DictComponent> sub = null;
            try
            {
                if (!int.TryParse(_rangeLimit, out var limit) || limit <= 0)
                    throw new Exception("Limit 必须是正整数");

                var left = ParseJsonValue(_rangeLeft);
                var right = ParseJsonValue(_rangeRight);

#if UNITY_6000_0_OR_NEWER
                sub = await HeTuClient.Instance.Range(
                    _rangeComponentName,
                    _rangeIndex,
                    left,
                    right,
                    limit,
                    _rangeDesc
                );
#else
                sub = await HeTuClient.Instance.Range(
                    _rangeComponentName,
                    _rangeIndex,
                    left,
                    right,
                    limit,
                    _rangeDesc
                    );
#endif

                if (sub == null)
                {
                    _resultText = "null";
                    return;
                }

                var rows = new Dictionary<long, object>();
                foreach (var pair in sub.Rows)
                    rows[pair.Key] = pair.Value;

                _resultText = MiniJson.PrettyPrint(rows);
            }
            catch (Exception ex)
            {
                _resultText = $"请求失败: {ex.Message}";
            }
            finally
            {
                sub?.Dispose(); // 自动反订阅
                _isRequesting = false;
                Repaint();
            }
        }

        private static object[] ParseArgsJson(string json)
        {
            var parsed = ParseJsonValue(json);
            if (parsed is List<object> list)
                return list.ToArray();

            return new[] { parsed };
        }

        private static object ParseJsonValue(string json)
        {
            if (string.IsNullOrWhiteSpace(json))
                return null;

            return MiniJson.Parse(json.Trim());
        }

        private static class MiniJson
        {
            public static object Parse(string json)
            {
                var parser = new Parser(json);
                return parser.ParseValue();
            }

            public static string PrettyPrint(object value)
            {
                var sb = new StringBuilder(1024);
                WriteValue(sb, value, 0);
                return sb.ToString();
            }

            private static void WriteValue(StringBuilder sb, object value, int indent)
            {
                switch (value)
                {
                    case null:
                        sb.Append("null");
                        return;
                    case string str:
                        sb.Append('"').Append(Escape(str)).Append('"');
                        return;
                    case bool b:
                        sb.Append(b ? "true" : "false");
                        return;
                    case sbyte or byte or short or ushort or int or uint or long or ulong
                        or float or double or decimal:
                        sb.Append(Convert.ToString(value, CultureInfo.InvariantCulture));
                        return;
                    case JsonObject jsonObject:
                        WriteValue(sb, jsonObject.ToUntyped(), indent);
                        return;
                    case IDictionary dict:
                        WriteDict(sb, dict, indent);
                        return;
                    case IEnumerable enumerable when value is not string:
                        WriteList(sb, enumerable, indent);
                        return;
                    default:
                        sb.Append('"').Append(Escape(value.ToString())).Append('"');
                        return;
                }
            }

            private static void WriteDict(StringBuilder sb, IDictionary dict, int indent)
            {
                sb.Append('{');
                var first = true;
                foreach (DictionaryEntry item in dict)
                {
                    if (!first)
                        sb.Append(',');
                    first = false;
                    sb.Append('\n');
                    AppendIndent(sb, indent + 1);
                    sb.Append('"').Append(Escape(item.Key?.ToString() ?? "null"))
                        .Append('"').Append(": ");
                    WriteValue(sb, item.Value, indent + 1);
                }

                if (!first)
                {
                    sb.Append('\n');
                    AppendIndent(sb, indent);
                }

                sb.Append('}');
            }

            private static void WriteList(StringBuilder sb, IEnumerable list, int indent)
            {
                sb.Append('[');
                var first = true;
                foreach (var item in list)
                {
                    if (!first)
                        sb.Append(',');
                    first = false;
                    sb.Append('\n');
                    AppendIndent(sb, indent + 1);
                    WriteValue(sb, item, indent + 1);
                }

                if (!first)
                {
                    sb.Append('\n');
                    AppendIndent(sb, indent);
                }

                sb.Append(']');
            }

            private static void AppendIndent(StringBuilder sb, int indent)
            {
                for (var i = 0; i < indent; i++)
                    sb.Append("  ");
            }

            private static string Escape(string text) =>
                text
                    .Replace("\\", "\\\\")
                    .Replace("\"", "\\\"")
                    .Replace("\n", "\\n")
                    .Replace("\r", "\\r")
                    .Replace("\t", "\\t");

            private sealed class Parser
            {
                private readonly string _json;
                private int _index;

                public Parser(string json) => _json = json;

                public object ParseValue()
                {
                    SkipWhiteSpace();
                    if (_index >= _json.Length)
                        throw new Exception("JSON 为空");

                    return _json[_index] switch
                    {
                        '{' => ParseObject(),
                        '[' => ParseArray(),
                        '"' => ParseString(),
                        't' => ParseTrue(),
                        'f' => ParseFalse(),
                        'n' => ParseNull(),
                        _ => ParseNumber()
                    };
                }

                private Dictionary<string, object> ParseObject()
                {
                    var dict = new Dictionary<string, object>();
                    Expect('{');
                    SkipWhiteSpace();
                    if (TryConsume('}')) return dict;

                    while (true)
                    {
                        SkipWhiteSpace();
                        var key = ParseString();
                        SkipWhiteSpace();
                        Expect(':');
                        var value = ParseValue();
                        dict[key] = value;
                        SkipWhiteSpace();
                        if (TryConsume('}')) break;
                        Expect(',');
                    }

                    return dict;
                }

                private List<object> ParseArray()
                {
                    var list = new List<object>();
                    Expect('[');
                    SkipWhiteSpace();
                    if (TryConsume(']')) return list;

                    while (true)
                    {
                        var value = ParseValue();
                        list.Add(value);
                        SkipWhiteSpace();
                        if (TryConsume(']')) break;
                        Expect(',');
                    }

                    return list;
                }

                private string ParseString()
                {
                    Expect('"');
                    var sb = new StringBuilder();

                    while (_index < _json.Length)
                    {
                        var ch = _json[_index++];
                        if (ch == '"')
                            return sb.ToString();
                        if (ch != '\\')
                        {
                            sb.Append(ch);
                            continue;
                        }

                        if (_index >= _json.Length)
                            throw new Exception("字符串转义错误");

                        var esc = _json[_index++];
                        sb.Append(esc switch
                        {
                            '"' => '"',
                            '\\' => '\\',
                            '/' => '/',
                            'b' => '\b',
                            'f' => '\f',
                            'n' => '\n',
                            'r' => '\r',
                            't' => '\t',
                            'u' => ParseUnicode(),
                            _ => throw new Exception("非法转义字符")
                        });
                    }

                    throw new Exception("字符串未闭合");
                }

                private char ParseUnicode()
                {
                    if (_index + 4 > _json.Length)
                        throw new Exception("unicode 转义长度不足");
                    var hex = _json.Substring(_index, 4);
                    _index += 4;
                    return (char)Convert.ToInt32(hex, 16);
                }

                private object ParseNumber()
                {
                    var start = _index;
                    while (_index < _json.Length)
                    {
                        var ch = _json[_index];
                        if ((ch >= '0' && ch <= '9') ||
                            ch is '-' or '+' or '.' or 'e' or 'E')
                            _index++;
                        else
                            break;
                    }

                    var token = _json.Substring(start, _index - start);
                    if (token.IndexOf('.') >= 0 || token.IndexOf('e') >= 0 ||
                        token.IndexOf('E') >= 0)
                    {
                        if (double.TryParse(token, NumberStyles.Float,
                                CultureInfo.InvariantCulture, out var d))
                            return d;
                    }
                    else
                    {
                        if (long.TryParse(token, NumberStyles.Integer,
                                CultureInfo.InvariantCulture, out var l))
                            return l;
                    }

                    throw new Exception($"非法数字: {token}");
                }

                private bool ParseTrue()
                {
                    ExpectWord("true");
                    return true;
                }

                private bool ParseFalse()
                {
                    ExpectWord("false");
                    return false;
                }

                private object ParseNull()
                {
                    ExpectWord("null");
                    return null;
                }

                private void SkipWhiteSpace()
                {
                    while (_index < _json.Length && char.IsWhiteSpace(_json[_index]))
                        _index++;
                }

                private void Expect(char ch)
                {
                    SkipWhiteSpace();
                    if (_index >= _json.Length || _json[_index] != ch)
                        throw new Exception($"期望字符 '{ch}'");
                    _index++;
                }

                private bool TryConsume(char ch)
                {
                    SkipWhiteSpace();
                    if (_index >= _json.Length || _json[_index] != ch)
                        return false;
                    _index++;
                    return true;
                }

                private void ExpectWord(string word)
                {
                    SkipWhiteSpace();
                    if (_index + word.Length > _json.Length)
                        throw new Exception($"期望关键字 {word}");
                    var span = _json.Substring(_index, word.Length);
                    if (!string.Equals(span, word, StringComparison.Ordinal))
                        throw new Exception($"期望关键字 {word}");
                    _index += word.Length;
                }
            }
        }
    }
}
