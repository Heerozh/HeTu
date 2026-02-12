using System;
using System.Collections.Generic;
using UnityEditor;
using UnityEngine;

namespace HeTu.Editor
{
    public class HeTuInspectorWindow : EditorWindow
    {
        private const int MaxRows = 2000;
        private const float RowHeight = 22f;
        private const float HeaderHeight = 24f;
        private const float MinColumnWidth = 60f;
        private const float ResizeHandleHalfWidth = 3f;

        private static readonly float[] DefaultColumnWidths =
        {
            105f, // Time
            82f, // Type
            110f, // Target
            86f, // Status
            95f, // Size
            82f // Duration
            // Payload 占用剩余宽度
        };

        private readonly List<InspectorTraceEvent> _rows = new();
        private readonly Dictionary<string, int> _indexByTraceId = new();
        private readonly Queue<InspectorTraceEvent> _pendingEvents = new();
        private readonly object _queueLock = new();
        private readonly float[] _columnWidths = (float[])DefaultColumnWidths.Clone();

        private HeTuInspectorDispatcher _dispatcher;
        private Vector2 _scroll;
        private GUIStyle _headerStyle;
        private GUIStyle _cellStyle;
        private GUIStyle _rowStyle;
        private bool _isRecording;
        private bool _samplerWasEnabledBeforeRecording;
        private string _selectedTraceId;
        private bool _isResizingColumn;
        private int _resizingColumnIndex = -1;
        private float _resizeStartMouseX;
        private float _resizeStartWidth;
        private string _filterKeyword = string.Empty;
        private bool _autoScrollToBottom = true;

        private void OnEnable()
        {
            titleContent = new GUIContent("HeTu Inspector");
            _dispatcher = new HeTuInspectorDispatcher(this);
            EditorApplication.update += OnEditorUpdate;
        }

        private void OnDisable()
        {
            EditorApplication.update -= OnEditorUpdate;
            StopRecording();
        }

        private void OnEditorUpdate()
        {
            FlushPendingEvents();
        }

        private void EnsureStyles()
        {
            if (_rowStyle != null && _headerStyle != null && _cellStyle != null)
                return;

            _rowStyle = new GUIStyle(EditorStyles.label)
            {
                richText = true,
                wordWrap = false,
                clipping = TextClipping.Clip,
                padding = new RectOffset(6, 6, 2, 2)
            };

            _headerStyle = new GUIStyle(EditorStyles.miniBoldLabel)
            {
                alignment = TextAnchor.MiddleLeft,
                clipping = TextClipping.Clip,
                padding = new RectOffset(6, 6, 2, 2)
            };

            _cellStyle = new GUIStyle(EditorStyles.label)
            {
                alignment = TextAnchor.MiddleLeft,
                clipping = TextClipping.Clip,
                padding = new RectOffset(6, 6, 2, 2)
            };
        }

        private void OnGUI()
        {
            EnsureStyles();
            DrawToolbar();
            DrawList();
        }

        private void DrawToolbar()
        {
            using (new EditorGUILayout.HorizontalScope(EditorStyles.toolbar))
            {
                var recordLabel = _isRecording ? "⏸" : "🔴";
                if (GUILayout.Button(recordLabel, EditorStyles.toolbarButton,
                        GUILayout.Width(30)))
                {
                    if (_isRecording)
                        StopRecording();
                    else
                        StartRecording();
                }

                if (GUILayout.Button("🗑️", EditorStyles.toolbarButton,
                        GUILayout.Width(30)))
                    ClearRows();

                GUILayout.Space(8);
                GUILayout.Label("🔍", EditorStyles.miniLabel, GUILayout.Width(30));
                var nextFilter = GUILayout.TextField(_filterKeyword,
                    EditorStyles.toolbarTextField,
                    GUILayout.MinWidth(140), GUILayout.MaxWidth(360));
                if (!string.Equals(nextFilter, _filterKeyword,
                        StringComparison.Ordinal))
                {
                    _filterKeyword = nextFilter;
                    Repaint();
                }

                _autoScrollToBottom = GUILayout.Toggle(_autoScrollToBottom,
                    "⬇️", EditorStyles.toolbarButton, GUILayout.Width(30));

                GUILayout.FlexibleSpace();
                GUILayout.Label($"Count: {GetFilteredCount()}/{_rows.Count}",
                    EditorStyles.miniLabel);
            }
        }

        private void DrawList()
        {
            var headerRect = GUILayoutUtility.GetRect(
                GUIContent.none,
                _headerStyle,
                GUILayout.ExpandWidth(true),
                GUILayout.Height(HeaderHeight));
            DrawGridHeader(headerRect);

            _scroll = EditorGUILayout.BeginScrollView(_scroll);
            for (var i = 0; i < _rows.Count; i++)
            {
                var row = _rows[i];
                if (!IsRowMatched(row))
                    continue;

                var rect = GUILayoutUtility.GetRect(
                    GUIContent.none,
                    _cellStyle,
                    GUILayout.ExpandWidth(true),
                    GUILayout.Height(RowHeight));

                DrawGridRow(rect, row, IsSelected(row));

                HandleRowInteraction(rect, i, row);
            }

            EditorGUILayout.EndScrollView();
        }

        private void DrawGridHeader(Rect rect)
        {
            EditorGUI.DrawRect(rect, new Color(0.18f, 0.18f, 0.18f, 1f));
            DrawHorizontalBorder(rect);

            var cells = BuildColumnRects(rect);
            var headers = new[]
            {
                "Time", "Type", "Target", "Status", "Size", "Duration", "Payload"
            };

            for (var i = 0; i < headers.Length; i++)
                EditorGUI.LabelField(cells[i], headers[i], _headerStyle);

            DrawVerticalSeparators(cells, rect.yMin, rect.yMax);
            HandleColumnResize(cells, rect);
        }

        private void DrawGridRow(Rect rect, InspectorTraceEvent row, bool selected)
        {
            var bg = selected
                ? new Color(0.24f, 0.48f, 0.90f, 0.25f)
                : new Color(0f, 0f, 0f, 0f);
            if (selected)
                EditorGUI.DrawRect(rect, bg);

            DrawHorizontalBorder(rect);
            var cells = BuildColumnRects(rect);

            var time = row.StartTimeUtc.ToLocalTime().ToString("HH:mm:ss.fff");
            var statusColor = GetStatusColor(row.Status);

            EditorGUI.LabelField(cells[0], time, _cellStyle);
            EditorGUI.LabelField(cells[1], row.Type, _cellStyle);
            EditorGUI.LabelField(cells[2], row.Target, _cellStyle);

            var prev = GUI.color;
            GUI.color = statusColor;
            EditorGUI.LabelField(cells[3], row.Status, _cellStyle);
            GUI.color = prev;

            EditorGUI.LabelField(cells[4], row.Size, _cellStyle);
            EditorGUI.LabelField(cells[5], row.CallDuration, _cellStyle);
            EditorGUI.LabelField(cells[6], row.Payload, _cellStyle);

            DrawVerticalSeparators(cells, rect.yMin, rect.yMax);
        }

        private Rect[] BuildColumnRects(Rect rowRect)
        {
            var result = new Rect[7];
            var x = rowRect.xMin;

            for (var i = 0; i < _columnWidths.Length; i++)
            {
                var w = _columnWidths[i];
                result[i] = new Rect(x, rowRect.yMin, w, rowRect.height);
                x += w;
            }

            result[6] = new Rect(x, rowRect.yMin,
                Mathf.Max(80f, rowRect.xMax - x), rowRect.height);
            return result;
        }

        private void HandleColumnResize(IReadOnlyList<Rect> cells, Rect headerRect)
        {
            var evt = Event.current;

            for (var i = 0; i < _columnWidths.Length; i++)
            {
                var separatorX = cells[i].xMax;
                var handleRect = new Rect(
                    separatorX - ResizeHandleHalfWidth,
                    headerRect.yMin,
                    ResizeHandleHalfWidth * 2f,
                    headerRect.height);

                EditorGUIUtility.AddCursorRect(handleRect, MouseCursor.ResizeHorizontal);

                if (evt.type == EventType.MouseDown && evt.button == 0 &&
                    handleRect.Contains(evt.mousePosition))
                {
                    _isResizingColumn = true;
                    _resizingColumnIndex = i;
                    _resizeStartMouseX = evt.mousePosition.x;
                    _resizeStartWidth = _columnWidths[i];
                    evt.Use();
                    return;
                }
            }

            if (_isResizingColumn && evt.type == EventType.MouseDrag)
            {
                var delta = evt.mousePosition.x - _resizeStartMouseX;
                _columnWidths[_resizingColumnIndex] = Mathf.Max(MinColumnWidth,
                    _resizeStartWidth + delta);
                Repaint();
                evt.Use();
                return;
            }

            if (_isResizingColumn &&
                (evt.type == EventType.MouseUp || evt.rawType == EventType.MouseUp))
            {
                _isResizingColumn = false;
                _resizingColumnIndex = -1;
                evt.Use();
            }
        }

        private static void DrawHorizontalBorder(Rect rowRect)
        {
            var border = new Color(0.25f, 0.25f, 0.25f, 1f);
            EditorGUI.DrawRect(new Rect(rowRect.xMin, rowRect.yMax - 1f, rowRect.width, 1f),
                border);
            EditorGUI.DrawRect(new Rect(rowRect.xMin, rowRect.yMin, rowRect.width, 1f),
                border);
        }

        private static void DrawVerticalSeparators(IReadOnlyList<Rect> cells, float yMin,
            float yMax)
        {
            var border = new Color(0.25f, 0.25f, 0.25f, 1f);
            for (var i = 0; i < cells.Count - 1; i++)
            {
                var x = cells[i].xMax;
                EditorGUI.DrawRect(new Rect(x, yMin, 1f, yMax - yMin), border);
            }
        }

        private static Color GetStatusColor(string status)
        {
            return status switch
            {
                "pending" => new Color(1f, 0.66f, 0.2f),
                "completed" => new Color(0.45f, 0.85f, 0.45f),
                "failed" => new Color(1f, 0.45f, 0.45f),
                "canceled" => new Color(0.72f, 0.72f, 0.72f),
                _ => GUI.color
            };
        }

        private void HandleRowInteraction(Rect rect, int rowIndex, InspectorTraceEvent row)
        {
            if (_isResizingColumn)
                return;

            var evt = Event.current;
            if (!rect.Contains(evt.mousePosition))
                return;

            if (evt.type == EventType.MouseDown && evt.button == 0)
            {
                _selectedTraceId = row.TraceId;
                Repaint();
                evt.Use();
                return;
            }

            if (evt.type == EventType.ContextClick)
            {
                _selectedTraceId = row.TraceId;
                ShowContextMenu(row);
                Repaint();
                evt.Use();
            }
        }

        private bool IsSelected(InspectorTraceEvent row) =>
            !string.IsNullOrEmpty(_selectedTraceId) &&
            string.Equals(_selectedTraceId, row.TraceId, StringComparison.Ordinal);

        private bool IsRowMatched(InspectorTraceEvent row)
        {
            if (string.IsNullOrWhiteSpace(_filterKeyword))
                return true;

            var keyword = _filterKeyword.Trim();
            return ContainsIgnoreCase(row.Type, keyword) ||
                   ContainsIgnoreCase(row.Target, keyword) ||
                   ContainsIgnoreCase(row.Payload, keyword);
        }

        private int GetFilteredCount()
        {
            if (string.IsNullOrWhiteSpace(_filterKeyword))
                return _rows.Count;

            var count = 0;
            for (var i = 0; i < _rows.Count; i++)
            {
                if (IsRowMatched(_rows[i]))
                    count++;
            }

            return count;
        }

        private static bool ContainsIgnoreCase(string source, string keyword)
        {
            if (string.IsNullOrEmpty(source) || string.IsNullOrEmpty(keyword))
                return false;
            return source.IndexOf(keyword, StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static string BuildCopyText(InspectorTraceEvent row)
        {
            var time = row.StartTimeUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss.fff");
            return
                $"{time}\t{row.Type}\t{row.Target}\t{row.Status}\t{row.Size}\t{row.CallDuration}\t{row.Payload}";
        }

        private static void ShowContextMenu(InspectorTraceEvent row)
        {
            var menu = new GenericMenu();
            menu.AddItem(new GUIContent("Copy"), false, () =>
            {
                EditorGUIUtility.systemCopyBuffer = BuildCopyText(row);
            });
            menu.ShowAsContext();
        }

        private void StartRecording()
        {
            var client = HeTuClient.Instance;
            _samplerWasEnabledBeforeRecording = client.InspectorEnabled;
            client.AddInspectorDispatcher(_dispatcher);
            client.ConfigureInspector(true);
            _isRecording = true;
        }

        private void StopRecording()
        {
            if (!_isRecording || _dispatcher == null)
                return;

            var client = HeTuClient.Instance;
            client.RemoveInspectorDispatcher(_dispatcher);

            if (!_samplerWasEnabledBeforeRecording)
                client.ConfigureInspector(false);

            _isRecording = false;
        }

        private void ClearRows()
        {
            _rows.Clear();
            _indexByTraceId.Clear();
            _selectedTraceId = null;
            Repaint();
        }

        internal void EnqueueTrace(InspectorTraceEvent traceEvent)
        {
            if (traceEvent == null)
                return;

            lock (_queueLock)
            {
                _pendingEvents.Enqueue(traceEvent);
            }
        }

        private void FlushPendingEvents()
        {
            var changed = false;
            while (true)
            {
                InspectorTraceEvent traceEvent;
                lock (_queueLock)
                {
                    if (_pendingEvents.Count == 0)
                        break;
                    traceEvent = _pendingEvents.Dequeue();
                }

                UpsertRow(traceEvent);
                changed = true;
            }

            if (changed)
            {
                if (_autoScrollToBottom)
                    _scroll.y = float.MaxValue;
                Repaint();
            }
        }

        private void UpsertRow(InspectorTraceEvent traceEvent)
        {
            if (!string.IsNullOrEmpty(traceEvent.TraceId) &&
                _indexByTraceId.TryGetValue(traceEvent.TraceId, out var idx))
            {
                _rows[idx] = traceEvent;
                return;
            }

            _rows.Add(traceEvent);
            if (!string.IsNullOrEmpty(traceEvent.TraceId))
                _indexByTraceId[traceEvent.TraceId] = _rows.Count - 1;

            if (_rows.Count <= MaxRows)
                return;

            var removed = _rows[0];
            _rows.RemoveAt(0);
            RebuildIndex();

            if (!string.IsNullOrEmpty(removed.TraceId))
            {
                if (string.Equals(_selectedTraceId, removed.TraceId,
                        StringComparison.Ordinal))
                    _selectedTraceId = null;
                _indexByTraceId.Remove(removed.TraceId);
            }
        }

        private void RebuildIndex()
        {
            _indexByTraceId.Clear();
            for (var i = 0; i < _rows.Count; i++)
            {
                var traceId = _rows[i].TraceId;
                if (!string.IsNullOrEmpty(traceId))
                    _indexByTraceId[traceId] = i;
            }
        }

        [MenuItem("HeTu/Inspector...")]
        public static void Open()
        {
            var window = GetWindow<HeTuInspectorWindow>("HeTu Inspector");
            window.minSize = new Vector2(720, 260);
            window.Show();
        }

        private sealed class HeTuInspectorDispatcher : IInspectorTraceDispatcher
        {
            private readonly HeTuInspectorWindow _owner;

            public HeTuInspectorDispatcher(HeTuInspectorWindow owner)
            {
                _owner = owner;
            }

            public void Dispatch(InspectorTraceEvent traceEvent)
            {
                _owner.EnqueueTrace(traceEvent);
            }
        }
    }
}
