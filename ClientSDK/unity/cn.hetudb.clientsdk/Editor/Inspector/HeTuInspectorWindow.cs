using System;
using System.Collections.Generic;
using UnityEditor;
using UnityEngine;

namespace HeTu.Editor
{
    public class HeTuInspectorWindow : EditorWindow
    {
        private const int MaxRows = 2000;

        private readonly List<InspectorTraceEvent> _rows = new();
        private readonly Dictionary<string, int> _indexByTraceId = new();
        private readonly Queue<InspectorTraceEvent> _pendingEvents = new();
        private readonly object _queueLock = new();

        private HeTuInspectorDispatcher _dispatcher;
        private Vector2 _scroll;
        private GUIStyle _rowStyle;
        private bool _isRecording;
        private bool _samplerWasEnabledBeforeRecording;
        private int _selectedIndex = -1;

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
            if (_rowStyle != null)
                return;

            _rowStyle = new GUIStyle(EditorStyles.label)
            {
                richText = true,
                wordWrap = false,
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
                var recordLabel = _isRecording ? "Pause" : "Record";
                if (GUILayout.Button(recordLabel, EditorStyles.toolbarButton,
                        GUILayout.Width(90)))
                {
                    if (_isRecording)
                        StopRecording();
                    else
                        StartRecording();
                }

                if (GUILayout.Button("Clear", EditorStyles.toolbarButton,
                        GUILayout.Width(60)))
                    ClearRows();

                GUILayout.FlexibleSpace();
                GUILayout.Label($"Count: {_rows.Count}", EditorStyles.miniLabel);
            }
        }

        private void DrawList()
        {
            var header =
                "Time | Type | Target | Status | Size | Duration | Payload";
            EditorGUILayout.LabelField(header, EditorStyles.boldLabel);

            _scroll = EditorGUILayout.BeginScrollView(_scroll);
            for (var i = 0; i < _rows.Count; i++)
            {
                var row = _rows[i];
                var rect = GUILayoutUtility.GetRect(
                    GUIContent.none,
                    _rowStyle,
                    GUILayout.ExpandWidth(true),
                    GUILayout.Height(20));

                if (i == _selectedIndex)
                    EditorGUI.DrawRect(rect, new Color(0.24f, 0.48f, 0.90f, 0.25f));

                var rowText = BuildRichRowText(row);
                EditorGUI.LabelField(rect, rowText, _rowStyle);

                HandleRowInteraction(rect, i, row);
            }

            EditorGUILayout.EndScrollView();
        }

        private void HandleRowInteraction(Rect rect, int rowIndex, InspectorTraceEvent row)
        {
            var evt = Event.current;
            if (!rect.Contains(evt.mousePosition))
                return;

            if (evt.type == EventType.MouseDown && evt.button == 0)
            {
                _selectedIndex = rowIndex;
                Repaint();
                evt.Use();
                return;
            }

            if (evt.type == EventType.ContextClick)
            {
                _selectedIndex = rowIndex;
                ShowContextMenu(row);
                Repaint();
                evt.Use();
            }
        }

        private static string BuildRichRowText(InspectorTraceEvent row)
        {
            var statusColor = row.Status switch
            {
                "pending" => "#FFAA00",
                "completed" => "#60D060",
                "failed" => "#FF6666",
                "canceled" => "#AAAAAA",
                _ => "#DDDDDD"
            };

            var time = row.StartTimeUtc.ToLocalTime().ToString("HH:mm:ss.fff");
            return
                $"{time} | {row.Type} | {row.Target} | <color={statusColor}>{row.Status}</color> | {row.Size} | {row.CallDuration} | {row.Payload}";
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
            _selectedIndex = -1;
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
                Repaint();
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

            if (_selectedIndex >= 0)
                _selectedIndex = Mathf.Max(0, _selectedIndex - 1);

            if (!string.IsNullOrEmpty(removed.TraceId))
                _indexByTraceId.Remove(removed.TraceId);
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
