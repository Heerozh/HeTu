// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Inspector请求采集</summary>

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace HeTu
{
    internal enum InspectorTraceCompletionMode
    {
        OnResponse,
        AfterSend
    }

    /// <summary>
    ///     Inspector 事件分发器接口。
    /// </summary>
    public interface IInspectorTraceDispatcher
    {
        /// <summary>
        ///     接收一条采集事件。
        /// </summary>
        void Dispatch(InspectorTraceEvent traceEvent);
    }

    /// <summary>
    ///     InspectorTraceDispatcher 的简单实现，将事件打印到控制台。
    /// </summary>
    public class LoggerInspectorTraceDispatcher : IInspectorTraceDispatcher
    {
        public void Dispatch(InspectorTraceEvent traceEvent) =>
            // 将InspectorTraceEvent打印到控制台，注意时间用本地时间
            Logger.Instance.Info(
                $"Inspector: {traceEvent.StartTimeUtc.ToLocalTime()} " +
                $"| {traceEvent.Type} " +
                $"| {traceEvent.Target} " +
                $"| {traceEvent.Status}" +
                $"| {traceEvent.Size}" +
                $"| {traceEvent.CallDuration} +" +
                $"| {traceEvent.Payload}" +
                $"| {traceEvent.Response}");
    }

    /// <summary>
    ///     Inspector 调用堆栈帧模型。
    /// </summary>
    public sealed class InspectorStackFrameInfo
    {
        public string DisplayText { get; set; }

        public string FilePath { get; set; }

        public int Line { get; set; }

        public bool IsVisible { get; set; }

        public bool IsUserCode { get; set; }

        public bool IsPrimaryUserFrame { get; set; }

        public InspectorStackFrameInfo Clone() =>
            new()
            {
                DisplayText = DisplayText,
                FilePath = FilePath,
                Line = Line,
                IsVisible = IsVisible,
                IsUserCode = IsUserCode,
                IsPrimaryUserFrame = IsPrimaryUserFrame
            };
    }

    /// <summary>
    ///     Inspector 事件模型。
    /// </summary>
    public sealed class InspectorTraceEvent
    {
        public string TraceId { get; set; }

        public DateTime StartTimeUtc { get; set; }

        public string Type { get; set; }

        public string Target { get; set; }

        public string Payload { get; set; }

        public string Response { get; set; }

        public string CallerStack { get; set; }

        public List<InspectorStackFrameInfo> CallerFrames { get; set; }

        internal InspectorTraceCompletionMode CompletionMode { get; set; }

        /// <summary>
        ///     形如 "源/传输"，单位字节。
        /// </summary>
        public string Size { get; set; }

        /// <summary>
        ///     Pending / xxms / 不可用
        /// </summary>
        public string CallDuration { get; set; }

        /// <summary>
        ///     pending / completed / canceled / failed
        /// </summary>
        public string Status { get; set; }

        public InspectorTraceEvent Clone() =>
            new()
            {
                TraceId = TraceId,
                StartTimeUtc = StartTimeUtc,
                Type = Type,
                Target = Target,
                Payload = Payload,
                Response = Response,
                CallerStack = CallerStack,
                CallerFrames = CallerFrames?
                    .Select(frame => frame.Clone())
                    .ToList(),
                CompletionMode = CompletionMode,
                Size = Size,
                CallDuration = CallDuration,
                Status = Status
            };
    }

    /// <summary>
    ///     拦截(Intercept)-采样(Sample)-分发(Dispatch) 收集器。
    /// </summary>
    public sealed class InspectorTraceCollector
    {
        private const string NotAvailable = "-";
        private readonly List<IInspectorTraceDispatcher> _dispatchers = new();

        private readonly Dictionary<string, Stopwatch> _pendingStopwatches = new();
        private readonly Dictionary<string, InspectorTraceEvent> _pendingTraces = new();
        private readonly object _syncRoot = new();

        private bool _enabled;

        /// <summary>
        ///     当前是否启用拦截。
        /// </summary>
        public bool Enabled
        {
            get
            {
                lock (_syncRoot) return _enabled;
            }
        }

        /// <summary>
        ///     动态配置开关与采样率。
        /// </summary>
        public void Configure(bool enabled)
        {
            lock (_syncRoot)
            {
                _enabled = enabled;
            }
        }

        public void AddDispatcher(IInspectorTraceDispatcher dispatcher)
        {
            if (dispatcher == null) return;
            lock (_syncRoot)
            {
                if (_dispatchers.Contains(dispatcher)) return;
                _dispatchers.Add(dispatcher);
            }
        }

        public void RemoveDispatcher(IInspectorTraceDispatcher dispatcher)
        {
            if (dispatcher == null) return;
            lock (_syncRoot)
            {
                _dispatchers.Remove(dispatcher);
            }
        }

        /// <summary>
        ///     拦截并立即分发一条 Pending 请求事件。
        /// </summary>
        internal string InterceptRequest(string type, string target, object payload,
            InspectorTraceCompletionMode completionMode =
                InspectorTraceCompletionMode.OnResponse)
        {
            lock (_syncRoot)
            {
                if (!_enabled)
                    return null;

                var callerStack = InspectorTraceStack.CaptureCurrent(skipFrames: 2);
                var traceId = Guid.NewGuid().ToString("N");
                var traceEvent = new InspectorTraceEvent
                {
                    TraceId = traceId,
                    StartTimeUtc = DateTime.UtcNow,
                    Type = type,
                    Target = target,
                    Payload = InspectorTraceStringify.Stringify(payload),
                    Response = NotAvailable,
                    CallerStack = callerStack.RawStack,
                    CallerFrames = callerStack.Frames,
                    CompletionMode = completionMode,
                    Size = NotAvailable,
                    CallDuration = "Pending",
                    Status = "pending"
                };

                _pendingTraces[traceId] = traceEvent;
                var sw = Stopwatch.StartNew();
                _pendingStopwatches[traceId] = sw;

                DispatchInternal(traceEvent);
                return traceId;
            }
        }

        internal void CompleteAfterSendIfNeeded(string traceId)
        {
            if (traceId == null) return;
            lock (_syncRoot)
            {
                if (!_pendingTraces.TryGetValue(traceId, out var trace))
                    return;
                if (trace.CompletionMode != InspectorTraceCompletionMode.AfterSend)
                    return;

                CompleteRequestInternal(traceId, trace, "completed", null);
            }
        }

        /// <summary>
        ///     更新请求尺寸并重新分发（仍保持 Pending）。
        /// </summary>
        internal void UpdateRequestSize(string traceId, int sourceSizeBytes,
            int transportSizeBytes)
        {
            if (traceId == null) return;
            lock (_syncRoot)
            {
                if (!_pendingTraces.TryGetValue(traceId, out var trace))
                    return;

                trace.Size = BuildSize(sourceSizeBytes, transportSizeBytes);
                DispatchInternal(trace);
            }
        }

        /// <summary>
        ///     完成请求并分发最终状态。
        /// </summary>
        internal void CompleteRequest(string traceId, string status,
            object responsePayload = null)
        {
            if (traceId == null) return;
            lock (_syncRoot)
            {
                if (!_pendingTraces.TryGetValue(traceId, out var trace))
                    return;
                CompleteRequestInternal(traceId, trace, status, responsePayload);
            }
        }

        /// <summary>
        ///     收集 MessageUpdate 推送。
        /// </summary>
        internal void InterceptMessageUpdate(string target, JsonObject payload,
            StackTrace callerTrace, int sourceSizeBytes, int transportSizeBytes)
        {
            lock (_syncRoot)
            {
                if (!_enabled)
                    return;

                var captured = InspectorTraceStack.FromStackTrace(callerTrace);
                var traceEvent = new InspectorTraceEvent
                {
                    TraceId = Guid.NewGuid().ToString("N"),
                    StartTimeUtc = DateTime.UtcNow,
                    Type = HeTuClientBase.MessageUpdate,
                    Target = target,
                    Payload = InspectorTraceStringify.Stringify(payload),
                    Response = NotAvailable,
                    CallerStack = captured.RawStack,
                    CallerFrames = captured.Frames,
                    Size = BuildSize(sourceSizeBytes, transportSizeBytes),
                    CallDuration = NotAvailable,
                    Status = "completed"
                };

                DispatchInternal(traceEvent);
            }
        }

        private void DispatchInternal(InspectorTraceEvent traceEvent)
        {
            // Unity 主线程场景中直接同步分发即可。
            var snapshot = traceEvent.Clone();
            foreach (var dispatcher in _dispatchers)
                dispatcher.Dispatch(snapshot);
        }

        private void CompleteRequestInternal(string traceId, InspectorTraceEvent trace,
            string status, object responsePayload)
        {
            if (_pendingStopwatches.TryGetValue(traceId, out var sw))
            {
                sw.Stop();
                trace.CallDuration = $"{sw.ElapsedMilliseconds}ms";
            }

            trace.Status = status;
            if (responsePayload != null)
                trace.Response = InspectorTraceStringify.Stringify(responsePayload);
            DispatchInternal(trace);

            _pendingTraces.Remove(traceId);
            _pendingStopwatches.Remove(traceId);
        }

        private static string BuildSize(int source, int transport)
        {
            if (source < 0 || transport < 0) return NotAvailable;
            return source switch
            {
                // 尺寸根据实际大小自动转换为b/kb/mb。源/传输
                >= 1024 * 1024 =>
                    $"{source / (1024 * 1024)}MB/{transport / (1024 * 1024)}MB",
                >= 1024 => $"{source / 1024}KB/{transport / 1024}KB",
                _ => $"{source}B/{transport}B"
            };
        }
    }

    internal static class InspectorTraceStringify
    {
        private const int MaxDepth = 6;

        public static string Stringify(object value)
        {
            var sb = new StringBuilder(256);
            AppendValue(sb, value, 0);
            return sb.ToString();
        }

        private static void AppendValue(StringBuilder sb, object value, int depth)
        {
            if (depth > MaxDepth)
            {
                sb.Append("...");
                return;
            }

            switch (value)
            {
                case null:
                    sb.Append("null");
                    return;
                case string str:
                    sb.Append('"').Append(str).Append('"');
                    return;
                case JsonObject jsonObj:
                    AppendValue(sb, jsonObj.ToUntyped(), depth + 1);
                    return;
                case IDictionary dict:
                    {
                        sb.Append('{');
                        var first = true;
                        foreach (DictionaryEntry item in dict)
                        {
                            if (!first) sb.Append(", ");
                            first = false;
                            AppendValue(sb, item.Key, depth + 1);
                            sb.Append(": ");
                            AppendValue(sb, item.Value, depth + 1);
                        }

                        sb.Append('}');
                        return;
                    }
                case IEnumerable enumerable when value is not byte[]:
                    {
                        sb.Append('[');
                        var first = true;
                        foreach (var item in enumerable)
                        {
                            if (!first) sb.Append(", ");
                            first = false;
                            AppendValue(sb, item, depth + 1);
                        }

                        sb.Append(']');
                        return;
                    }
                case byte[] bytes:
                    sb.Append($"<bytes:{bytes.Length}>");
                    return;
                default:
                    sb.Append(value);
                    return;
            }
        }
    }

    internal sealed class InspectorCapturedStack
    {
        public string RawStack { get; set; }

        public List<InspectorStackFrameInfo> Frames { get; set; }
    }

    internal static class InspectorTraceStack
    {
        internal static InspectorCapturedStack CaptureCurrent(int skipFrames = 0)
        {
#if DEBUG
            var trace = new StackTrace(skipFrames, true);
            return FromStackTrace(trace);
#else
            return new InspectorCapturedStack
            {
                RawStack = "-",
                Frames = BuildUnavailableFrames()
            };
#endif
        }

        internal static InspectorCapturedStack FromStackTrace(StackTrace trace)
        {
            if (trace == null)
                return new InspectorCapturedStack
                {
                    RawStack = "-",
                    Frames = BuildUnavailableFrames()
                };

            var frames = trace.GetFrames();
            var callerFrames = frames == null
                ? BuildUnavailableFrames()
                : BuildFrameInfos(frames);
            return new InspectorCapturedStack
            {
                RawStack = string.IsNullOrWhiteSpace(trace.ToString())
                    ? "-"
                    : trace.ToString(),
                Frames = callerFrames
            };
        }

        internal static InspectorStackFrameInfo CreateFrameInfo(string declaringTypeName,
            string methodName, string filePath, int line,
            bool isUserCode = false, bool isPrimaryUserFrame = false)
        {
            var hasVisibleSource = !string.IsNullOrWhiteSpace(filePath) && line > 0;
            var fileName = hasVisibleSource ? Path.GetFileName(filePath) : "-";
            var lineText = hasVisibleSource ? line.ToString() : "-";
            return new InspectorStackFrameInfo
            {
                DisplayText =
                    $"{declaringTypeName}.{methodName} ({fileName}:{lineText})",
                FilePath = hasVisibleSource ? filePath : null,
                Line = hasVisibleSource ? line : 0,
                IsVisible = hasVisibleSource,
                IsUserCode = isUserCode,
                IsPrimaryUserFrame = isPrimaryUserFrame
            };
        }

        private static List<InspectorStackFrameInfo> BuildFrameInfos(StackFrame[] frames)
        {
            var callerFrames = new List<InspectorStackFrameInfo>();
            var firstUserFrameIndex = -1;
            foreach (var frame in frames)
            {
                var method = frame.GetMethod();
                var methodName = method?.Name;
                var typeName = method?.DeclaringType?.Name;
                if (string.IsNullOrWhiteSpace(methodName) ||
                    string.IsNullOrWhiteSpace(typeName) ||
                    ShouldSkipFrame(typeName, methodName))
                    continue;

                var isUserCode = IsUserCodeDeclaringType(method.DeclaringType);
                callerFrames.Add(CreateFrameInfo(typeName, methodName,
                    frame.GetFileName(),
                    frame.GetFileLineNumber(),
                    isUserCode));
                if (isUserCode && firstUserFrameIndex < 0)
                    firstUserFrameIndex = callerFrames.Count - 1;
            }

            if (firstUserFrameIndex >= 0)
                callerFrames[firstUserFrameIndex].IsPrimaryUserFrame = true;

            return callerFrames.Count == 0 ? BuildUnavailableFrames() : callerFrames;
        }

        internal static bool IsUserCodeDeclaringType(Type declaringType)
        {
            if (declaringType == null)
                return false;

            var assembly = declaringType.Assembly;
            if (assembly == typeof(HeTuClientBase).Assembly)
                return false;

            var assemblyName = assembly.GetName().Name ?? string.Empty;
            if (assemblyName.StartsWith("System", StringComparison.Ordinal) ||
                assemblyName.StartsWith("mscorlib", StringComparison.Ordinal) ||
                assemblyName.StartsWith("netstandard", StringComparison.Ordinal) ||
                assemblyName.StartsWith("Unity", StringComparison.Ordinal) ||
                assemblyName.StartsWith("nunit", StringComparison.OrdinalIgnoreCase) ||
                assemblyName.StartsWith("Mono.", StringComparison.Ordinal))
                return false;

            var ns = declaringType.Namespace ?? string.Empty;
            if (ns.StartsWith("System", StringComparison.Ordinal) ||
                ns.StartsWith("UnityEngine", StringComparison.Ordinal) ||
                ns.StartsWith("UnityEditor", StringComparison.Ordinal) ||
                ns.StartsWith("HeTu", StringComparison.Ordinal) ||
                ns.StartsWith("NUnit", StringComparison.Ordinal) ||
                ns.StartsWith("Mono.", StringComparison.Ordinal))
                return false;

            return true;
        }

        private static List<InspectorStackFrameInfo> BuildUnavailableFrames() =>
            new()
            {
                new InspectorStackFrameInfo
                {
                    DisplayText = "-",
                    FilePath = null,
                    Line = 0,
                    IsVisible = false
                }
            };

        private static bool ShouldSkipFrame(string typeName, string methodName) =>
            (typeName == nameof(Environment) && methodName == "get_StackTrace") ||
            (typeName == nameof(InspectorTraceStack) && methodName == nameof(CaptureCurrent)) ||
            (typeName == nameof(HeTuClientBase) &&
             methodName == "CaptureCreationTrace");
    }
}
