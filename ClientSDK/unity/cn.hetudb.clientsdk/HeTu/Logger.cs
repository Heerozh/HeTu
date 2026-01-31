// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Logger库</summary>.


namespace HeTu
{
    public class Logger
    {
        public delegate void LogFunction(object message);

        public static readonly Logger Instance = new();

        private LogFunction _logDebug;
        private LogFunction _logError;
        private LogFunction _logInfo;

        public bool IsSetup => _logError != null;

        // 设置日志函数，info为信息日志，err为错误日志。可以直接传入Unity的Debug.Log和Debug.LogError
        public void SetLogger(LogFunction info, LogFunction err, LogFunction dbg = null)
        {
            _logInfo = info;
            _logError = err;
            _logDebug = dbg;
        }

        public void Info(object message) => _logInfo?.Invoke(message);

        public void Error(object message) => _logError?.Invoke(message);

        public void Debug(object message) => _logDebug?.Invoke(message);
    }
}
