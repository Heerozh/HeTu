// <copyright>
// Copyright 2024, Heerozh. All rights reserved.
// </copyright>
// <summary>河图客户端SDK的Logger库</summary>.


namespace HeTu
{
    /// <summary>
    ///     SDK 日志入口。
    ///     默认不输出日志，需要在运行前通过 <see cref="SetLogger"/> 注入日志函数。
    /// </summary>
    public class Logger
    {
        /// <summary>
        ///     日志函数签名。
        /// </summary>
        /// <param name="message">日志消息对象。</param>
        public delegate void LogFunction(object message);

        /// <summary>
        ///     全局单例。
        /// </summary>
        public static readonly Logger Instance = new();

        private LogFunction _logDebug;
        private LogFunction _logError;
        private LogFunction _logInfo;

        /// <summary>
        ///     是否已配置日志函数。
        /// </summary>
        public bool IsSetup => _logError != null;

        /// <summary>
        ///     设置日志输出函数。可以直接传入Unity的Debug.Log和Debug.LogError
        /// </summary>
        /// <param name="info">信息日志输出。</param>
        /// <param name="err">错误日志输出。</param>
        /// <param name="dbg">调试日志输出，可选。</param>
        public void SetLogger(LogFunction info, LogFunction err, LogFunction dbg = null)
        {
            _logInfo = info;
            _logError = err;
            _logDebug = dbg;
        }

        /// <summary>
        ///     输出信息日志。
        /// </summary>
        public void Info(object message) => _logInfo?.Invoke(message);

        /// <summary>
        ///     输出错误日志。
        /// </summary>
        public void Error(object message) => _logError?.Invoke(message);

        /// <summary>
        ///     输出调试日志。
        /// </summary>
        public void Debug(object message) => _logDebug?.Invoke(message);
    }
}
