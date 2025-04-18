enum LogLevel {
    DEBUG = 0,
    INFO = 1,
    WARN = 2,
    ERROR = 3,
    SILENT = 4, // 关闭所有日志
}

class Logger {
    private level: LogLevel = LogLevel.INFO // 默认级别为 INFO

    // 设置日志级别
    setLevel(level: LogLevel): void {
        this.level = level
    }

    // DEBUG 级别日志
    debug(...args: any[]): void {
        if (this.level <= LogLevel.DEBUG) {
            console.debug('[DEBUG]', ...args)
        }
    }

    // INFO 级别日志
    info(...args: any[]): void {
        if (this.level <= LogLevel.INFO) {
            console.info('[INFO]', ...args)
        }
    }

    // WARN 级别日志
    warn(...args: any[]): void {
        if (this.level <= LogLevel.WARN) {
            console.warn('[WARN]', ...args)
        }
    }

    // ERROR 级别日志
    error(...args: any[]): void {
        if (this.level <= LogLevel.ERROR) {
            console.error('[ERROR]', ...args)
        }
    }
}

export const logger = new Logger()
