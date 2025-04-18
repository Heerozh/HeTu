import { defineConfig } from 'vitest/config'

export default defineConfig({
    test: {
        globals: true,        // 支持 describe/test/expect 无需 import
        environment: 'node',  // 或 'jsdom' (模拟浏览器)
        coverage: {
            reporter: ['text', 'json', 'html']
        }
    },
})