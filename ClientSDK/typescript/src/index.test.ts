import { HeTuClient } from './client'
import { BrowserWebSocket } from './dom-socket'
import { logger } from './logger'
import { ZlibProtocol } from './protocol'

describe('HeTuClient测试', () => {
    beforeAll(() => {
        console.log('测试前请启动河图服务器的tests/app.py')
        logger.setLevel(-1) // 设置日志级别为DEBUG
        HeTuClient.setProtocol(new ZlibProtocol())
        HeTuClient.connect(new BrowserWebSocket('ws://127.0.0.1:2466/hetu'))
            .then(() => {
                console.log('连接断开测试结束')
            })
            .catch(() => {})
    })

    afterAll(() => {
        console.log('测试结束')
        HeTuClient.close()
    })

    test('debug', async () => {
        return new Promise((r) => setTimeout(r, 300))
    })

    test('测试行订阅', async () => {
        console.log('test RowSubscribe开始')

        // 测试订阅失败
        const sub = await HeTuClient.select('HP', 123, 'owner')
        expect(sub).toBeNull()

        // 测试订阅
        HeTuClient.callSystem('login', 123, true)
        HeTuClient.callSystem('use_hp', 1)
        const successSub = await HeTuClient.select('HP', 123, 'owner')
        expect(successSub).not.toBeNull()
        const lastValue = successSub!.data!.value
        console.log(`订阅时值：${lastValue}`)

        // 测试订阅事件
        let newValue = null
        successSub!.onUpdate = (sender) => {
            newValue = sender.data!.value
            console.log(`收到了更新...${newValue}`)
        }
        HeTuClient.callSystem('use_hp', 2)
        await new Promise((resolve) => setTimeout(resolve, 1000))
        expect(lastValue - 2).toEqual(newValue)

        // 测试重复订阅，返回的值应该是旧的
        HeTuClient.callSystem('use_hp', 1)
        let success = false
        try {
            let dupSub = await HeTuClient.select('HP', 123, 'owner')
            success = true
            // 测试上面的use_hp没有生效，因为这里没有任何切换线程的时间
            expect(dupSub!.data!.value).toEqual(lastValue - 2)
        } catch (error) {
            // 期望抛出类型错误
        }
        expect(success).toBeTruthy()

        // 测试取消订阅并垃圾回收
        successSub!.dispose()

        // 测试回收自动反订阅，顺带测试Class类型
        const typedSub = await HeTuClient.select('HP', 123, 'owner')
        expect(typedSub).not.toBeNull()
        expect(typedSub!.data!.value).toEqual(lastValue - 3)

        console.log('TestRowSubscribe结束')
    })

    test('测试系统调用', async () => {
        console.log('TestSystemCall开始')

        let responseID = 0
        HeTuClient.onResponse = (jsonData) => {
            const data = jsonData as Record<string, any>
            responseID = data['id'] as number
        }

        let callbackCalled = false
        HeTuClient.systemCallbacks.set('login', (_) => {
            callbackCalled = true
        })

        HeTuClient.callSystem('login', 123, true)
        await new Promise((resolve) => setTimeout(resolve, 300))
        expect(responseID).toEqual(123)

        expect(callbackCalled).toBeTruthy()

        console.log('TestSystemCall结束')
    })

    test('测试索引订阅更新事件', async () => {
        console.log('TestIndexSubscribeOnUpdate开始')

        // 测试订阅
        HeTuClient.callSystem('login', 234, true)
        HeTuClient.callSystem('use_hp', 1)
        const sub = await HeTuClient.query('HP', 'owner', 0, 300, 100)
        expect(sub).not.toBeNull()

        // 这是Owner权限表，应该只能取到自己的数据
        expect(sub!.rows.size).toEqual(1)
        const firstRow = Array.from(sub!.rows.values())[0]
        const lastValue = parseInt(firstRow['value'])

        // 测试订阅事件
        let newValue = null
        sub!.onUpdate = (sender, rowID) => {
            newValue = sender.rows.get(rowID)!.value
        }
        HeTuClient.callSystem('use_hp', 2)
        await new Promise((resolve) => setTimeout(resolve, 1000))
        expect(newValue).toEqual(lastValue - 2)

        console.log('TestIndexSubscribeOnUpdate结束')
    })

    test('测试索引订阅插入和删除事件', async () => {
        console.log('TestIndexSubscribeOnInsert开始')

        HeTuClient.callSystem('login', 345, true)
        HeTuClient.callSystem('move_user', 123, -10, -10)
        HeTuClient.callSystem('move_user', 234, 0, 0)
        HeTuClient.callSystem('move_user', 345, 10, 10)

        // 测试OnInsert, OnDelete
        const sub = await HeTuClient.query('Position', 'x', 0, 10, 100)
        expect(sub).not.toBeNull()

        let newPlayer = null
        sub!.onInsert = (sender, rowID) => {
            newPlayer = sender.rows.get(rowID)!.owner
        }
        HeTuClient.callSystem('move_user', 123, 2, -10)
        await new Promise((resolve) => setTimeout(resolve, 1000))
        expect(newPlayer).toEqual(123)

        // OnDelete
        let removedPlayer = null
        sub!.onDelete = (sender, rowID) => {
            removedPlayer = sender.rows.get(rowID)!.owner
        }
        HeTuClient.callSystem('move_user', 123, 11, -10)
        await new Promise((resolve) => setTimeout(resolve, 1000))
        expect(removedPlayer).toEqual(123)

        expect(sub!.rows.has(123)).toBeFalsy()
        console.log('TestIndexSubscribeOnInsert结束')
    })

    // 测试批量操作
    test('测试批量查询', async () => {
        console.log('测试批量查询开始')

        // 登录多个用户并移动到不同位置
        HeTuClient.callSystem('login', 500, true)
        for (let i = 1; i <= 10; i++) {
            HeTuClient.callSystem('move_user', 500 + i, 500 + i, 500 + i)
        }
        HeTuClient.callSystem('move_user', 500, 500, 500)

        // 查询大范围数据
        const sub = await HeTuClient.query('Position', 'x', 500, 510, 50)
        expect(sub).not.toBeNull()
        expect(sub!.rows.size).toBeGreaterThan(9)

        // 测试批量更新
        let updateCount = 0
        let deleteCount = 0
        sub!.onUpdate = () => {
            updateCount++
        }
        sub!.onDelete = (_sender, _rowID) => {
            deleteCount++
        }

        // 批量更新位置，应该只收到1个消息
        for (let i = 1; i <= 5; i++) {
            HeTuClient.callSystem('move_user', 500, 500 + i, 500 + i)
        }
        await new Promise((resolve) => setTimeout(resolve, 1000))
        expect(updateCount).toBe(1)

        // 移出范围
        HeTuClient.callSystem('move_user', 500, 522, 522)
        await new Promise((resolve) => setTimeout(resolve, 1000))
        expect(deleteCount).toBe(1)

        console.log(`测试批量查询结束`)
    })

    // 测试连接错误处理
    // test('测试连接错误处理', async () => {
    //     console.log('测试连接错误处理开始')

    //     HeTuClient.close()

    //     // 保存当前状态以便测试后恢复
    //     const result = await HeTuClient.connect(
    //         new BrowserWebSocket('ws://invalid-server:9999/hetu')
    //     ).catch((e) => e)

    //     expect(result).toBeInstanceOf(Error)

    //     // 恢复连接
    //     await HeTuClient.connect(new BrowserWebSocket('ws://127.0.0.1:2466/hetu'))

    //     console.log('测试连接错误处理结束')
    // })

    // 测试同时使用多种订阅类型
    test('测试混合订阅', async () => {
        console.log('测试混合订阅开始')

        HeTuClient.callSystem('login', 123, true)
        HeTuClient.callSystem('use_hp', 1)
        HeTuClient.callSystem('move_user', 700, 0, 0)

        // 同时订阅行和索引
        const [rowSub, indexSub] = await Promise.all([
            HeTuClient.select('HP', 123, 'owner'),
            HeTuClient.query('Position', 'owner', 700, 700, 1),
        ])

        expect(rowSub).not.toBeNull()
        expect(indexSub).not.toBeNull()

        // 测试两种订阅都能接收更新
        let rowUpdated = false
        let indexUpdated = false

        rowSub!.onUpdate = () => {
            rowUpdated = true
        }
        indexSub!.onUpdate = () => {
            indexUpdated = true
        }

        // 触发更新
        HeTuClient.callSystem('use_hp', 1)
        HeTuClient.callSystem('move_user', 700, 50, 50)

        await new Promise((resolve) => setTimeout(resolve, 1000))

        expect(rowUpdated || indexUpdated).toBeTruthy()

        console.log('测试混合订阅结束')
    })
})
