/**
 * 河图Client SDK
 */

import { logger } from './logger'
import { IProtocol } from './protocol'

/// WebSocket API基类，为了各种平台不同的实现
export interface IWebSocket {
    url: string
    connect(): void
    onopen: ((ev: Event) => any) | null
    onmessage: ((ev: MessageEvent) => any) | null
    onclose: ((ev: CloseEvent) => any) | null
    onerror: ((ev: Event) => any) | null
    send(data: ArrayBuffer | string): void
    close(): void
    readonly readyState: number
}

export abstract class BaseSubscription {
    readonly componentName: string
    private readonly _subscriptID: string

    protected constructor(subscriptID: string, componentName: string) {
        this._subscriptID = subscriptID
        this.componentName = componentName
    }

    abstract update(rowID: number, data: any): void

    /// 销毁远端订阅对象。
    /// Dispose应该明确调用，虽然gc回收时会调用，但时间不确定，这会导致服务器端该对象销毁不及时。
    dispose(): void {
        HeTuClient._unsubscribe(this._subscriptID, 'dispose')
    }
}

export class RowSubscription extends BaseSubscription {
    private _data: Record<string, any> | null = null

    onUpdate: ((subscription: RowSubscription) => void) | null = null
    onDelete: ((subscription: RowSubscription) => void) | null = null

    constructor(subscriptID: string, componentName: string, row: Record<string, any>) {
        super(subscriptID, componentName)
        this._data = row
    }

    get data(): Record<string, any> | null {
        return this._data
    }

    override update(_rowID: number, data: Record<string, any> | null): void {
        if (data === null) {
            this.onDelete?.(this)
            this._data = null
        } else {
            this._data = data
            this.onUpdate?.(this)
        }
    }
}

export class IndexSubscription extends BaseSubscription {
    private readonly _rows: Map<number, Record<string, any>>

    onUpdate: ((subscription: IndexSubscription, rowID: number) => void) | null = null
    onDelete: ((subscription: IndexSubscription, rowID: number) => void) | null = null
    onInsert: ((subscription: IndexSubscription, rowID: number) => void) | null = null

    constructor(subscriptID: string, componentName: string, rows: Record<string, any>[]) {
        super(subscriptID, componentName)
        this._rows = new Map()
        rows.forEach((row) => this._rows.set(row.id, row))
    }

    get rows(): Map<number, Record<string, any>> {
        return this._rows
    }

    override update(rowID: number, data: Record<string, any> | null): void {
        const exist = this._rows.has(rowID)
        const isDelete = data === null

        if (isDelete) {
            if (!exist) return
            this.onDelete?.(this, rowID)
            this._rows.delete(rowID)
        } else {
            this._rows.set(rowID, data)
            if (exist) {
                this.onUpdate?.(this, rowID)
            } else {
                this.onInsert?.(this, rowID)
            }
        }
    }
}

class HeTuClientImpl {
    private _sendingQueue: Uint8Array[] = []
    private _waitingCallbacks: ((result: any) => void)[] = []
    private _subscriptions: Map<string, WeakRef<BaseSubscription>> = new Map()
    private _socket: IWebSocket | null = null
    private _protocol: IProtocol | null = null

    // 连接成功时的回调
    public onConnected: (() => void) | null = null
    // 收到System返回的`ResponseToClient`时的回调，根据你服务器发送的是什么数据类型来转换
    public onResponse: ((data: any) => void) | null = null
    // 本地调用System时的回调。调用时立即就会回调，是否成功调用未知。
    public systemCallbacks: Map<string, (args: any[]) => void> = new Map()

    constructor() {
        logger.info('HeTuClientSDK initialized')
        this._runLeakDetectionTask()
    }

    private _runLeakDetectionTask(): void {
        setInterval(() => {
            for (const [subID, weakRef] of this._subscriptions.entries()) {
                if (!weakRef.deref()) {
                    this._unsubscribe(subID, '检测到泄露')
                }
            }
        }, 30000) // 每30秒检查一次
    }

    private _cancelAllTasks(): void {
        logger.info('[HeTuClient] 取消所有等待任务...')
        this._waitingCallbacks = []
    }

    // 设置封包的编码/解码协议，封包可以进行压缩和加密。默认不加密，使用zlib压缩。
    // 协议要和你的河图服务器中的配置一致
    public setProtocol(protocol: IProtocol): void {
        this._protocol = protocol
    }

    /*
     * 连接到河图url，url格式为"wss://host:port/hetu"
     * 此方法为async/await异步堵塞，在连接断开前不会结束。
     *
     * 返回异常（而不是抛出异常）。
     * - 连接异常断开返回Exception；
     * - 正常断开返回null。
     *
     * 使用示例：
     * ```typescript
     * import { HeTuClient } from 'hetu-client-sdk';
     * import { logger } from 'hetu-client-sdk/logger';
     * import { BrowserWebSocket } from 'hetu-client-sdk/dom-socket';
     * // 微信使用hetu-client-sdk/wx-socket库
     * logger.setLevel(0); // 设置日志级别为DEBUG
     * HeTuClient.setProtocol(new Protocol())
     * HeTuClient.onConnected = () => {
     *   HeTuClient.callSystem("login", "userToken");
     * }
     * // 手游可以放入while循环，实现断线自动重连
     * while (true) {
     *     var e = await HeTuClient.connect(new BrowserWebSocket("wss://host:port/hetu"));
     *     // 断线处理...是否重连等等
     *     if (e is null)
     *         break;
     *     else
     *         logger.warn("连接断开, 将继续重连：" + e.Message);
     *     await sleep(1000);
     * }
     * ```
     */
    public async connect(socket: IWebSocket): Promise<Error | null> {
        // 检查连接状态(应该不会遇到）
        if (this._socket && this._socket.readyState !== WebSocket.CLOSED) {
            logger.error('[HeTuClient] Connect前请先Close Socket。')
            return null
        }

        // 前置清理
        logger.info(`[HeTuClient] 正在创建新连接...${socket.url}`)
        this._subscriptions = new Map()

        // 连接，并等待连接关闭
        let lastState = 'ReadyForConnect'
        return new Promise((resolve) => {
            this._socket = socket
            this._socket.onopen = () => {
                logger.info('[HeTuClient] 连接成功。')
                lastState = 'Connected'
                this.onConnected?.()

                for (const data of this._sendingQueue) {
                    this._socket?.send(data)
                }
                this._sendingQueue = []
            }

            this._socket.onmessage = (event) => {
                const data =
                    event.data instanceof Blob
                        ? (typeof window !== 'undefined'
                              ? new Promise<ArrayBuffer>((resolve) => {
                                    const reader = new FileReader()
                                    reader.onload = () => resolve(reader.result as ArrayBuffer)
                                    reader.readAsArrayBuffer(event.data as Blob)
                                })
                              : (event.data as Blob).arrayBuffer()
                          ).then((buffer) => new Uint8Array(buffer))
                        : new Uint8Array(event.data as ArrayBuffer)

                Promise.resolve(data).then((buffer) => this._onReceived(buffer))
            }

            this._socket.onerror = (event) => {
                switch (lastState) {
                    case 'ReadyForConnect':
                        logger.error(`[HeTuClient] 连接失败: ${event}`)
                        break
                    case 'Connected':
                        logger.error(`[HeTuClient] 接受消息时发生异常: ${event}`)
                        break
                }
            }

            this._socket.onclose = (event) => {
                this._cancelAllTasks()
                if (event.code === 1000) {
                    logger.info('[HeTuClient] 连接断开，收到了服务器Close消息。')
                    resolve(null)
                } else {
                    resolve(new Error(event.reason || '连接异常断开'))
                }
            }

            this._socket.connect()
        })
    }

    // 关闭河图连接
    public close(): void {
        logger.info('[HeTuClient] 主动调用了Close')
        this._cancelAllTasks()
        this._socket?.close()
    }

    /// 后台发送System调用，此方法立即返回。
    /// 可通过`HeTuClient.systemCallbacks["system_name"] = (args) => {}`
    /// 注册客户端调用回调（非服务器端回调）。
    public callSystem(systemName: string, ...args: any[]): void {
        const payload = ['sys', systemName, ...args]
        this._send(payload)
        logger.debug(`[HeTuClient] 发送System调用: ${systemName}(${args.join(', ')})`)
        const callback = this.systemCallbacks.get(systemName)
        callback?.(args)
    }

    // todo 异步堵塞的CallSystem，会等待并返回服务器回应

    /*
     * 订阅组件的行数据。订阅`where`属性值==`value`的第一行数据。
     * `select`只对“单行”订阅，如果没有查询到行，会返回`null`。
     * 如果想要订阅不存在的行，请用`query`订阅索引。
     *
     * 返回`null`如果没查询到行，否则返回`RowSubscription`对象。
     * 可通过`RowSubscription.data`获取数据。
     * 可以注册`RowSubscription.onUpdate`和`onDelete`事件处理数据更新。
     *
     *
     * 使用示例
     * ```typescript
     * // 假设HP组件有owner属性，表示属于哪个玩家，value属性表示hp值。
     * var subscription = await HeTuClient.select("HP", user_id, "owner");
     * logger.debug("My HP:" + subscription.data["value"]);
     *
     * subscription.onUpdate += (sender, rowID) => {
     *     logger.debug("My New HP:" + sender.data["value"]);
     * }
     * var subscription = await HeTuClient.select("HP", user_id, "owner");
     * logger.debug("My HP:" + subscription.data.value);
     * ```
     */
    public async select(
        componentName: string,
        value: any,
        where: string = 'id'
    ): Promise<RowSubscription | null> {
        // 如果where是id，我们可以事先判断是否已经订阅过
        if (where === 'id') {
            const predictID = this._makeSubID(componentName, 'id', value, null, 1, false)
            const subscribed = this._subscriptions.get(predictID)?.deref()
            if (subscribed instanceof RowSubscription) {
                return subscribed as RowSubscription
            }
        }

        // 向服务器订阅
        const payload = ['sub', componentName, 'select', value, where]
        logger.debug(`[HeTuClient] 发送Select订阅: ${componentName}.${where}[${value}:]`)
        this._send(payload)

        // 等待服务器结果
        return new Promise<RowSubscription | null>((resolve) => {
            this._waitingCallbacks.push((subMsg) => {
                // 如果没有查询到值
                const subID = subMsg[1] as string
                if (subID === null) {
                    logger.replay(`[HeTuClient] select没有查询到值；也可能无权限`)
                    resolve(null)
                    return
                }
                // 如果依然是重复订阅，直接返回副本
                const duplicateSubscribed = this._subscriptions.get(subID)?.deref()
                if (duplicateSubscribed instanceof RowSubscription) {
                    resolve(duplicateSubscribed as RowSubscription)
                    return
                }

                const data = subMsg[2]
                const newSub = new RowSubscription(subID, componentName as string, data)
                this._subscriptions.set(subID, new WeakRef(newSub))
                logger.info(`[HeTuClient] 成功订阅了 ${subID}`)
                resolve(newSub)
            })
        })
    }

    /*
     * 订阅组件的索引数据。`index`是开启了索引的属性名，`left`和`right`为索引范围，
     * `limit`为返回数量，`desc`为是否降序，`force`为未查询到数据时是否也强制订阅。
     *
     * 返回`IndexSubscription`对象。
     * 可通过`IndexSubscription.rows`获取数据。
     * 并可以注册`IndexSubscription.onInsert`和`onUpdate`，`onDelete`数据事件。
     *
     * 如果使用了服务器端工具自动生成了ts interface定义并导入，则`query`会
     * 自动返回该结构数据，否则返回Json格式。使用结构数据要自己维护版本，
     * 防止和服务器不一致。
     * 如果目标组件权限为Owner，则只能查询到`owner`属性==自己的行。
     *
     * 使用示例
     * ```typescript
     * var subscription = await HeTuClient.query("HP", "owner", 0, 9999, 10);
     * foreach (var row in subscription.rows)
     *     logger.info($"HP: {row}");
     * subscription.onUpdate += (sender, rowID) => {
     *     logger.info($"New HP: {rowID}:{sender.rows[rowID]}");
     * }
     * subscription.onDelete += (sender, rowID) => {
     *     logger.info($"Delete row: {rowID}，之前的数据：{sender.rows[rowID]}");
     * }
     * ```
     */
    public async query(
        componentName: string,
        index: string,
        left: any,
        right: any,
        limit: number,
        desc: boolean = false,
        force: boolean = true
    ): Promise<IndexSubscription | null> {
        // 先要组合sub_id看看是否已订阅过
        const predictID = this._makeSubID(componentName, index, left, right, limit, desc)
        const subscribed = this._subscriptions.get(predictID)?.deref()
        if (subscribed instanceof IndexSubscription) {
            return subscribed as IndexSubscription
        }

        // 发送订阅请求
        const payload = ['sub', componentName, 'query', index, left, right, limit, desc, force]
        logger.debug(`[HeTuClient] 发送Query订阅: ${predictID}`)
        this._send(payload)

        return new Promise<IndexSubscription | null>((resolve) => {
            this._waitingCallbacks.push((subMsg) => {
                const subID = subMsg[1] as string
                // 如果没有查询到值
                if (subID === null) {
                    logger.replay(`[HeTuClient] select没有查询到值；也可能无权限`)
                    resolve(null)
                    return
                }
                // 如果依然是重复订阅，直接返回副本
                const duplicateSubscribed = this._subscriptions.get(subID)?.deref()
                if (duplicateSubscribed instanceof IndexSubscription) {
                    resolve(duplicateSubscribed as IndexSubscription)
                    return
                }

                const rows = subMsg[2] as Record<string, any>[]
                const newSub = new IndexSubscription(subID, componentName as string, rows)
                this._subscriptions.set(subID, new WeakRef(newSub))
                logger.info(`[HeTuClient] 成功订阅了 ${subID}`)
                resolve(newSub)
            })
        })
    }

    // --------------以下为内部方法----------------

    _unsubscribe(subID: string, from: string): void {
        if (!this._subscriptions.has(subID)) return
        this._subscriptions.delete(subID)
        const payload = ['unsub', subID]
        this._send(payload)
        logger.info(`[HeTuClient] 因BaseSubscription ${from}，已取消订阅 ${subID}`)
    }

    private _makeSubID(
        table: string,
        index: string,
        left: any,
        right: any,
        limit: number,
        desc: boolean
    ): string {
        return `${table}.${index}[${left}:${right ?? 'None'}:${desc ? -1 : 1}][${limit}]`
    }

    private _send(payload: any): void {
        const jsonString = JSON.stringify(payload)
        let buffer = new TextEncoder().encode(jsonString)

        if (this._protocol) {
            buffer = this._protocol.compress(buffer)
            if (this._protocol.crypt) {
                buffer = this._protocol.crypt(buffer)
            }
        }

        if (this._socket && this._socket.readyState === WebSocket.OPEN) {
            this._socket.send(buffer)
        } else {
            logger.info('尝试发送数据但连接未建立，将加入队列在建立后发送。')
            this._sendingQueue.push(buffer)
        }
    }

    private _onReceived(buffer: Uint8Array): void {
        // 解码消息
        if (this._protocol) {
            if (this._protocol.decrypt) {
                buffer = this._protocol.decrypt(buffer)
            }
            buffer = this._protocol.decompress(buffer)
        }

        const decoded = new TextDecoder().decode(buffer)
        const structuredMsg = JSON.parse(decoded)

        if (!structuredMsg) return
        // 处理消息
        switch (structuredMsg[0]) {
            case 'rsp':
                this.onResponse?.(structuredMsg[1])
                break
            case 'sub':
                const callback = this._waitingCallbacks.shift()
                if (structuredMsg[2] !== null) {
                    logger.replay(`[HeTuClient] 收到了订阅值: ${JSON.stringify(structuredMsg[2])}`)
                }
                if (callback) callback(structuredMsg)
                break
            case 'updt':
                const subID = structuredMsg[1] as string
                logger.replay(
                    `[HeTuClient] 收到了更新: ${subID}: ${JSON.stringify(structuredMsg[2])}`
                )
                const subscription = this._subscriptions.get(subID)?.deref()
                if (!subscription) break

                const rows = structuredMsg[2] as Record<number, any>
                for (const [rowID, data] of Object.entries(rows)) {
                    subscription.update(parseInt(rowID), data)
                }
                break
        }
    }
}

export const HeTuClient = new HeTuClientImpl()

export type { HeTuClient as HeTuClientSDK }
