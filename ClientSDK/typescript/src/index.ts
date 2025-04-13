/**
 * 河图Client SDK
 */

interface WebSocketInterface {
    constructor(url: string): void;
    onopen: ((this: WebSocketInterface, ev: Event) => any) | null;
    onmessage: ((this: WebSocketInterface, ev: MessageEvent) => any) | null;
    onclose: ((this: WebSocketInterface, ev: CloseEvent) => any) | null;
    onerror: ((this: WebSocketInterface, ev: Event) => any) | null;
    send(data: ArrayBuffer | string): void;
    close(code?: number, reason?: string): void;
    readonly readyState: number;
}

interface IBaseComponent {
    id: number;
}

class DictComponent extends Map<string, string> implements IBaseComponent {
    get id(): number {
        return Number(this.get('id'));
    }
}

abstract class BaseSubscription {
    readonly componentName: string;
    private subscriptID: string;

    constructor(subscriptID: string, componentName: string) {
        this.subscriptID = subscriptID;
        this.componentName = componentName;
    }

    abstract update(rowID: number, data: any): void;

    dispose(): void {
        HeTuClient._unsubscribe(this.subscriptID);
    }
}

class RowSubscription<T extends IBaseComponent> extends BaseSubscription {
    private _data: T;

    onUpdate: ((subscription: RowSubscription<T>) => void) | null = null;
    onDelete: ((subscription: RowSubscription<T>) => void) | null = null;

    constructor(subscriptID: string, componentName: string, row: T) {
        super(subscriptID, componentName);
        this._data = row;
    }

    get data(): T {
        return this._data;
    }

    override update(_rowID: number, data: any): void {
        if (data === null) {
            if (this.onDelete) this.onDelete(this);
            this._data = null as any;
        } else {
            this._data = data as T;
            if (this.onUpdate) this.onUpdate(this);
        }
    }
}

class IndexSubscription<T extends IBaseComponent> extends BaseSubscription {
    private _rows: Map<number, T>;

    onUpdate: ((subscription: IndexSubscription<T>, rowID: number) => void) | null = null;
    onDelete: ((subscription: IndexSubscription<T>, rowID: number) => void) | null = null;
    onInsert: ((subscription: IndexSubscription<T>, rowID: number) => void) | null = null;

    constructor(subscriptID: string, componentName: string, rows: T[]) {
        super(subscriptID, componentName);
        this._rows = new Map();
        rows.forEach(row => this._rows.set(row.id, row));
    }

    get rows(): Map<number, T> {
        return this._rows;
    }

    override update(rowID: number, data: any): void {
        const exist = this._rows.has(rowID);
        const isDelete = data === null;

        if (isDelete) {
            if (!exist) return;
            if (this.onDelete) this.onDelete(this, rowID);
            this._rows.delete(rowID);
        } else {
            const tData = data as T;
            this._rows.set(rowID, tData);
            if (exist) {
                if (this.onUpdate) this.onUpdate(this, rowID);
            } else {
                if (this.onInsert) this.onInsert(this, rowID);
            }
        }
    }
}

interface IProtocol {
    compress(data: Uint8Array): Uint8Array;
    decompress(data: Uint8Array): Uint8Array;
    crypt?(data: Uint8Array): Uint8Array;
    decrypt?(data: Uint8Array): Uint8Array;
}


class HeTuClientImpl {
    private static _socketLib: new (url: string) => WebSocketInterface;
    private _sendingQueue: Uint8Array[] = [];
    private _waitingCallbacks: ((result: any) => void)[] = [];
    private _subscriptions: Map<string, WeakRef<BaseSubscription>> = new Map();
    private _socket: WebSocketInterface | null = null;
    private _protocol: IProtocol | null = null;

    public onConnected: (() => void) | null = null;
    public onResponse: ((data: any) => void) | null = null;
    public systemCallbacks: Map<string, (args: any[]) => void> = new Map();

    constructor() {
        console.log("HeTuClientSDK initialized");
    }

    private _cancelAllTasks(): void {
        console.log("[HeTuClient] 取消所有等待任务...");
        this._waitingCallbacks = [];
    }

    public setProtocol(protocol: IProtocol): void {
        this._protocol = protocol;
    }

    public setWebSocketLib(socketLib: new (url: string) => WebSocketInterface): void {
        HeTuClientImpl._socketLib = socketLib;
    }

    public connect(url: string): Promise<Error | null> {
        return new Promise((resolve) => {
            if (this._socket && this._socket.readyState !== WebSocket.CLOSED) {
                console.error("[HeTuClient] Please close socket before connecting again.");
                resolve(null);
                return;
            }

            console.log(`[HeTuClient] Connecting to: ${url}...`);
            this._subscriptions = new Map();

            let lastState = "ReadyForConnect";
            this._socket = new HeTuClientImpl._socketLib(url);

            this._socket.onopen = () => {
                console.log("[HeTuClient] Connection established.");
                lastState = "Connected";
                if (this.onConnected) this.onConnected();

                for (const data of this._sendingQueue) {
                    this._socket?.send(data);
                }
                this._sendingQueue = [];
            };

            this._socket.onmessage = (event) => {
                const data = event.data instanceof Blob
                    ? new Promise<ArrayBuffer>(resolve => {
                        const reader = new FileReader();
                        reader.onload = () => resolve(reader.result as ArrayBuffer);
                        reader.readAsArrayBuffer(event.data as Blob);
                    }).then(buffer => new Uint8Array(buffer))
                    : new Uint8Array(event.data as ArrayBuffer);

                Promise.resolve(data).then(buffer => this._onReceived(buffer));
            };

            this._socket.onclose = (event) => {
                this._cancelAllTasks();
                if (event.code === 1000) {
                    console.log("[HeTuClient] Connection closed normally.");
                    resolve(null);
                } else {
                    resolve(new Error(event.reason || "Connection closed abnormally"));
                }
            };

            this._socket.onerror = (event) => {
                switch (lastState) {
                    case "ReadyForConnect":
                        console.error(`[HeTuClient] Connection failed: ${event}`);
                        break;
                    case "Connected":
                        console.error(`[HeTuClient] Error receiving message: ${event}`);
                        break;
                }
            };
        });
    }

    public close(): void {
        console.log("[HeTuClient] Actively calling Close");
        this._cancelAllTasks();
        this._socket?.close();
    }

    public callSystem(systemName: string, ...args: any[]): void {
        const payload = ["sys", systemName, ...args];
        this._send(payload);
        const callback = this.systemCallbacks.get(systemName);
        if (callback) callback(args);
    }

    public async select<T extends IBaseComponent>(
        value: any,
        where: string = "id",
        componentName?: string
    ): Promise<RowSubscription<T> | null> {
        componentName = componentName ?? (typeof T !== 'function' ? "DictComponent" : T.name);

        if (where === "id") {
            const predictID = this._makeSubID(componentName, "id", value, null, 1, false);
            const subscribed = this._subscriptions.get(predictID)?.deref();
            if (subscribed instanceof RowSubscription) {
                return subscribed as RowSubscription<T>;
            }
        }

        const payload = ["sub", componentName, "select", value, where];
        this._send(payload);
        console.debug(`[HeTuClient] Sending Select subscription: ${componentName}.${where}[${value}:]`);

        return new Promise<RowSubscription<T> | null>((resolve, reject) => {
            this._waitingCallbacks.push((subMsg) => {
                const subID = subMsg[1] as string;
                if (subID === null) {
                    resolve(null);
                    return;
                }

                const stillSubscribed = this._subscriptions.get(subID)?.deref();
                if (stillSubscribed instanceof RowSubscription) {
                    resolve(stillSubscribed as RowSubscription<T>);
                    return;
                }

                const data = subMsg[2] as T;
                const newSub = new RowSubscription<T>(subID, componentName as string, data);
                this._subscriptions.set(subID, new WeakRef(newSub));
                console.log(`[HeTuClient] Successfully subscribed to ${subID}`);
                resolve(newSub);
            });
        });
    }

    public async query<T extends IBaseComponent>(
        index: string,
        left: any,
        right: any,
        limit: number,
        desc: boolean = false,
        force: boolean = true,
        componentName?: string
    ): Promise<IndexSubscription<T> | null> {
        componentName = componentName ?? (typeof T !== 'function' ? "DictComponent" : T.name);

        const predictID = this._makeSubID(componentName, index, left, right, limit, desc);
        const subscribed = this._subscriptions.get(predictID)?.deref();
        if (subscribed instanceof IndexSubscription) {
            return subscribed as IndexSubscription<T>;
        }

        const payload = ["sub", componentName, "query", index, left, right, limit, desc, force];
        this._send(payload);
        console.debug(`[HeTuClient] Sending Query subscription: ${predictID}`);

        return new Promise<IndexSubscription<T> | null>((resolve, reject) => {
            this._waitingCallbacks.push((subMsg) => {
                const subID = subMsg[1] as string;
                if (subID === null) {
                    resolve(null);
                    return;
                }

                const stillSubscribed = this._subscriptions.get(subID)?.deref();
                if (stillSubscribed instanceof IndexSubscription) {
                    resolve(stillSubscribed as IndexSubscription<T>);
                    return;
                }

                const rows = subMsg[2] as T[];
                const newSub = new IndexSubscription<T>(subID, componentName as string, rows);
                this._subscriptions.set(subID, new WeakRef(newSub));
                console.log(`[HeTuClient] Successfully subscribed to ${subID}`);
                resolve(newSub);
            });
        });
    }

    // Internal methods
    _unsubscribe(subID: string): void {
        this._subscriptions.delete(subID);
        const payload = ["unsub", subID];
        this._send(payload);
        console.log(`[HeTuClient] Unsubscribed ${subID} due to BaseSubscription disposal`);
    }

    private _makeSubID(
        table: string,
        index: string,
        left: any,
        right: any,
        limit: number,
        desc: boolean
    ): string {
        return `${table}.${index}[${left}:${right ?? "None"}:${desc ? -1 : 1}][${limit}]`;
    }

    private _send(payload: any): void {
        const jsonString = JSON.stringify(payload);
        let buffer = new TextEncoder().encode(jsonString);

        if (this._protocol) {
            buffer = this._protocol.compress(buffer);
            if (this._protocol.crypt) {
                buffer = this._protocol.crypt(buffer);
            }
        }

        if (this._socket && this._socket.readyState === WebSocket.OPEN) {
            this._socket.send(buffer);
        } else {
            console.log("Trying to send data but connection not established, will queue for later");
            this._sendingQueue.push(buffer);
        }
    }

    private _onReceived(buffer: Uint8Array): void {
        if (this._protocol) {
            if (this._protocol.decrypt) {
                buffer = this._protocol.decrypt(buffer);
            }
            buffer = this._protocol.decompress(buffer);
        }

        const decoded = new TextDecoder().decode(buffer);
        const structuredMsg = JSON.parse(decoded);

        if (!structuredMsg) return;

        switch (structuredMsg[0]) {
            case "rsp":
                if (this.onResponse) this.onResponse(structuredMsg[1]);
                break;
            case "sub":
                const callback = this._waitingCallbacks.shift();
                if (callback) callback(structuredMsg);
                break;
            case "updt":
                const subID = structuredMsg[1] as string;
                const subscription = this._subscriptions.get(subID)?.deref();
                if (!subscription) break;

                const rows = structuredMsg[2] as Record<number, any>;
                for (const [rowID, data] of Object.entries(rows)) {
                    subscription.update(parseInt(rowID), data);
                }
                break;
        }
    }
}


export const HeTuClient = new HeTuClientImpl();

export type { HeTuClient as HeTuClientSDK };