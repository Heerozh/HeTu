import { IWebSocket } from "./index.ts";

/**
 * 浏览器/NodeJs的WebSocket实现
 * 包装原生WebSocket以实现IWebSocket接口
 */
export class BrowserWebSocket implements IWebSocket {
    private _socket: WebSocket | null;
    public url: string;
    public onopen: ((ev: Event) => any) | null = null;
    public onmessage: ((ev: MessageEvent) => any) | null = null;
    public onclose: ((ev: CloseEvent) => any) | null = null;
    public onerror: ((ev: Event) => any) | null = null;

    constructor(url: string) {
        this.url = url;
        this._socket = null;
    }

    connect(): void {
        if (this._socket && this._socket.readyState !== WebSocket.CLOSED) {
            this._socket.close();
        }
        this._socket = new WebSocket(this.url);
        this._socket.onopen = this.onopen;
        this._socket.onmessage = this.onmessage;
        this._socket.onclose = this.onclose;
        this._socket.onerror = this.onerror;
    }

    send(data: ArrayBuffer | string): void {
        this._socket?.send(data);
    }

    close(): void {
        this._socket?.close();
    }

    get readyState(): number {
        if (!this._socket) {
            return WebSocket.CLOSED;
        }
        return this._socket.readyState;
    }
}