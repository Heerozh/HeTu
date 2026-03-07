import multiprocessing
import sys


def run_aiohttp():
    from aiohttp import web

    async def handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                await ws.send_str(msg.data)
            elif msg.type == web.WSMsgType.ERROR:
                pass
        return ws

    app = web.Application()
    app.router.add_get("/", handler)
    import logging

    logging.getLogger("aiohttp.access").setLevel(logging.CRITICAL)
    web.run_app(app, host="127.0.0.1", port=18001, access_log=None, print=False)


def run_websockets():
    import websockets
    import asyncio

    async def handler(websocket):
        async for message in websocket:
            await websocket.send(message)

    async def main():
        async with websockets.serve(handler, "127.0.0.1", 18002):
            await asyncio.Future()  # run forever

    asyncio.run(main())


def run_sanic():
    from sanic import Sanic
    import logging

    app = Sanic("BenchSanic")
    app.config.ACCESS_LOG = False
    logging.getLogger("sanic.root").setLevel(logging.CRITICAL)

    @app.websocket("/")
    async def handler(request, ws):
        while True:
            try:
                msg = await ws.recv()
                if msg is not None:
                    await ws.send(msg)
            except BaseException:
                break

    app.run(host="127.0.0.1", port=18003, access_log=False, single_process=True)


def run_socketify():
    from socketify import App

    app = App()
    app.ws("/*", {"message": lambda ws, message, opcode: ws.send(message, opcode)})
    app.listen(18004, lambda config: None)
    app.run()


if __name__ == "__main__":
    print("Starting websocket servers for benchmark...")

    procs = [
        multiprocessing.Process(target=run_aiohttp, name="aiohttp"),
        multiprocessing.Process(target=run_websockets, name="websockets"),
        multiprocessing.Process(target=run_sanic, name="sanic"),
        multiprocessing.Process(target=run_socketify, name="socketify"),
    ]

    for p in procs:
        p.start()

    print("Servers started on localhost ports:")
    print("  18001 -> aiohttp")
    print("  18002 -> websockets")
    print("  18003 -> sanic")
    print("  18004 -> socketify")
    print("\nPlease run the benchmark script in another terminal:")
    print("  cd benchmark")
    print("  uv run ya hypothesis/ya_ws_ping.py")
    print("\nPress Ctrl+C to stop all servers.")

    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        print("\nStopping servers...")
        for p in procs:
            p.terminate()
        sys.exit(0)
