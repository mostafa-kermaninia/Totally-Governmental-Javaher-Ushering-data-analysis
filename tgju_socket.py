import asyncio
import aiohttp


async def main() :
    
    async with aiohttp.ClientSession() as session :
        ws = await session.ws_connect('wss://stream.tgju.org/connection/websocket')    
        await ws.send_str('{"params":{"name":"js"},"id":1}')
        await ws.send_str('{"method":1,"params":{"channel":"tgju:stream"},"id":2}')
        
        while True :
            msg = await ws.receive()
            for m in msg:
                print(m)


asyncio.run(main())