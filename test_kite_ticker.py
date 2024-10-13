
import time
import asyncio
import logging
import platform
from kite_ticker import KiteTicker, MODE

logging.basicConfig(level=logging.DEBUG)

if platform.system() == "Windows":
    import winloop
    asyncio.set_event_loop_policy(winloop.EventLoopPolicy())
else:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

async def on_tick(msg):
    print(f"Tick :: {msg}")

async def on_order_update(msg):
    print(f"Order :: {msg}")

async def on_message(msg):
    print(f"Message :: {msg}")

async def on_error(msg):
    print(f"Error :: {msg}")

async def on_open():
    print(f"Socket opened on :: {time.asctime()}")

async def on_close():
    print(f"Socket closed on :: {time.asctime()}")

async def main(ticker, instruments):
    #All callbacks are optional
    ticker.start_websocket(
                subscribe_callback= on_tick,
                order_update_callback= on_order_update,
                message_update_callback= on_message,
                error_callback= on_error,
                open_callback= on_open,
                close_callback= on_close
                )
    #Wait for connection to establish
    await ticker.IS_CONNECTED.wait()
    # Modes are MODE.LTP, MODE.QUOTE and MODE.FULL 
    await ticker.subscribe(instruments, MODE.FULL)
    #Sample unsubscribe
    await asyncio.sleep(10) 
    instruments_to_unsubscribe = [263177, 263433, 263689]
    await ticker.unsubscribe(instruments_to_unsubscribe)
    #Run for sometime to check
    await asyncio.sleep(10)
    #Close websocket
    ticker.close_websocket()

if __name__ == "__main__":   
    api_key = "Your Api Key"
    access_token = "Your access token"

    instrument_tokens = [
        262665,
        262921,
        263177,
        263433,
        263689,
        263945,
        264457
    ]

    loop = asyncio.get_event_loop()
    ticker = KiteTicker(
                api_key= api_key, 
                access_token = access_token,
                loop= loop                
                )
    loop.create_task(main(ticker, instrument_tokens))
    loop.run_forever()