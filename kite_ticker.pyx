
# -*- coding: utf-8 -*-
"""
    :description: Cython Kite Ticker based on picows.
    :author: Tapan Hazarika
    :created: On Sunday Oct 13, 2024 00:58:40 GMT+05:30
"""
__author__ = "Tapan Hazarika"

import ssl
import socket
import signal
import orjson
import struct
cimport cython
import logging
import asyncio
from functools import wraps
from urllib.parse import quote
from libc.stdint cimport int32_t
from picows.picows cimport WSFrame, WSTransport, WSListener
from picows import ws_connect, WSMsgType, WSCloseCode, WSError

logger = logging.getLogger(__name__)

cdef class MODE:
    LTP: str = "ltp"
    QUOTE: str = "quote"
    FULL: str = "full"

cdef class WSMessage:
    SUBSCRIBE: str= "subscribe"
    UNSUBSCRIBE: str= "unsubscribe"
    MODE: str= "mode"

cdef enum EXCHANGE:
    NSE = 1
    NFO = 2
    CDS = 3
    BSE = 4
    BFO = 5
    BCD = 6
    MCX = 7
    MCXSX = 8
    INDICES = 9


cdef class KiteTicker:
    cdef: 
        WSTransport _transport
        str _ws_endpoint
        str _api_key
        str _access_token
        str _ws_url    
        bint _web
        str _user_id    
        bint _disconnect_socket
        object _loop
        set _ltp_mode_tokens
        set _quote_mode_tokens
        set _full_mode_tokens
        public object IS_CONNECTED
        object _stop_event
        object _subscribe_callback
        object _message_update_callback
        object _order_update_callback
        object _error_callback
        object _open_callback
        object _close_callback
        bytes _disconnect_msg
        
    def __init__(
                self,
                str api_key,
                str access_token,
                object loop,
                str ws_endpoint= "wss://ws.kite.trade/",
                bint web= False,
                str user_id= None
            ):
        self._transport = None
        self._api_key = api_key
        self._access_token = access_token
        self._ws_endpoint = ws_endpoint
        self._loop = loop
        self._web = web
        self._user_id = user_id

        self.IS_CONNECTED = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._disconnect_socket = False
        self._disconnect_msg = KiteTicker._encode("Connection closed by the user.")

        self._ltp_mode_tokens = set()
        self._quote_mode_tokens = set()
        self._full_mode_tokens = set()

        self._subscribe_callback = KiteTicker.__dummy_callback
        self._order_update_callback = KiteTicker.__dummy_callback
        self._message_update_callback = KiteTicker.__dummy_callback
        self._error_callback = KiteTicker.__dummy_callback        
        self._open_callback = None
        self._close_callback = None

        self._ws_url = self.create_url()
        self.add_signal_handler()
    
    @staticmethod
    def run_in_thread():
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                return await asyncio.to_thread(lambda: func(*args, **kwargs))
            return wrapper
        return decorator
    
    @staticmethod
    async def __dummy_callback(msg: object):
        logger.info(msg)

    @staticmethod
    cdef bytes _encode(object msg):
        return orjson.dumps(msg)
    
    cdef str create_url(self):
        cdef str base_uri = self._ws_endpoint + "?api_key=" + self._api_key
        if self._web:
            return base_uri + "&enctoken=" + quote(self._access_token) + "&user_id" + self._user_id
        return base_uri + "&access_token=" + self._access_token
    
    async def stop_signal_handler(self, *args, **kwargs)-> None:
        logger.info(f"WebSocket closure initiated by user interrupt.")
        self.close_websocket()  
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=2)
        except TimeoutError:
            self._initiate_shutdown() 

    cdef void add_signal_handler(self):
        for signame in ('SIGINT', 'SIGTERM'):
            self._loop.add_signal_handler(
                                getattr(signal, signame),
                                lambda: asyncio.create_task(self.stop_signal_handler())
                                )

    async def check_round_trip_time(self, count: int= 5)-> list:
        rtts: list = await self._transport.measure_roundtrip_time(count)
        return rtts 
    
    cdef void _ws_send(self, dict msg):
        cdef bytes message = KiteTicker._encode(msg)
        self._transport.send(WSMsgType.TEXT, message)   

    cdef void _on_order_or_error_callback(self, str msg):  
        cdef: 
            dict message
            str msg_type
        try:      
            message = orjson.loads(msg)
            msg_type = message["type"]
            if msg_type == "order":
                self._loop.create_task(self._order_update_callback(message))
                return
            if msg_type == "message" or msg_type == "instruments_meta":
                self._loop.create_task(self._message_update_callback(message))
                return
            if msg_type == "error":
                self._loop.create_task(self._error_callback(message))    
        except (KeyError, Exception) as e:
            logger.error(f"WS Text message error :: {e}")
    
    cdef void _on_data_callback(self, bytes msg):
        cdef list message = KiteMessageDecoder.parse_binary(msg)
        self._loop.create_task(self._subscribe_callback(message))
    
    cdef void __store_tokens(self, list instruments, str mode):
        if mode == MODE.LTP:
            self._ltp_mode_tokens.update(instruments)
        elif mode == MODE.QUOTE:
            self._quote_mode_tokens.update(instruments)
        elif mode == MODE.FULL:
            self._full_mode_tokens.update(instruments)
    
    cdef void __remove_tokens(self, list instruments):
        self._ltp_mode_tokens.difference_update(set(instruments))
        self._quote_mode_tokens.difference_update(set(instruments))
        self._full_mode_tokens.difference_update(set(instruments))
    
    cdef void __subscribe(self, list instruments, str mode):
        cdef dict sub_msg= {"a": WSMessage.SUBSCRIBE, "v": instruments} 
        cdef dict set_mode_msg= {"a": WSMessage.MODE, "v": [mode, instruments]}
        self._ws_send(sub_msg)
        self._ws_send(set_mode_msg)
        self.__store_tokens(instruments, mode)
    
    cdef void __unsubscribe(self, list instruments):
        cdef dict unsub_msg= {"a": WSMessage.UNSUBSCRIBE, "v": instruments}
        self._ws_send(unsub_msg)
        self.__remove_tokens(instruments) 
    
    cdef void __resubscribe(self):
        cdef:
            list ltp_temp
            list quote_temp
            list full_temp
        if self._ltp_mode_tokens:
            ltp_temp = list(self._ltp_mode_tokens)
            self._ltp_mode_tokens.clear()
            self._loop.create_task(self.subscribe(ltp_temp, MODE.LTP)) 
        if self._quote_mode_tokens:
            quote_temp = list(self._quote_mode_tokens)
            self._quote_mode_tokens.clear()
            self._loop.create_task(self.subscribe(quote_temp, MODE.QUOTE)) 
        if self._full_mode_tokens:
            full_temp = list(self._full_mode_tokens)
            self._full_mode_tokens.clear()
            self._loop.create_task(self.subscribe(full_temp, MODE.FULL)) 
    
    @run_in_thread()
    def subscribe(self, instruments: list, mode: str= MODE.FULL)-> None:
        self.__subscribe(instruments, mode)   
    
    @run_in_thread()
    def unsubscribe(self, instruments: list)-> None:
        self.__unsubscribe(instruments)
    
    @run_in_thread()
    def _resubscribe(self):
        self.__resubscribe()

    async def start_ticker(self, bint reconnect= False):
        ssl_context = ssl.create_default_context() 
        client = KiteListener(self, self._loop)

        try:
            _, client = await ws_connect(
                                lambda: client,
                                self._ws_url,
                                ssl_context= ssl_context,
                                enable_auto_ping= True,
                                auto_ping_idle_timeout= 3,
                                auto_ping_reply_timeout= 2    
                                )
            await client.transport.wait_disconnected()            
        except (socket.gaierror, OSError, WSError) as e:
            logger.error(f"Error occured on connect :: {e}")            
            if reconnect:
                await asyncio.sleep(1)
                return await self.start_ticker(reconnect= True)
            else:
                self._initiate_shutdown()
       
    cpdef start_websocket(
                self,
                object subscribe_callback = None,
                object order_update_callback = None,
                object message_update_callback = None,
                object error_callback = None,
                object open_callback = None,
                object close_callback = None                   
            ):
        if subscribe_callback:
            self._subscribe_callback = subscribe_callback
        if order_update_callback:
            self._order_update_callback = order_update_callback
        if message_update_callback:
            self._message_update_callback = message_update_callback    
        if error_callback:    
            self._error_callback = error_callback
        self._open_callback = open_callback
        self._close_callback = close_callback

        self._loop.create_task(self.start_ticker())
    
    cpdef close_websocket(self):
        self._disconnect_socket = True
        if self._transport:
            self._transport.send_close(
                            close_code= WSCloseCode.OK, 
                            close_message=self._disconnect_msg
                            )
    
    cdef void _initiate_shutdown(self):
        logger.info("Websocket disconnected.")
        self._loop.call_soon_threadsafe(asyncio.create_task, KiteTicker.shutdown(self._loop))
        self.IS_CONNECTED.clear()
    
    @staticmethod
    async def shutdown(loop):
        tasks = [
            t for t in asyncio.all_tasks() if t is not asyncio.current_task()
            ]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        loop.stop() 
        

cdef class KiteListener(WSListener):
    cdef:
        KiteTicker parent
        WSTransport _transport
        object _loop
        bytes __ping_msg

    def __init__(
            self, 
            KiteTicker parent,
            object loop
            ) -> None:
        super().__init__()
        self._transport = None
        self._loop = loop
        self.parent = parent     

        self.__ping_msg= KiteTicker._encode("")
    
    cpdef send_user_specific_ping(self, WSTransport transport):
        logger.info("sending ping")
        transport.send_ping(message=self.__ping_msg)
    
    cpdef on_ws_connected(self, WSTransport transport):
        self._transport = transport
        self.parent._transport = transport
        self.parent.IS_CONNECTED.set()
        self._loop.create_task(self.parent._resubscribe())
        self._loop.create_task(self.parent._open_callback())
    
    cpdef on_ws_frame(self, WSTransport transport, WSFrame frame):
        if frame.msg_type == WSMsgType.BINARY:
            msg = frame.get_payload_as_bytes()
            if len(msg) > 2:
                self.parent._on_data_callback(msg)
        if frame.msg_type == WSMsgType.TEXT:
            msg = frame.get_payload_as_utf8_text()
            self.parent._on_order_or_error_callback(msg)
        if frame.msg_type == WSMsgType.PONG:
            #self.parent._pong_event.set()
            transport.notify_user_specific_pong_received()
        if frame.msg_type == WSMsgType.CLOSE:
            close_msg = frame.get_close_message()
            close_code = frame.get_close_code()
            if close_msg:
                close_msg = close_msg.decode()
            if close_code == 1000 and not close_msg:
                close_msg = "Connection closed by the user."
            logger.info(f"Kite Ticker disconnected with code :: {close_code}, message :: {close_msg} ")
    
    cpdef on_ws_disconnected(self, WSTransport transport):
        if self.parent._close_callback:
            self.parent._loop.create_task(self.parent._close_callback())
        if self.parent._disconnect_socket:
            self.parent._stop_event.set()
            self.parent._initiate_shutdown() 
        else: 
            logger.info("Trying to reconnect..")
            transport.disconnect()
            self.parent.IS_CONNECTED.clear()
            self.parent._loop.create_task(self.parent.start_ticker(reconnect=True))  


@cython.boundscheck(False)
@cython.wraparound(False)
cdef class KiteMessageDecoder:    
    @staticmethod
    cdef float unpack_float(bytes bin, int start, int end, str byte_format="I"):
        return struct.unpack('>f' + byte_format, bin[start:end])[0]

    @staticmethod
    cdef list split_packets(bytes bin):
        cdef: 
            int offset
            int number_of_packets
            int packet_length
            list packets = []

        if len(bin) < 2:
            return None

        offset = 2
        number_of_packets = int.from_bytes(bin[0:2], byteorder="big")

        for i in range(number_of_packets):
            packet_length = int.from_bytes(bin[offset:offset + 2], byteorder="big")
            packets.append(bin[offset + 2: offset + 2 + packet_length])
            offset += 2 + packet_length
        return packets

    @staticmethod
    cdef dict parse_binary_packet(bytes packet):
        cdef:
            dict tick_data = {}
            int instrument_token
            int segment
            bint tradable
            int divisor
            str mode
            dict depth
            dict extended_depth

        instrument_token = int.from_bytes(packet[0:4], byteorder="big")
        segment = instrument_token & 0xFF

        if segment == EXCHANGE.CDS:
            divisor = 10000000
        elif segment == EXCHANGE.BCD:
            divisor = 10000
        else:
            divisor = 100

        tradable = not (segment == EXCHANGE.INDICES)

        if len(packet) == 8:
            tick_data.update({
                "tradable": tradable,
                "mode": "ltp",
                "instrument_token": instrument_token,
                "last_price": int.from_bytes(packet[4:8], byteorder="big"),
            })

        elif len(packet) == 28 or len(packet) == 32:
            mode = "quote" if len(packet) == 28 else "full"
            tick_data.update({
                "tradable": tradable,
                "mode": mode,
                "instrument_token": instrument_token,
                "last_price": int.from_bytes(packet[4:8], byteorder="big") / divisor,
                "ohlc": {
                    "high": int.from_bytes(packet[8:12], byteorder="big")/ divisor,
                    "low": int.from_bytes(packet[12:16], byteorder="big") / divisor,
                    "open": int.from_bytes(packet[16:20], byteorder="big") / divisor,
                    "close": int.from_bytes(packet[20:24], byteorder="big") / divisor,
                },
                #"change": KiteMessageDecoder.unpack_float(packet, 24, 28, "H")
            })
            tick_data["change"] = 0
            if tick_data["ohlc"]["close"] != 0:
                tick_data["change"] = (
                    (tick_data["last_price"] - tick_data["ohlc"]["close"])
                    * 100
                    / tick_data["ohlc"]["close"]
                )
            if len(packet) == 32:
                tick_data["exchange_timestamp"] = int.from_bytes(packet[28:32], byteorder="big")

        elif len(packet) == 44 or len(packet) == 184:
            mode = "quote" if len(packet) == 44 else "full"
            tick_data.update({
                "tradable": tradable,
                "mode": mode,
                "instrument_token": instrument_token,
                "last_price": int.from_bytes(packet[4:8], byteorder="big") / divisor,
                "last_traded_quantity": int.from_bytes(packet[8:12], byteorder="big"),
                "average_traded_price": int.from_bytes(packet[12:16], byteorder="big") / divisor,
                "volume_traded": int.from_bytes(packet[16:20], byteorder="big"),
                "total_buy_quantity": int.from_bytes(packet[20:24], byteorder="big"),
                "total_sell_quantity": int.from_bytes(packet[24:28], byteorder="big"),
                "ohlc": {
                    "open": int.from_bytes(packet[28:32], byteorder="big") / divisor,
                    "high": int.from_bytes(packet[32:36], byteorder="big") / divisor,
                    "low": int.from_bytes(packet[36:40], byteorder="big") / divisor,
                    "close": int.from_bytes(packet[40:44], byteorder="big") / divisor,
                },
            })
            tick_data["change"] = 0
            if tick_data["ohlc"]["close"] != 0:
                tick_data["change"] = (
                    (tick_data["last_price"] - tick_data["ohlc"]["close"])
                    * 100
                    / tick_data["ohlc"]["close"]
                )

            if len(packet) == 184:
                tick_data["last_trade_time"] = int.from_bytes(packet[44:48], byteorder="big")
                tick_data["oi"] = int.from_bytes(packet[48:52], byteorder="big")
                tick_data["oi_day_high"] = int.from_bytes(packet[52:56], byteorder="big")
                tick_data["oi_day_low"] = int.from_bytes(packet[56:60], byteorder="big")
                tick_data["exchange_timestamp"] = int.from_bytes(packet[60:64], byteorder="big")

                depth = {"buy": [], "sell": []}
                for i, p in enumerate(range(64, len(packet), 12)):
                    depth["sell" if i >= 5 else "buy"].append({
                        "quantity": int.from_bytes(packet[p:p + 4], byteorder="big"),
                        "price": int.from_bytes(packet[p + 4:p +8], byteorder="big") / divisor,
                        "orders": int.from_bytes(packet[p + 8: p + 10], byteorder="big")
                    })
                tick_data["depth"] = depth

        elif len(packet) == 492:
            tick_data.update({
                "tradable": tradable,
                "mode": "full",
                "instrument_token": instrument_token,
                "last_price": int.from_bytes(packet[4:8], byteorder="big") / divisor
            })
            tick_data["exchange_timestamp"] = int.from_bytes(packet[8:12], byteorder="big")
            extended_depth = {"buy": [], "sell": []}
            for i, p in enumerate(range(12, len(packet), 12)):
                extended_depth["sell" if i >= 20 else "buy"].append({
                    "quantity": int.from_bytes(packet[p:p + 4], byteorder="big"),
                    "price": int.from_bytes(packet[p + 4:p +8], byteorder="big") / divisor,
                    "orders": int.from_bytes(packet[p + 8: p + 10], byteorder="big"),
                })
            tick_data["20_depth"] = extended_depth
        return tick_data

    @staticmethod
    cdef list parse_binary(bytes bin):  
        cdef list packets       
        packets = KiteMessageDecoder.split_packets(bin)
        return [KiteMessageDecoder.parse_binary_packet(packet) for packet in packets] if packets else []




        

