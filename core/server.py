import asyncio
import logging
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod

from core.ttl_dict import TTLDictionary
from core.utils import parse_message, ensure_json_output, TCPMessage


class TCPServer(ABC):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.handlers = {}

    def message_handler(self, message_type):

        logging.info(f"\thandler : {message_type}")

        def decorator(func):
            self.handlers[message_type] = func
            return func

        return decorator

    @abstractmethod
    def init_handlers(self):
        pass

    async def handle_echo(self, reader, writer):

        buffer = ""
        while True:
            try:
                data = await asyncio.wait_for(reader.read(100), 10)
                buffer += data.decode()

                # Check if buffer ends with any of the registered message tags
                if any(buffer.endswith(f"</{message_type}>") for message_type in self.handlers):
                    break

            except asyncio.TimeoutError:
                logging.error("Connection timed out")
                writer.close()
                return

        try:
            message = parse_message(buffer)
        except ET.ParseError:
            logging.error('Invalid XML format')
            return

        addr = writer.get_extra_info('peername')

        if message.cls in self.handlers:
            response = await self.handlers[message.cls](message)
            writer.write(f"<{message.cls}>{response}</{message.cls}>".encode())
            logging.info(f"Processed {message.cls} from {addr!r}, sent: {response!r}")

        await writer.drain()
        logging.info("Closing the connection")
        writer.close()

    async def run(self):

        self.init_handlers()

        server = await asyncio.start_server(
            self.handle_echo, self.host, self.port)

        addr = server.sockets[0].getsockname()
        logging.info(f'Serving on {addr}')

        async with server:
            await server.serve_forever()


class KVServer(TCPServer):
    def __init__(self, host, port, expirable_dict_path="kvserver.db", default_ttl=60):
        super().__init__(host, port)
        self.kv = TTLDictionary(expirable_dict_path, default_ttl)  # default TTL 60 seconds

    def init_handlers(self):
        logging.info("Initializing handlers")

        @self.message_handler('kv_get')
        @ensure_json_output
        async def get_handler(message: TCPMessage):
            key = message.payload['key']
            try:
                value = await self.kv.__getitem__(key)
                return {"response": value}
            except KeyError:
                return {"error": f"No entry found for key {key}"}

        @self.message_handler('kv_set')
        @ensure_json_output
        async def set_handler(message: TCPMessage):
            key = message.payload['key']
            value = message.payload['value']
            ttl = message.payload.get('ttl')  # ttl is optional
            await self.kv.__setitem__(key, value, ttl)
            return {"response": f"Value set for key {key}"}

    async def run(self):
        # Start the expiration loop in the background
        asyncio.create_task(self.kv.expiration_loop())

        # Continue as normal
        await super().run()
