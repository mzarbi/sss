import asyncio
import logging
import warnings
warnings.filterwarnings("ignore")
from core.petals import PetalsServer
from core.utils import ensure_json_output, TCPMessage

logging.basicConfig(level=logging.INFO)

server = PetalsServer('127.0.0.1', 8888, r"C:\Users\medzi\Desktop\bnp\petals-framework\draft\stores")


if __name__ == "__main__":
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        logging.info("Server stopping...")
    except Exception as e:
        logging.exception("Unexpected exception")
    finally:
        logging.info("Server shutdown.")
