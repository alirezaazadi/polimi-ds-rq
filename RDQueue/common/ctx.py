import asyncio
import logging
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def handle_connection(writer):
    try:
        yield
    except ConnectionResetError:
        logging.info(f'Peer: {writer.get_extra_info("peername")}: Connection reset by peer')
    except asyncio.TimeoutError:
        logging.info(f'Peer: {writer.get_extra_info("peername")}: Connection timed out')
    except Exception as e:
        logging.error(f'Peer: {writer.get_extra_info("peername")}: Unexpected error: {e}')
