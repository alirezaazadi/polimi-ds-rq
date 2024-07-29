import asyncio
import logging

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__file__)


def handle_conn_err(function):
    async def wrapper(*args, **kwargs):
        try:
            await function(*args, **kwargs)
        except ConnectionResetError:
            logger.error(f'Connection reset by peer')
        except asyncio.TimeoutError:
            logger.error(f'Connection timed out')
        except Exception as e:
            logger.error(f'Unexpected error: {e}')


    return wrapper


def periodic_task(interval: int = 1):
    def decorator(function):
        async def wrapper(*args, **kwargs):
            while True:
                await function(*args, **kwargs)
                await asyncio.sleep(interval)

        return wrapper

    return decorator
