from typing import AsyncGenerator

from common import config


async def read_from_client(reader) -> AsyncGenerator[str, None]:
    while True:
        # Read the size of the upcoming message (1 byte)
        size_data = await reader.read(1)
        if not size_data:
            break

        # Extract the size from the first byte
        message_size = int.from_bytes(size_data, byteorder='big')

        # Read the actual message data based on the size
        data = await reader.read(min(message_size, config.MAX_MESSAGE_SIZE))
        if not data:
            break

        yield data
