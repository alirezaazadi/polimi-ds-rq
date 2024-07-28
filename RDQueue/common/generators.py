from typing import AsyncGenerator

from RDQueue.common.config import settings

EOF = b'EOF'


async def read_from_client(reader) -> AsyncGenerator[str, None]:
    data = b''
    while True:
        chunk = await reader.read(1024)
        if EOF in chunk:
            data += chunk[:chunk.index(EOF)]
            break
        data += chunk

        if not chunk:
            break

    yield data
