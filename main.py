from RDQueue.client.dq import DQueue
from RDQueue.common.address import address_factory
import asyncio


async def main():
    queue = DQueue(
        connection_addr=address_factory.from_str('localhost:5000'),
        name='test'
    )

    await queue.async_init()
    await queue.push('Azadi')


if __name__ == '__main__':
    asyncio.run(main())
