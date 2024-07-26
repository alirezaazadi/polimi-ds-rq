import logging

import asyncio
from typing import Set

from common import address as addr, message as msg
from common.config import settings as cfg
from common.ctx import handle_connection
from common.exceptions import InvalidMessageStructure
from common.generators import read_from_client


class LoadBalancer:
    def __init__(self, address: addr.Address, brokers: Set[addr.Address]):
        self._brokers: Set[addr.Address] = brokers
        self._address: addr.Address = address

        self._leader: addr.Address | None = None

    @property
    def brokers(self):
        return self._brokers

    @property
    def address(self):
        return self._address

    def add_broker(self, broker_addr: addr.Address):
        self.brokers.add(broker_addr)

    def remove_server(self, broker_addr: addr.Address):
        self.brokers.remove(broker_addr)

    async def start(self):
        server = await asyncio.start_server(self.handle_client, self.address.host_str, self.address.port)
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        async with handle_connection(writer):
            raw_message: bytes
            async for raw_message in read_from_client(reader):
                try:
                    await self.handle_message(raw_message)
                except InvalidMessageStructure:
                    writer.write(b'Invalid message structure\n')

    async def handle_message(self, message: bytes):
        message = msg.Message.from_bytes(message)
        print(f'Handling message: {message}')


async def main():
    load_balancer = LoadBalancer(
        address=addr.address_factory.from_str(cfg.LOAD_BALANCER_ADDRESS),
        brokers={addr.address_factory.from_str(broker_addr) for broker_addr in cfg.BROKER_ADDRESSES}
    )

    logging.info(f'Load balancer started at {load_balancer.address}')
    await load_balancer.start()


if __name__ == '__main__':
    asyncio.run(main())
