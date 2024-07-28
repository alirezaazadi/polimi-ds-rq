import asyncio
import heapq
import json
from typing import Set, List

from RDQueue.common.message import factory as message_factory, MessageType, Message, Operation
from RDQueue.common.address import Address, address_factory
from RDQueue.common.config import settings
from RDQueue.common.ctx import handle_connection
from RDQueue.common.exceptions import InvalidMessageStructure
from RDQueue.common.generators import read_from_client
import logging

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


class Broker:
    def __init__(self, address: Address, load_balancer_addr: Address):
        self._address: Address = address
        self._load = 0
        self._is_alive = False
        self._id = None
        self._load_balancer_addr: Address = load_balancer_addr

        asyncio.create_task(self._get_broker_info())

    async def _get_broker_info(self):
        """
        open a connection with the broker and get its information.
        :return:
        """
        reader, writer = await asyncio.open_connection(self.address.host_str, self.address.port)

        binary_message: bytes = message_factory.broker_info_req(
            sender_addr=self.load_balancer_addr.connection_str,
            receiver_addr=self.address.connection_str
        ).to_bytes()
        writer.write(binary_message)
        await writer.drain()

        raw_message: bytes
        async for raw_message in read_from_client(reader):
            try:

                msg = message_factory.from_bytes(raw_message)
                self._id = msg.body
                self._is_alive = True
                logger.info(f'Broker {self.address} is alive and ready to serve clients.')
            except InvalidMessageStructure:
                pass

    @property
    def id(self) -> str | None:
        return self._id

    @property
    def address(self) -> Address:
        return self._address

    @property
    def load_balancer_addr(self) -> Address:
        return self._load_balancer_addr

    @property
    def load(self) -> int:
        return self._load

    def increment_load(self):
        self._load += 1

    @property
    def is_alive(self) -> bool:
        return self._is_alive

    def inc_load(self):
        self._load += 1

    def dec_load(self):
        self._load -= 1

    def __hash__(self):
        return hash(self.address)

    def __lt__(self, other: 'Broker'):
        return self.is_alive < other.is_alive and self.load < other.load

    def __str__(self):
        return f'{self.address}: #({self.load}) clients'

    def __repr__(self):
        return str(self)


class LoadBalancer:
    def __init__(self, address: Address, brokers: Set[Address]):
        self._brokers: List = list()
        self._address: Address = address
        self._leader: Address | None = None

        self.register_brokers(brokers)
        heapq.heapify(self._brokers)

    @property
    def brokers(self):
        return self._brokers

    @property
    def address(self):
        return self._address

    def register_brokers(self, brokers: Set[Address]):
        self._brokers = [Broker(broker, load_balancer_addr=self.address) for broker in brokers]

    def add_broker(self, broker_addr: Address):

        broker = Broker(broker_addr, load_balancer_addr=self.address)

        if broker in self.brokers:
            return

        self.brokers.append(broker)
        heapq.heapify(self.brokers)

    def remove_broker(self, broker_addr: Broker):
        self.brokers.remove(broker_addr)
        heapq.heapify(self.brokers)

    def disconnect_client(self, broker_addr: Address):
        for broker in self.brokers:
            if broker.address == broker_addr:
                broker.dec_load()
                heapq.heapify(self.brokers)
                break

    def get_next_broker(self) -> Broker:
        min_broker = heapq.heappop(self.brokers)
        min_broker.inc_load()
        heapq.heappush(self.brokers, min_broker)
        return min_broker

    async def start(self):
        server = await asyncio.start_server(self.handle_client, self.address.host_str, self.address.port)
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        async with handle_connection(writer):
            raw_message: bytes
            async for raw_message in read_from_client(reader):
                try:
                    await self.handle_message(raw_message, writer)
                except InvalidMessageStructure:
                    err = message_factory.error_res(
                        sender_addr=self.address.connection_str,
                        receiver_addr=message_factory.from_bytes(raw_message).sender_addr,
                        body=b'Invalid message structure\n'
                    ).to_bytes()

                    writer.write(err)

    async def handle_message(self, raw_message: bytes, writer):
        message = message_factory.from_bytes(raw_message)

        logger.info(f'Received message: {message}')

        if message.message_type == MessageType.REQUEST:
            await self.handle_request(message, writer)
        elif message.message_type == MessageType.RESPONSE:
            await self.handle_response(message, writer)

    async def handle_request(self, message: Message, writer):
        if message.operation == Operation.REGISTER_CLIENT:
            broker = self.get_next_broker()

            binary_message = message_factory.register_client_res(
                sender_addr=self.address.connection_str,
                receiver_addr=message.sender_addr,
                body=json.dumps({'id': broker.id, 'address': broker.address.connection_str})
            ).to_bytes()

            writer.write(binary_message)
            await writer.drain()

    async def handle_response(self, message: Message, writer):
        pass


async def main():
    load_balancer = LoadBalancer(
        address=address_factory.from_str(settings.LOAD_BALANCER_ADDRESS),
        brokers={address_factory.from_str(broker_addr) for broker_addr in settings.BROKER_ADDRESSES}
    )

    await load_balancer.start()


if __name__ == '__main__':
    asyncio.run(main())
