import asyncio
import heapq
import logging
from typing import Set, List

from RDQueue.common.address import Address
from RDQueue.common.config import settings
from RDQueue.common.decorator import handle_conn_err, periodic_task
from RDQueue.common.message import message_factory, MessageType, Message, Operation
from RDQueue.common.networking import send_message_to_writer, receive_message

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


class Broker:
    def __init__(self, connect_address: Address):
        self._connect_address: Address = connect_address
        self._load = 0
        self._is_alive = False
        self._id = None

        asyncio.create_task(self._get_broker_info())

    @property
    def id(self) -> str | None:
        return self._id

    @property
    def connect_address(self) -> Address:
        return self._connect_address

    @property
    def load(self) -> int:
        return self._load

    def increment_load(self):
        self._load += 1

    @property
    def is_alive(self) -> bool:
        return self._is_alive

    @periodic_task(interval=5)
    async def _get_broker_info(self):
        """
        open a connection with the broker and get its information.
        :return:
        """
        try:
            # Enforce a timeout for the connection attempt
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(*self.connect_address.tuple),
                timeout=1  # Timeout duration in seconds
            )
        except asyncio.TimeoutError:
            self._is_alive = False
            logger.error(f'Broker {self.connect_address} is not available (connection timed out).')
            return
        except Exception as e:
            self._is_alive = False
            logger.error(f'Broker {self.connect_address} is not available: {e}.')
            return

        await send_message_to_writer(writer, message=message_factory.broker_info_req(
            sender_addr=settings.LOAD_BALANCER_ADDRESS.connection_str,
            receiver_addr=self.connect_address.connection_str
        ))

        message = await receive_message(reader)

        if not self._is_alive:
            self._id = message.body
            self._is_alive = True
            logger.info(f'Broker {self.connect_address} is alive and ready to serve clients.')

        writer.close()
        await writer.wait_closed()

    def inc_load(self):
        self._load += 1

    def dec_load(self):
        self._load -= 1

    def __hash__(self):
        return hash(self.connect_address)

    def __lt__(self, other: 'Broker'):
        return self.is_alive < other.is_alive and self.load < other.load

    def __str__(self):
        return f'{self.connect_address}: #({self.load}) clients'

    def __repr__(self):
        return str(self)


class LoadBalancer:
    def __init__(self, connection_address: Address, brokers: Set[Address]):
        self._brokers: List = list()
        self._connection_address: Address = connection_address
        self._leader: Address | None = None

        self.register_brokers(brokers)
        heapq.heapify(self._brokers)

    @property
    def brokers(self):
        return self._brokers

    @property
    def connection_address(self):
        return self._connection_address

    def register_brokers(self, brokers: Set[Address]):
        self._brokers = [Broker(broker_addr) for broker_addr in brokers]

    def add_broker(self, broker_addr: Address):

        broker = Broker(broker_addr)

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

    def get_next_broker(self) -> Broker | None:
        min_broker = heapq.heappop(self.brokers)
        min_broker.inc_load()
        heapq.heappush(self.brokers, min_broker)

        if not min_broker.is_alive:
            logger.error(f'Broker {min_broker.connect_address} is not available. Trying the next broker.')
            return None

        return min_broker

    async def start(self):
        server = await asyncio.start_server(self.handle_client, *self.connection_address.tuple)
        async with server:
            await server.serve_forever()

    @handle_conn_err
    async def handle_client(self, reader, writer):
        message = await receive_message(reader)
        await self.handle_message(message, writer)

    async def handle_message(self, message, writer):

        logger.info(f'Received message: {message}')

        if message.message_type == MessageType.REQUEST:
            await self.handle_request(message, writer)

    async def handle_request(self, message: Message, writer):
        if message.operation == Operation.BROKER_INFO:
            broker = self.get_next_broker()

            if broker is None:
                logger.error('No broker is available to handle the client registration request.')
                return

            logger.info(f'Broker {broker} is selected to handle the client registration request.')

            await send_message_to_writer(writer, message_factory.register_client_res(
                sender_addr=self.connection_address.connection_str,
                receiver_addr=message.sender_addr,
                body={'id': broker.id, 'address': broker.connect_address.connection_str}
            ))

            logger.info(f'Broker information sent to client: {broker.connect_address}')

    async def handle_response(self, message: Message, writer):
        pass


async def main():
    load_balancer = LoadBalancer(
        connection_address=settings.LOAD_BALANCER_ADDRESS,
        brokers={broker_addr for broker_addr in settings.BROKER_ADDRESSES}
    )

    await load_balancer.start()


if __name__ == '__main__':
    asyncio.run(main())
