import argparse
import asyncio
import logging
import uuid
from pathlib import Path

from RDQueue.common.address import Address, address_factory
from RDQueue.common.config import settings
from RDQueue.common.decorator import handle_conn_err, periodic_task
from RDQueue.common.message import Message, MessageType, Operation, message_factory as message_factory
from RDQueue.common.networking import send_message_to_writer, receive_message
from RDQueue.server.message_queue import QueueManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


class Broker:
    def __init__(self, connection_address: Address):
        self._connection_address: Address = connection_address
        self.snapshot_file = Path(__file__).parent / 'snapshots' / f'{self.connection_address}.pickle'
        self._q_manager = QueueManager(snapshot_file=self.snapshot_file)

        self._id: str = str(uuid.uuid4().hex)

        logger.info(f'Broker ({self.id}) started at {self.connection_address.connection_str}')

    @property
    def id(self) -> str:
        return self._id

    @property
    def connection_address(self) -> Address:
        return self._connection_address

    async def start(self):

        server = await asyncio.start_server(
            self.handle_client,
            self.connection_address.host_str,
            self.connection_address.port
        )
        async with server:
            await server.serve_forever()

    @handle_conn_err
    async def handle_client(self, reader, writer):
        message = await receive_message(reader)
        await self.handle_message(message, writer)

    async def handle_message(self, message, writer):

        if not message.operation == Operation.BROKER_INFO:
            logger.info(f'Received message: {message}')

        if message.message_type == MessageType.REQUEST:
            await self.handle_request(message, writer)

    async def handle_request(self, message: Message, writer):
        if message.operation == Operation.BROKER_INFO:
            await send_message_to_writer(writer, message=message_factory.broker_info_res(
                sender_addr=self.connection_address.connection_str,
                receiver_addr=message.sender_addr,
                body=self.id
            ))

        elif message.operation == Operation.QUEUE_CREATE:
            q = self._q_manager.create_queue(
                owner=message.sender_id,
                name=message.body
            )

            q_info = {
                'name': q.name,
                'id': q.id,
            }

            await send_message_to_writer(writer, message=message_factory.queue_create_res(
                sender_addr=self.connection_address.connection_str,
                receiver_addr=message.sender_addr,
                body=q_info
            ))

        elif message.operation == Operation.QUEUE_PUSH:

            body = message.body

            self._q_manager.push(message=message)

            await send_message_to_writer(writer, message=message_factory.queue_push_res(
                sender_addr=self.connection_address.connection_str,
                receiver_addr=message.sender_addr,
                body='OK'
            ))

            logger.info(f'Pushed message to queue: {body["queue_name"]}')

        elif message.operation == Operation.QUEUE_POP:
            msg = self._q_manager.pop(message)

            await send_message_to_writer(writer, message=message_factory.queue_pop_res(
                sender_addr=self.connection_address.connection_str,
                receiver_addr=message.sender_addr,
                receiver_id=message.sender_id,
                body=msg
            ))


async def main():
    arg_parser = argparse.ArgumentParser(description='Running the broker server')
    arg_parser.add_argument('--host', type=str, help='The host to bind the broker server')
    arg_parser.add_argument('--port', type=int, help='The port to bind the broker server')
    arg_parser.add_argument('--all', action='store_true', help='Run all the brokers in the cluster')

    args = arg_parser.parse_args()

    if args.all:
        brokers = [Broker(addr) for addr in settings.BROKER_ADDRESSES]

    else:
        brokers = [Broker(address_factory.from_tuple(args.host, args.port))]

    await asyncio.gather(*(b.start() for b in brokers))


if __name__ == '__main__':
    asyncio.run(main())
