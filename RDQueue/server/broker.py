import argparse
import asyncio
import json
import logging
import uuid

import Pyro4

from RDQueue.common.address import Address, address_factory
from RDQueue.common.config import settings
from RDQueue.common.ctx import handle_connection
from RDQueue.common.exceptions import InvalidMessageStructure
from RDQueue.common.generators import read_from_client
from RDQueue.common.message import Message, MessageType, Operation, factory as message_factory
from RDQueue.server.q import manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


@Pyro4.expose
class Broker:
    def __init__(self, address: Address):
        self._address: Address = address
        self._q_manager = manager

        self._id: str = str(uuid.uuid4().hex)

        logger.info(f'Broker ({self.id}) started at {self.address.connection_str}')

    @property
    def id(self) -> str:
        return self._id

    @property
    def address(self) -> Address:
        return self._address

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
                        receiver_addr=Message.from_bytes(raw_message).sender_addr,
                        body=b'Invalid message structure\n'
                    ).to_bytes()
                    writer.write(err)

    async def handle_message(self, raw_message: bytes, writer):
        message = message_factory.from_bytes(raw_message)

        logger.info(f'Received message: {message}')

        if message.receiver_id and message.receiver_id != self.id:
            binary_message = message_factory.error_res(sender_addr=self.address.connection_str,
                                                       receiver_addr=message.sender_addr).to_bytes()
            writer.write(binary_message)
            await writer.drain()
            return

        if message.message_type == MessageType.REQUEST:
            await self.handle_request(message, writer)
        elif message.message_type == MessageType.RESPONSE:
            await self.handle_response(message, writer)

    async def handle_request(self, message: Message, writer):
        if message.operation == Operation.BROKER_INFO:
            binary_message = message_factory.broker_info_res(sender_addr=self.address.connection_str,
                                                             receiver_addr=message.sender_addr,
                                                             body=self.id).to_bytes()

            writer.write(binary_message)
            await writer.drain()

        elif message.operation == Operation.QUEUE_CREATE:
            q = self._q_manager.create_queue(
                owner=message.sender_id,
                name=message.body
            )

            q_info = {
                'name': q.name,
                'id': q.id,
            }

            binary_message = message_factory.queue_create_res(sender_addr=self.address.connection_str,
                                                              receiver_addr=message.sender_addr,
                                                              body=json.dumps(q_info)).to_bytes()

            writer.write(binary_message)
            await writer.drain()

        elif message.operation == Operation.QUEUE_PUSH:

            body = json.loads(message.body)

            self._q_manager.push(queue_name=body['queue_name'], message=body['message'])
            binary_message = message_factory.queue_push_res(sender_addr=self.address.connection_str,
                                                            receiver_addr=message.sender_addr).to_bytes()

            writer.write(binary_message)
            await writer.drain()

        elif message.operation == Operation.QUEUE_POP:
            msg = self._q_manager.pop(message.body, client_id=message.sender_id)

            binary_message = message_factory.queue_pop_res(sender_addr=self.address.connection_str,
                                                           receiver_addr=message.sender_addr,
                                                           body=msg).to_bytes()

            writer.write(binary_message)
            await writer.drain()

    async def handle_response(self, message: Message, writer):
        pass


async def main():
    arg_parser = argparse.ArgumentParser(description='Running the broker server')
    arg_parser.add_argument('--host', type=str, help='The host to bind the broker server')
    arg_parser.add_argument('--port', type=int, help='The port to bind the broker server')
    arg_parser.add_argument('--all', action='store_true', help='Run all the brokers in the cluster')

    args = arg_parser.parse_args()

    if args.all:
        brokers = [Broker(address_factory.from_str(addr)) for addr in settings.BROKER_ADDRESSES]

    else:
        brokers = [Broker(address_factory.from_tuple(args.host, args.port))]

    await asyncio.gather(*(b.start() for b in brokers))


if __name__ == '__main__':
    asyncio.run(main())
