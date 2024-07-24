import asyncio
import uuid

from .queue import QueueManager


class Broker:
    def __init__(self, address):
        self._id = uuid.uuid4()
        self._address = address
        self._q_manager = QueueManager()

    async def start(self):
        print(f'Broker {self._id} started at {self._address}')
        server = await asyncio.start_server(self.handle_client, *self._address.split(':'))
        await server.serve_forever()

    async def handle_client(self, reader, writer):
        data = await reader.read(100)
        message = data.decode()

        response = await self.process_message(message)

        writer.write(response.encode())
        await writer.drain()
        writer.close()

    async def process_message(self, message):
        _, name = message.split(' ')
        q = self._q_manager.create_queue(name)
        print(f'Queue {name} created with ID {q.id} by Broker {self._id}')
        return 'OK'
