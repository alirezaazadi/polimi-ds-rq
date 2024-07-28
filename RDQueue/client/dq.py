import asyncio
import json
import logging
import uuid
import socket
from RDQueue.common.address import Address, address_factory
from RDQueue.common.config import settings
from RDQueue.common.exceptions import InvalidMessageStructure
from RDQueue.common.generators import read_from_client
from RDQueue.common.message import factory as message_factory

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


class RDQueue:

    def __init__(self, address: tuple, name: str):
        self._address: Address = address_factory.from_tuple(*address)
        self._id = str(uuid.uuid4().hex)
        self._load_balancer_addr: Address = address_factory.from_str(settings.LOAD_BALANCER_ADDRESS)
        self._name: str = name
        self._broker_id: str | None = None
        self._broker_addr: Address | None = None
        self._remote_queue_name: str | None = None
        self._remote_queue_id: str | None = None

        self.register()

        self.broker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.broker_socket.connect((self.broker_addr.host_str, self.broker_addr.port))

        self.create_queue()

    @property
    def address(self) -> Address:
        return self._address

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def broker_id(self) -> str | None:
        return self._broker_id

    @property
    def broker_addr(self) -> Address | None:
        return self._broker_addr

    @property
    def load_balancer_addr(self) -> Address:
        return self._load_balancer_addr

    @property
    def remote_queue_name(self) -> str | None:
        return self._remote_queue_name

    @property
    def remote_queue_id(self) -> str | None:
        return self._remote_queue_id

    def register(self):
        binary_message = message_factory.register_client_req(
            sender_addr=self.address.connection_str,
            receiver_addr=self.load_balancer_addr.connection_str,
            sender_id=self.id,
            body=self.address.connection_str.encode()
        ).to_bytes()

        load_balancer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        load_balancer_socket.connect((self.load_balancer_addr.host_str, self.load_balancer_addr.port))
        load_balancer_socket.sendall(binary_message)

        data = b''
        while True:
            chunk = load_balancer_socket.recv(1024)
            if b'EOF' in chunk:
                data += chunk[:chunk.index(b'EOF')]
                break
            data += chunk

        msg = message_factory.from_bytes(data)
        broker_data = json.loads(msg.body)
        self._broker_id = broker_data['id']
        self._broker_addr = address_factory.from_str(broker_data['address'])
        logger.info(f'Registered with broker {self.broker_id} at {self.broker_addr.connection_str}')

        load_balancer_socket.close()

    def create_queue(self):
        binary_message = message_factory.queue_create_req(
            sender_addr=self.address.connection_str,
            receiver_addr=self.broker_addr.connection_str,
            sender_id=self.id,
            body=self.name
        ).to_bytes()

        self.broker_socket.sendall(binary_message)

        data = b''
        while True:
            chunk = self.broker_socket.recv(1024)
            if b'EOF' in chunk:
                data += chunk[:chunk.index(b'EOF')]
                break
            data += chunk

        msg = message_factory.from_bytes(data)
        queue_data = json.loads(msg.body)
        self._remote_queue_name = queue_data['name']
        self._remote_queue_id = queue_data['id']
        logger.info(f'Queue created: {self.remote_queue_name} with id {self.remote_queue_id}')

    def push(self, message):
        binary_message = message_factory.queue_push_req(
            sender_addr=self.address.connection_str,
            receiver_addr=self.broker_addr.connection_str,
            sender_id=self.id,
            receiver_id=self.broker_id,
            body=json.dumps({'queue_name': self.remote_queue_name, 'message': message})
        ).to_bytes()

        self.broker_socket.sendall(binary_message)

        data = b''
        while True:
            chunk = self.broker_socket.recv(10)
            if b'EOF' in chunk:
                data += chunk[:chunk.index(b'EOF')]
                break
            data += chunk

        msg = message_factory.from_bytes(data)
        logger.info(f'Message pushed: {msg}')

    def pop(self):
        binary_message = message_factory.queue_pop_req(
            sender_addr=self.address.connection_str,
            receiver_addr=self.broker_addr.connection_str,
            sender_id=self.id,
            receiver_id=self.broker_id
        ).to_bytes()

        self.broker_socket.sendall(binary_message)

        data = b''
        while True:
            chunk = self.broker_socket.recv(10)
            if b'EOF' in chunk:
                data += chunk[:chunk.index(b'EOF')]
                break
            data += chunk

        msg = message_factory.from_bytes(data)
        logger.info(f'Message popped: {msg}')

        return msg.body
