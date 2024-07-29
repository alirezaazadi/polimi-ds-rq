import uuid
from collections import defaultdict
from typing import Dict, List
from RDQueue.common.message import Message, message_factory as message_factory


class Queue:
    def __init__(self, name: str, owner: str):
        self._owner: str = owner
        self._id: str = str(uuid.uuid4().hex)
        self._clients_positions: Dict[str, int] = defaultdict(lambda: -1)
        self._name: str = name
        self._messages: List[Message] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def owner(self) -> str:
        return self._owner

    @property
    def id(self) -> str:
        return self._id

    def push(self, message: Message):
        self._messages.append(message.body['message'])

        if message.sender_id not in self._clients_positions:
            self._clients_positions[message.sender_id] = 0

    def pop(self, client_id: str) -> Message:
        """
        Return the next message for the client.
        It does not remove the message from the queue, but it increments the client's position.
        :param client_id:
        :return:
        """
        position = self._clients_positions[client_id]

        if position is None or position < 0:
            raise ValueError(f"Client {client_id} not pushed any message in the queue {self._name}")

        message = self._messages[position]
        self._clients_positions[client_id] += 1

        return message


class QueueManager:
    def __init__(self):
        self._queues: Dict[str, Queue] = {}

    def create_queue(self, name: str, owner: str):
        queue = Queue(name, owner)
        self._queues[name] = queue

        return queue

    def push(self, message: Message):
        queue_name = message.body['queue_name']
        queue = self._queues[queue_name]
        queue.push(message)

    def pop(self, message: Message) -> Message:
        queue_name = message.body
        client_id = message.sender_id
        queue = self._queues[queue_name]
        return queue.pop(client_id)


manager = QueueManager()
