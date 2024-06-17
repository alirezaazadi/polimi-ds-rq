import uuid
from collections import deque, OrderedDict
from datetime import datetime
from typing import OrderedDict as OrderedDictType, Deque, Dict

from server import exceptions as sex
from server.broker.client import Client
from server.broker.message import Message
from server.utils import Singleton


class Queue:
    def __init__(self, name: str, owner: Client):
        self._id: uuid.UUID = uuid.uuid4()
        self._name: str = name if name else str(self._id)
        self._owner: Client = owner
        self._initialized_at: datetime = datetime.now()
        self._messages: Deque[Message] = deque()

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def owner(self):
        return self._owner

    def push(self, message: Message):
        self._messages.append(message)

    def pull(self, index: int) -> Message:
        try:
            return self._messages[index]
        except IndexError:
            raise sex.InvalidQueueIndexException(self, index)

    def __str__(self):
        return self._name

    def __repr__(self):
        return self.__str__()


class QueueManager(metaclass=Singleton):
    def __init__(self):
        # an ordered map from queue id to the queue object
        self._queues: OrderedDictType[uuid.UUID, Queue] = OrderedDict()
        # a map from qname-client to queue id to let clients access their queues by name
        self._name_to_id: Dict[(Client, str), uuid.UUID] = dict()

    def add_queue(self, name: str | None, client: Client):
        queue = Queue(name, client)
        self._queues[queue.id] = queue
        self._name_to_id[(client, name)] = queue.id
        return queue

    def remove_queue(self, queue_id: uuid.UUID, client: Client):
        try:
            queue = self._queues[queue_id]
        except KeyError:
            raise sex.InvalidQueueIDException(queue_id)

        if queue.owner != client:
            raise sex.PermissionDeniedException(queue, client, "remove")

        try:
            del self._name_to_id[(client, queue.name)]
        except KeyError:
            pass

        try:
            del self._queues[queue_id]
        except KeyError:
            pass

    def get_queue_by_id(self, queue_id: uuid.UUID) -> Queue:
        try:
            return self._queues[queue_id]
        except KeyError:
            raise sex.InvalidQueueIDException(queue_id)

    def get_queue_by_name(self, client: Client, name: str) -> Queue:
        try:
            return self._queues[self._name_to_id[(client, name)]]
        except KeyError:
            raise sex.InvalidQueueNameException(name)

    def get_queues_list(self):
        return self._queues.values()


queue_manager = QueueManager()
