import uuid
from collections import defaultdict, OrderedDict
from datetime import datetime
from typing import Dict, Set

from common import exceptions


class Queue:
    def __init__(self, owner: uuid.UUID, name: str | None = None):
        self._id = uuid.uuid4()
        self._owner = owner
        self.created_at = datetime.now()
        self._name = name
        self._data = list()
        self._client_positions: Dict[uuid.UUID, int] = defaultdict(int)

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data

    @property
    def size(self):
        return len(self.data)

    @property
    def owner(self):
        return self._owner

    def get_client_position(self, client_id: uuid.UUID):
        return self._client_positions.get(client_id)

    def append(self, message):
        self.data.append(message)

    def read(self, client_id: uuid.UUID):
        position = self.get_client_position(client_id)

        if position is None:
            raise exceptions.ClientNotInQueue(client_id, self.id, self.name)

        if position < self.size:

            if self.size == 0:
                raise exceptions.EmptyQueue(self.id, self.name)

            value = self.data[position]
            self._client_positions[client_id] = position + 1
            return value

        return None

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id

    def __le__(self, other):
        return self.created_at <= other.created_at

    def __str__(self):
        return (f"Queue: {self.name} - {self.id} | Owner: {self.owner} | Size: {self.size} |"
                f" created at {self.created_at.strftime('%Y-%m-%d %H:%M:%S')}")


class QueueManager:
    def __init__(self):
        self._queues: OrderedDict[uuid.UUID, Queue] = OrderedDict()
        self._clients_queues: Dict[uuid.UUID, Set[uuid.UUID]] = defaultdict(set)

    def create_queue(self, owner: uuid.UUID, name: str | None = None):
        queue = Queue(owner, name)
        self._queues[queue.id] = queue
        self._clients_queues[owner].add(queue.id)
        return queue

    def append_to_queue(self, queue_id: uuid.UUID, message):
        queue = self._queues.get(queue_id)
        if queue is None:
            raise exceptions.QueueDoesNotExist(queue_id)
        queue.append(message)

    def read_from_queue(self, queue_id: uuid.UUID, client_id: uuid.UUID):
        queue = self._queues.get(queue_id)
        if queue is None:
            raise exceptions.QueueDoesNotExist(queue_id)
        return queue.read(client_id)
