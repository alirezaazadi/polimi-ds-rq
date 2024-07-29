import os
import pickle
import uuid
from collections import defaultdict
from typing import Dict, List
import json
from RDQueue.common.message import Message
import logging

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)


class Queue:
    def __init__(self, name: str, owner: str):
        self._owner: str = owner
        self._id: str = str(uuid.uuid4().hex)
        self._clients_positions: Dict[str, int] = dict()
        self._name: str = name
        self._messages: List[str] = []

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

    def pop(self, client_id: str) -> str:
        """
        Return the next message for the client.
        It does not remove the message from the queue, but it increments the client's position.
        :param client_id:
        :return:
        """
        position = self._clients_positions.get(client_id)

        if position is None or position < 0:
            raise ValueError(f"Client {client_id} not pushed any message in the queue {self._name}")

        message = self._messages[position]
        self._clients_positions[client_id] += 1

        return message

    def __str__(self):
        return json.dumps({
            'name': self._name,
            'owner': self._owner,
            'id': self._id,
            'clients_positions': self._clients_positions,
            'messages': self._messages
        })

    def __repr__(self):
        return self.__str__()


class QueueManager:
    def __init__(self, snapshot_file):
        self._queues: Dict[str, Queue] = {}
        self._snapshot_file = snapshot_file
        os.makedirs(snapshot_file.parent, exist_ok=True)
        self.restore_from_snapshot()

    def create_snapshot(self):
        logger.info(f"Creating snapshot at {self._snapshot_file}")
        with open(self._snapshot_file, 'wb+') as f:
            pickle.dump(self._queues, f)

    def restore_from_snapshot(self):
        if os.path.exists(self._snapshot_file):
            logger.info(f"Restoring snapshot from {self._snapshot_file}")
            self._queues = pickle.load(open(self._snapshot_file, 'rb'))
            logger.info(f"Restored queues: {self._queues}")
        else:
            logger.info(f"Snapshot file {self._snapshot_file} does not exist")

    @staticmethod
    def _snapshot_decorator(func):
        def wrapper(self, *args, **kwargs):
            ret = func(self, *args, **kwargs)
            self.create_snapshot()
            return ret

        return wrapper

    @_snapshot_decorator
    def create_queue(self, name: str, owner: str):

        if name in self._queues:
            return self._queues[name]

        queue = Queue(name, owner)
        self._queues[name] = queue

        return queue

    @_snapshot_decorator
    def push(self, message: Message):
        queue_name = message.body['queue_name']
        queue = self._queues[queue_name]
        queue.push(message)

    @_snapshot_decorator
    def pop(self, message: Message) -> str:
        queue_name = message.body
        client_id = message.sender_id
        queue = self._queues[queue_name]
        return queue.pop(client_id)
