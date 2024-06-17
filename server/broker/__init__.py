import uuid

from .client import Client, client_manager
from .queue import Queue, queue_manager


class Broker:
    def __init__(self):
        self._name = f'BROKER-{uuid.uuid4().hex}'
        self._q_manager = queue_manager
        self._c_manager = client_manager

    def create_queue(self, _c: Client, name: str | None = None):
        return self._q_manager.add_queue(name, _c)

    def remove_queue(self, _c: Client, queue_name: str):
        return self._q_manager.remove_queue(queue_name, _c)

    def get_queues_list(self):
        return self._q_manager.get_queues_list()

    def get_queue_by_id(self, queue_id: uuid.UUID) -> Queue:
        return self._q_manager.get_queue_by_id(queue_id)

    def get_queue_by_name(self, _c: Client, name: str) -> Queue:
        return self._q_manager.get_queue_by_name(_c, name)
