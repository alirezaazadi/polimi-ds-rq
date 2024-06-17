import dataclasses
import uuid
from datetime import datetime
from typing import Dict, Any

from server.utils import Singleton


@dataclasses.dataclass
class Client:
    socket: Any = None
    name: str = dataclasses.field(default=None, repr=True, metadata={'help': 'Client Name'})
    id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4, repr=True, metadata={'help': 'Client Unique ID'})

    created_at: datetime = dataclasses.field(default_factory=datetime.now, repr=True,
                                             metadata={'help': 'Client Connection Time'})

    def set_socket(self, socket):
        self.socket = socket

    def __hash__(self):
        return self.id.__hash__()

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __repr__(self):
        return str(self.id) if self.name is None else f"{self.name} ({str(self.id)})"

    def __str__(self):
        return self.__repr__()


class ClientManager(metaclass=Singleton):
    def __init__(self):
        self._clients: Dict[uuid.UUID, Client] = dict()

    def get_clients_list(self):
        return self._clients.keys()

    def add_client(self, socket, name=None):
        client = Client(socket=socket, name=name)
        self._clients[client.id] = client
        return client

    def remove_client(self, client_id: uuid.UUID):

        try:
            del self._clients[client_id]
        except KeyError:
            return False
        else:
            return True


client_manager = ClientManager()
