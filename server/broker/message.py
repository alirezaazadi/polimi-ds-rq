import dataclasses
import uuid
from datetime import datetime
from typing import Any, Literal

from server.broker.client import Client


@dataclasses.dataclass(frozen=True)
class Message:
    sender: Client
    data: Any
    data_type: Literal['str', 'int', 'float', 'bytes', 'json', 'pickle']

    id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4, repr=True, metadata={'help': 'Message Unique ID'})
    sent_at: datetime = dataclasses.field(default_factory=datetime.now, repr=True,
                                          metadata={'help': 'Message Sent Time'})

    def __hash__(self):
        return self.id.__hash__()

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __repr__(self):
        return f"{self.sender} ({self.sent_at}) -> {self.data}"

    def __str__(self):
        return self.__repr__()
