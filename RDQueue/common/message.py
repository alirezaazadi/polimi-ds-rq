import enum
import time
import uuid
from typing import Any

import msgpack

from RDQueue.common.exceptions import InvalidMessageStructure


class MessageType(enum.IntEnum):
    REQUEST = 0x1
    RESPONSE = 0x2


class Operation(enum.IntEnum):
    NO_OP = 0x0
    QUEUE_CREATE = 0x1
    QUEUE_PUSH = 0x2
    QUEUE_POP = 0x3
    BROKER_INFO = 0x4
    REGISTER_CLIENT = 0x5


class Status(enum.IntEnum):
    SUCCESS = 0x1
    ERROR = 0x2


class Message:
    def __init__(
            self,
            sender_addr: str,
            receiver_addr: str,
            sender_id: str | None = None,
            receiver_id: str | None = None,
            message_type: MessageType = MessageType.REQUEST,
            operation: Operation = Operation.NO_OP,
            status: Status = Status.SUCCESS,
            body: Any | None = None,

            timestamp: float | None = None,
            _id: str | None = None
    ):
        self._sender_addr: str = sender_addr
        self._receiver_addr: str = receiver_addr
        self._sender_id: str | None = sender_id
        self._receiver_id: str | None = receiver_id
        self._message_type: MessageType = message_type
        self._operation: Operation = operation
        self._status: Status = status
        self._body: Any | None = body

        self._timestamp: float = time.time() if timestamp is None else timestamp
        self._id: str = str(uuid.uuid4().hex) if _id is None else _id

    @property
    def sender_addr(self) -> str:
        return self._sender_addr

    @property
    def sender_id(self) -> str | None:
        return self._sender_id

    @property
    def receiver_addr(self) -> str:
        return self._receiver_addr

    @property
    def receiver_id(self) -> str | None:
        return self._receiver_id

    @property
    def message_type(self) -> MessageType:
        return self._message_type

    @property
    def operation(self) -> Operation:
        return self._operation

    @property
    def status(self) -> Status:
        return self._status

    @property
    def is_ok(self) -> bool:
        return self._status == Status.SUCCESS

    @property
    def body(self) -> Any | None:
        return self._body

    @property
    def id(self) -> str:
        return self._id

    @property
    def timestamp(self) -> float:
        return self._timestamp

    def to_bytes(self) -> bytes:
        return msgpack.packb({
            'sender_addr': self.sender_addr,
            'receiver_addr': self.receiver_addr,
            'sender_id': self.sender_id,
            'receiver_id': self.receiver_id,
            'message_type': self.message_type,
            'operation': self.operation,
            'status': self.status,
            'body': self.body,
            'timestamp': self.timestamp,
            '_id': self.id
        }) + b'EOF'

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Message':
        try:
            message = msgpack.unpackb(data)

            message['message_type'] = MessageType(message['message_type'])
            message['operation'] = Operation(message['operation'])
            message['status'] = Status(message['status'])

            return cls(**message)
        except:  # noqa
            raise InvalidMessageStructure()

    def __str__(self):
        return (
            f'Message('
            f'sender={self.sender_addr}, '
            f'receiver={self.receiver_addr}, '
            f'type={self.message_type.name}, '
            f'operation={self.operation.name}, '
            f'status={self.status.name})'
        )

    def __repr__(self):
        return self.__str__()


class MessageFactory:
    @staticmethod
    def create_request(**kwargs):
        return Message(message_type=MessageType.REQUEST, **kwargs)

    @staticmethod
    def create_response(**kwargs):
        return Message(message_type=MessageType.RESPONSE, **kwargs)

    @classmethod
    def queue_create_req(cls, sender_addr: str, receiver_addr: str, **kwargs):
        return cls.create_request(sender_addr=sender_addr, receiver_addr=receiver_addr,
                                  operation=Operation.QUEUE_CREATE, **kwargs)

    @classmethod
    def queue_create_res(cls, sender_addr: str, receiver_addr: str, **kwargs):
        return cls.create_response(sender_addr=sender_addr, receiver_addr=receiver_addr,
                                   operation=Operation.QUEUE_CREATE, **kwargs)

    @classmethod
    def queue_push_req(cls, sender_addr: str, receiver_addr: str, **kwargs):
        return cls.create_request(sender_addr=sender_addr, receiver_addr=receiver_addr,
                                  operation=Operation.QUEUE_PUSH, **kwargs)

    @classmethod
    def queue_push_res(cls, sender_addr: str, receiver_addr: str, **kwargs):
        return cls.create_response(sender_addr=sender_addr, receiver_addr=receiver_addr,
                                   operation=Operation.QUEUE_PUSH, **kwargs)

    @classmethod
    def queue_pop_req(cls, sender_addr: str, receiver_addr: str, **kwargs):
        return cls.create_request(sender_addr=sender_addr, receiver_addr=receiver_addr,
                                  operation=Operation.QUEUE_POP, **kwargs)

    @classmethod
    def queue_pop_res(cls, sender_addr: str, receiver_addr: str, **kwargs):
        return cls.create_response(sender_addr=sender_addr, receiver_addr=receiver_addr,
                                   operation=Operation.QUEUE_POP, **kwargs)

    @classmethod
    def broker_info_req(cls, sender_addr: str, receiver_addr: str, **kwargs):
        return cls.create_request(sender_addr=sender_addr, receiver_addr=receiver_addr,
                                  operation=Operation.BROKER_INFO, **kwargs)

    @classmethod
    def broker_info_res(cls, sender_addr: str, receiver_addr: str, **kwargs):
        return cls.create_response(sender_addr=sender_addr, receiver_addr=receiver_addr,
                                   operation=Operation.BROKER_INFO, **kwargs)

    @classmethod
    def register_client_req(cls, sender_addr: str, receiver_addr: str, **kwargs):
        return cls.create_request(sender_addr=sender_addr, receiver_addr=receiver_addr,
                                  operation=Operation.REGISTER_CLIENT, **kwargs)

    @classmethod
    def register_client_res(cls, sender_addr: str, receiver_addr: str, **kwargs):
        return cls.create_response(sender_addr=sender_addr, receiver_addr=receiver_addr,
                                   operation=Operation.REGISTER_CLIENT, **kwargs)

    @classmethod
    def error_res(cls, sender_addr: str, receiver_addr: str, **kwargs):
        return cls.create_response(sender_addr=sender_addr, receiver_addr=receiver_addr,
                                   status=Status.ERROR, **kwargs)

    @classmethod
    def from_bytes(cls, data: bytes) -> Message:
        return Message.from_bytes(data)


factory = MessageFactory()
