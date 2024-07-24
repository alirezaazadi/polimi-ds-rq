import enum
import msgpack
import time
import uuid

from common.exceptions import InvalidMessageStructure


class Status(enum.IntEnum):
    NO_STATUS = 0x0
    SUCCESS = 0x1
    ERROR = 0x2


class Operation(enum.IntEnum):
    NO_OPERATION = 0x0
    CREATE_QUEUE = 0x1
    APPEND_QUEUE = 0x2
    READ_QUEUE = 0x3
    REGISTER_CLIENT = 0x4


class MessageType(enum.IntEnum):
    REQUEST = 0x0
    RESPONSE = 0x1


class Message:

    def __init__(
            self, operation: Operation, message_type: MessageType, receiver_id: str,
            sender_id: str | None = None,
            queue_id: str | None = None,
            status: Status | None = None,
            timestamp: float | None = None,
            body: bytes | None = None,
            message_id: str | None = None
    ):
        self._queue_id: str = queue_id
        self._receiver_id: str = receiver_id
        self._sender_id: str = sender_id
        self._operation: Operation = operation
        self._message_type: MessageType = message_type
        self._status: Status = status if status is not None else Status.NO_STATUS
        self._message_id: str = message_id if message_id is not None else str(uuid.uuid4())
        self._timestamp: float = timestamp
        self._body: bytes = body if body is not None else b''

    @property
    def operation(self) -> Operation:
        return self._operation

    @property
    def message_type(self) -> MessageType:
        return self._message_type

    @property
    def status(self) -> Status:
        return self._status

    @property
    def message_id(self) -> str:
        return self._message_id

    @property
    def queue_id(self) -> str:
        return self._queue_id

    @property
    def receiver_id(self) -> str:
        return self._receiver_id

    @property
    def sender_id(self) -> str:
        return self._sender_id

    @property
    def timestamp(self) -> float:
        return self._timestamp

    @property
    def body(self) -> bytes:
        return self._body

    def to_bytes(self) -> bytes:
        return msgpack.packb(
            {
                'operation': self.operation,
                'message_type': self.message_type,
                'status': self.status,
                'message_id': self.message_id,
                'queue_id': self.queue_id,
                'receiver_id': self.receiver_id,
                'sender_id': self.sender_id,
                'timestamp': self.timestamp if self.timestamp is not None else time.time(),
                'body': self.body
            }
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Message':
        try:
            message = msgpack.unpackb(data)
        except:  # noqa
            raise InvalidMessageStructure()

        try:
            return cls(
                operation=message['operation'],
                message_type=message['message_type'],
                status=message['status'],
                message_id=message['message_id'],
                queue_id=message['queue_id'],
                receiver_id=message['receiver_id'],
                sender_id=message['sender_id'],
                timestamp=message['timestamp'],
                body=message['body']
            )
        except:  # noqa
            raise InvalidMessageStructure()

    def __str__(self):
        return (f'Message ({self.sender_id} -> {self.receiver_id}) ID: {self._message_id},'
                f' Operation: {self._operation}, Message Type: {self._message_type}, '
                f'Status: {self._status}, Queue ID: {self._queue_id} @ {self._timestamp}')

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other: 'Message') -> bool:
        return self._message_id == other.message_id and \
            self._timestamp == other.timestamp and \
            self._operation == other.operation and \
            self._message_type == other.message_type and \
            self._status == other.status and \
            self._queue_id == other.queue_id and \
            self._receiver_id == other.receiver_id and \
            self._sender_id == other.sender_id and \
            self._body == other.body

    def __ge__(self, other):
        """
        lamport clock comparison
        :param other:
        :return:
        """

        return self._timestamp >= other.timestamp


class MessageFactory:

    @staticmethod
    def _create_request(
            operation: Operation, receiver_id: str, sender_id: str,
            body: bytes | None = None, queue_id: str | None = None
    ) -> Message:
        return Message(
            operation=operation,
            message_type=MessageType.REQUEST,
            queue_id=queue_id,
            receiver_id=receiver_id,
            sender_id=sender_id,
            body=body
        )

    @staticmethod
    def _create_response(
            operation: Operation, receiver_id: str, sender_id: str,
            status: Status, body: bytes | None = None, queue_id: str | None = None
    ) -> Message:
        return Message(
            operation=operation,
            message_type=MessageType.RESPONSE,
            queue_id=queue_id,
            receiver_id=receiver_id,
            sender_id=sender_id,
            status=status,
            body=body
        )

    @staticmethod
    def create_queue_request(queue_id: str, receiver_id: str, sender_id: str, body: bytes) -> Message:
        return MessageFactory._create_request(
            operation=Operation.CREATE_QUEUE, receiver_id=receiver_id, sender_id=sender_id, body=body, queue_id=queue_id
        )

    @staticmethod
    def create_queue_response(queue_id: str, receiver_id: str, sender_id: str, status: Status,
                              body: bytes) -> Message:
        return MessageFactory._create_response(
            operation=Operation.CREATE_QUEUE, receiver_id=receiver_id, sender_id=sender_id, status=status, body=body,
            queue_id=queue_id
        )

    @staticmethod
    def append_queue_request(queue_id: str, receiver_id: str, sender_id: str, body: bytes) -> Message:
        return MessageFactory._create_request(
            operation=Operation.APPEND_QUEUE, receiver_id=receiver_id, sender_id=sender_id, body=body, queue_id=queue_id
        )

    @staticmethod
    def append_queue_response(queue_id: str, receiver_id: str, sender_id: str, status: Status,
                              body: bytes) -> Message:
        return MessageFactory._create_response(
            operation=Operation.APPEND_QUEUE, receiver_id=receiver_id, sender_id=sender_id, status=status, body=body,
            queue_id=queue_id
        )

    @staticmethod
    def read_queue_request(queue_id: str, receiver_id: str, sender_id: str) -> Message:
        return MessageFactory._create_request(
            Operation.READ_QUEUE, receiver_id, sender_id, queue_id=queue_id
        )

    @staticmethod
    def read_queue_response(queue_id: str, receiver_id: str, sender_id: str, status: Status,
                            body: bytes) -> Message:
        return MessageFactory._create_response(
            operation=Operation.READ_QUEUE, receiver_id=receiver_id, sender_id=sender_id, status=status, body=body,
            queue_id=queue_id
        )

    @staticmethod
    def register_client_request(receiver_id: str, sender_id: str, body: bytes) -> Message:
        return MessageFactory._create_request(
            operation=Operation.REGISTER_CLIENT, receiver_id=receiver_id, sender_id=sender_id, body=body
        )

    @staticmethod
    def register_client_response(receiver_id: str, sender_id: str, status: Status, body: bytes) -> Message:
        return MessageFactory._create_response(
            operation=Operation.REGISTER_CLIENT, receiver_id=receiver_id, sender_id=sender_id, status=status, body=body
        )

    @staticmethod
    def from_bytes(data: bytes) -> Message:
        return Message.from_bytes(data)


factory = MessageFactory()
