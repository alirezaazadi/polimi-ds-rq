class ClientNotFoundException(Exception):
    def __init__(self, client):
        self.client = client
        self.message = f"Client {client} not found"
        super().__init__(self.message)


class InvalidQueueIndexException(Exception):
    def __init__(self, queue, index):
        self.queue = queue
        self.index = index
        self.message = f"Index {index} is invalid for the queue {queue}"
        super().__init__(self.message)


class InvalidQueueNameException(Exception):
    def __init__(self, name):
        self.name = name
        self.message = f"No Queue found with name {name}"
        super().__init__(self.message)


class InvalidQueueIDException(Exception):
    def __init__(self, queue_id):
        self.queue_id = queue_id
        self.message = f"No Queue found with name {queue_id}"
        super().__init__(self.message)


class PermissionDeniedException(Exception):
    def __init__(self, queue, client, action):
        self.queue = queue
        self.client = client
        self.action = action
        self.message = f"Permission denied for {action} on queue {queue} by client {client}"
        super().__init__(self.message)
