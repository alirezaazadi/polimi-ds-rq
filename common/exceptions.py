class QueueDoesNotExist(Exception):
    def __init__(self, queue_id):
        self.queue_id = queue_id
        super().__init__(f"Queue with id {queue_id} does not exist")


class EmptyQueue(Exception):
    def __init__(self, queue_id, queue_name):
        self.queue_id = queue_id
        self.queue_name = queue_name

        if queue_name is None:
            super().__init__(f"Queue with id {queue_id} is empty")

        super().__init__(f"Queue {queue_name} with id {queue_id} is empty")


class ClientNotInQueue(Exception):
    def __init__(self, client_id, queue_id, queue_name):
        self.client_id = client_id
        self.queue_id = queue_id
        self.queue_name = queue_name

        if queue_name is None:
            super().__init__(f"Client {client_id} is not in queue with id {queue_id}")

        super().__init__(f"Client {client_id} is not in queue {queue_name} with id {queue_id}")


class InvalidMessageStructure(Exception):
    def __init__(self):
        self.message = 'Message structure is invalid'
