class InvalidMessageStructure(Exception):
    def __init__(self):
        self.message = 'Message structure is invalid'


class NoBrokerAvailable(Exception):
    def __init__(self):
        self.message = 'No broker is available to handle the request'
