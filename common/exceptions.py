class InvalidMessageStructure(Exception):
    def __init__(self):
        self.message = 'Message structure is invalid'
