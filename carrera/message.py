"""Message representation."""


class Message(object):
    def __init__(self, dispatcher, message, target_id, target_name,
                 sender_id, msgid=None):
        self.id = msgid or dispatcher.uuid()
        self.data = message
        self.target_id = target_id
        self.target_name = target_name
        self.sender_id = sender_id

    def __str__(self):
        args = (self.id, str(self.target), str(self.sender))
        return '-'.join(args)

    def to_dict(self):
        return {
            'msgid': self.id,
            'message': self.data,
            'target_name': self.target_name,
            'target_id': self.target_id,
            'sender_id': self.sender_id
        }

    @staticmethod
    def from_dict(dispatcher, message):
        return Message(dispatcher, **message)
