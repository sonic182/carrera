"""Message representation."""
from datetime import datetime
from datetime import timedelta


class Message(object):
    def __init__(self, dispatcher, message, target_id, target_name,
                 sender_id, msgid=None):
        self.id = msgid or dispatcher.uuid()
        self._dispatcher = dispatcher
        self.data = message
        self.target_id = target_id
        self.target_name = target_name
        self.sender_id = sender_id
        self.created_at = datetime.utcnow()

    @property
    def config(self):
        return self._dispatcher.config['messages']

    def to_dict(self):
        return {
            'msgid': self.id,
            'message': self.data,
            'target_name': self.target_name,
            'target_id': self.target_id,
            'sender_id': self.sender_id
        }

    def set_target(self, actor):
        """Set target data."""
        self.target_id = actor.id
        self.target_name = actor.name

    def expired(self):
        expiration = self.config.getint('expiration')
        if not expiration:
            return False
        return datetime.utcnow() > self.created_at + timedelta(
            seconds=expiration)

    @staticmethod
    def from_dict(dispatcher, message):
        return Message(dispatcher, **message)
