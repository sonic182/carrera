
from uuid import uuid4


class Dispatcher(object):
    instance = None

    def __new__(cls, *args, **kwargs):
        if not cls.instance:
            cls.instance = object.__new__(cls)
        return cls.instance

    def __init__(self):
        self.actors = {}

    def add_actor(self, actor):
        self.actors[actor.id] = actor

    def remove_actor(self, actor):
        if actor.id in self.actors:
            del self.actors[actor.id]

    def dispatch(self, message, sender_id, receiver_id):
        """Dispatch message."""
        msgid = uuid4().hex
        self.actors[receiver_id or sender_id].receive({
            'msgid': msgid,
            'message': message,
            'sender_id': sender_id,
            'receiver_id': receiver_id
        })
        return msgid

    def response(self, response, msgid, message, sender_id, receiver_id):
        """Dispatch message."""
        self.actors[sender_id].response_result(response, msgid)
        return msgid
