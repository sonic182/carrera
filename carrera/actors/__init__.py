"""Actor class definitions."""
import threading
from uuid import uuid4

try:
    from queue import Queue as TQueue
except ImportError:  # python 2.7 compatibility
    from Queue import Queue as TQueue
from time import sleep


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


class Actor(object):
    """Actor base class."""

    def __init__(self):
        self.id = uuid4().hex
        self.dispatcher = Dispatcher()
        self.dispatcher.add_actor(self)

    def __del__(self):
        """Remove from dispatcher."""
        self.dispatcher.remove_actor(self)
        self.exit = True

    def send(self, msg, receiver_id=None):
        """Send msg to actor."""
        return self.dispatcher.dispatch(msg, self.id, receiver_id)


class ThreadActor(Actor):
    """Thread actor."""

    def __init__(self):
        super(ThreadActor, self).__init__()
        self.queue = TQueue()
        self.response_q = TQueue()
        self.exit = False
        self.msg = None
        self.thread = threading.Thread(target=self.loop)
        self.thread.start()

    def loop(self):
        while not (self.exit and self.queue.empty()):
            if not self.queue.empty():
                self.msg = self.queue.get()
                res = self.on_message(self.msg['message'])
                self.dispatcher.response(res, **self.msg)
            sleep(0.02)

    def receive(self, message):
        self.queue.put(message)

    def on_message(self, *args, **kwargs):
        raise NotImplementedError()

    def response_result(self, response, msgid):
        self.response_q.put((response, msgid))

    def result(self, _id, exit=False):
        while True:
            response, msgid = self.response_q.get()
            if msgid == _id:
                if exit:
                    self.exit = True
                return response
