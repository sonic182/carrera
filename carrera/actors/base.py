"""Actors base class."""
from uuid import uuid4
from carrera.dispatcher import Dispatcher


class Actor(object):
    """Actor base class."""

    def __init__(self):
        self.id = uuid4().hex
        self.dispatcher = Dispatcher()
        self.dispatcher.add_actor(self)

    def __del__(self):
        """Remove from dispatcher."""
        self.dispatcher.remove_actor(self)
        self.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.cleanup()

    def cleanup(self, *args, **kwargs):
        pass

    def send(self, msg, receiver_id=None):
        """Send msg to actor."""
        return self.dispatcher.dispatch(msg, self.id, receiver_id)
