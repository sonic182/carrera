"""Actors base class."""
import re
from carrera.dispatcher import Dispatcher

RGX = re.compile(r'([a-z]{1})([A-Z]{1})')


class Actor(object):
    """Actor base class."""

    def __init__(self, *args, **kwargs):
        self._dispatcher = Dispatcher()
        self.id = self._dispatcher.uuid()
        self._dispatcher.add_actor(self)

    def __del__(self):
        """Remove from dispatcher."""
        self._dispatcher.remove_actor(self)
        self.cleanup()

    @property
    def name(self):
        return re.sub(RGX, '\\1_\\2', self.__class__.__name__).lower()

    def __enter__(self):
        return self

    def __str__(self):
        return self.name

    def __exit__(self, *args, **kwargs):
        self.cleanup()

    def cleanup(self, *args, **kwargs):
        pass

    def setup(self, *args, **kwargs):
        pass

    def send(self, msg, target_id=None, sender_id=None):
        """Send msg to actor."""
        return self._dispatcher.dispatch(msg, self.name, target_id, sender_id)

    def result(self, *args, **kwargs):
        """Result method."""
        raise NotImplementedError()
