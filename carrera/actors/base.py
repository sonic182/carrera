"""Actors base class."""
import re
from carrera.dispatcher import Dispatcher

RGX = re.compile(r'([a-z]{1})([A-Z]{1})')


class Actor():
    """Actor base class.

    New implementation of actors should be child of this class.
    """

    @property
    def name(self):
        """Get actor name."""
        return re.sub(RGX, '\\1_\\2', self.__class__.__name__).lower()

    def __init__(self, *_args, **_kwargs):
        """Initialize actor."""
        self._dispatcher = Dispatcher()
        self.id = self._dispatcher.uuid()
        self._dispatcher.add_actor(self)

    def __del__(self):
        """Remove from dispatcher."""
        self._dispatcher.remove_actor(self)
        self.cleanup()

    def __enter__(self):
        return self

    def __str__(self):
        return self.name

    def __exit__(self, *args, **kwargs):
        self.cleanup()

    def cleanup(self, *args, **kwargs):
        """Cleanup method.

        Util for cleanup information when actor exits.

        By default does nothing, this could be implemented on child
        actors.
        """

    def setup(self):
        """Setup method.

        Util for define configurations and associate objects to the actor at
        startup.

        By default does nothing, this could be implemented on child
        actors.
        """

    def send(self, msg: object, target_id: str = None, sender_id: str = None):
        """Send msg to actor.

        Params:
          * msg (object): Message data to be sent to an actor. It should
            be serializable
          * target_id(str): if provided send message to specified actor
            instance
          * sender_id(str): if provided specify message sender to target agent
        """
        return self._dispatcher.dispatch(
            msg, self.name, target_id or self.id, sender_id)

    def result(self, *args, **kwargs):
        """Result method.

        Method for waiting result about executed task. This should
        be implemented in child actors.
        """
        raise NotImplementedError()

    def loop(self):
        """Loop method

        For reading incoming messages. This should be implemented in child
        actors.
        """
        raise NotImplementedError()
