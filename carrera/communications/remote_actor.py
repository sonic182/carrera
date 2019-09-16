"""Remote actor representation."""


class RemoteActor(object):
    """Remote actor representation."""

    def __init__(self, dispatcher, actor_name, _id, node):
        self.id = _id
        self.node = node
        self.name = actor_name
        self._dispatcher = dispatcher

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
        pass

    def setup(self, *args, **kwargs):
        pass

    def send(self, msg, target_id=None, sender_id=None):
        """Send msg to actor."""
        return self._dispatcher.dispatch(msg, self.name, target_id, sender_id)

    def receive(self, message):
        self._dispatcher.server.workers[self.node].post_job(message)

    def result(self, message, timeout):
        """Message result."""
        res = self._dispatcher.server.workers[self.node].get_job(
            message.to_dict(), timeout=timeout)
        return res['response']
