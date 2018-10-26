"""Remote actor representation."""
import json


class RemoteActor(object):
    """Remote actor representation."""

    def __init__(self, dispatcher, actor_name, _id, node):
        self.id = _id
        self.node = node
        self.name = actor_name
        self.dispatcher = dispatcher

    def __del__(self):
        """Remove from dispatcher."""
        self.dispatcher.remove_actor(self)
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
        return self.dispatcher.dispatch(msg, self.name, target_id, sender_id)

    def receive(self, message):
        self.dispatcher.server.workers[self.node].command(
            'post_job', json.dumps(message).encode())

    def result(self, message, timeout):
        """Message result."""
        self.dispatcher.server.workers[self.node].command(
            'get_job', json.dumps(message.to_dict()).encode())
        res = self.dispatcher.server.workers[self.node].recv_sized()
        return json.loads(res.split(b' ', 2)[2].decode())['response']
