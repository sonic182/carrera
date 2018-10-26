"""Dispatcher."""
import random
from time import sleep
from uuid import uuid4
from queue import Queue
from carrera.message import Message
from carrera.communication import TCPServer
from carrera.communication import TCPClient
from carrera.utils import get_logger


class Dispatcher(object):
    instance = None

    def __new__(cls, *args, **kwargs):
        if not cls.instance:
            cls.instance = object.__new__(cls)
        return cls.instance

    def __init__(self):
        self.actors = {}
        self.id = self.uuid()
        self.type = 'node'
        self.server = None
        self.client = None
        self.send_q = Queue()

    def setup(self, verbose=True, debug=False):
        """Setup dispatcher."""
        self.logger, _ = get_logger(verbose=verbose, debug=debug)

    def add_actor(self, actor, node=None):
        actors = self.actors[actor.name] = self.actors.get(actor.name, {})
        actors[actor.id] = {
            'node': node or self.id,
            'actor': actor
        }

    def remove_actor(self, actor):
        if actor.id in self.actors:
            del self.actors[actor.id]

    def dispatch(self, message, target_name, target_id=None, sender_id=None):
        """Dispatch message."""
        actor = self.select_actor(target_name, target_id)['actor']
        message = Message(self, message, target=actor, sender=sender_id)
        msg_data = {
            'msgid': message.id,
            'message': message.data,
            'target_name': target_name,
            'target_id': target_id,
            'sender_id': sender_id
        }
        actor.receive(msg_data)
        self.logger.debug('dispatched_message', msg_data)
        return message

    def select_actor(self, sender_name, sender_id):
        if sender_id:
            return self.actors[sender_name][sender_id]
        key = random.choice(list(self.actors[sender_name]))
        return self.actors[sender_name][key]

    def send(self, actor, msg, **kwargs):
        """Send task to actor."""
        self.logger.debug('sending_message', {'msg': msg, **kwargs})
        return self.dispatch(msg, actor, **kwargs)

    @staticmethod
    def uuid():
        """Get unique identifier."""
        return uuid4().hex

    def result(self, message, timeout=None):
        """Send task to actor."""
        return self.select_actor(
            message.target.name, message.target.id)['actor'].result(
            message, timeout=timeout)

    def response(self, response, msgid, message, target_name, target_id=None,
                 sender_id=None):
        """Dispatch message."""
        self.logger.debug('got_response', {
            'response': response, 'msgid': msgid, 'message': message,
            'target_name': target_name, 'target_id': target_id})
        self.select_actor(
            target_name, target_id)['actor'].response_result(response, msgid)
        return msgid

    def join(self):
        """Join to dispatcher loop."""
        while True:
            sleep(1)

    def setup_server(self, host, port):
        """Setup master tcp server.

        On new connection:
        * fetch node info and actors
        """
        self.type = 'master'
        self.server = TCPServer(self, host, port).start()

    def connect_to_server(self, host, port):
        """Setup tcp connection.

        On new connection:
        * Wait for commands.
        """
        self.tcpclient = TCPClient(host, port)
        self.client = self.tcpclient.start()
