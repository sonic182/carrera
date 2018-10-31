"""Dispatcher."""
import random
from uuid import uuid4
from queue import Queue
from carrera.message import Message
from carrera.communications import TCPServer
from carrera.communications import TCPClient
from carrera.communications.remote_actor import RemoteActor
from carrera.utils import get_logger


class Dispatcher(object):
    instance = None
    initialized = False

    def __new__(cls, *args, **kwargs):
        if not cls.instance:
            cls.instance = object.__new__(cls)
        return cls.instance

    def __init__(self):
        # singleton initialized flag...
        if self.initialized:
            return
        self.initialized = True

        self.actors = {}
        self.id = self.uuid()
        self.type = 'node'
        self.server = None
        self.tcpclient = None
        self.client = None
        self.send_q = Queue()

    def setup(self, verbose=True, debug=False):
        """Setup dispatcher."""
        self.logger, _ = get_logger(verbose=verbose, debug=debug)

    def add_actor(self, actor):
        actors = self.actors[actor.name] = self.actors.get(actor.name, {})
        actors[actor.id] = {
            'node': self.id,
            'actor': actor
        }

    def add_node_actors(self, actors, node):
        for actor_name, ids in actors.items():
            for _id in ids:
                actor = RemoteActor(self, actor_name, _id, node)
                actors = self.actors[actor.name] = self.actors.get(
                    actor.name, {})
                actors[actor.id] = {
                    'node': node,
                    'actor': actor
                }

    def remove_actor(self, actor):
        if actor.name in self.actors and actor.id in self.actors[actor.name]:
            del self.actors[actor.name][actor.id]

    def dispatch(self, message, target_name, target_id=None, sender_id=None,
                 msgid=None):
        """Dispatch message."""
        actor = self.select_actor(target_name, target_id)['actor']
        message = Message(self, message, target_id=actor.id,
                          target_name=actor.name, sender_id=sender_id,
                          msgid=msgid)
        msg_data = message.to_dict()
        actor.receive(msg_data)
        self.logger.debug('dispatched_message', extra=msg_data)
        return message

    def select_actor(self, target_name, target_id):
        if target_id:
            return self.actors[target_name][target_id]
        key = random.choice(list(self.actors[target_name]))
        return self.actors[target_name][key]

    def send(self, actor, msg, **kwargs):
        """Send task to actor."""
        self.logger.debug('sending_message', extra={'msg': msg, **kwargs})
        return self.dispatch(msg, actor, **kwargs)

    @staticmethod
    def uuid():
        """Get unique identifier."""
        return uuid4().hex

    def result(self, message, timeout=None):
        """Send task to actor."""
        if not isinstance(message, Message):
            message = Message.from_dict(self, message)
        return self.select_actor(
            message.target_name, message.target_id)['actor'].result(
            message, timeout=timeout)

    def response(self, response, msgid, message, target_name, target_id=None,
                 sender_id=None):
        """Dispatch message."""
        self.logger.debug('got_response', extra={
            'response': response, 'msgid': msgid, 'message': message,
            'target_name': target_name, 'target_id': target_id,
            'sender_id': sender_id
        })
        self.select_actor(
            target_name, target_id)['actor'].response_result(response, msgid)
        return msgid

    def setup_server(self, host, port):
        """Setup master tcp server.

        On new connection:
        * fetch node info and actors
        """
        self.type = 'master'
        self.logger.debug('starting_server', extra={
            'host': host, 'port': port})
        self.server = TCPServer(self, host, port).start()

    def connect_to_server(self, host, port):
        """Setup tcp connection.

        On new connection:
        * Join client orders
        """
        self.logger.debug('connecting_to_master', extra={
            'host': host, 'port': port})
        self.tcpclient = TCPClient(self, host, port)
        self.client = self.tcpclient.start()

    def client_join(self):
        """Join client to master."""
        self.client.join()
