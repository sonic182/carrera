"""Dispatcher."""
from uuid import uuid4
from configparser import ConfigParser
from carrera.dispatcher.dispatcher_unit import DispatcherUnit
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

    def __init__(self, _id=None):
        # singleton initialized flag...
        if self.initialized:
            return
        self.initialized = True
        self.logger = None
        self.config = None

        self.actors = {}
        self.id = _id or self.uuid()
        self.type = 'node'
        self.server = None
        self.client = None
        self.unit = DispatcherUnit(self)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.cleanup()

    def cleanup(self):
        self.instance = None
        self.initialized = False
        self.unit.cleanup()

    def setup(self, verbose=True, debug=False,
              config: ConfigParser = ConfigParser()):
        """Setup dispatcher."""
        if 'messages' not in config:
            config.add_section('messages')
        self.logger, _ = get_logger(verbose=verbose, debug=debug)
        self.logger.info('dispatcher_info', extra={'id': self.id})
        self.config = config

    def add_actor(self, actor):
        actors = self.actors[actor.name] = self.actors.get(actor.name, {})
        actors[actor.id] = {
            'node': self.id,
            'actor': actor
        }

    def add_node_actors(self, actors, node):
        for actor_name, ids in actors.items():
            for _id in ids:
                actor = RemoteActor(self, actor_name, _id,
                                    actors[actor_name][_id]['node'])
                actors = self.actors[actor.name] = self.actors.get(
                    actor.name, {})
                actors[actor.id] = {
                    'node': actor.node,
                    'actor': actor
                }

    def remove_actor(self, actor):
        if actor.name in self.actors and actor.id in self.actors[actor.name]:
            del self.actors[actor.name][actor.id]

    def dispatch(self, message_data, target_name, target_id=None,
                 sender_id=None, msgid=None):
        """Dispatch message."""
        message = Message(self, message_data, target_id, target_name,
                          sender_id, msgid=msgid)
        self.unit.dispatch(message)
        self.logger.debug('dispatched_message', extra=message.to_dict())
        return message

    def send(self, actor, msg, **kwargs):
        """Send task to actor."""
        self.logger.debug('sending_message', extra={'msg': msg, **kwargs})
        return self.dispatch(msg, actor, **kwargs)

    @staticmethod
    def uuid():
        """Get unique identifier."""
        return uuid4().hex

    def result(self, message, timeout=None):
        """Get actor's message result."""
        if not isinstance(message, Message):
            message = Message.from_dict(self, message)
        self.logger.debug('result_message', extra=message.to_dict())
        return self.unit.result(message, timeout)

    def response(self, response, msgid, message, target_name, target_id=None,
                 sender_id=None):
        """Dispatch actor's response to actor's requester."""
        self.logger.debug('got_response', extra={
            'response': response, 'msgid': msgid, 'message': message,
            'target_name': target_name, 'target_id': target_id,
            'sender_id': sender_id
        })
        self.unit.response(response, msgid, target_name, target_id,
                           message=message, sender_id=sender_id)
        return msgid

    def setup_server(self, host, port):
        """Setup tcp server.

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
        * fetch node info and actors
        """
        self.logger.debug('connecting_to_master', extra={
            'host': host, 'port': port})
        self.server = TCPClient(self, host, port)
        self.client = self.server.start()

    def join(self):
        """Join client to master."""
        getattr(self, 'client', self.server).join()
