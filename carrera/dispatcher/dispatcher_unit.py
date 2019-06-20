
import random
from queue import Queue
from datetime import datetime
from datetime import timedelta
from time import sleep
from threading import Thread
from carrera.message import Message


class DispatcherUnit(object):

    def __init__(self, dispatcher):
        """Dispatcher unit."""
        self.dispatcher = dispatcher
        self.dispatch_q = Queue()
        self.exit = False
        self.dispatcher_t = Thread(
            target=self.dispatcher_unit,
            name=self.dispatcher.id)
        self.dispatcher_t.start()
        self.cache = {}

    @property
    def actors(self):
        return self.dispatcher.actors

    @property
    def logger(self):
        return self.dispatcher.logger

    @property
    def config(self):
        return self.dispatcher.config['dispatcher']

    def cleanup(self):
        """Do cleanup."""
        self.exit = True

    def dispatcher_unit(self):
        """Unit of messages dispatching."""
        queue = self.dispatch_q

        while not (self.exit and queue.empty()):
            if not queue.empty():
                msg = queue.get()
                try:
                    if not self.actor_present(msg):
                        if not msg.expired():
                            # This could lock actors dict being editable
                            # so we need a little sleep here
                            if queue.empty():
                                sleep(0.02)
                            queue.put(msg)
                        continue
                    if msg.broadcast:
                        self.broadcast_to_actors(msg)
                    else:
                        self.dispatch_to_actor(msg)
                    queue.task_done()
                except Exception:
                    self.logger.exception('dispatch_exception')
            sleep(0.02)

    def dispatch(self, message: Message):
        """Set message into dispatch queue."""
        self.dispatch_q.put(message)

    def result(self, message: Message, timeout=None):
        """Set message into dispatch queue."""
        self.wait_actor(message, timeout)
        self.logger.debug(
            'getting_result_response', extra={
                'target': message.target_name, 'id': message.target_id})
        return self.select_actor(
            message.target_name, message.target_id)['actor'].result(
                message, timeout=timeout)

    def response(self, response, msgid, target_name, target_id=None, **kwargs):
        """Tell actor it's response."""
        self.logger.debug('setting_response', extra={'target': target_name,
                                                     'id': target_id,
                                                     'kwargs': kwargs})
        self.select_actor(
            target_name, target_id)['actor'].response_result(
            response, msgid)

    def actor_present(self, message: Message):
        """Check if actor's present."""
        return message.target_name in self.actors

    def wait_actor(self, message: Message, timeout):
        """Check if actor's present and assigned to message."""
        until = datetime.now() + timedelta(seconds=timeout or 60)
        while message.target_name not in self.actors or not message.target_id:
            if datetime.now() > until:
                raise TimeoutError('Actor never got available')
        return message.target_name in self.actors

    def dispatch_to_actor(self, message: Message):
        """Dispatch message to actor."""
        self.logger.debug('select_actor', extra=self.actors)
        actor = self.select_actor(
            message.target_name, message.target_id)['actor']
        message.set_target(actor)
        self.logger.debug('actor_selected', extra={'target_id': actor.id})
        return actor.receive(message.to_dict())

    def broadcast_to_actors(self, message: Message):
        """Broadcast message to actors."""
        self.logger.debug('select_actor', extra=self.actors)
        actors = self.actors[message.target_name]

        for _id, actor_data in actors.items():
            actor = actor_data['actor']
            message2 = message.copy()
            message2.set_target(actor)
            self.logger.debug('actor_selected', extra={'target_id': actor.id})
            actor.receive(message2.to_dict())

    def select_actor(self, target_name, target_id):
        """Select actor."""
        if target_id:
            return self.actors[target_name][target_id]
        select_method = self.config.get('selection', 'random')

        if select_method == 'random':
            key = random.choice(list(self.actors[target_name]))

        elif select_method == 'round_robin':
            cache_key = target_name + '_generator'
            self.cache[cache_key] = self.cache.get(
                cache_key) or self.round_robin(target_name)
            key = next(self.cache[cache_key])

        else:
            raise NotImplementedError('selection not implemented')

        return self.actors[target_name][key]

    def round_robin(self, target_name):
        """Round robin selection."""
        while True:
            for key in self.actors[target_name]:
                yield key
