
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

    @property
    def actors(self):
        return self.dispatcher.actors

    @property
    def logger(self):
        return self.dispatcher.logger

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
                        queue.put(msg)
                        continue
                    self.dispatch_to_actor(msg)
                except Exception:
                    self.logger.exception('dispatch_exception')
            sleep(0.02)

    def dispatch(self, message: Message):
        """Set message into dispatch queue."""
        self.dispatch_q.put(message)

    def result(self, message: Message, timeout=None):
        """Set message into dispatch queue."""
        # self.dispatch_q.put(message)
        self.wait_actor(message, timeout)
        self.logger.debug(
            'getting_result_response', extra={
                'target': message.target_name, 'id': message.target_id})
        return self.select_actor(
            message.target_name, message.target_id)['actor'].result(
                message, timeout=timeout)

    def response(self, response, msgid, message, target_name, target_id=None,
                 sender_id=None):
        """Tell actor it's response."""
        self.logger.debug('setting_response', extra={'target': target_name,
                                                     'id': target_id})
        self.select_actor(
            target_name, target_id)['actor'].response_result(
            response, msgid)

    def actor_present(self, message: Message):
        """Check if actor's present."""
        return message.target_name in self.actors

    def wait_actor(self, message: Message, timeout):
        """Check if actor's present."""
        until = datetime.now() + timedelta(seconds=timeout or 60)
        while message.target_name not in self.actors:
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

    def select_actor(self, target_name, target_id):
        if target_id:
            return self.actors[target_name][target_id]
        key = random.choice(list(self.actors[target_name]))
        return self.actors[target_name][key]
