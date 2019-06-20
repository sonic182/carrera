
import threading
from time import sleep
from queue import Queue
from carrera.actors.base import Actor
from carrera.message import Message


class ThreadActor(Actor):
    """Thread actor."""

    def __init__(self, *_args, **_kwargs):
        super(ThreadActor, self).__init__(*_args, **_kwargs)
        self.queue = Queue()
        self.response_q = Queue()
        self.exit = False
        self.msg = None
        self.thread = threading.Thread(target=self.loop, name=self.name)
        self.thread.start()

    def loop(self):
        """Read messages from thread queue."""
        self.setup()

        while not (self.exit and self.queue.empty()):
            if not self.queue.empty():
                self.msg = self.queue.get()
                res = self.on_message(self.msg['message'])
                if not self.msg['broadcast']:
                    self.response_q.put((res, self.msg['msgid']))
                self.queue.task_done()
            sleep(0.02)

    def cleanup(self, force=False):
        """Cleanup actor."""
        self.exit = True

    def receive(self, message):
        self.queue.put(message)

    def on_message(self, *args, **kwargs):
        raise NotImplementedError()

    def response_result(self, response, msgid):
        self.response_q.put((response, msgid))

    def result(self, message: Message, exit: bool = False, timeout: int = None):
        """Get result of executed task.

        Params:
          * msg (carrera.messages.Message): sender Message instance.
          * exit(bool): if provided, this actor exists when failed result
            retrival.
          * timeout(str): if provided specify message sender to target agent
        """
        while True:
            response, msgid = self.response_q.get(timeout=timeout)
            if msgid != message.id:
                self.response_q.put((response, msgid))
                self.response_q.task_done()
            else:
                if exit:
                    self.exit = True
                self.response_q.task_done()
                return response
