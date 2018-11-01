
import threading
from time import sleep
from queue import Queue
from carrera.actors.base import Actor


class ThreadActor(Actor):
    """Thread actor."""

    def __init__(self, *args, **kwargs):
        super(ThreadActor, self).__init__(*args, **kwargs)
        self.queue = Queue()
        self.response_q = Queue()
        self.exit = False
        self.msg = None
        self.thread = threading.Thread(target=self.loop, name=self.name)
        self.thread.start()

    def loop(self):
        while not (self.exit and self.queue.empty()):
            if not self.queue.empty():
                self.msg = self.queue.get()
                res = self.on_message(self.msg['message'])
                self._dispatcher.response(res, **self.msg)
            sleep(0.02)

    def cleanup(self, force=False):
        self.exit = True

    def receive(self, message):
        self.queue.put(message)

    def on_message(self, *args, **kwargs):
        raise NotImplementedError()

    def response_result(self, response, msgid):
        self.response_q.put((response, msgid))

    def result(self, message, exit=False, timeout=None):
        while True:
            response, msgid = self.response_q.get(timeout=timeout)
            if msgid != message.id:
                self.response_q.put((response, msgid))
            else:
                if exit:
                    self.exit = True
                return response
