
import multiprocessing as mp
from time import sleep
from carrera.actors.base import Actor


class ProcessActor(Actor):
    """Thread actor."""

    def __init__(self):
        super(ProcessActor, self).__init__()
        ctx = mp.get_context('spawn')
        self.queue = ctx.Queue()
        self.response_q = ctx.Queue()
        self.msg = None
        self.process = mp.Process(target=self.loop)
        self.process.start()

    def loop(self):
        while True:
            if not self.queue.empty():
                self.msg = self.queue.get()
                if self.msg.get('exit'):
                    break
                res = self.on_message(self.msg['message'])
                self.dispatcher.response(res, **self.msg)
            sleep(0.02)

    def cleanup(self, force=False):
        if force:
            self.process.terminate()
        else:
            self.queue.put({'exit': True})

    def receive(self, message):
        self.queue.put(message)

    def on_message(self, *args, **kwargs):
        raise NotImplementedError()

    def response_result(self, response, msgid):
        self.response_q.put((response, msgid))

    def result(self, _id, exit=False, timeout=None):
        while True:
            response, msgid = self.response_q.get(timeout=timeout)
            if msgid == _id:
                if exit:
                    self.queue.put({'exit': True})
                return response
