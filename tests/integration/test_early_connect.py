
# from time import sleep
import multiprocessing as mp
from random import randint
from carrera.dispatcher import Dispatcher
from carrera import actors


class Hello(actors.ThreadActor):

    def on_message(self, msg):
        return 'Hello ' + msg


def master(port, conn, result_conn, barrier):
    with Dispatcher() as dispatcher:  # noqa
        dispatcher.setup(True, True)
        dispatcher.setup_server('0.0.0.0', port)
        msg = dispatcher.send('hello', 'world')
        barrier.wait()
        result_conn.send(dispatcher.result(msg))

        dispatcher.logger.debug('exit_text')
        dispatcher.server.close()


def node(port, conn, barrier):
    with Dispatcher() as dispatcher:  # noqa
        dispatcher.setup(True, True)
        with Hello() as _actor:  # noqa
            barrier.wait()
            dispatcher.connect_to_server('0.0.0.0', port)
            conn.recv()  # wait to exit


def test_early_connect():
    """Test early connection."""
    port = randint(1025, 9999)
    parent_conn, child_conn = mp.Pipe()
    result_conn, result_child_conn = mp.Pipe()
    barrier = mp.Barrier(2)
    p1 = mp.Process(
        target=master, args=(port, child_conn, result_child_conn, barrier))
    p2 = mp.Process(target=node, args=(port, parent_conn, barrier))
    p1.start()
    p2.start()
    assert result_conn.poll(3)
    assert result_conn.recv() == 'Hello world'
    p1.terminate()
    p2.terminate()
