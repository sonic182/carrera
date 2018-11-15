
from time import sleep
import multiprocessing as mp
from random import randint
from carrera.dispatcher import Dispatcher
from carrera import actors
import pytest


class Hello(actors.ThreadActor):

    def on_message(self, msg):
        return 'Hello ' + msg


def master(port, barrier):
    dispatcher = Dispatcher()
    dispatcher.setup_server('0.0.0.0', port)
    with Hello() as _actor:  # noqa
        barrier.wait()
        sleep(1)
        dispatcher.server.close()


def node(port, barrier, conn):
    dispatcher = Dispatcher()
    barrier.wait()
    dispatcher.connect_to_server('0.0.0.0', port)
    sleep(0.5)
    msg = dispatcher.send('hello', 'world')
    conn.send(dispatcher.result(msg))


class TestCase(object):

    @pytest.fixture(autouse=True)
    def transact(self):
        Dispatcher().setup(True, True)
        yield
        Dispatcher().cleanup()

    def test_master_node_bidirectional(self):
        """Test master-node task."""
        port = randint(1025, 9999)
        parent_conn, child_conn = mp.Pipe()
        barrier = mp.Barrier(2, timeout=2)
        p1 = mp.Process(target=master, args=(port, barrier))
        p2 = mp.Process(target=node, args=(port, barrier, child_conn))
        p1.start()
        p2.start()
        p1.join(2)
        p2.join(1)
        assert parent_conn.poll(2)
        assert parent_conn.recv() == 'Hello world'
        p1.terminate()
        p2.terminate()
