
from time import sleep
import multiprocessing as mp
from random import randint
from carrera.dispatcher import Dispatcher
from carrera import actors
import pytest


class Hello(actors.ThreadActor):

    def on_message(self, msg):
        return 'Hello ' + msg


def master(port, conn):
    dispatcher = Dispatcher()
    dispatcher.setup_server('0.0.0.0', port)
    sleep(1)
    msg = dispatcher.send('hello', 'world')
    conn.send(dispatcher.result(msg))
    dispatcher.logger.debug('exit_text')
    dispatcher.server.close()


def node(port):
    dispatcher = Dispatcher()
    with Hello() as _actor:
        sleep(0.5)
        dispatcher.connect_to_server('0.0.0.0', port)
        dispatcher.join()


class TestCase(object):

    @pytest.fixture(autouse=True)
    def transact(self):
        Dispatcher().setup(True, True)
        yield
        Dispatcher().cleanup()

    def test_master_node(self):
        """Test master-node task."""
        port = randint(1025, 9999)
        parent_conn, child_conn = mp.Pipe()
        p1 = mp.Process(target=master, args=(port, child_conn))
        p2 = mp.Process(target=node, args=(port, ))
        p1.start()
        p2.start()
        p1.join(3)
        p2.join(3)
        assert parent_conn.poll(2)
        assert parent_conn.recv() == 'Hello world'
        p1.terminate()
        p2.terminate()
