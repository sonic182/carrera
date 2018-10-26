
from time import sleep
import multiprocessing as mp
from random import randint
from carrera.dispatcher import Dispatcher
from carrera import actors
import pytest


class Hello(actors.ThreadActor):

    def on_message(self, msg):
        return 'Hello ' + msg


def master(port):
    dispatcher = Dispatcher()
    dispatcher.setup_server('0.0.0.0', port)
    sleep(1)
    msg = dispatcher.send('hello', 'world')
    assert dispatcher.result(msg) == 'Hello world'
    return dispatcher


def node(port):
    dispatcher = Dispatcher()
    with Hello() as _actor:
        sleep(0.5)
        dispatcher.connect_to_server('0.0.0.0', port)
        dispatcher.client_join()


class TestCase(object):

    @pytest.fixture(autouse=True)
    def transact(self):
        Dispatcher().setup(True, True)
        yield

    def test_master_node(self):
        """Test master-node task."""
        port = randint(1025, 9999)
        p2 = mp.Process(target=node, args=(port, ))
        p2.start()
        dispatcher = master(port)
        p2.terminate()
        dispatcher.server.close()
