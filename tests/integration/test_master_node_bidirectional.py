
from time import sleep
import multiprocessing as mp
from random import randint
from carrera.dispatcher import Dispatcher
from carrera import actors


class Hello(actors.ThreadActor):

    def on_message(self, msg):
        return 'Hello ' + msg


def master(port, barrier, conn):
    with Dispatcher() as dispatcher:
        dispatcher.setup(True, True)
        dispatcher.setup_server('0.0.0.0', port)
        with Hello() as _actor:  # noqa
            barrier.wait()
            conn.send(True)
            conn.recv()
            dispatcher.server.close()


def node(port, barrier, conn, result_conn):
    with Dispatcher() as dispatcher:
        dispatcher.setup(True, True)
        barrier.wait()
        dispatcher.connect_to_server('0.0.0.0', port)
        conn.recv()
        sleep(0.3)  # nodes synchronization
        msg = dispatcher.send('hello', 'world')
        result_conn.send(dispatcher.result(msg))
        conn.send(True)


def test_master_node_bidirectional():
    """Test master-node task."""
    port = randint(1025, 9999)
    parent_conn, child_conn = mp.Pipe()
    result_conn, result_child_conn = mp.Pipe()
    barrier = mp.Barrier(2)
    p1 = mp.Process(target=master, args=(port, barrier, parent_conn))
    p2 = mp.Process(
        target=node, args=(port, barrier, child_conn, result_child_conn))
    p1.start()
    p2.start()
    assert result_conn.poll(2)
    assert result_conn.recv() == 'Hello world'
    p1.terminate()
    p2.terminate()
