"""Test say hello."""

import pytest
from carrera import actors
from carrera.dispatcher import Dispatcher


class HelloActor(actors.ThreadActor):

    def on_message(self, msg):
        return 'Hello ' + msg


class PHelloActor(actors.ThreadActor):

    def on_message(self, msg):
        return 'Hello ' + msg


class P2HelloActor(actors.ProcessActor):

    def on_message(self, msg):
        with HelloActor() as actor:
            key = actor.send(msg, sender_id=actor.id)
            return actor.result(key)


class TestCase(object):

    @pytest.fixture(autouse=True)
    def transact(self):
        Dispatcher().setup(True, True)
        yield
        Dispatcher().cleanup()

    def test_hello_creating_actor(self):
        """Test hello actor creates another actor (thread) to resolve."""
        with PHelloActor() as actor:
            message = actor.send('world')
            assert 'Hello world' == actor.result(message)

    def test_hello_p_creating_actor(self):
        """Test hello actor creates another actor (process) to resolve."""
        with PHelloActor() as actor:
            message = actor.send('world')
            assert 'Hello world' == actor.result(message)
