"""Test say hello."""

import pytest
from carrera import actors
from carrera.dispatcher import Dispatcher


class HelloActor(actors.ThreadActor):

    def on_message(self, msg):
        return 'Hello ' + msg


class PHelloActor(actors.ProcessActor):

    def on_message(self, msg):
        return 'Hello ' + msg


class TestCase(object):

    @pytest.fixture(autouse=True)
    def transact(self):
        Dispatcher().setup(True, True)
        yield

    def test_hello(self):
        """Test hello."""
        with HelloActor() as actor:
            message = actor.send('world')
            assert 'Hello world' == actor.result(message, timeout=2)

    def test_hello_p(self):
        """Test hello process."""
        with PHelloActor() as actor:
            message = actor.send('world')
            assert 'Hello world' == actor.result(message, timeout=2)

    def test_hello_dispatch(self):
        """Test hello."""
        with HelloActor() as actor:
            message = actor.dispatcher.send('hello_actor', 'world')
            assert 'Hello world' == actor.dispatcher.result(message, timeout=2)
            assert False
