"""Test say hello."""

import pytest
from carrera import actors
from carrera.dispatcher import Dispatcher


class HelloActor(actors.ThreadActor):
    """Sample Hello actor."""

    def on_message(self, msg):
        return 'Hello ' + msg


class PHelloActor(actors.ProcessActor):
    """Sample Process Hello actor."""

    def on_message(self, msg):
        return 'Hello ' + msg


class TestCase():

    @pytest.fixture(autouse=True)
    def setUp(self):
        with Dispatcher() as dispatcher:
            dispatcher.setup(True, True)
            yield

    @staticmethod
    def test_hello():
        """Test hello."""
        with HelloActor() as actor:
            message = actor.send('world')
            assert actor.result(message) == 'Hello world'

    @staticmethod
    def test_hello_p():
        """Test hello process."""
        with PHelloActor() as actor:
            message = actor.send('world')
            assert actor.result(message) == 'Hello world'

    @staticmethod
    def test_hello_dispatch():
        """Test hello."""
        with HelloActor() as actor:
            message = actor._dispatcher.send('hello_actor', 'world')
            assert actor._dispatcher.result(message) == 'Hello world'
