"""Test say hello."""

import pytest
from carrera import actors
from carrera.dispatcher import Dispatcher


class Hello(actors.ProcessActor):

    def on_message(self, msg):
        return 'Hello ' + msg


class TestCase(object):

    @pytest.fixture(autouse=True)
    def transact(self):
        self.actors = []
        with Dispatcher() as dispatcher:
            dispatcher.setup(True, True)
            yield
            for actor in self.actors:
                actor.cleanup()

    def test_hello(self):
        """Test hello."""
        for i in range(4):
            self.actors.append(Hello())

        dispatcher = Dispatcher()
        msg = dispatcher.send('hello', 'word')
        assert 'Hello word' == dispatcher.result(msg, timeout=2)
