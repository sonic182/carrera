"""Test say hello."""

import pytest
from configparser import ConfigParser
from carrera import actors
from carrera.dispatcher import Dispatcher


class Counter(actors.ProcessActor):

    def setup(self):
        """Setup counter."""
        self.counter = 0

    def on_message(self, msg):
        self.counter += 1
        return self.counter


class TestCase(object):

    @pytest.fixture(autouse=True)
    def transact(self):
        self.actors = []
        with Dispatcher() as dispatcher:
            config = ConfigParser()
            config.add_section('dispatcher')
            config.set('dispatcher', 'selection', 'round_robin')
            dispatcher.setup(True, True, config)
            yield
            for actor in self.actors:
                actor.cleanup()

    def test_hello(self):
        """Test hello."""
        for i in range(2):
            self.actors.append(Counter())

        dispatcher = Dispatcher()
        msg = dispatcher.send('counter', None)
        assert dispatcher.result(msg, timeout=2) == 1
        msg = dispatcher.send('counter', None)
        assert dispatcher.result(msg, timeout=2) == 1
        msg = dispatcher.send('counter', None)
        assert dispatcher.result(msg, timeout=2) == 2
        msg = dispatcher.send('counter', None)
        assert dispatcher.result(msg, timeout=2) == 2
