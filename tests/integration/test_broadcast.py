"""Test broadcast."""

import pytest
from carrera import actors
from carrera.dispatcher import Dispatcher


# class Counter(actors.ThreadActor):
class Counter(actors.ProcessActor):
    """Sample counter actor."""

    def setup(self):
        """Setup counter."""
        self.counter = 0

    def on_message(self, msg):
        (action, data) = msg
        if action == 'add':
            self.counter += data
        return self.counter


class TestCase():

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test."""
        with Dispatcher() as dispatcher:
            self.dispatcher = dispatcher
            dispatcher.setup(True, True)
            yield

    def test_broadcast(self):
        """Test broadcast same message to actors with same name."""
        with Counter() as counter1:
            with Counter() as counter2:
                with Counter() as counter3:
                    counters = [counter1, counter2, counter3]
                    self.dispatcher.broadcast('counter', ('add', 3))

                    results = []
                    for counter in counters:
                        msg = counter.send(('get', ''))
                        res = counter.result(msg, timeout=3) == 3
                        results.append(res)

                    assert all(results)
