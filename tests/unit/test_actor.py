
import pytest

from carrera.dispatcher import Dispatcher
from carrera.actors.base import Actor
from carrera.actors import ThreadActor
from carrera.actors import ProcessActor


class HelloActor(ThreadActor):
    def on_message(self, msg):
        return 'Hello ' + msg


class PHelloActor(ProcessActor):
    def on_message(self, msg):
        return 'Hello ' + msg


class TestCase(object):

    @pytest.fixture(autouse=True)
    def transact(self):
        with Dispatcher() as dispatcher:
            dispatcher.setup(True, True)
            yield

    def test_cover_actor(self):
        """Cover actor default methods"""
        with pytest.raises(NotImplementedError):
            with Actor() as actor:
                actor.result()

    def test_cleanup(self):
        """Cover cleanup force"""
        with ProcessActor() as actor:
            actor.cleanup(True)

    def test_on_message_not_implemented_thread(self):
        """Cover not implemented on message"""
        with pytest.raises(NotImplementedError):
            with ThreadActor() as actor:
                actor.on_message('hello')

    def test_on_message_not_implemented_process(self):
        """Cover not implemented on message"""
        with pytest.raises(NotImplementedError):
            with ProcessActor() as actor:
                actor.on_message('hello')
