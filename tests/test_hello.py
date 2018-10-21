"""Test say hello."""

import pytest
from carrera import actors


class HelloActor(actors.ThreadActor):

    def on_message(self, msg):
        return 'Hello ' + msg


@pytest.mark.timeout(2)
def test_hello():
    """Test hello."""
    actor = HelloActor()
    msgid = actor.send('world')
    assert 'Hello world' == actor.result(msgid, exit=True)


class PHelloActor(actors.ProcessActor):

    def on_message(self, msg):
        return 'Hello ' + msg


@pytest.mark.timeout(2)
def test_hello_p():
    """Test hello process."""
    actor = PHelloActor()
    msgid = actor.send('world')
    assert 'Hello world' == actor.result(msgid, exit=True)
    actor.process.terminate()
