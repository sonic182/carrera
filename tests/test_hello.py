"""Test say hello."""

import pytest
from carrera import actors


class HelloActor(actors.ThreadActor):

    def on_message(self, msg):
        return 'Hello ' + msg


@pytest.mark.timeout(2)
def test_hello():
    """Test hello."""
    with HelloActor() as actor:
        msgid = actor.send('world')
        assert 'Hello world' == actor.result(msgid)


class PHelloActor(actors.ProcessActor):

    def on_message(self, msg):
        return 'Hello ' + msg


@pytest.mark.timeout(2)
def test_hello_p():
    """Test hello process."""
    with PHelloActor() as actor:
        msgid = actor.send('world')
        assert 'Hello world' == actor.result(msgid)
