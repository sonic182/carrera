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
