"""Worker module.

Representations of nodes of cluster.

This classes communicates with dispatcher to send/receive commands.
"""

import json
import struct

from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from datetime import datetime


COMMANDS = {
    'get_server_info': b'GET WORKER_INFO',
    'post_server_info': b'POST SERVER_INFO',
    'post_job': b'POST JOB',
    'post_job_result': b'POST JOB_RESULT',
    'get_job': b'GET JOB',
}


class NodeBase:
    """Base node class.

    This class has common methods for nodes communications.
    """

    def __init__(self, server, connection, address):
        """Initalize Node.

        id represents the remote dispatcher.
        """
        self.id = None
        self.connection = connection
        self.address = address
        self.server = server
        self.dispatcher = server.dispatcher
        self.logger = self.dispatcher.logger
        self.messages = Queue()
        self.exit = False
        self.executor = ThreadPoolExecutor()
        self.worker_t = self.executor.submit(self.target)

    def cleanup(self):
        """Cleanup."""
        self.messages.task_done()
        try:
            self.dispatcher.remove_node(self.id)
        except Exception:
            self.logger.exception('remove_node_exception')
            raise
        self.exit = True

    def target(self):
        """Target method Node instance should have for handling commands."""
        raise NotImplementedError()

    def _command(self, command, data=None):
        """Send command via socket.

        First 2 bytes is message size.
        """
        self.logger.debug('sending_command', extra={'command': command})
        info = COMMANDS[command] + b' '
        if data:
            info += data
        self.connection.send(struct.pack('!h', len(info)) + info)

    def _recv_sized(self):
        """Read sized message in socket."""
        size = struct.unpack('!h', self.connection.recv(2))[0]
        res = self.connection.recv(size)
        self.logger.debug('recv_sized', extra={'data': res})
        return res

    def _read(self, category, timeout=None):
        """Read data from messages queue."""
        start = datetime.now()
        self.logger.debug('reading', extra={
            'category': category, 'timeout': timeout})
        while True:
            command = self.messages.get(timeout=timeout)
            current = datetime.now()
            _method, _category, data = command.split(b' ', 2)
            if category.lower().encode() == _category.lower():
                self.logger.debug('read', extra={
                    'category': category, 'data': data})
                return command

            self.messages.put(command)
            if timeout and (current - start).total_seconds() > timeout:
                raise TimeoutError()

    def synchronize(self):
        """Syncronize worker and master."""
        self._command('get_server_info')
        data = self._read('server_info', 1)
        info = json.loads(data.split(b' ', 2)[2].decode())
        self.logger.debug('worker_info', extra={'info': info})
        self.id = info['id']
        self.server.workers[self.id] = self
        self.dispatcher.add_node_actors(info['actors'], self.id)

    def _send_worker_info(self):
        actors = self.dispatcher.actors
        info = {
            'id': self.dispatcher.id,
            'actors': {
                name: {
                    _id: {
                        'node': actors[name][_id]['node']
                    } for _id in ids
                }
                for name, ids in actors.items()
            }
        }
        self._command('post_server_info', json.dumps(info).encode())

    def _send_job_result(self, info, res):
        info['response'] = res
        self._command('post_job_result', json.dumps(info).encode())

    def get_job(self, info, timeout=None):
        """Get job result from actor."""
        self._command('get_job', json.dumps(info).encode())
        res = self._read('job_result', timeout)
        return json.loads(res.split(b' ', 2)[2].decode())

    def post_job(self, info):
        """Post job.

        Post job to be executed in another node.
        """
        self._command('post_job', json.dumps(info).encode())

    def listener(self):
        """Listen to tcp requests."""
        while not self.exit:
            try:
                command = self._recv_sized()
                self.logger.debug('command_received', extra={
                    'command': command})
                _type, category, data = command.split(b' ', 2)
                if _type == b'GET':
                    self.executor.submit(
                        self._get_command, category, data, command)

                elif _type == b'POST':
                    self.executor.submit(
                        self._post_command, category, data, command)

            except KeyboardInterrupt:
                self.connection.close()

            except (struct.error, OSError):
                self.cleanup()
                return

    def _get_command(self, category, data, _command):
        """Get command.

        Handles requested get command.
        """
        if category == b'WORKER_INFO':
            self._send_worker_info()

        elif category == b'JOB':
            info = json.loads(data.decode())
            res = self.dispatcher.result(info)
            self._send_job_result(info, res)

    def _post_command(self, category, data, command):
        """Post command."""
        if category == b'JOB':
            info = json.loads(data.decode())
            info['msg'] = info.pop('message')
            info['actor'] = info.pop('target_name')
            if 'msgid' in info:
                self.dispatcher.send(**info)

        elif category in (b'JOB_RESULT', b'SERVER_INFO'):
            self.messages.put(command)

    def join(self):
        """Join to worker thread.

        This method is usefull for attaching main thread to Node.
        """
        self.worker_t.join()


class WorkerNode(NodeBase):
    """Worker node representation, to be use in master nodes."""

    def target(self):
        """Listen to commands."""
        self.listener()


class MasterNode(NodeBase):
    """Master node representation, to be use in worker nodes."""

    def target(self):
        """Listen to commands."""
        self.listener()
