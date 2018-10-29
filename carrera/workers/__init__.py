"""Worker class."""

import json
import struct

from threading import Lock


COMMANDS = {
    'get_server_info': b'GET WORKER_INFO',
    'post_server_info': b'POST SERVER_INFO',
    'post_job': b'POST JOB',
    'post_job_result': b'POST JOB_RESULT',
    'get_job': b'GET JOB',
}


class Node(object):

    def __init__(self, server, connection, address):
        self.id = None
        self.connection = connection
        self.address = address
        self.server = server
        self.dispatcher = server.dispatcher
        self.logger = self.dispatcher.logger
        self.lock = Lock()

    def command(self, command, data=None):
        """Send command via socket.

        First 2 bytes is message size.
        """
        self.logger.debug('sending_command', extra={'command': command})
        info = COMMANDS[command] + b' '
        if data:
            info += data
        self.connection.send(struct.pack('!h', len(info)) + info)

    def recv_sized(self):
        """Read sized message in socket."""
        size = struct.unpack('!h', self.connection.recv(2))[0]
        res = self.connection.recv(size)
        self.logger.debug('recv_sized', extra={'data': res})
        return res


class WorkerNode(Node):
    """Worker node representation, to be use in master nodes."""

    def synchronize(self):
        """Syncronize worker and master."""
        with self.lock:
            self.command('get_server_info')
            worker_info = self.recv_sized()
        info = json.loads(worker_info.split(b' ', 2)[2].decode())
        self.logger.debug('worker_info', extra={'info': info})
        self.id = info['id']
        self.server.workers[self.id] = self
        self.dispatcher.add_node_actors(info['actors'], self.id)

    def get_job(self, info):
        with self.lock:
            self.command('get_job', json.dumps(info).encode())
            res = self.recv_sized()
        return json.loads(res.split(b' ', 2)[2].decode())

    def post_job(self, info):
        with self.lock:
            self.command('post_job', json.dumps(info).encode())


class MasterNode(Node):
    """Master node representation, to be use in worker nodes."""

    def join(self):
        """Join to orders."""
        while True:
            try:
                command = self.recv_sized()
                self.logger.debug('command_received', extra={
                    'command': command})
                _type, category, data = command.split(b' ', 2)
                if _type == b'GET':
                    if category == b'WORKER_INFO':
                        self.send_worker_info()

                    elif category == b'JOB':
                        info = json.loads(data.decode())
                        res = self.dispatcher.result(info)
                        self.send_job_result(info, res)

                elif _type == b'POST':
                    if category == b'JOB':
                        info = json.loads(data.decode())
                        info['msg'] = info.pop('message')
                        info['actor'] = info.pop('target_name')
                        if 'msgid' in info:
                            self.dispatcher.send(**info)

            except KeyboardInterrupt:
                self.connection.close()

    def send_worker_info(self):
        info = {
            'id': self.dispatcher.id,
            'actors': {
                name: [_id for _id in ids]
                for name, ids in self.dispatcher.actors.items()
            }
        }
        self.command('post_server_info', json.dumps(info).encode())

    def send_job_result(self, info, res):
        info['response'] = res
        self.command('post_job_result', json.dumps(info).encode())
