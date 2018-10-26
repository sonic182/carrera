"""Start."""
import socket
import threading

from carrera.workers import WorkerNode
from carrera.workers import MasterNode


class TCPServer(object):

    def __init__(self, dispatcher, host, port, backlog=1024):
        self.host = host
        self.port = port
        self.workers = {}
        self.backlog = backlog
        self.dispatcher = dispatcher
        self.logger = dispatcher.logger

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (self.host, self.port)
        self.sock.bind(server_address)

        # Listen for incoming connections
        self.sock.listen(self.backlog)

        self.acceptor_t = threading.Thread(target=self.acceptor)
        self.acceptor_t.start()
        return self

    def close(self):
        self.sock.close()

    def acceptor(self):
        while True:
            try:
                connection, address = self.sock.accept()
            except ConnectionAbortedError:
                break
            self.logger.debug('new_connection', extra={'host': address[0],
                                                       'port': address[1]})
            worker = WorkerNode(self, connection, address)
            worker.synchronize()


class TCPClient(object):

    def __init__(self, dispatcher, host, port):
        self.dispatcher = dispatcher
        self.host = host
        self.port = port
        self.logger = dispatcher.logger

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (self.host, self.port)
        self.sock.connect(server_address)
        self.dispatcher.logger.debug('connected')
        return MasterNode(self, self.sock, server_address)
