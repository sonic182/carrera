"""Start."""
import socket
import threading

from carrera.communications.workers import WorkerNode
from carrera.communications.workers import MasterNode


class TCPServer(object):

    def __init__(self, dispatcher, host, port, backlog=1024):
        self.host = host
        self.port = port
        self.workers = {}
        self.backlog = backlog
        self.dispatcher = dispatcher
        self.logger = dispatcher.logger
        self.exit = False

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
        self.exit = True
        self.sock.close()
        self.logger.info('cleaning_up', extra=self.workers)
        for worker in self.workers:
            self.workers[worker].cleanup()
        self.logger.info('cleaned')

    def acceptor(self):
        while not self.exit:
            try:
                connection, address = self.sock.accept()
            except ConnectionAbortedError:
                if self.exit:
                    return
                continue
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
        self.sock = None
        self.workers = {}


    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (self.host, self.port)
        self.sock.connect(server_address)
        self.dispatcher.logger.debug('connected')
        worker = MasterNode(self, self.sock, server_address)
        worker.synchronize()
        return worker
