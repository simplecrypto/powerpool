import json
import socket

from time import time
from gevent.queue import Queue
from gevent.pool import Pool
from gevent.server import StreamServer
from gevent import with_timeout

from .server import GenericClient
from .lib import Component, loop
from .exceptions import LoopExit


class AgentServer(Component, StreamServer):
    """ The agent server that pairs with a single port binding of a stratum
    server. Accepts connections from ppagent and reports more details
    statistics. """

    # Don't spawn a greenlet to handle creation of clients, we start one for
    # reading and one for writing in their own class...
    _spawn = None

    def __init__(self, stratum_server):
        self.server = stratum_server
        self.config = stratum_server.config

    def start(self, *args, **kwargs):
        self.logger = self.server.logger
        self.listener = (self.config['address'],
                         self.config['port'] +
                         self.config['agent']['port_diff'] +
                         self.server.manager.config['server_number'])
        StreamServer.__init__(self, self.listener, spawn=Pool())
        self.logger.info("Agent server starting up on {}".format(self.listener))
        StreamServer.start(self, *args, **kwargs)
        Component.start(self)

    def stop(self, *args, **kwargs):
        self.logger.info("Agent server {} stopping".format(self.listener))
        StreamServer.close(self)
        for serv in self.server.agent_clients.values():
            serv.stop()
        Component.stop(self)
        self.logger.info("Exit")

    def handle(self, sock, address):
        self.logger.info("Recieving agent connection from addr {} on sock {}"
                         .format(address, sock))
        self.server.agent_id_count += 1
        client = AgentClient(
            sock=sock,
            address=address,
            id=self.server.agent_id_count,
            server=self.server,
            config=self.config,
            logger=self.logger,
            reporter=self.server.reporter)
        client.start()


class AgentClient(GenericClient):
    """ Object representation of a single ppagent agent connected to the server
    """

    # Our (very stratum like) protocol errors
    errors = {
        20: 'Other/Unknown',
        25: 'Not subscribed',
        30: 'Unkown command',
        31: 'Worker not connected',
        32: 'Already associated',
        33: 'No hello exchanged',
        34: 'Worker not authed',
        35: 'Type not accepted',
        36: 'Invalid format for method',
    }

    def __init__(self, sock, address, id, server, config, logger, reporter):
        self.logger = logger
        self.sock = sock
        self.server = server
        self.config = config
        self.reporter = reporter

        # Seconds before sending keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 120)
        # Interval in seconds between keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 1)
        # Failed keepalive probles before declaring other end dead
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 5)

        self._disconnected = False
        self._authenticated = False
        self._client_state = None
        self._authed = {}
        self._client_version = None
        self._connection_time = time()
        self._id = id

        # where we put all the messages that need to go out
        self.write_queue = Queue()
        self.fp = None
        self._stopped = False

    @property
    def summary(self):
        return dict(workers=self._authed,
                    connection_time=self._connection_time_dt)

    def send_error(self, num=20):
        """ Utility for transmitting an error to the client """
        err = {'result': None, 'error': (num, self.errors[num], None)}
        self.logger.debug("error response: {}".format(err))
        self.write_queue.put(json.dumps(err, separators=(',', ':')) + "\n")

    def send_success(self):
        """ Utility for transmitting success to the client """
        succ = {'result': True, 'error': None}
        self.logger.debug("success response: {}".format(succ))
        self.write_queue.put(json.dumps(succ, separators=(',', ':')) + "\n")

    @loop(fin='stop', exit_exceptions=(socket.error, ))
    def read(self):
        if self._disconnected:
            self.logger.info("Agent client {} write loop exited, exiting read loop"
                             .format(self._id))
            return

        line = with_timeout(self.config['agent']['timeout'],
                            self.fp.readline,
                            timeout_value='timeout')

        # push a new job every timeout seconds if requested
        if line == 'timeout':
            raise LoopExit("Agent client timeout")

        line = line.strip()

        # Reading from a defunct connection yeilds an EOF character which gets
        # stripped off
        if not line:
            raise LoopExit("Closed file descriptor encountered")

        try:
            data = json.loads(line)
        except ValueError:
            self.logger.info("Data {} not JSON".format(line))
            self.send_error()
            return

        self.logger.debug("Data {} recieved on client {}".format(data, self._id))

        if 'method' not in data:
            self.logger.info("Unkown action for command {}".format(data))
            self.send_error()

        meth = data['method'].lower()
        if meth == 'hello':
            if self._client_version is not None:
                self.send_error(32)
                return
            self._client_version = data.get('params', [0.1])[0]
            self.logger.info("Agent {} identified as version {}"
                             .format(self._id, self._client_version))
        elif meth == 'worker.authenticate':
            if self._client_version is None:
                self.send_error(33)
                return
            username = data.get('params', [""])[0]
            user_worker = self.convert_username(username)
            # setup lookup table for easier access from other read sources
            self.client_state = self.server.address_worker_lut.get(user_worker)
            if not self.client_state:
                self.send_error(31)
                return

            # here's where we do some top security checking...
            self._authed[username] = user_worker
            self.send_success()
            self.logger.info("Agent {} authenticated worker {}"
                             .format(self._id, username))
        elif meth == "stats.submit":
            if self._client_version is None:
                self.send_error(33)
                return

            if data.get('params', [''])[0] not in self._authed:
                self.send_error(34)
                return

            if 'params' not in data or len(data['params']) != 4:
                self.send_error(36)
                return

            user_worker, typ, data, stamp = data['params']
            # lookup our authed usernames translated creds
            address, worker = self._authed[user_worker]
            if typ in self.config['agent']['accepted_types']:
                self.reporter.agent_send(address, worker, typ, data, stamp)
                self.send_success()
                self.logger.info("Agent {} transmitted payload for worker "
                                 "{}.{} of type {} and length {}"
                                 .format(self._id, address, worker, typ, len(line)))
            else:
                self.send_error(35)
