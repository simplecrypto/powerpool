import json
import socket
import logging

from time import time
from gevent.queue import Queue
from gevent.pool import Pool
from gevent import sleep, with_timeout, spawn

from .server import GenericServer, GenericClient


class AgentServer(GenericServer):
    """ The agent server that pairs with a stratum server. """
    def _set_config(self, **config):
        self.config = dict(port_diff=1111,
                           accepted_types=['temp', 'status', 'hashrate', 'thresholds'])
        self.config.update(config)
        self.config['address'] = self.stratum_config['address']
        self.config['port'] = self.config['port_diff'] + self.stratum_config['port']

    def __init__(self, server, stratum_config, **config):
        self.stratum_config = stratum_config
        self._set_config(**config)
        self.logger = server.register_logger('stratum_server_{}'.
                                             format(self.config['port']))
        listener = (self.config['address'], self.config['port'])
        super(GenericServer, self).__init__(listener, spawn=Pool())
        self.server = server
        self.id_count = 0

    def start(self, *args, **kwargs):
        self.logger.info("Agent server starting up on {address}:{port}"
                         .format(**self.config))
        GenericServer.start(self, *args, **kwargs)

    def stop(self, *args, **kwargs):
        self.logger.info("Agent server {address}:{port} stopping"
                         .format(**self.config))
        GenericServer.stop(self, *args, **kwargs)

    def handle(self, sock, address):
        self.id_count += 1
        self.server.agent_connects.incr()
        AgentClient(sock, address, self.id_count, self.server)


class AgentClient(GenericClient):
    logger = logging.getLogger('agent')

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

    def __init__(self, sock, address, id_count, server):
        self.logger.info("Recieving agent connection from addr {} on sock {}"
                         .format(address, sock))

        # Seconds before sending keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 120)
        # Interval in seconds between keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 1)
        # Failed keepalive probles before declaring other end dead
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 5)

        self._sock = sock
        self._fp = sock.makefile()

        # convenient access to global state
        self.server = server
        self.config = server.config
        self.stratum_clients = server.stratum_clients
        self.agent_clients = server.agent_clients
        self.celery = server.celery

        self._disconnected = False
        self._authenticated = False
        self._client_state = None
        self._authed = {}
        self._client_version = None
        self._connection_time = time()
        self._id = id_count

        self.agent_clients[self._id] = self

        # where we put all the messages that need to go out
        self._write_queue = Queue()

        try:
            write_greenlet = spawn(self.write_loop)
            self.read_loop()
        except socket.error:
            pass
        except Exception:
            self.logger.error("Unhandled exception!", exc_info=True)
        finally:
            write_greenlet.kill()
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except socket.error:
                pass
            try:
                self.fp.close()
                self.sock.close()
            except socket.error:
                pass
            self.server.agent_disconnects.incr()
            if self.client_state:
                try:
                    del self.agent_clients[self._id]
                except KeyError:
                    pass

            self.logger.info("Closing agent connection for client {}".format(self.id))

    @property
    def summary(self):
        return dict(workers=self._authed, connection_time=self._connection_time_dt)

    def send_error(self, num=20):
        """ Utility for transmitting an error to the client """
        err = {'result': None, 'error': (num, self.errors[num], None)}
        self.logger.debug("error response: {}".format(err))
        self._write_queue.put(json.dumps(err, separators=(',', ':')) + "\n")

    def send_success(self):
        """ Utility for transmitting success to the client """
        succ = {'result': True, 'error': None}
        self.logger.debug("success response: {}".format(succ))
        self._write_queue.put(json.dumps(succ, separators=(',', ':')) + "\n")

    def read_loop(self):
        # do a finally call to cleanup when we exit
        while True:
            if self._disconnected:
                self.logger.info("Agent client {} write loop exited, exiting read loop"
                                 .format(self.id))
                break

            line = with_timeout(self.config['agent']['timeout'],
                                self._fp.readline,
                                timeout_value='timeout')

            # push a new job every timeout seconds if requested
            if line == 'timeout':
                break

            line = line.strip()

            # if there's data to read, parse it as json
            if line:
                try:
                    data = json.loads(line)
                except ValueError:
                    self.logger.info("Data {} not JSON".format(line))
                    self.send_error()
                    continue
            else:
                self.send_error()
                sleep(1)
                continue

            self.logger.debug("Data {} recieved on client {}"
                              .format(data, self.id))

            if 'method' in data:
                meth = data['method'].lower()
                if meth == 'hello':
                    if self.client_version is not None:
                        self.send_error(32)
                        continue
                    self._client_version = data.get('params', [0.1])[0]
                    self.logger.info("Agent {} identified as version {}"
                                     .format(self._id, self._client_version))
                elif meth == 'worker.authenticate':
                    if self._client_version is None:
                        self.send_error(33)
                        continue
                    username = data.get('params', [""])[0]
                    user_worker = self.convert_username(username)
                    # setup lookup table for easier access from other read sources
                    self.client_state = self.stratum_clients['addr_worker_lut'].get(user_worker)
                    if not self.client_state:
                        self.send_error(31)

                    # here's where we do some top security checking...
                    self._authed[username] = user_worker
                    self.send_success()
                    self.logger.info("Agent {} authenticated worker {}"
                                     .format(self.id, username))
                elif meth == "stats.submit":
                    if self._client_version is None:
                        self.send_error(33)
                        continue

                    if data.get('params', [''])[0] not in self._authed:
                        self.send_error(34)
                        continue

                    if 'params' not in data or len(data['params']) != 4:
                        self.send_error(36)
                        continue

                    user_worker, typ, data, stamp = data['params']
                    # lookup our authed usernames translated creds
                    address, worker = self._authed[user_worker]
                    if typ in self.config['agent']['accepted_types']:
                        self.celery.send_task_pp(
                            'agent_receive', address, worker, typ, data, stamp)
                        self.send_success()
                        self.logger.info("Agent {} transmitted payload for worker {}.{} of type {} and length {}"
                                         .format(self.id, address, worker, typ, len(line)))
                    else:
                        self.send_error(35)
            else:
                self.logger.info("Unkown action for command {}"
                                 .format(data))
                self.send_error()
