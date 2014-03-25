import json
import socket
import logging

from time import time
from gevent.queue import Queue
from gevent import sleep, with_timeout, spawn

from .server import GenericServer, GenericClient


class AgentServer(GenericServer):

    def __init__(self, listener, stratum_clients, config, agent_clients,
                 server_state, celery, **kwargs):
        super(GenericServer, self).__init__(listener, **kwargs)
        self.stratum_clients = stratum_clients
        self.agent_clients = agent_clients
        self.config = config
        self.server_state = server_state
        self.celery = celery
        self.id_count = 0

    def handle(self, sock, address):
        # Warning: Not thread safe in the slightest, should be good for gevent
        self.id_count += 1
        self.server_state['agent_connects'].incr()
        AgentClient(sock, address, self.id_count, self.stratum_clients,
                    self.config, self.agent_clients, self.server_state, self.celery)


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

    def __init__(self, sock, address, id_count, stratum_clients, config,
                 agent_clients, server_state, celery):
        self.logger.info("Recieving agent connection from addr {} on sock {}"
                         .format(address, sock))

        # Seconds before sending keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 120)
        # Interval in seconds between keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 1)
        # Failed keepalive probles before declaring other end dead
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 5)

        self.sock = sock
        self.fp = sock.makefile()

        # global items
        self.config = config
        self.stratum_clients = stratum_clients
        self.agent_clients = agent_clients
        self.server_state = server_state
        self.celery = celery

        self._disconnected = False
        self.authenticated = False
        self.client_state = None
        self.authed = {}
        self.client_version = None
        self.connection_time = time()
        self.id = id_count

        self.agent_clients[self.id] = self

        # where we put all the messages that need to go out
        self.write_queue = Queue()

        read_greenlet = spawn(self.write_loop)
        try:
            self.read_loop()
        except socket.error:
            pass
        except Exception:
            self.logger.error("Unhandled exception!", exc_info=True)
        finally:
            read_greenlet.kill()
            self.server_state['agent_disconnects'].incr()
            if self.client_state:
                try:
                    del self.agent_clients[self.id]
                except KeyError:
                    pass

        self.logger.info("Closing agent connection for client {}".format(self.id))

    @property
    def summary(self):
        return dict(workers=self.authed, connection_time=self.connection_time_dt)

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

    def read_loop(self):
        # do a finally call to cleanup when we exit
        while True:
            if self._disconnected:
                self.logger.info("Agent client {} write loop exited, exiting read loop"
                                 .format(self.id))
                break

            line = with_timeout(self.config['agent']['timeout'],
                                self.fp.readline,
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

                    self.client_version = data.get('params', [0.1])[0]
                elif meth == 'worker.authenticate':
                    if self.client_version is None:
                        self.send_error(33)
                        continue
                    username = data.get('params', [""])[0]
                    user_worker = self.convert_username(username)
                    # setup lookup table for easier access from other read sources
                    self.client_state = self.stratum_clients['addr_worker_lut'].get(user_worker)
                    if not self.client_state:
                        self.send_error(31)

                    # here's where we do some top security checking...
                    self.authed[username] = user_worker
                    self.send_success()
                elif meth == "stats.submit":
                    if self.client_version is None:
                        self.send_error(33)
                        continue

                    if data.get('params', [''])[0] not in self.authed:
                        self.send_error(34)
                        continue

                    if 'params' not in data or len(data['params']) != 4:
                        self.send_error(36)
                        continue

                    user_worker, typ, data, stamp = data['params']
                    # lookup our authed usernames translated creds
                    address, worker = self.authed[user_worker]
                    if typ in self.config['agent']['accepted_types']:
                        self.celery.send_task_pp(
                            'agent_receive', address, worker, typ, data, stamp)
                        self.send_success()
                    else:
                        self.send_error(35)
            else:
                self.logger.info("Unkown action for command {}"
                                 .format(data))
                self.send_error()

        self.sock.shutdown(socket.SHUT_WR)
        self.sock.close()
