import urllib3
import time
import gevent

from gevent.event import Event
from cryptokit.rpc import CoinRPCException, CoinserverRPC

from ..lib import loop, Component
from ..exceptions import RPCException


class Jobmanager(Component):
    pass


class NodeMonitor(gevent.Greenlet):
    maxsize = 10
    rpc_ping_int = 2

    def __init__(self, config, logger, conn_state_change, rpc_ping_int, *args,
                 **kwargs):
        super(NodeMonitor, self).__init__(*args, **kwargs)
        self.__dict__.update(config)
        self._conn_state_change = conn_state_change
        self._conn = CoinserverRPC(
            "http://{0}:{1}@{2}:{3}/"
            .format(self.username,
                    self.password,
                    self.address,
                    self.port),
            pool_kwargs=dict(maxsize=self.maxsize))

        self.name = "{}:{}".format(config['address'], config['port'])

    @loop(setup='_start_monitor_nodes', interval='rpc_ping_int')
    def _run(self):
        t = time.time()
        try:
            self._last_info = self._conn.getinfo()
        except (urllib3.exceptions.HTTPError, CoinRPCException, ValueError):
            self.logger.info("RPC connection {} still down!".format(self.name))
            if not self._down:
                self._down = True
                self._conn_state_change.set()
            return
        self.last_ping = time.time()
        self.last_ping_rtt = self.last_ping - t

    def __getattr__(self):
        return getattr(self._conn)


class NodeMonitorMixin(object):
    def __init__(self):
        self._connections = []  # list of RPC conns that are down
        self._poll_connection = None  # our currently active RPC connection
        # Used to let consumers know when poll_connection is live. Push instead
        # of pull
        self._conn_state_change = Event()
        self._conn_state_change.rawlink(self._pick_best)
        self._connected = Event()

    def _start_monitor_nodes(self):
        for serv in self.config['coinservs']:
            conn = NodeMonitor(
                serv, self.logger, self._conn_state_change, self.rpc_ping_int)
            self._connections.append(conn)

    def _pick_best(self):
        for conn in self._connections:
            if self._poll_connection is not None:
                curr_poll = self._poll_connection.config['poll_priority']
                if conn.config['poll_priority'] > curr_poll:
                    self.logger.info("RPC connection {} has higher poll priority than "
                                     "current poll connection, switching..."
                                     .format(conn.name))
                    self._poll_connection = conn
            else:
                self._connected.set()
                self._poll_connection = conn
                self.logger.info("RPC connection {} defaulting poll connection"
                                 .format(conn.name))

        self._conn_state_change.clear()

    def call_rpc(self, command, *args, **kwargs):
        self._connected.wait()
        try:
            return getattr(self._poll_connection, command)(*args, **kwargs)
        except (urllib3.exceptions.HTTPError, CoinRPCException) as e:
            self.logger.warn("Unable to perform {} on RPC server. Got: {}"
                             .format(command, e))
            self.down_connection(self._poll_connection)
            raise RPCException(e)
