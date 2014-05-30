import yaml
import argparse
import collections
import datetime
import setproctitle
import gevent
import signal

from cryptokit.base58 import get_bcaddress_version
from collections import deque
from gevent import spawn
from gevent.monkey import patch_all
from gevent.event import Event
from gevent.wsgi import WSGIServer
from gevent.pool import Pool
patch_all()
import logging
from celery import Celery
from pprint import pformat

from .netmon import MonitorNetwork, MonitorAuxChain
from .stratum_server import StratumServer
from .agent_server import AgentServer
from .stats import StatMonitor, StatManager
from .monitor import monitor_app


logger = logging.getLogger('manager')


def main():
    parser = argparse.ArgumentParser(description='Run powerpool!')
    parser.add_argument('config', type=argparse.FileType('r'),
                        help='yaml configuration file to run with')
    args = parser.parse_args()

    # implement some defaults, these are all explained in the example
    # configuration file
    config = dict(stratum={'port': 3333, 'address': '0.0.0.0'},
                  procname='powerpool',
                  coinserv=[],
                  extranonce_serv_size=8,
                  extranonce_size=4,
                  diff1=0x0000FFFF00000000000000000000000000000000000000000000000000000000,
                  loggers=[{'type': 'StreamHandler',
                            'level': 'DEBUG'}],
                  start_difficulty=128,
                  term_timeout=3,
                  merged=[{'enabled': False,
                          'work_interval': 1}],
                  monitor={'DEBUG': False,
                           'address': '127.0.0.1',
                           'port': 3855,
                           'enabled': True},
                  agent={'address': '0.0.0.0',
                         'port': 4444,
                         'timeout': 120,
                         'enabled': False,
                         'accepted_types': ['temp', 'status', 'hashrate', 'thresholds']},
                  pow_func='ltc_scrypt',
                  aliases={},
                  block_poll=0.2,
                  job_generate_int=75,
                  rpc_ping_int=2,
                  keep_share=600,
                  send_new_block=True,
                  vardiff={'enabled': False,
                           'historesis': 1.5,
                           'interval': 400,
                           'spm_target': 2.5,
                           'tiers': [8, 16, 32, 64, 96, 128, 192, 256, 512]},
                  celery={'CELERY_DEFAULT_QUEUE': 'celery'},
                  push_job_interval=30,
                  celery_task_prefix=None)
    # override those defaults with a loaded yaml config
    add_config = yaml.load(args.config) or {}

    def update(d, u):
        """ Simple recursive dictionary update """
        for k, v in u.iteritems():
            if isinstance(v, collections.Mapping):
                r = update(d.get(k, {}), v)
                d[k] = r
            else:
                d[k] = u[k]
        return d
    update(config, add_config)

    for log_cfg in config['loggers']:
        ch = getattr(logging, log_cfg['type'])()
        log_level = getattr(logging, log_cfg['level'].upper())
        ch.setLevel(log_level)
        fmt = log_cfg.get('format', '%(asctime)s [%(name)s] [%(levelname)s] %(message)s')
        formatter = logging.Formatter(fmt)
        ch.setFormatter(formatter)
        keys = log_cfg.get('listen', ['stats', 'stratum_server', 'netmon',
                                      'manager', 'monitor', 'agent'])
        for key in keys:
            log = logging.getLogger(key)
            log.addHandler(ch)
            log.setLevel(log_level)

    logger.info("=" * 80)
    logger.info("PowerPool stratum server ({}) starting up..."
                .format(config['procname']))
    logger.debug(pformat(config))

    setproctitle.setproctitle(config['procname'])

    # setup the pow function
    if config['pow_func'] == 'ltc_scrypt':
        from cryptokit.block import scrypt_int
        config['pow_func'] = scrypt_int
    elif config['pow_func'] == 'vert_scrypt':
        from cryptokit.block import vert_scrypt_int
        config['pow_func'] = vert_scrypt_int
    elif config['pow_func'] == 'darkcoin':
        from cryptokit.block import drk_hash_int
        config['pow_func'] = drk_hash_int
    elif config['pow_func'] == 'sha256':
        from cryptokit.block import sha256_int
        config['pow_func'] = sha256_int
    else:
        logger.error("pow_func option not valid!")
        exit()

    # check that config has a valid address
    if (not get_bcaddress_version(config['pool_address']) or
            not get_bcaddress_version(config['donate_address'])):
        logger.error("No valid donation/pool address configured! Exiting.")
        exit()

    # check that we have at least one configured coin server
    if not config['coinserv']:
        logger.error("Shit won't work without a coinserver to connect to")
        exit()

    # check that we have at least one configured coin server
    if not config['celery_task_prefix']:
        logger.error("You need to specify a celery prefix")
        exit()

    server = PowerPool(config)
    server.run()


class StratumClients(dict):
    """ A simple class that wraps and manages some lookup tables for quickly
    finding stratum clients based on address or (address, worker) tuples. Also
    houses data structure holding all client references. """
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self.addr_worker_lut = {}
        self.address_lut = {}

    def set_user(self, client):
        # setup lookup table for easier access from other read sources
        user_worker = (client.address, client.worker)
        self.addr_worker_lut[user_worker] = self
        self.address_lut.setdefault(user_worker[0], [])
        self.address_lut[user_worker[0]].append(self)

    def __delitem__(self, key):
        """ Manages removing the client from the luts on regular delete """
        obj = self[key]
        dict.__delitem__(self, key)
        # clear the address from the luts
        try:
            # remove from lut for address
            self.address_lut[obj.address].remove(self)
            # delete the list if its empty
            if not len(self.address_lut[obj.address]):
                del self.address_lut[obj.address]
        except (ValueError, KeyError):
            pass
        # clear the worker from the luts
        try:
            del self.addr_worker_lut[(obj.address, obj.worker)]
        except KeyError:
            pass


class PowerPool(object):
    def __init__(self, config):
        self.config = config

        # bookkeeping for things to request exit from at exit time
        # A list of all the greenlets that are running
        self.greenlets = []
        # A list of all the StreamServers
        self.servers = []

        # setup our celery agent and monkey patch
        self.celery = Celery()
        self.celery.conf.update(config['celery'])

        # monkey patch the celery object to make sending tasks easy
        def send_task_pp(self, name, *args, **kwargs):
            self.send_task(config['celery_task_prefix'] + '.' + name, args, kwargs)
        Celery.send_task_pp = send_task_pp

        # Primary systems
        self.stratum_clients = StratumClients()
        self.agent_clients = {}
        # The network monitor object
        self.netmon = None
        # Aux network monitors (merged mining)
        self.auxmons = []

        self.stratum_servers = []
        self.agent_servers = []
        self.monitor_server = None

        # Stats tracking for the whole server
        #####
        self.server_start = datetime.datetime.utcnow()
        # shares
        self.shares = StatManager()
        self.reject_low = StatManager()
        self.reject_dup = StatManager()
        self.reject_stale = StatManager()
        # connections
        self.stratum_connects = StatManager()
        self.stratum_disconnects = StatManager()
        self.agent_connects = StatManager()
        self.agent_disconnects = StatManager()

    def run(self):
        # Start the main chain network monitor and aux chain monitors
        logger.info("Network monitor starting up")
        network = MonitorNetwork(self)
        # start each aux chain monitor for merged mining
        for coin in self.config['merged']:
            if not coin['enabled']:
                logger.info("Skipping aux chain support because it's disabled")
                continue

            logger.info("Aux network monitor for {} starting up"
                        .format(coin['name']))
            aux_network = MonitorAuxChain(self, **coin)
            aux_network.start()
            self.greenlets.append(("{} Aux network monitor".format(coin['name']), aux_network))
            self.auxmons.append(aux_network)

        network.start()
        self.greenlets.append(("Main network monitor", network))
        self.netmon = network

        # start the stratum stream server
        ######
        # allow the user to specify the address in two formats for reverse
        # compatibility. One form specifies an address and port at root
        # level of the configuration, while the other way gives a list
        # of dictionaries on the "binds" key
        for cfg in self.config['stratum'].get('binds', [dict(address=self.config['agent']['address'],
                                                        port=self.config['agent']['port'])]):
            logger.info("Stratum server starting up on {address}:{port}"
                        .format(**cfg))
            sserver = StratumServer(
                (self.config['stratum']['address'], self.config['stratum']['port']),
                self,
                spawn=Pool())
            sserver.start()
            self.servers.append(("Stratum server {address}:{port}".format(**cfg), sserver))
            self.stratum_servers.append(sserver)

        logger.info("Stat manager starting up")
        # a simple greenlet that rotates some of the servers stats
        rotater = StatMonitor(self)
        self.greenlets.append(("Stats rotater", rotater))

        # the agent server. allows peers to connect and send stat data about
        # a stratum worker
        if self.config['agent']['enabled']:
            # allow the user to specify the address in two formats for reverse
            # compatibility. One form specifies an address and port at root
            # level of the configuration, while the other way gives a list
            # of dictionaries on the "binds" key
            for cfg in self.config['agent'].get('binds', [dict(address=self.config['agent']['address'],
                                                          port=self.config['agent']['port'])]):
                logger.info("Agent server starting up on {address}:{port}"
                            .format(**cfg))
                sserver = AgentServer((cfg['address'], cfg['port']), self)
                sserver.start()
                self.servers.append(("Agent server {address}:{port}".format(**cfg), sserver))
                self.agent_servers.append(sserver)

        # the monitor server. a simple flask http server that lets you view
        # internal data structures to monitor server health
        if self.config['monitor']['enabled']:
            # Logic for setting up the HTTP monitor
            logger.info("HTTP statistics server starting up")
            monitor_app.config.update(self.config['monitor'])
            monitor_app.config.update(dict(server=self, config=self.config))
            monitor_server = WSGIServer((self.config['monitor']['address'],
                                         self.config['monitor']['port']),
                                        monitor_app)
            monitor_server.start()
            self.servers.append(("HTTP statistics server", monitor_server))
            self.monitor_server = monitor_server

        gevent.signal(signal.SIGINT, self.exit, "SIGINT")
        gevent.signal(signal.SIGHUP, self.exit, "SIGHUP")

        self._exit_signal = Event()
        self._exit_signal.wait()

        # stop all stream servers
        for name, server in self.servers:
            logger.info("Requesting stop for {}".format(name))
            spawn(server.stop, timeout=self.config['term_timeout'])

        # stop all greenlets
        for name, gl in self.greenlets:
            logger.info("Requesting stop for {} greenlet".format(name))
            gl.kill(timeout=self.config['term_timeout'], block=False)

        try:
            if gevent.wait(timeout=self.config['term_timeout']):
                logger.info("All threads exited normally")
            else:
                logger.info("Timeout reached, shutting down forcefully")
        except KeyboardInterrupt:
            logger.info("Shutdown requested again by system, "
                        "exiting without cleanup")

        logger.info("=" * 80)

    def exit(self, signal=None):
        logger.info("*" * 80)
        logger.info("Exiting requested via {}, allowing {} seconds for cleanup."
                    .format(signal, self.config['term_timeout']))
        self._exit_signal.set()
