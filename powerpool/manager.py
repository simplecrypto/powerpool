import yaml
import argparse
import collections

from cryptokit.base58 import get_bcaddress_version
from collections import deque
from gevent import Greenlet
from gevent.monkey import patch_all, patch_thread
from gevent.wsgi import WSGIServer
patch_all(thread=False)
# Patch our threading events so we can use thread safe event with gevent
patch_thread(threading=False, _threading_local=False, Event=True)
import threading
import logging
from celery import Celery

from .netmon import monitor_network, monitor_nodes
from .stratum_server import StratumServer
from .stats import stat_rotater
from .monitor import monitor_app


logger = logging.getLogger('manager')


def monitor_runner(net_state, config, stratum_clients, server_state, exit_event):
    logger.info("Monitor server starting up; Thread ID {}"
                .format(threading.current_thread()))
    monitor_app.config.update(config['monitor_config'])
    monitor_app.config.update(dict(net_state=net_state,
                                   config=config,
                                   stratum_clients=stratum_clients,
                                   server_state=server_state))
    wsgiserver = WSGIServer((config['monitor_config']['address'],
                             config['monitor_config']['port']), monitor_app)
    wsgiserver.start()
    try:
        exit_event.wait()
    finally:
        logger.info("Server monitor shutting down...")
        Greenlet.spawn(wsgiserver.stop, timeout=None).join()


def stat_runner(server_state, celery, exit_event):
    logger.info("Stat manager starting up; Thread ID {}"
                .format(threading.current_thread()))
    # a simple greenlet that rotates some of the servers stats
    rotater = Greenlet.spawn(stat_rotater, server_state, celery)
    try:
        exit_event.wait()
    finally:
        logger.info("Stat manager shutting down...")
        rotater.kill()


def net_runner(net_state, config, stratum_clients, server_state, celery,
               exit_event):
    logger.info("Network monitor starting up; Thread ID {}"
                .format(threading.current_thread()))
    network = Greenlet(monitor_network, stratum_clients, net_state, config,
                       server_state, celery)
    nodes = Greenlet(monitor_nodes, config, net_state)
    nodes.start()
    network.start()
    try:
        exit_event.wait()
    finally:
        logger.info("Network monitor thread shutting down...")


def stratum_runner(net_state, config, stratum_clients, server_state, celery,
                   exit_event):
    # start the stratum server reactor thread
    logger.info("Stratum Server starting up; Thread ID {}"
                .format(threading.current_thread()))
    sserver = StratumServer(
        (config['stratum']['address'], config['stratum']['port']),
        stratum_clients,
        config,
        net_state,
        server_state,
        celery,
        spawn=10000)
    sserver.start()
    try:
        exit_event.wait()
    finally:
        logger.info("Stratum Server shutting down...")
        Greenlet.spawn(sserver.stop, timeout=None).join()


def main():
    parser = argparse.ArgumentParser(description='Run powerpool!')
    parser.add_argument('config', type=argparse.FileType('r'),
                        help='yaml configuration file to run with')
    args = parser.parse_args()

    # implement some defaults, these are all explained in the example
    # configuration file
    config = dict(stratum={'port': 3333, 'address': '127.0.0.1'},
                  coinserv=[],
                  extranonce_serv_size=8,
                  extranonce_size=4,
                  diff1=0x0000FFFF00000000000000000000000000000000000000000000000000000000,
                  loggers=[{'type': 'StreamHandler',
                            'level': 'DEBUG'}],
                  start_difficulty=16,
                  term_timeout=3,
                  monitor_config={'DEBUG': True,
                                  'address': '127.0.0.1',
                                  'port': 3855,
                                  'enabled': True},
                  aliases={},
                  stat_window=60,
                  block_poll=0.2,
                  job_generate_int=75,
                  rpc_ping_int=2,
                  keep_share=600,
                  celery={},
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
    logger.debug(config)

    # setup our celery agent
    celery = Celery()
    celery.conf.update(config['celery'])

    # monkey patch the celery object to make sending tasks easy
    def send_task_pp(self, name, *args, **kwargs):
        self.send_task(config['celery_task_prefix'] + '.' + name, args, kwargs)
    Celery.send_task_pp = send_task_pp

    # stored state of all greenlets. holds events that can be triggered, etc
    stratum_clients = {'user_worker_lut': {}}
    net_state = {
        # rpc connections in either state
        'live_connections': [],
        'down_connections': [],
        # current known height of blockchain. used to track if we
        # need to reset our mining clients
        'current_height': 0,
        # a collection of known transaction objects
        'transactions': {},
        # index of all jobs currently accepting work. Contains complete
        # block templates
        'jobs': {},
        'latest_job': None,
        'job_counter': 0,
        'difficulty': -1,
    }

    # holds counters, timers, etc that have to do with overall server state
    server_state = {
        # stat tracking of shares
        'share_ticks': deque([], config['stat_window']),
        'latest_shares': SafeIterator(),
        'block_solve': None,
    }

    exit_event = threading.Event()

    for log_cfg in config['loggers']:
        ch = getattr(logging, log_cfg['type'])()
        log_level = getattr(logging, log_cfg['level'].upper())
        ch.setLevel(log_level)
        fmt = log_cfg.get('format', '%(asctime)s [%(name)s] [%(levelname)s] %(message)s')
        formatter = logging.Formatter(fmt)
        ch.setFormatter(formatter)
        keys = log_cfg.get('listen',
                           ['stats', 'stratum_server', 'netmon', 'manager', 'monitor'])
        for key in keys:
            log = logging.getLogger(key)
            log.addHandler(ch)
            log.setLevel(log_level)

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

    threads = []
    # the thread that monitors the network for new jobs and blocks
    net_thread = threading.Thread(target=net_runner, args=(
        net_state, config, stratum_clients, server_state, celery, exit_event))
    net_thread.daemon = True
    threads.append(net_thread)
    net_thread.start()

    # stratum thread. interacts with clients. sends them jobs and accepts work
    stratum_thread = threading.Thread(target=stratum_runner, args=(
        net_state, config, stratum_clients, server_state, celery, exit_event))
    stratum_thread.daemon = True
    threads.append(stratum_thread)
    stratum_thread.start()

    # task in charge of rotating stats as needed
    stat_thread = threading.Thread(target=stat_runner, args=(
        server_state, celery, exit_event))
    stat_thread.daemon = True
    threads.append(stat_thread)
    stat_thread.start()

    # the monitor server. a simple flask http server that lets you view
    # internal data structures to monitor server health
    if config['monitor_config']['enabled']:
        monitor_thread = threading.Thread(target=monitor_runner, args=(
            net_state, config, stratum_clients, server_state, exit_event))
        monitor_thread.daemon = True
        threads.append(monitor_thread)
        monitor_thread.start()

    try:
        while True:
            for thread in threads:
                thread.join(0.2)
    except KeyboardInterrupt:
        exit_event.set()
        logger.info("Exiting requested via SIGINT, cleaning up...")
        try:
            net_thread.join(config['term_timeout'])
            if net_thread.isAlive() or stratum_thread.isAlive():
                logger.info("Timeout reached, exiting without cleanup")
            else:
                logger.info("Cleanup complete, shutting down...")
        except KeyboardInterrupt:
            logger.info("Shutdown forced by system, exiting without cleanup")


class SafeIterator(object):
    def __init__(self):
        self._val = 0
        self.lock = threading.Lock()

    def incr(self, amount):
        with self.lock:
            self._val += amount
    __add__ = incr

    def reset(self):
        with self.lock:
            curr = self._val
            self._val = 0
            return curr
