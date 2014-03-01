from cryptokit.base58 import get_bcaddress_version
import yaml
import argparse

from collections import deque
from gevent import Greenlet
from gevent.monkey import patch_all, patch_thread
from gevent.wsgi import WSGIServer
patch_all(thread=False)
# Patch our threading events so we can use thread safe event with gevent
patch_thread(threading=False, _threading_local=False, Event=True)
import threading
import logging
import simpledoge.tasks as tasks

from .netmon import monitor_network, monitor_nodes
from .server import StratumServer
from .stats import stats_app, stat_rotater


logger = logging.getLogger('manager')


def stats_thread(net_state, config, client_states, exit_event):
    logger.info("Stats server starting up; Thread ID {}"
                .format(threading.current_thread()))
    stats_app.config.update(config['stats_config'])
    stats_app.config.update(dict(net_state=net_state,
                                 config=config,
                                 client_states=client_states))
    wsgiserver = WSGIServer((config['stats_config']['address'],
                             config['stats_config']['port']), stats_app)
    wsgiserver.start()
    rotater = Greenlet.spawn(stat_rotater, net_state)
    try:
        exit_event.wait()
    finally:
        logger.info("Stats server shutting down...")
        rotater.kill()
        Greenlet.spawn(wsgiserver.stop, timeout=None).join()


def network_thread(net_state, config, client_states, exit_event):
    logger.info("Network monitor starting up; Thread ID {}"
                .format(threading.current_thread()))
    network = Greenlet(monitor_network, client_states, net_state, config)
    nodes = Greenlet(monitor_nodes, config, net_state)
    nodes.start()
    network.start()
    try:
        exit_event.wait()
    finally:
        logger.info("Network monitor thread shutting down...")


def server_thread(net_state, config, client_states, exit_event):
    # start the stratum server reactor thread
    logger.info("Stratum Server starting up; Thread ID {}"
                .format(threading.current_thread()))
    sserver = StratumServer(
        (config['stratum']['address'], config['stratum']['port']),
        client_states,
        config,
        net_state,
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
                  stats_config={'DEBUG': True,
                                'address': '127.0.0.1',
                                'port': 3855},
                  aliases={},
                  stat_window=60,
                  block_poll=0.2,
                  job_generate_int=0.2,
                  rpc_ping_int=2,
                  keep_share=600,
                  celery={},
                  push_job_interval=30)
    # override those defaults with a loaded yaml config
    add_config = yaml.load(args.config) or {}
    config.update(add_config)
    logger.debug(config)

    # stored state of all greenlets. holds events that can be triggered, etc
    client_states = {}
    net_state = {
        # rpc connections in either state
        'live_connections': [],
        'down_connections': [],
        # stat tracking of shares
        'share_ticks': deque([], config['stat_window']),
        'latest_shares': SafeIterator(),
        'block_solve': None,
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
        # the difficulty that will be transmitted to clients
        'difficulty': config['start_difficulty']}

    tasks.celery.conf.update(config['celery'])

    gstate = {'net_state': net_state,
              'client_states': client_states,
              'config': config,
              'exit_event': threading.Event()}

    for log_cfg in config['loggers']:
        ch = getattr(logging, log_cfg['type'])()
        log_level = getattr(logging, log_cfg['level'].upper())
        ch.setLevel(log_level)
        fmt = log_cfg.get('format', '%(asctime)s [%(levelname)s] %(message)s')
        formatter = logging.Formatter(fmt)
        ch.setFormatter(formatter)
        keys = log_cfg.get('listen',
                           ['stats', 'stratum_server', 'netmon', 'manager'])
        for key in keys:
            log = logging.getLogger(key)
            log.addHandler(ch)
            log.setLevel(log_level)

    # check that config has a valid address
    if not get_bcaddress_version(config['pool_address']) or not get_bcaddress_version(config['donate_address']):
        logger.error("No valid donation/pool address configured! Exiting.")
        exit()

    net_thread = threading.Thread(target=network_thread, kwargs=gstate)
    net_thread.setDaemon(True)
    net_thread.start()

    serv_thread = threading.Thread(target=server_thread, kwargs=gstate)
    serv_thread.setDaemon(True)
    serv_thread.start()

    stat_thread = threading.Thread(target=stats_thread, kwargs=gstate)
    stat_thread.setDaemon(True)
    stat_thread.start()

    try:
        while True:
            net_thread.join(0.2)
            serv_thread.join(0.2)
            stat_thread.join(0.2)
    except KeyboardInterrupt:
        gstate['exit_event'].set()
        logger.info("Exiting requested via SIGINT, cleaning up...")
        try:
            net_thread.join(config['term_timeout'])
            if net_thread.isAlive() or serv_thread.isAlive():
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
