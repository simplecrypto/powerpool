import yaml
import argparse
from gevent import Greenlet
from gevent.monkey import patch_all, patch_thread
patch_all(thread=False)
# Patch our threading events so we can use thread safe event with gevent
patch_thread(threading=False, _threading_local=False, Event=True)
import threading

from .logging import PrintLogger
from .netmon import monitor_network, monitor_nodes
from .server import StratumServer


def network_thread(net_state, config, logger, client_states, exit_event):
    logger.info("Network monitor starting up; Thread ID {}"
                .format(threading.current_thread()))
    network = Greenlet(monitor_network, logger, client_states,
                       net_state, config)
    nodes = Greenlet(monitor_nodes, config['coinserv'], logger, net_state)
    nodes.start()
    network.start()
    try:
        exit_event.wait()
    finally:
        logger.info("Network monitor thread shutting down...")


def server_thread(net_state, config, logger, client_states, exit_event):
    # start the stratum server reactor thread
    logger.info("Stratum Server starting up; Thread ID {}"
                .format(threading.current_thread()))
    sserver = StratumServer(
        (config['stratum']['address'], config['stratum']['port']),
        logger,
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

    # implement some defaults
    config = dict(stratum={'port': 3333, 'address': '127.0.0.1'},
                  coinserv=[],
                  extranonce_serv_size=8,
                  extranonce_size=4,
                  diff1=0x0000FFFF00000000000000000000000000000000000000000000000000000000,
                  start_difficulty=16,
                  term_timeout=3)
    # override those defaults with a loaded yaml config
    add_config = yaml.load(args.config) or {}
    config.update(add_config)
    print(config)

    # stored state of all greenlets. holds events that can be triggered, etc
    client_states = {}
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
        # the difficulty that will be transmitted to clients
        'difficulty': config['start_difficulty']}

    logger = PrintLogger()

    gstate = {'net_state': net_state,
              'client_states': client_states,
              'config': config,
              'logger': logger,
              'exit_event': threading.Event()}

    net_thread = threading.Thread(target=network_thread, kwargs=gstate)
    net_thread.setDaemon(True)
    net_thread.start()

    serv_thread = threading.Thread(target=server_thread, kwargs=gstate)
    serv_thread.setDaemon(True)
    serv_thread.start()

    try:
        while True:
            net_thread.join(0.2)
            serv_thread.join(0.2)
    except KeyboardInterrupt:
        gstate['exit_event'].set()
        logger.info("Exiting requested via SIGINT, shutting down...")
        try:
            net_thread.join(config['term_timeout'])
            if net_thread.isAlive() or serv_thread.isAlive():
                logger.info("Timeout reached, exiting without cleanup")
            else:
                logger.info("Cleanup complete, shutting down...")
        except KeyboardInterrupt:
            logger.info("Shutdown forced by system, exiting without cleanup")
