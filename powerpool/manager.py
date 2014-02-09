import yaml
import argparse
from gevent import sleep
from gevent.event import Event
from gevent.monkey import patch_all
patch_all(thread=False)

from .logging import PrintLogger
from .server import StratumServer


def main():
    parser = argparse.ArgumentParser(
        description='Run powerpool!')
    parser.add_argument('config', type=argparse.FileType('r'),
                        help='yaml configuration file to run with')
    args = parser.parse_args()

    # implement some defaults
    config = dict(stratum={'port': 8123, 'address': '127.0.0.1'},
                  coinserv=[{'port': 22555, 'address': '127.0.0.1'}],
                  pool_address='',
                  extranonce_size=4)
    add_config = yaml.load(args.config) or {}
    config.update(add_config)

    # stored state of all greenlets. holds events that can be triggered, etc
    client_states = set()
    net_state = {'live_connections': [],
                 'down_connections': [],
                 'current_height': 0,
                 'transactions': {},
                 'block_found': Event()}
    # shared data between greenlets, like the block they should be on, etc
    logger = PrintLogger()
    # start the stratum server reactor thread
    sserver = StratumServer(
        (config['stratum']['address'], config['stratum']['port']),
        logger,
        client_states,
        config,
        spawn=10000)
    sserver.start()
    while True:
        for idx, dct in enumerate(client_states):
            if 'new_block_event' in dct:
                print("Setting for {}".format(idx))
                dct['new_block_event'].set()
        sleep(1)
