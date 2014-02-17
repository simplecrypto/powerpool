import yaml
import argparse
from gevent import Greenlet
from gevent.event import Event
from gevent.monkey import patch_all
patch_all(thread=False)

from .logging import PrintLogger
from .netmon import monitor_network, monitor_nodes
from .server import StratumServer


def main():
    parser = argparse.ArgumentParser(description='Run powerpool!')
    parser.add_argument('config', type=argparse.FileType('r'),
                        help='yaml configuration file to run with')
    args = parser.parse_args()

    # implement some defaults
    config = dict(stratum={'port': 8123, 'address': '127.0.0.1'},
                  coinserv=[],
                  donate_address='',
                  pool_address='D7QJyeBNuwEqxsyVCLJi3pHs64uPdMDuBa',
                  extranonce_serv_size=8,
                  extranonce_size=4,
                  diff1=0x0000FFFF00000000000000000000000000000000000000000000000000000000)
    add_config = yaml.load(args.config) or {}
    config.update(add_config)
    print(config)

    # stored state of all greenlets. holds events that can be triggered, etc
    client_states = {}
    net_state = {'live_connections': [],
                 'down_connections': [],
                 # current known height of blockchain. used to track if we
                 # need to reset our mining clients
                 'current_height': 0,
                 # a collection of known transaction objects
                 'transactions': {},
                 # event to broadcast a block being found by workers
                 'block_found': Event(),
                 # Store completed block information here for network monitor
                 # to broadcase
                 'complete_block': None,
                 # index of all jobs currently accepting work. Contains complete
                 # block templates
                 'jobs': {},
                 'latest_job': None,
                 'job_counter': 0,
                 # the difficulty that will be transmitted to clients
                 'difficulty': 4}
    # shared data between greenlets, like the block they should be on, etc
    logger = PrintLogger()
    # start the stratum server reactor thread
    sserver = StratumServer(
        (config['stratum']['address'], config['stratum']['port']),
        logger,
        client_states,
        config,
        net_state,
        spawn=10000)
    sserver.start()
    network = Greenlet(monitor_network, logger, client_states,
                       net_state, config)
    nodes = Greenlet(monitor_nodes, config['coinserv'], logger, net_state)
    nodes.start()
    network.start()
    network.join()
