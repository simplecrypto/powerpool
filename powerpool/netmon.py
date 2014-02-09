import json
import socket

from cryptokit.transaction import Transaction
from gevent import sleep


def monitor_nodes(conserv, logger, net_state):
    """ Pings rpc interfaces periodically to see if they're up """
    try:

        while True:
            pass

    finally:
        net_state = {}


def monitor_network(logger, client_states, net_state):
    def found_block(event):
        for conn in net_state['live_connections']:
            conn.submitblock(net_state['complete_block'])

    net_state['block_found'].rawlink(found_block)
    try:
        while True:
            try:
                conn = net_state['live_connections'][0]
            except IndexError:
                sleep(1)
                continue

            # check the block height
            height = conn.getblocktemplate()
            if net_state['current_height'] != height:
                net_state['current_height'] = height
                for idx, dct in enumerate(client_states):
                    if 'new_block_event' in dct:  # ensure they've inited...
                        print("Signaling new block for client {}".format(idx))
                        dct['new_block_event'].set()

            # request local memory pool and load it in
            trans = conn.getblocktemplate()
            local_trans = net_state['transactions']
            dirty = 0   # track a change in the transaction pool
            for trans in trans['tx']:
                if trans['hash'] not in local_trans:
                    dirty += 1
                    local_trans[trans['hash']] = Transaction(trans['data'],
                                                             fees=trans['fees'])
            if dirty:
                logger.debug("Adding {} new transactions to transaction pool"
                             .format(dirty))
                # here we should recalculate the current merkle branch
                # and partial coinbases
            sleep(1)

    finally:
        net_state = {}
