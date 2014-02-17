import json
import socket

from future.builtins import bytes
from future.utils import viewvalues
from binascii import unhexlify, hexlify
from bitcoinrpc.proxy import AuthServiceProxy, JSONRPCException
from cryptokit.transaction import Transaction, Input, Output
from cryptokit.block import BlockTemplate
from gevent import sleep
from struct import pack
from copy import copy


def monitor_nodes(coinserv, logger, net_state):
    """ Pings rpc interfaces periodically to see if they're up """
    try:
        connections = []
        for serv in coinserv:
            conn = AuthServiceProxy(
                "http://{0}:{1}@{2}:{3}/"
                .format(serv['username'],
                        serv['password'],
                        serv['address'],
                        serv['port']))
            connections.append(conn)
        while True:
            for serv, conn in zip(coinserv, connections):
                try:
                    conn.getinfo()
                except (JSONRPCException, socket.error, ValueError):
                    if conn in net_state['live_connections']:
                        net_state['live_connections'].remove(conn)
                    if conn not in net_state['down_connections']:
                        logger.info("Server at {} now reporting down"
                                    .format(serv['address']), exc_info=True)
                        net_state['down_connections'].append(conn)
                else:
                    if conn not in net_state['live_connections']:
                        net_state['live_connections'].append(conn)
                    if conn in net_state['down_connections']:
                        net_state['down_connections'].remove(conn)
            sleep(2)
    finally:
        net_state = {}


def monitor_network(logger, client_states, net_state, config):
    def found_block(event):
        """ Called when a mining client submits work that solves the network
        target. Will submit the block to the RPC server and push new work to
        all clients if it's successfully accepted. """
        for conn in net_state['live_connections']:
            conn.submitblock(net_state['complete_block'])

    def push_new_block():
        """ Called when a new block was discovered in the longest blockchain.
        This will dump current jobs, create a new job, and then push the
        new job to all mining clients """
        for idx, dct in enumerate(client_states):
            if 'new_block_event' in dct:  # ensure they've inited...
                logger.debug("Signaling new block for client {}".format(idx))
                dct['new_block_event'].set()

    def update_pool(conn):
        # request local memory pool and load it in
        bt = conn.getblocktemplate({'capabilities': [
            'coinbasevalue',
            'coinbase/append',
            'coinbase',
            'generation',
            'time',
            'transactions/remove',
            'prevblock',
        ]})
        dirty = 0   # track a change in the transaction pool
        for trans in bt['transactions']:
            if trans['hash'] not in net_state['transactions']:
                dirty += 1
                new_trans = Transaction(unhexlify(trans['data']), fees=trans['fee'])
                assert trans['hash'] == new_trans.lehexhash
                net_state['transactions'][trans['hash']] = new_trans
        if dirty or len(net_state['jobs']) == 0:
            # here we recalculate the current merkle branch and partial
            # coinbases for passing to the mining clients
            coinbase = Transaction()
            coinbase.version = 2
            # create a coinbase input with encoded height and padding for the
            # extranonces so script length is accurate
            extranonce_length = (config['extranonce_size'] +
                                 config['extranonce_serv_size'])
            coinbase.inputs.append(
                Input.coinbase(bt['height'], b'\0' * extranonce_length))
            # simple output to the proper address and value
            coinbase.outputs.append(
                Output.to_address(bt['coinbasevalue'], config['pool_address']))
            job_id = hexlify(pack(str("I"), net_state['job_counter']))
            bt_obj = BlockTemplate.from_gbt(
                bt, coinbase, extranonce_length,
                copy(net_state['transactions'].values()))
            bt_obj.job_id = job_id
            net_state['job_counter'] += 1
            net_state['jobs'][job_id] = bt_obj
            net_state['latest_job'] = job_id
            logger.debug("Adding {} new transactions to transaction pool, "
                         "created job {}".format(dirty, job_id))

    def check_height(conn):
        # check the block height
        height = conn.getblockcount()
        if net_state['current_height'] != height:
            net_state['current_height'] = height
            return True
        return False

    net_state['block_found'].rawlink(found_block)
    try:
        while True:
            try:
                try:
                    conn = net_state['live_connections'][0]
                except IndexError:
                    logger.info(
                        "Couldn't connect to any RPC servers, sleeping for {}"
                        .format(1))
                    sleep(1)
                    continue

                # if there's a new block registered
                if check_height(conn):
                    # dump the current transaction pool, refresh and push the event
                    logger.debug("New block announced! Wiping previous jobs...")
                    net_state['job_counter'] = 0
                    net_state['transactions'] = {}
                    net_state['jobs'] = {}
                    net_state['latest_job'] = None
                    update_pool(conn)
                    push_new_block()
                else:
                    # update the pool
                    update_pool(conn)
            except Exception:
                logger.error("Unhandled exception!", exc_info=True)
                pass

            sleep(1)

    finally:
        net_state = {}
