import logging
import bitcoinrpc

from future.utils import viewitems
from binascii import unhexlify, hexlify
from cryptokit.transaction import Transaction, Input, Output
from cryptokit.block import BlockTemplate
from cryptokit import bits_to_difficulty
from gevent import sleep
from struct import pack
from copy import copy

logger = logging.getLogger('netmon')


def monitor_nodes(config, net_state):
    """ Pings rpc interfaces periodically to see if they're up and makes the
    initial connection to coinservers. """
    coinserv = config['coinserv']
    for serv in coinserv:
        conn = bitcoinrpc.AuthServiceProxy(
            "http://{0}:{1}@{2}:{3}/"
            .format(serv['username'],
                    serv['password'],
                    serv['address'],
                    serv['port']),
            pool_kwargs=dict(maxsize=serv.get('maxsize', 10)))
        conn.config = serv
        conn.name = "{}:{}".format(serv['address'], serv['port'])
        net_state['down_connections'].append(conn)
    while True:
        remlist = []
        for conn in net_state['down_connections']:
            try:
                conn.getinfo()
            except Exception:
                logger.info("RPC connection {} still down!".format(conn.name))
                continue

            net_state['live_connections'].append(conn)
            remlist.append(conn)
            logger.info("Connected to RPC Server {0}. Yay!".format(conn.name))
            # if this connection has a higher priority than current
            if net_state['poll_connection'] is not None:
                curr_poll = net_state['poll_connection'].config['poll_priority']
                if conn.config['poll_priority'] > curr_poll:
                    logger.info("RPC connection {} has higher poll priority than "
                                "current poll connection, switching...".format(conn.name))
                    net_state['poll_connection'] = conn
            else:
                net_state['poll_connection'] = conn
                logger.info("RPC connection {} defaulting poll connection"
                            .format(conn.name))

        for conn in remlist:
            net_state['down_connections'].remove(conn)

        sleep(config['rpc_ping_int'])


def down_connection(conn, net_state):
    """ Called when a connection goes down. Removes if from the list of live
    connections and recomputes a new. """
    if conn in net_state['live_connections']:
        net_state['live_connections'].remove(conn)

    if net_state['poll_connection'] is conn:
        # find the next best poll connection
        try:
            net_state['poll_connection'] = min(net_state['live_connections'],
                                               key=lambda x: x.config['poll_priority'])
        except ValueError:
            net_state['poll_connection'] = None
            logger.error("No RPC connections available for polling!!!")
        else:
            logger.warn("RPC connection {} switching to poll_connection after {} went down!"
                        .format(net_state['poll_connection'].name, conn.name))

    if conn not in net_state['down_connections']:
        logger.info("Server at {} now reporting down".format(conn.name))
        net_state['down_connections'].append(conn)


def monitor_network(stratum_clients, net_state, config, server_state, celery):
    def push_new_block():
        """ Called when a new block was discovered in the longest blockchain.
        This will dump current jobs, create a new job, and then push the
        new job to all mining clients """
        for idx, client in viewitems(stratum_clients):
            try:
                logger.debug("Signaling new block for client {}".format(idx))
                client.new_block_event.set()
            except AttributeError:
                pass

    def update_pool(conn):
        try:
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
        except Exception:
            logger.warn("Failed to fetch new job, RPC must be down..")
            down_connection(conn, net_state)
            return False
        dirty = 0   # track a change in the transaction pool
        for trans in bt['transactions']:
            if trans['hash'] not in net_state['transactions']:
                dirty += 1
                new_trans = Transaction(unhexlify(trans['data']),
                                        fees=trans['fee'])
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
            fees = 0
            for t in net_state['transactions'].itervalues():
                fees += t.fees
            coinbase.outputs.append(
                Output.to_address(bt['coinbasevalue'] - fees, config['pool_address']))
            job_id = hexlify(pack(str("I"), net_state['job_counter']))
            logger.info("Generating new block template with {} trans"
                        .format(len(net_state['transactions'])))
            bt_obj = BlockTemplate.from_gbt(bt, coinbase, extranonce_length, [])
            bt_obj.job_id = job_id
            bt_obj.block_height = bt['height']
            bt_obj.acc_shares = set()
            net_state['job_counter'] += 1
            net_state['jobs'][job_id] = bt_obj
            net_state['latest_job'] = job_id
            logger.debug("Adding {} new transactions to transaction pool, "
                         "created job {}".format(dirty, job_id))

            return bt_obj

    def check_height(conn):
        # check the block height
        try:
            height = conn.getblockcount()
        except Exception:
            logger.warn("Unable to communicate with server that thinks it's live.")
            down_connection(conn, net_state)
            return False

        if net_state['current_height'] != height:
            net_state['current_height'] = height
            return True
        return False

    i = 0
    while True:
        try:
            conn = net_state['poll_connection']
            if conn is None:
                logger.warn("Couldn't connect to any RPC servers, sleeping for 1")
                sleep(1)
                continue

            # if there's a new block registered
            if check_height(conn):
                # dump the current transaction pool, refresh and push the
                # event
                logger.debug("New block announced! Wiping previous jobs...")
                net_state['transactions'].clear()
                net_state['jobs'].clear()
                net_state['latest_job'] = None
                bt_obj = update_pool(conn)
                if not bt_obj:
                    continue
                push_new_block()
                if bt_obj is None:
                    logger.error("None returned from push_new_block after "
                                 "clearning jobs...")
                else:
                    hex_bits = hexlify(bt_obj.bits)
                    celery.send_task_pp('new_block', bt_obj.block_height, hex_bits, bt_obj.total_value)
                    net_state['difficulty'] = bits_to_difficulty(hex_bits)
            else:
                # check for new transactions when count interval has passed
                if i >= config['job_generate_int']:
                    i = 0
                    update_pool(conn)
                i += 1
        except Exception:
            logger.error("Unhandled exception!", exc_info=True)
            pass

        sleep(config['block_poll'])
