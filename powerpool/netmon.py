import logging
import bitcoinrpc
import struct

from future.utils import viewitems
from binascii import unhexlify, hexlify
from cryptokit.transaction import Transaction, Input, Output
from cryptokit.block import BlockTemplate
from cryptokit import bits_to_difficulty
from cryptokit.util import pack
from cryptokit.bitcoin import data as bitcoin_data
from gevent import sleep, Greenlet
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


class MonitorNetwork(Greenlet):
    def __init__(self, stratum_clients, net_state, config, server_state, celery):
        Greenlet.__init__(self)
        self.stratum_clients = stratum_clients
        self.net_state = net_state
        self.config = config
        self.server_state = server_state
        self.celery = celery

    def _run(self):
        i = 0
        while True:
            try:
                if self.net_state['poll_connection'] is None:
                    logger.warn("Couldn't connect to any RPC servers, sleeping for 1")
                    sleep(1)
                    continue

                # if there's a new block registered
                if self.check_height():
                    # dump the current transaction pool, refresh and push the
                    # event
                    logger.debug("New block announced! Wiping previous jobs...")
                    self.net_state['transactions'].clear()
                    self.net_state['jobs'].clear()
                    self.net_state['latest_job'] = None
                    bt_obj = self.update_pool()
                    if not bt_obj:
                        continue
                    self.push_new_block()
                    if bt_obj is None:
                        logger.error("None returned from push_new_block after "
                                    "clearning jobs...")
                    else:
                        if self.config['send_new_block']:
                            hex_bits = hexlify(bt_obj.bits)
                            self.celery.send_task_pp('new_block',
                                                     bt_obj.block_height,
                                                     hex_bits,
                                                     bt_obj.total_value)
                        self.net_state['difficulty'] = bits_to_difficulty(hex_bits)
                else:
                    # check for new transactions when count interval has passed
                    if i >= self.config['job_generate_int']:
                        i = 0
                        self.update_pool()
                    i += 1
            except Exception:
                logger.error("Unhandled exception!", exc_info=True)
                pass

            sleep(self.config['block_poll'])

    def check_height(self):
        # check the block height
        try:
            height = self.net_state['poll_connection'].getblockcount()
        except Exception:
            logger.warn("Unable to communicate with server that thinks it's live.")
            down_connection(self.net_state['poll_connection'], self.net_state)
            return False

        if self.net_state['current_height'] != height:
            self.net_state['current_height'] = height
            return True
        return False

    def push_new_block(self):
        """ Called when a new block was discovered in the longest blockchain.
        This will dump current jobs, create a new job, and then push the
        new job to all mining clients """
        for idx, client in viewitems(self.stratum_clients):
            try:
                logger.debug("Signaling new block for client {}".format(idx))
                client.new_block_event.set()
            except AttributeError:
                pass

    def update_pool(self, aux_work=False):
        try:
            # request local memory pool and load it in
            bt = self.net_state['poll_connection'].getblocktemplate({'capabilities': [
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
            down_connection(self.net_state['poll_connection'], self.net_state)
            return False

        dirty = 0   # track a change in the transaction pool
        for trans in bt['transactions']:
            if trans['hash'] not in self.net_state['transactions']:
                dirty += 1
                new_trans = Transaction(unhexlify(trans['data']),
                                        fees=trans['fee'])
                assert trans['hash'] == new_trans.lehexhash
                self.net_state['transactions'][trans['hash']] = new_trans

        if dirty or len(self.net_state['jobs']) == 0 or aux_work:

            merged_work = self.net_state['merged_work']
            mm_data = None
            if self.net_state['merged_work']:
                tree, size = bitcoin_data.make_auxpow_tree(merged_work)
                mm_hashes = [merged_work.get(tree.get(i), dict(hash=0))['hash']
                             for i in xrange(size)]
                mm_data = '\xfa\xbemm'
                mm_data += bitcoin_data.aux_pow_coinbase_type.pack(dict(
                    merkle_root=bitcoin_data.merkle_hash(mm_hashes),
                    size=size,
                    nonce=0,
                ))
                mm_later = [(aux_work, mm_hashes.index(aux_work['hash']), mm_hashes)
                            for chain_id, aux_work in merged_work.iteritems()]
            else:
                mm_later = None

            # here we recalculate the current merkle branch and partial
            # coinbases for passing to the mining clients
            coinbase = Transaction()
            coinbase.version = 2
            # create a coinbase input with encoded height and padding for the
            # extranonces so script length is accurate
            extranonce_length = (self.config['extranonce_size'] +
                                 self.config['extranonce_serv_size'])
            coinbase.inputs.append(
                Input.coinbase(bt['height'],
                               addtl_push=[mm_data] if mm_data else [],
                               extra_script_sig=b'\0' * extranonce_length))
            # simple output to the proper address and value
            fees = 0
            for t in self.net_state['transactions'].itervalues():
                fees += t.fees
            coinbase.outputs.append(
                Output.to_address(bt['coinbasevalue'] - fees, self.config['pool_address']))
            job_id = hexlify(struct.pack(str("I"), self.net_state['job_counter']))
            logger.info("Generating new block template with {} trans. Diff {}"
                        .format(len(self.net_state['transactions']),
                                bits_to_difficulty(bt['bits'])))
            bt_obj = BlockTemplate.from_gbt(bt, coinbase, extranonce_length, [])
            bt_obj.mm_later = copy(mm_later)
            #hashes = [bitcoin_data.hash256(tx.raw) for tx in bt_obj.transactions]
            bt_obj.merkle_link = bitcoin_data.calculate_merkle_link([None], 0)
            bt_obj.job_id = job_id
            bt_obj.block_height = bt['height']
            bt_obj.acc_shares = set()
            self.net_state['job_counter'] += 1
            self.net_state['jobs'][job_id] = bt_obj
            self.net_state['latest_job'] = job_id
            logger.debug("Adding {} new transactions to transaction pool, "
                         "created job {}".format(dirty, job_id))

            return bt_obj

    def new_block(self):
        # dump the current transaction pool, refresh and push the
        # event
        logger.info("New block announced! Wiping previous jobs...")
        self.net_state['transactions'].clear()
        self.net_state['jobs'].clear()
        self.net_state['latest_job'] = None
        bt_obj = self.update_pool()
        if bt_obj is None:
            logger.error("None returned from push_new_block after "
                         "clearning jobs...")
        else:
            self.push_new_block()
            if self.config['send_new_block']:
                hex_bits = hexlify(bt_obj.bits)
                self.celery.send_task_pp('new_block',
                                         bt_obj.block_height,
                                         hex_bits,
                                         bt_obj.total_value)
            self.net_state['difficulty'] = bits_to_difficulty(hex_bits)


class MonitorAuxChain(Greenlet):
    def __init__(self, net_state, config, monitor_network):
        Greenlet.__init__(self)
        self.net_state = net_state
        self.config = config
        self.monitor_network = monitor_network
        self.coinserv = bitcoinrpc.AuthServiceProxy(
            "http://{0}:{1}@{2}:{3}/"
            .format(config['merged']['coinserv'][0]['username'],
                    config['merged']['coinserv'][0]['password'],
                    config['merged']['coinserv'][0]['address'],
                    config['merged']['coinserv'][0]['port']),
            pool_kwargs=dict(maxsize=config['merged']['coinserv'][0].get('maxsize', 10)))

    def _run(self):
        while True:
            if self.net_state['poll_connection'] is None:
                logger.warn("Couldn't connect to any RPC servers, sleeping for 1")
                sleep(1)
                continue
            try:
                auxblock = self.coinserv.getauxblock()
            except Exception:
                logger.warn("Unable to communicate with aux chain server", exc_info=True)
                sleep(2)
                continue
            logger.debug("Aux RPC returned: {}".format(auxblock))
            new_merged_work = {auxblock['chainid']: dict(
                hash=int(auxblock['hash'], 16),
                target=pack.IntType(256).unpack(auxblock['target'].decode('hex')),
                merged_proxy=self.coinserv
            )}
            if new_merged_work != self.net_state['merged_work']:
                logger.info("New aux work annocuned! Diff {}. RPC returned: {}"
                            .format(bitcoin_data.target_to_difficulty(new_merged_work.values()[0]['target']),
                                    new_merged_work))
                self.net_state['merged_work'] = new_merged_work
                logger.info("New aux work announced! Wiping previous jobs...")
                self.net_state['transactions'].clear()
                self.net_state['jobs'].clear()
                self.net_state['latest_job'] = None
                bt_obj = self.monitor_network.update_pool(aux_work=True)
                if bt_obj is None:
                    logger.error("None returned from push_new_block after "
                                 "clearning jobs...")
                else:
                    self.monitor_network.push_new_block()
                    self.net_state['aux_difficulty'] = bitcoin_data.target_to_difficulty(pack.IntType(256).unpack(auxblock['target'].decode('hex')))
            sleep(self.config['merged']['work_interval'])
