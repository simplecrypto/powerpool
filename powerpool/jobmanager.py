import bitcoinrpc
import struct
import urllib3
import gevent
import signal
import socket
import time

from future.utils import viewitems
from binascii import unhexlify, hexlify
from collections import deque
from cryptokit import bits_to_difficulty
from cryptokit.transaction import Transaction, Input, Output
from cryptokit.block import BlockTemplate
from cryptokit.util import pack
from cryptokit.bitcoin import data as bitcoin_data
from cryptokit.base58 import get_bcaddress_version
from gevent import sleep, Greenlet, spawn


class MonitorNetwork(Greenlet):
    def _set_config(self, **kwargs):
        # A fast way to set defaults for the kwargs then set them as attributes
        self.config = dict(coinserv=None, extranonce_serv_size=8,
                           extranonce_size=4, diff1=0x0000FFFF00000000000000000000000000000000000000000000000000000000,
                           merged=None, block_poll=0.2, job_generate_int=75,
                           rpc_ping_int=2, pow_func='ltc_scrypt', pool_address=None,
                           donate_address=None)
        self.config.update(kwargs)

        if (not get_bcaddress_version(self.config['pool_address']) or
                not get_bcaddress_version(self.config['donate_address'])):
            self.logger.error("No valid donation/pool address configured! Exiting.")
            exit()

        # check that we have at least one configured coin server
        if not self.config['main_coinservs']:
            self.logger.error("Shit won't work without a coinserver to connect to")
            exit()

    def __init__(self, server, **config):
        Greenlet.__init__(self)
        self._set_config(**config)
        self.logger = server.register_logger('jobmanager')

        # convenient access to global objects
        self.stratum_manager = server.stratum_manager
        self.server = server
        self.reporter = server.reporter

        # Aux network monitors (merged mining)
        self.auxmons = {}

        # internal vars
        self._last_gbt = None
        self._poll_connection = None
        self._down_connections = []
        self._job_counter = 0
        self._last_aux_update = dict()
        self._node_monitor = None

        self.jobs = {}
        self.live_connections = []
        self.latest_job = None
        self.merged_work = {}
        # general current network stats
        self.current_net = dict(difficulty=None,
                                height=None,
                                subsidy=None)
        self.block_stats = dict(accepts=0,
                                rejects=0,
                                solves=0,
                                last_solve_height=None,
                                last_solve_time=None,
                                last_solve_worker=None)
        self.recent_blocks = deque(maxlen=15)

        # start each aux chain monitor for merged mining
        for coin in self.config['merged']:
            if not coin['enabled']:
                self.logger.info("Skipping aux chain support because it's disabled")
                continue

            self.logger.info("Aux network monitor for {} starting up"
                             .format(coin['name']))
            aux_network = MonitorAuxChain(server, self, **coin)
            aux_network.start()
            self.auxmons[coin['name']] = aux_network

        for serv in self.config['main_coinservs']:
            conn = bitcoinrpc.AuthServiceProxy(
                "http://{0}:{1}@{2}:{3}/"
                .format(serv['username'],
                        serv['password'],
                        serv['address'],
                        serv['port']),
                pool_kwargs=dict(maxsize=serv.get('maxsize', 10)))
            conn.config = serv
            conn.name = "{}:{}".format(serv['address'], serv['port'])
            self._down_connections.append(conn)

    def found_merged_block(self, address, worker, hash_hex, header, job_id, coinbase_raw, typ):
        job = self.jobs[job_id]
        self.auxmons[typ].found_block(address, worker, hash_hex, header, coinbase_raw, job)

    def found_block(self, address, worker, hash_hex, header, job_id):
        job = self.jobs[job_id]
        self.block_stats['solves'] += 1
        self.block_stats['last_solves_height'] = job.block_height
        self.block_stats['last_solves_worker'] = "{}.{}".format(address, worker)
        block = hexlify(job.submit_serial(header))

        def submit_block(conn):
            retries = 0
            while retries < 5:
                retries += 1
                res = "failed"
                try:
                    res = conn.getblocktemplate({'mode': 'submit',
                                                 'data': block})
                except (bitcoinrpc.CoinRPCException, socket.error, ValueError) as e:
                    self.logger.info("Block failed to submit to the server {} with submitblock!"
                                     .format(conn.name))
                    if getattr(e, 'error', {}).get('code', 0) != -8:
                        self.logger.error(getattr(e, 'error'), exc_info=True)
                    try:
                        res = conn.submitblock(block)
                    except (bitcoinrpc.CoinRPCException, socket.error, ValueError) as e:
                        self.logger.error("Block failed to submit to the server {}!"
                                          .format(conn.name), exc_info=True)
                        self.logger.error(getattr(e, 'error'))

                if res is None:
                    self.block_stats['accepts'] += 1
                    self.recent_blocks.append(
                        dict(height=job.block_height, timestamp=int(time.time())))
                    self.reporter.add_block(
                        address,
                        job.block_height,
                        job.total_value,
                        job.fee_total,
                        hexlify(job.bits),
                        hash_hex,
                        worker=worker)
                    self.logger.info("NEW BLOCK ACCEPTED by {}!!!"
                                     .format(conn.name))
                    self.block_stats['last_solve_time'] = int(time.time())
                    break  # break retry loop if success
                else:
                    self.logger.error(
                        "Block failed to submit to the server {}, "
                        "server returned {}!".format(conn.name, res),
                        exc_info=True)
                sleep(1)
                self.logger.info("Retry {} for connection {}".format(retries, conn.name))
            else:
                self.block_stats['rejects'] += 1

        tries = 0
        while tries < 200:
            if not self.live_connections:
                tries += 1
                self.logger.error("No live connections to submit new block to!"
                                  " Retry {} / 200.".format(tries))
                sleep(0.1)
                continue

            for conn in self.live_connections:
                # spawn a new greenlet for each submission to do them all async.
                # lower orphan chance
                spawn(submit_block, conn)

            break

        self.logger.log(35, "Valid network block identified!")
        self.logger.info("New block at height {} with hash {} and subsidy {}"
                         .format(self.jobmanager.current_net['height'],
                                 job.total_value, hash_hex))
        self.logger.debug("New block hex dump:\n{}".format(block))
        self.logger.info("Coinbase: {}".format(str(job.coinbase.to_dict())))
        for trans in job.transactions:
            self.logger.debug(str(trans.to_dict()))

    def call_rpc(self, command, *args, **kwargs):
        try:
            getattr(self.coinserv, command)(*args, **kwargs)
        except (urllib3.exceptions.HTTPError, bitcoinrpc.CoinRPCException) as e:
            self.logger.warn("Unable to perform {} on RPC server. Got: {}"
                             .format(command, e))
            self.down_connection(self._poll_connection)
            raise RPCException(e)

    def down_connection(self, conn):
        """ Called when a connection goes down. Removes if from the list of
        live connections and recomputes a new. """
        if conn in self.live_connections:
            self.live_connections.remove(conn)

        if self._poll_connection is conn:
            # find the next best poll connection
            try:
                self._poll_connection = min(self.live_connections,
                                            key=lambda x: x.config['poll_priority'])
            except ValueError:
                self._poll_connection = None
                self.logger.error("No RPC connections available for polling!!!")
            else:
                self.logger.warn("RPC connection {} switching to poll_connection "
                                 "after {} went down!"
                                 .format(self._poll_connection.name, conn.name))

        if conn not in self._down_connections:
            self.logger.info("Server at {} now reporting down".format(conn.name))
            self._down_connections.append(conn)

    def _monitor_nodes(self):
        while True:
            remlist = []
            for conn in self._down_connections:
                try:
                    conn.getinfo()
                except (urllib3.exceptions.HTTPError, bitcoinrpc.CoinRPCException):
                    self.logger.info("RPC connection {} still down!".format(conn.name))
                    continue

                self.live_connections.append(conn)
                remlist.append(conn)
                self.logger.info("Connected to RPC Server {0}. Yay!".format(conn.name))

                # if this connection has a higher priority than current
                if self._poll_connection is not None:
                    curr_poll = self._poll_connection.config['poll_priority']
                    if conn.config['poll_priority'] > curr_poll:
                        self.logger.info("RPC connection {} has higher poll priority than "
                                         "current poll connection, switching..."
                                         .format(conn.name))
                        self._poll_connection = conn
                else:
                    self._poll_connection = conn
                    self.logger.info("RPC connection {} defaulting poll connection"
                                     .format(conn.name))

            for conn in remlist:
                self._down_connections.remove(conn)

            sleep(self.config['rpc_ping_int'])

    def kill(self, *args, **kwargs):
        """ Override our default kill method and kill our child greenlets as
        well """
        self.logger.info("Network monitoring jobmanager shutting down...")
        self._node_monitor.kill(*args, **kwargs)
        # stop all greenlets
        for gl in self.auxmons.itervalues():
            gl.kill(timeout=kwargs.get('timeout'), block=False)
        Greenlet.kill(self, *args, **kwargs)

    def _run(self):
        self.logger.info("Network monitoring jobmanager starting up...")
        # start watching our nodes to see if they're up or not
        self._node_monitor = spawn(self._monitor_nodes)
        i = 0
        while True:
            try:
                if self._poll_connection is None:
                    self.logger.warn("Couldn't connect to any RPC servers, sleeping for 1")
                    sleep(1)
                    continue

                # if there's a new block registered
                if self.check_height():
                    self.logger.info("New block on main network detected")
                    # dump the current transaction pool, refresh and push the
                    # event
                    self.getblocktemplate(new_block=True)
                else:
                    # check for new transactions when count interval has passed
                    if i >= self.config['job_generate_int']:
                        i = 0
                        self.getblocktemplate()
                    i += 1
            except Exception:
                self.logger.error("Unhandled exception!", exc_info=True)
                pass

            sleep(self.config['block_poll'])

    def check_height(self):
        # check the block height
        try:
            height = self._poll_connection.getblockcount()
        except Exception:
            self.logger.warn("Unable to communicate with server that thinks it's live.")
            self.down_connection(self._poll_connection)
            return False

        if self.current_net['height'] != height:
            self.current_net['height'] = height
            return True
        return False

    def getblocktemplate(self, new_block=False, signal=False):
        if signal:
            self.logger.info("Generating new job from signal!")
        dirty = False
        try:
            # request local memory pool and load it in
            bt = self._poll_connection.getblocktemplate(
                {'capabilities': [
                    'coinbasevalue',
                    'coinbase/append',
                    'coinbase',
                    'generation',
                    'time',
                    'transactions/remove',
                    'prevblock',
                ]})
        except Exception as e:
            self.logger.warn("Failed to fetch new job. Reason: {}".format(e))
            self._down_connection(self._poll_connection)
            return False

        # generate a new job if we got some new work!
        if bt != self._last_gbt:
            self._last_gbt = bt
            dirty = True

        if new_block or dirty:
            self.logger.info("Generating new block template with {} trans. Diff {:,.4f}. Subsidy {:,.2f}. Height {:,}."
                             .format(len(self._last_gbt['transactions']),
                                     bits_to_difficulty(self._last_gbt['bits']),
                                     self._last_gbt['coinbasevalue'] / 100000000.0,
                                     self._last_gbt['height']))
            # generate a new job and push it if there's a new block on the
            # network
            self.generate_job(push=new_block, flush=new_block, new_block=new_block)

    def generate_job(self, push=False, flush=False, new_block=False):
        """ Creates a new job for miners to work on. Push will trigger an
        event that sends new work but doesn't force a restart. If flush is
        true a job restart will be triggered. """

        # aux monitors will often call this early when not needed at startup
        if self._last_gbt is None:
            return

        if self.merged_work:
            tree, size = bitcoin_data.make_auxpow_tree(self.merged_work)
            mm_hashes = [self.merged_work.get(tree.get(i), dict(hash=0))['hash']
                         for i in xrange(size)]
            mm_data = '\xfa\xbemm'
            mm_data += bitcoin_data.aux_pow_coinbase_type.pack(dict(
                merkle_root=bitcoin_data.merkle_hash(mm_hashes),
                size=size,
                nonce=0,
            ))
            merged_data = {}
            for aux_work in self.merged_work.itervalues():
                data = dict(target=aux_work['target'],
                            hash=aux_work['hash'],
                            height=aux_work['height'],
                            index=mm_hashes.index(aux_work['hash']),
                            type=aux_work['type'],
                            hashes=mm_hashes)
                merged_data[aux_work['type']] = data
        else:
            merged_data = {}
            mm_data = None

        # here we recalculate the current merkle branch and partial
        # coinbases for passing to the mining clients
        coinbase = Transaction()
        coinbase.version = 2
        # create a coinbase input with encoded height and padding for the
        # extranonces so script length is accurate
        extranonce_length = (self.config['extranonce_size'] +
                             self.config['extranonce_serv_size'])
        coinbase.inputs.append(
            Input.coinbase(self._last_gbt['height'],
                           addtl_push=[mm_data] if mm_data else [],
                           extra_script_sig=b'\0' * extranonce_length))
        # simple output to the proper address and value
        coinbase.outputs.append(
            Output.to_address(self._last_gbt['coinbasevalue'], self.config['pool_address']))
        job_id = hexlify(struct.pack(str("I"), self._job_counter))
        bt_obj = BlockTemplate.from_gbt(self._last_gbt,
                                        coinbase,
                                        extranonce_length,
                                        [Transaction(unhexlify(t['data']), fees=t['fee'])
                                         for t in self._last_gbt['transactions']])
        # add in our merged mining data
        if mm_data:
            hashes = [bitcoin_data.hash256(tx.raw) for tx in bt_obj.transactions]
            bt_obj.merkle_link = bitcoin_data.calculate_merkle_link([None] + hashes, 0)
        bt_obj.merged_data = merged_data
        bt_obj.job_id = job_id
        bt_obj.diff1 = self.config['diff1']
        bt_obj.algo = self.config['algo']
        bt_obj.block_height = self._last_gbt['height']
        bt_obj.acc_shares = set()

        if push:
            if flush:
                self.logger.info("New work announced! Wiping previous jobs...")
                self.jobs.clear()
                self.latest_job = None
            else:
                self.logger.info("New work announced!")

        self._job_counter += 1
        self.jobs[job_id] = bt_obj
        self.latest_job = job_id
        if push:
            for idx, client in viewitems(self.stratum_manager.clients):
                try:
                    if flush:
                        client.new_block_event.set()
                    else:
                        client.new_work_event.set()
                except AttributeError:
                    pass

        if new_block:
            hex_bits = hexlify(bt_obj.bits)
            self.current_net['difficulty'] = bits_to_difficulty(hex_bits)


class RPCException(Exception):
    pass


class MonitorAuxChain(Greenlet):
    def _set_config(self, **config):
        # A fast way to set defaults for the kwargs then set them as attributes
        self.config = dict(enabled=False,
                           name=None,
                           signal=None,
                           coinserv=[],
                           flush=False,
                           send=True)
        self.config.update(config)

        # check that we have at least one configured coin server
        if not self.config['coinservs']:
            self.logger.error("Shit won't work without a coinserver to connect to")
            exit()

        # check that we have at least one configured coin server
        if not self.config['coin_id']:
            self.logger.error("Merge mined coins must have an coin_id")
            exit()

    def __init__(self, server, jobmanager, **config):
        Greenlet.__init__(self)
        self._set_config(**config)
        self.jobmanager = jobmanager
        self.server = server
        self.reporter = server.reporter
        self.logger = server.register_logger('auxmonitor_{}'.format(self.config['name']))
        self.state = {'difficulty': None,
                      'height': None,
                      'chain_id': None,
                      'block_solve': None,
                      'work_restarts': 0,
                      'new_jobs': 0,
                      'solves': 0,
                      'rejects': 0,
                      'accepts': 0,
                      'recent_blocks': deque(maxlen=15)}

        self.coinservs = self.config['coinservs']
        self.coinserv = bitcoinrpc.AuthServiceProxy(
            "http://{0}:{1}@{2}:{3}/"
            .format(self.coinservs[0]['username'],
                    self.coinservs[0]['password'],
                    self.coinservs[0]['address'],
                    self.coinservs[0]['port']),
            pool_kwargs=dict(maxsize=self.coinservs[0].get('maxsize', 10)))
        self.coinserv.config = self.coinservs[0]

        if self.config['signal']:
            gevent.signal(self.config['signal'], self.update, reason="Signal recieved")

    def call_rpc(self, command, *args, **kwargs):
        try:
            getattr(self.coinserv, command)(*args, **kwargs)
        except (urllib3.exceptions.HTTPError, bitcoinrpc.CoinRPCException) as e:
            self.logger.warn("Unable to perform {} on RPC server. Got: {}"
                             .format(command, e))
            raise RPCException(e)

    def found_block(self, address, worker, hash_hex, header, coinbase_raw, job):
        aux_data = job.merged_data[self.config['name']]
        self.state['solves'] += 1
        self.logger.log(36, "New {} Aux Block identified!".format(self.config['name']))
        aux_block = (
            pack.IntType(256, 'big').pack(aux_data['hash']).encode('hex'),
            bitcoin_data.aux_pow_type.pack(dict(
                merkle_tx=dict(
                    tx=bitcoin_data.tx_type.unpack(job.coinbase.raw),
                    block_hash=bitcoin_data.hash256(header),
                    merkle_link=job.merkle_link,
                ),
                merkle_link=bitcoin_data.calculate_merkle_link(aux_data['hashes'],
                                                               aux_data['index']),
                parent_block_header=bitcoin_data.block_header_type.unpack(header),
            )).encode('hex'),
        )

        retries = 0
        while retries < 5:
            retries += 1
            new_height = aux_data['height'] + 1
            try:
                res = self.coinserv.getauxblock(*aux_block)
            except (bitcoinrpc.CoinRPCException, socket.error, ValueError) as e:
                self.logger.error("{} Aux block failed to submit to the server {}!"
                                  .format(self.config['name']), exc_info=True)
                self.logger.error(getattr(e, 'error'))

            if res is True:
                self.logger.info("NEW {} Aux BLOCK ACCEPTED!!!".format(self.config['name']))
                self.state['block_solve'] = int(time.time())
                self.state['accepts'] += 1
                self.state['recent_blocks'].append(
                    dict(height=new_height, timestamp=int(time.time())))
                if self.config['send']:
                    self.logger.info("Submitting {} new block to reporter"
                                     .format(self.config['name']))
                    try:
                        hsh = self.coinserv.getblockhash(new_height)
                    except Exception:
                        self.logger.info("", exc_info=True)
                        hsh = ''
                    try:
                        block = self.coinserv.getblock(hsh)
                    except Exception:
                        self.logger.info("", exc_info=True)
                    try:
                        trans = self.coinserv.gettransaction(block['tx'][0])
                        amount = trans['details'][0]['amount']
                    except Exception:
                        self.logger.info("", exc_info=True)
                        amount = -1
                    self.reporter.add_block(
                        address,
                        new_height,
                        int(amount * 100000000),
                        -1,
                        "%0.6X" % bitcoin_data.FloatingInteger.from_target_upper_bound(aux_data['target']).bits,
                        hsh,
                        merged=self.config['coin_id'],
                        worker=worker)
                break  # break retry loop if success
            else:
                self.logger.error(
                    "{} Aux Block failed to submit to the server, "
                    "server returned {}!".format(self.config['name'], res),
                    exc_info=True)
            sleep(1)
        else:
            self.state['rejects'] += 1

    def update(self, reason=None):
        if reason:
            self.logger.info("Updating {} aux work from a signal recieved!"
                             .format(self.config['name']))

        # cheap hack to prevent a race condition...
        if self.jobmanager._poll_connection is None:
            self.logger.warn("Couldn't connect to any RPC servers, sleeping for 1")
            sleep(1)
            return False

        try:
            auxblock = self.coinserv.getauxblock()
        except RPCException:
            sleep(2)
            return False

        new_merged_work = dict(
            hash=int(auxblock['hash'], 16),
            target=pack.IntType(256).unpack(auxblock['target'].decode('hex')),
            type=self.config['name']
        )
        self.state['chain_id'] = auxblock['chainid']
        if new_merged_work['hash'] != self.jobmanager.merged_work.get(auxblock['chainid'], {'hash': None})['hash']:
            try:
                height = self.coinserv.getblockcount()
            except RPCException:
                sleep(2)
                return False
            self.logger.info("New aux work announced! Diff {:,.4f}. Height {:,}"
                             .format(bitcoin_data.target_to_difficulty(new_merged_work['target']),
                                     height))
            # add height to work spec for future logging
            new_merged_work['height'] = height

            self.jobmanager.merged_work[auxblock['chainid']] = new_merged_work
            self.state['difficulty'] = bitcoin_data.target_to_difficulty(pack.IntType(256).unpack(auxblock['target'].decode('hex')))
            # only push the job if there's a new block height discovered.
            if self.state['height'] != height:
                self.state['height'] = height
                self.jobmanager.generate_job(push=True, flush=self.config['flush'])
                self.state['work_restarts'] += 1
            else:
                self.jobmanager.generate_job()
                self.state['new_jobs'] += 1

    def kill(self, *args, **kwargs):
        """ Override our default kill method and kill our child greenlets as
        well """
        self.logger.info("Auxilury network monitor for {} shutting down..."
                         .format(self.config['name']))
        Greenlet.kill(self, *args, **kwargs)

    def _run(self):
        self.logger.info("Auxilury network monitor for {} starting up..."
                         .format(self.config['name']))
        while True:
            if not self.update():
                continue
            sleep(self.work_interval)
