import bitcoinrpc
import struct
import urllib3
import gevent
import socket
import time
import datetime

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

from .utils import time_format


class MonitorNetwork(Greenlet):
    one_min_stats = ['work_restarts', 'new_jobs', 'work_pushes']

    def _set_config(self, **kwargs):
        # A fast way to set defaults for the kwargs then set them as attributes
        self.config = dict(coinserv=None,
                           extranonce_serv_size=8,
                           extranonce_size=4,
                           diff1=0x0000FFFF00000000000000000000000000000000000000000000000000000000,
                           hashes_per_share=0xFFFF,
                           merged=None,
                           block_poll=0.2,
                           job_refresh=15,
                           rpc_ping_int=2,
                           pow_block_hash=False,
                           poll=None,
                           prefix='',
                           coin='',
                           pool_address='',
                           signal=None)
        self.config.update(kwargs)

        if not get_bcaddress_version(self.config['pool_address']):
            self.logger.error("No valid pool address configured! Exiting.")
            exit()

        # check that we have at least one configured coin server
        if not self.config['main_coinservs']:
            self.logger.error("Shit won't work without a coinserver to connect to")
            exit()

    def __init__(self, server, **config):
        Greenlet.__init__(self)
        self._set_config(**config)
        self.logger = server.register_logger(self.config['prefix'] + 'jobmanager')

        # convenient access to global objects
        self.stratum_manager = server.stratum_manager
        self.server = server
        self.server.register_stat_counters(self.config['prefix'] + s for s in self.one_min_stats)
        self.reporter = server.reporter

        # Aux network monitors (merged mining)
        self.auxmons = {}

        # internal vars
        self._last_gbt = {}
        self._poll_connection = None  # our currently active RPC connection
        self._down_connections = []  # list of RPC conns that are down
        self._job_counter = 0  # a unique job ID counter

        # internal greenlets
        self._node_monitor = None
        self._height_poller = None

        # Currently active jobs keyed by their unique ID
        self.jobs = {}
        self.live_connections = []  # list of live RPC connections
        self.latest_job = None  # The last job that was generated
        # Latest job generation data from merge mining RPC servers
        self.merged_work = {}

        # general current network stats
        self.current_net = dict(difficulty=None,
                                height=None,
                                prev_hash=None,
                                transactions=None,
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

        if self.config['signal']:
            self.logger.info("Listening for push block notifs on signal {}"
                             .format(self.config['signal']))
            gevent.signal(self.config['signal'], self.getblocktemplate, signal=True)

    @property
    def status(self):
        """ For display in the http monitor """
        dct = dict(net_state=self.current_net,
                   block_stats=self.block_stats,
                   job_count=len(self.jobs))

        for key, mon in self.auxmons.iteritems():
            dct[key] = mon.status

        dct.update({key: self.server[self.config['prefix'] + key].summary()
                    for key in self.one_min_stats})
        return dct

    def found_merged_block(self, address, worker, header, job_id, coinbase_raw, typ):
        """ Proxy method that sends merged blocks to the AuxChainMonitor for
        submission """
        try:
            job = self.jobs[job_id]
        except KeyError:
            self.logger.error("Unable to submit block for job id {}, it "
                              "doesn't exist anymore! Only {}".format(job_id, self.jobs.keys()))
            return
        self.auxmons[typ].found_block(address, worker, hash_hex, header, coinbase_raw, job)

    def found_block(self, raw_coinbase, address, worker, hash_hex, header, job_id, start):
        """ Submit a valid block (hopefully!) to the RPC servers """
        try:
            job = self.jobs[job_id]
        except KeyError:
            self.logger.error("Unable to submit block for job id {}, it "
                              "doesn't exist anymore! only {}".format(job_id, self.jobs.keys()))
            return
        block = hexlify(job.submit_serial(header, raw_coinbase=raw_coinbase))
        recorded = []

        def record_outcome(success):
            if recorded:
                return
            recorded.append(True)
            self.logger.info("Recording block submission outcome {} after {}"
                             .format(success, time.time() - start))

            if success:
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
                    worker=worker,
                    currency=self.config['coin'])
            else:
                self.block_stats['rejects'] += 1

        def submit_block(conn):
            retries = 0
            while retries < 5:
                retries += 1
                res = "failed"
                try:
                    res = conn.submitblock(block)
                except (bitcoinrpc.CoinRPCException, socket.error, ValueError) as e:
                    self.logger.info("Block failed to submit to the server {} with submitblock! {}"
                                     .format(conn.name, e))
                    if getattr(e, 'error', {}).get('code', 0) != -8:
                        self.logger.error(getattr(e, 'error'), exc_info=True)
                    try:
                        res = conn.getblocktemplate({'mode': 'submit', 'data': block})
                    except (bitcoinrpc.CoinRPCException, socket.error, ValueError) as e:
                        self.logger.error("Block failed to submit to the server {}!"
                                          .format(conn.name), exc_info=True)
                        self.logger.error(getattr(e, 'error'))

                if res is None:
                    self.logger.info("NEW BLOCK ACCEPTED by {}!"
                                     .format(conn.name))
                    record_outcome(True)
                    break  # break retry loop if success
                else:
                    self.logger.error(
                        "Block failed to submit to the server {}, "
                        "server returned {}!".format(conn.name, res),
                        exc_info=True)
                sleep(1)
                self.logger.info("Retry {} for connection {}".format(retries, conn.name))

        for tries in xrange(200):
            if not self.live_connections:
                self.logger.error("No live connections to submit new block to!"
                                  " Retry {} / 200.".format(tries))
                sleep(0.1)
                continue

            gl = []
            for conn in self.live_connections:
                # spawn a new greenlet for each submission to do them all async.
                # lower orphan chance
                gl.append(spawn(submit_block, conn))

            gevent.joinall(gl)
            # If none of the submission threads were successfull then record a
            # failure
            if not recorded:
                record_outcome(False)
            break

        self.logger.log(35, "Valid network block identified!")
        self.logger.info("New block at height {} with hash {} and subsidy {}"
                         .format(job.block_height,
                                 hash_hex,
                                 job.total_value))

        self.block_stats['solves'] += 1
        self.block_stats['last_solve_hash'] = hash_hex
        self.block_stats['last_solve_height'] = job.block_height
        self.block_stats['last_solve_worker'] = "{}.{}".format(address, worker)
        self.block_stats['last_solve_time'] = datetime.datetime.utcnow()

        if __debug__:
            self.logger.debug("New block hex dump:\n{}".format(block))
            self.logger.debug("Coinbase: {}".format(str(job.coinbase.to_dict())))
            for trans in job.transactions:
                self.logger.debug(str(trans.to_dict()))

    def call_rpc(self, command, *args, **kwargs):
        try:
            return getattr(self.coinserv, command)(*args, **kwargs)
        except (urllib3.exceptions.HTTPError, bitcoinrpc.CoinRPCException) as e:
            self.logger.warn("Unable to perform {} on RPC server. Got: {}"
                             .format(command, e))
            self.down_connection(self._poll_connection)
            raise RPCException(e)

    def down_connection(self, conn):
        """ Called when a connection goes down. Removes if from the list of
        live connections and recomputes a new. """
        if not conn:
            self.logger.warn("Tried to down a NoneType connection")
            return
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
                except (urllib3.exceptions.HTTPError, bitcoinrpc.CoinRPCException, ValueError):
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
        if self._height_poller:
            self.logger.info("Killing height poller...")
            self._height_poller.kill(*args, **kwargs)
        # stop all greenlets
        for gl in self.auxmons.itervalues():
            gl.kill(timeout=kwargs.get('timeout'), block=False)
        Greenlet.kill(self, *args, **kwargs)

    def _run(self):
        self.logger.info("Network monitoring jobmanager starting up...")
        # start watching our nodes to see if they're up or not
        self._node_monitor = spawn(self._monitor_nodes)
        if (not self.config['signal'] and self.config['poll'] is None) or self.config['poll']:
            self.logger.info("No push block notif signal defined, polling RPC "
                             "server every {} seconds".format(self.config['block_poll']))
            self._height_poller = spawn(self._poll_height)

        while True:
            try:
                if self._poll_connection is None:
                    self.logger.warn("Couldn't connect to any RPC servers, sleeping for 1")
                    sleep(1)
                    continue

                self.getblocktemplate()
            except Exception:
                self.logger.error("Unhandled exception!", exc_info=True)
                pass

            sleep(self.config['job_refresh'])

    def _poll_height(self):
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
            self.down_connection(self._poll_connection)
            return False

        # If this was from a push signal and the
        if signal and self._last_gbt.get('height') != bt['height']:
            self.logger.info("Push block signal notified us of a new block!")
            new_block = True
        elif signal:
            self.logger.info("Push block signal notified us of a block we "
                             "already know about!")

        # generate a new job if we got some new work!
        if bt != self._last_gbt:
            self._last_gbt = bt
            dirty = True

        if new_block or dirty:
            # generate a new job and push it if there's a new block on the
            # network
            self.generate_job(push=new_block, flush=new_block, new_block=new_block)

    def generate_job(self, push=False, flush=False, new_block=False):
        """ Creates a new job for miners to work on. Push will trigger an
        event that sends new work but doesn't force a restart. If flush is
        true a job restart will be triggered. """

        # aux monitors will often call this early when not needed at startup
        if not self._last_gbt:
            self.logger.warn("Cannot generate new job, missing last GBT info")
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

        self.logger.info("Generating new block template with {} trans. "
                         "Diff {:,.4f}. Subsidy {:,.2f}. Height {:,}. "
                         "Merged chains: {}"
                         .format(len(self._last_gbt['transactions']),
                                 bits_to_difficulty(self._last_gbt['bits']),
                                 self._last_gbt['coinbasevalue'] / 100000000.0,
                                 self._last_gbt['height'],
                                 ', '.join(merged_data.keys())))

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

        # Darkcoin payee amount
        if self._last_gbt.get('payee', '') != '':
            payout = self._last_gbt['coinbasevalue'] / 5
            self._last_gbt['coinbasevalue'] -= payout
            coinbase.outputs.append(
                Output.to_address(payout, self._last_gbt['payee']))
            self.logger.info("Paying out masternode at addr {}. Payout {}. Blockval reduced to {}"
                             .format(self._last_gbt['payee'], payout, self._last_gbt['coinbasevalue']))

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
        bt_obj.pow_block_hash = self.config['pow_block_hash']
        bt_obj.block_height = self._last_gbt['height']
        bt_obj.acc_shares = set()

        if push:
            if flush:
                self.server[self.config['prefix'] + 'work_restarts'].incr()
            self.server[self.config['prefix'] + 'work_pushes'].incr()

        self.server[self.config['prefix'] + 'new_jobs'].incr()

        if new_block:
            hex_bits = hexlify(bt_obj.bits)
            self.current_net['difficulty'] = bits_to_difficulty(hex_bits)
            self.current_net['subsidy'] = bt_obj.total_value
            self.current_net['height'] = bt_obj.block_height - 1
            self.current_net['prev_hash'] = bt_obj.hashprev_be_hex
            self.current_net['transactions'] = len(bt_obj.transactions)

        self.new_job(bt_obj, job_id, push, flush, new_block)

    def new_job(self, bt_obj, job_id, push=False, flush=False, new_block=False):
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
            t = time.time()
            bt_obj.stratum_string()
            for idx, client in viewitems(self.stratum_manager.clients):
                try:
                    if client.authenticated:
                        client._push(bt_obj, flush=flush)
                except AttributeError:
                    pass
            self.logger.info("New job enqueued for transmission to {} users in {}"
                             .format(len(self.stratum_manager.clients),
                                     time_format(time.time() - t)))


class RPCException(Exception):
    pass


class MonitorAuxChain(Greenlet):
    one_min_stats = ['work_restarts', 'new_jobs']

    def _set_config(self, **config):
        # A fast way to set defaults for the kwargs then set them as attributes
        self.config = dict(enabled=False,
                           name=None,
                           work_interval=1,
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
        if not self.config['name'] or not self.config['reporting_id']:
            self.logger.error("Merge mined coins must have an reporting_id and name")
            exit()

    def __init__(self, server, jobmanager, **config):
        Greenlet.__init__(self)
        self._set_config(**config)
        self.jobmanager = jobmanager
        self.server = server
        self.reporter = server.reporter
        self.logger = server.register_logger('auxmonitor_{}'.format(self.config['name']))
        self.block_stats = dict(accepts=0,
                                rejects=0,
                                solves=0,
                                last_solve_height=None,
                                last_solve_time=None,
                                last_solve_worker=None)
        self.current_net = dict(difficulty=None, height=None)
        self.recent_blocks = deque(maxlen=15)

        self.prefix = self.config['name'] + "_"
        # create an instance local one_min_stats for use in the def status func
        self.one_min_stats = [self.prefix + key for key in self.one_min_stats]
        self.server.register_stat_counters(self.one_min_stats)

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
            return getattr(self.coinserv, command)(*args, **kwargs)
        except (urllib3.exceptions.HTTPError, bitcoinrpc.CoinRPCException) as e:
            self.logger.warn("Unable to perform {} on RPC server. Got: {}"
                             .format(command, e))
            raise RPCException(e)

    def found_block(self, address, worker, header, coinbase_raw, job):
        aux_data = job.merged_data[self.config['name']]
        self.block_stats['solves'] += 1
        self.logger.info("New {} Aux block at height {}"
                         .format(self.config['name'], aux_data['height']))
        aux_block = (
            pack.IntType(256, 'big').pack(aux_data['hash']).encode('hex'),
            bitcoin_data.aux_pow_type.pack(dict(
                merkle_tx=dict(
                    tx=bitcoin_data.tx_type.unpack(coinbase_raw),
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
            res = False
            try:
                res = self.coinserv.getauxblock(*aux_block)
            except (bitcoinrpc.CoinRPCException, socket.error, ValueError) as e:
                self.logger.error("{} Aux block failed to submit to the server!"
                                  .format(self.config['name']), exc_info=True)
                self.logger.error(getattr(e, 'error'))

            if res is True:
                self.logger.info("NEW {} Aux BLOCK ACCEPTED!!!".format(self.config['name']))
                # Record it for the stats
                self.block_stats['accepts'] += 1
                self.recent_blocks.append(
                    dict(height=new_height, timestamp=int(time.time())))

                # submit it to our reporter if configured to do so
                if self.config['send']:
                    self.logger.info("Submitting {} new block to reporter"
                                     .format(self.config['name']))
                    # A horrible mess that grabs the required information for
                    # reporting the new block. Pretty failsafe so at least
                    # partial information will be reporter regardless
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

                    self.block_stats['last_solve_hash'] = hsh
                    self.reporter.add_block(
                        address,
                        new_height,
                        int(amount * 100000000),
                        -1,
                        "%0.6X" % bitcoin_data.FloatingInteger.from_target_upper_bound(aux_data['target']).bits,
                        hsh,
                        merged=self.config['reporting_id'],
                        worker=worker)

                break  # break retry loop if success
            else:
                self.logger.error(
                    "{} Aux Block failed to submit to the server, "
                    "server returned {}!".format(self.config['name'], res),
                    exc_info=True)
            sleep(1)
        else:
            self.block_stats['rejects'] += 1

        self.block_stats['last_solve_height'] = aux_data['height'] + 1
        self.block_stats['last_solve_worker'] = "{}.{}".format(address, worker)
        self.block_stats['last_solve_time'] = datetime.datetime.utcnow()

    def update(self, reason=None):
        if reason:
            self.logger.info("Updating {} aux work from a signal recieved!"
                             .format(self.config['name']))

        try:
            auxblock = self.call_rpc('getauxblock')
        except RPCException:
            sleep(2)
            return False

        new_merged_work = dict(
            hash=int(auxblock['hash'], 16),
            target=pack.IntType(256).unpack(auxblock['target'].decode('hex')),
            type=self.config['name']
        )
        if new_merged_work['hash'] != self.jobmanager.merged_work.get(auxblock['chainid'], {'hash': None})['hash']:
            # We fetch the block height so we can see if the hash changed
            # because of a new network block, or because new transactions
            try:
                height = self.call_rpc('getblockcount')
            except RPCException:
                sleep(2)
                return False
            self.logger.info("New aux work announced! Diff {:,.4f}. Height {:,}"
                             .format(bitcoin_data.target_to_difficulty(new_merged_work['target']),
                                     height))
            # add height to work spec for future logging
            new_merged_work['height'] = height

            self.jobmanager.merged_work[auxblock['chainid']] = new_merged_work
            self.current_net['difficulty'] = bitcoin_data.target_to_difficulty(pack.IntType(256).unpack(auxblock['target'].decode('hex')))

            # only push the job if there's a new block height discovered.
            if self.current_net['height'] != height:
                self.current_net['height'] = height
                self.jobmanager.generate_job(push=True, flush=self.config['flush'])
                self.server[self.prefix + "work_restarts"].incr()
                self.server[self.prefix + "new_jobs"].incr()
            else:
                self.jobmanager.generate_job()
                self.server[self.prefix + "new_jobs"].incr()

        return True

    @property
    def status(self):
        dct = dict(block_stats=self.block_stats,
                   current_net=self.current_net)
        chop = len(self.prefix)
        dct.update({key[chop:]: self.server[key].summary()
                    for key in self.one_min_stats})
        return dct

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
            # if update returns unable to connect, retry...
            if not self.update():
                continue

            # repeat this on an interval
            sleep(self.config['work_interval'])
