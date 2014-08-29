import struct
import gevent
import socket
import time
import datetime

from binascii import unhexlify, hexlify
from collections import deque
from cryptokit import bits_to_difficulty
from cryptokit.rpc import CoinRPCException
from cryptokit.transaction import Transaction, Input, Output
from cryptokit.block import BlockTemplate
from cryptokit.bitcoin import data as bitcoin_data
from cryptokit.base58 import get_bcaddress_version
from gevent import sleep, spawn
from gevent.event import Event

from . import NodeMonitorMixin, Jobmanager
from ..lib import loop, REQUIRED
from ..exceptions import ConfigurationError, RPCException


class MonitorNetwork(Jobmanager, NodeMonitorMixin):
    one_min_stats = ['work_restarts', 'new_jobs', 'work_pushes']
    defaults = config = dict(coinservs=REQUIRED,
                             extranonce_serv_size=8,
                             extranonce_size=4,
                             diff1=0x0000FFFF00000000000000000000000000000000000000000000000000000000,
                             hashes_per_share=0xFFFF,
                             merged=tuple(),
                             block_poll=0.2,
                             job_refresh=15,
                             rpc_ping_int=2,
                             pow_block_hash=False,
                             poll=None,
                             currency=REQUIRED,
                             algo=REQUIRED,
                             pool_address='',
                             signal=None)

    def __init__(self, config):
        NodeMonitorMixin.__init__(self)
        self._configure(config)
        if get_bcaddress_version(self.config['pool_address']) is None:
            raise ConfigurationError("No valid pool address configured! Exiting.")

        # Since some MonitorNetwork objs are polling and some aren't....
        self.gl_methods = ['_monitor_nodes', '_check_new_jobs']

        # Aux network monitors (merged mining)
        self.auxmons = []

        # internal vars
        self._last_gbt = {}
        self._job_counter = 0  # a unique job ID counter

        # Currently active jobs keyed by their unique ID
        self.jobs = {}
        self.latest_job = None  # The last job that was generated
        self.new_job = Event()

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

        # Run the looping height poller if we aren't getting push notifications
        if (not self.config['signal'] and self.config['poll'] is None) or self.config['poll']:
            self.gl_methods.append('_poll_height')

    @property
    def status(self):
        """ For display in the http monitor """
        return dict(net_state=self.current_net,
                    block_stats=self.block_stats,
                    job_count=len(self.jobs))

    def start(self):
        Jobmanager.start(self)

        if self.config['signal']:
            self.logger.info("Listening for push block notifs on signal {}"
                             .format(self.config['signal']))
            gevent.signal(self.config['signal'], self.getblocktemplate, signal=True)

        # Find desired auxmonitors
        self.config['merged'] = set(self.config['merged'])
        found_merged = set()

        for mon in self.manager.component_types['Jobmanager']:
            if mon.config.get('currency') in self.config['merged']:
                self.auxmons.append(mon)
                found_merged.add(mon.key)
                mon.new_job.rawlink(self.new_merged_work)

        for monitor in self.config['merged'] - found_merged:
            self.logger.error("Unable to locate Auxmonitor(s) '{}'".format(monitor))

    def found_block(self, raw_coinbase, address, worker, hash_hex, header, job, start):
        """ Submit a valid block (hopefully!) to the RPC servers """
        block = hexlify(job.submit_serial(header, raw_coinbase=raw_coinbase))
        result = {}

        def record_outcome(success):
            if result:
                return
            self.logger.info("Recording block submission outcome {} after {}"
                             .format(success, time.time() - start))

            if success:
                self.block_stats['accepts'] += 1
                self.recent_blocks.append(
                    dict(height=job.block_height, timestamp=int(time.time())))
            else:
                self.block_stats['rejects'] += 1

            result.update(dict(
                address=address,
                height=job.block_height,
                total_subsidy=job.total_value,
                fees=job.fee_total,
                hex_bits=hexlify(job.bits),
                hex_hash=hash_hex,
                worker=worker,
                algo=job.algo,
                merged=False,
                success=success,
                currency=self.config['currency']
            ))

        def submit_block(conn):
            retries = 0
            while retries < 5:
                retries += 1
                res = "failed"
                try:
                    res = conn.submitblock(block)
                except (CoinRPCException, socket.error, ValueError) as e:
                    self.logger.info("Block failed to submit to the server {} with submitblock! {}"
                                     .format(conn.name, e))
                    if getattr(e, 'error', {}).get('code', 0) != -8:
                        self.logger.error(getattr(e, 'error'), exc_info=True)
                    try:
                        res = conn.getblocktemplate({'mode': 'submit', 'data': block})
                    except (CoinRPCException, socket.error, ValueError) as e:
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
            if not self._live_connections:
                self.logger.error("No live connections to submit new block to!"
                                  " Retry {} / 200.".format(tries))
                sleep(0.1)
                continue

            gl = []
            for conn in self._live_connections:
                # spawn a new greenlet for each submission to do them all async.
                # lower orphan chance
                gl.append(spawn(submit_block, conn))

            gevent.joinall(gl)
            # If none of the submission threads were successfull then record a
            # failure
            if not result:
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

        # Pass back all the results to the reporter who's waiting
        return result

    @loop(interval='block_poll')
    def _poll_height(self):
        try:
            height = self.call_rpc('getblockcount')
        except RPCException:
            return

        if self.current_net['height'] != height:
            self.logger.info("New block on main network detected with polling")
            self.current_net['height'] = height
            self.getblocktemplate(new_block=True)

    @loop(interval='job_refresh')
    def _check_new_jobs(self):
        self.getblocktemplate()

    def getblocktemplate(self, new_block=False, signal=False):
        try:
            # request local memory pool and load it in
            bt = self.call_rpc('getblocktemplate',
                               {'capabilities': [
                                   'coinbasevalue',
                                   'coinbase/append',
                                   'coinbase',
                                   'generation',
                                   'time',
                                   'transactions/remove',
                                   'prevblock',
                               ]})
        except RPCException:
            return False

        if self._last_gbt.get('height') != bt['height']:
            new_block = True
        # If this was from a push signal and the
        if signal and new_block:
            self.logger.info("Push block signal notified us of a new block!")
        elif signal:
            self.logger.info("Push block signal notified us of a block we "
                             "already know about!")
            return

        # generate a new job if we got some new work!
        dirty = False
        if bt != self._last_gbt:
            self._last_gbt = bt
            dirty = True

        if new_block or dirty:
            # generate a new job and push it if there's a new block on the
            # network
            self.generate_job(push=new_block, flush=new_block, new_block=new_block)

    def new_merged_work(self, event):
        self.generate_job(push=True, flush=event.flush)

    def generate_job(self, push=False, flush=False, new_block=False):
        """ Creates a new job for miners to work on. Push will trigger an
        event that sends new work but doesn't force a restart. If flush is
        true a job restart will be triggered. """

        # aux monitors will often call this early when not needed at startup
        if not self._last_gbt:
            self.logger.warn("Cannot generate new job, missing last GBT info")
            return

        if self.auxmons:
            merged_work = {}
            auxdata = {}
            for auxmon in self.auxmons:
                if auxmon.last_work['hash'] is None:
                    continue
                merged_work[auxmon.last_work['chainid']] = dict(
                    hash=auxmon.last_work['hash'],
                    target=auxmon.last_work['type']
                )

            tree, size = bitcoin_data.make_auxpow_tree(merged_work)
            mm_hashes = [merged_work.get(tree.get(i), dict(hash=0))['hash']
                         for i in xrange(size)]
            mm_data = '\xfa\xbemm'
            mm_data += bitcoin_data.aux_pow_coinbase_type.pack(dict(
                merkle_root=bitcoin_data.merkle_hash(mm_hashes),
                size=size,
                nonce=0,
            ))

            for auxmon in self.auxmons:
                if auxmon.last_work['hash'] is None:
                    continue
                data = dict(target=auxmon.last_work['target'],
                            hash=auxmon.last_work['hash'],
                            height=auxmon.last_work['height'],
                            found_block=auxmon.found_block,
                            index=mm_hashes.index(auxmon.last_work['hash']),
                            type=auxmon.last_work['type'],
                            hashes=mm_hashes)
                auxdata[auxmon.config['currency']] = data
        else:
            auxdata = {}
            mm_data = None

        self.logger.info("Generating new block template with {} trans. "
                         "Diff {:,.4f}. Subsidy {:,.2f}. Height {:,}. "
                         "Merged chains: {}"
                         .format(len(self._last_gbt['transactions']),
                                 bits_to_difficulty(self._last_gbt['bits']),
                                 self._last_gbt['coinbasevalue'] / 100000000.0,
                                 self._last_gbt['height'],
                                 ', '.join(auxdata.keys())))

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
        bt_obj.merged_data = auxdata
        bt_obj.job_id = job_id
        bt_obj.diff1 = self.config['diff1']
        bt_obj.algo = self.config['algo']
        bt_obj.currency = self.config['currency']
        bt_obj.pow_block_hash = self.config['pow_block_hash']
        bt_obj.block_height = self._last_gbt['height']
        bt_obj.acc_shares = set()
        bt_obj.flush = flush
        bt_obj.found_block = self.found_block

        # Push the fresh job to users after updating details
        self._job_counter += 1
        if flush:
            self.jobs.clear()
        self.jobs[job_id] = bt_obj
        self.latest_job = job_id
        self.new_job.set()
        self.new_job.clear()

        # Stats and notifications now that it's pushed
        if flush:
            self._incr('work_restarts')
            self.logger.info("New main network block announced! Wiping previous jobs and pushing")
        elif push:
            self.logger.info("New aux network block announced, pushing new job!")
            self._incr('work_pushes')

        if new_block:
            hex_bits = hexlify(bt_obj.bits)
            self.current_net['difficulty'] = bits_to_difficulty(hex_bits)
            self.current_net['subsidy'] = bt_obj.total_value
            self.current_net['height'] = bt_obj.block_height - 1
            self.current_net['prev_hash'] = bt_obj.hashprev_be_hex
            self.current_net['transactions'] = len(bt_obj.transactions)
        self._incr('new_jobs')
