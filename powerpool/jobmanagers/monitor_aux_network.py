import bitcoinrpc
import urllib3
import gevent
import socket
import time
import datetime

from collections import deque
from cryptokit.jobmanaagers import RPCException
from cryptokit.util import pack
from cryptokit.bitcoin import data as bitcoin_data
from gevent import sleep, Greenlet


class MonitorAuxNetwork(Greenlet):
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
        self.prefix = self.config['name'] + "_"
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
