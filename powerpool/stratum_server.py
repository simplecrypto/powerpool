import json
import socket
import logging
import argparse
import struct
import random

from time import time
from binascii import hexlify, unhexlify
from bitcoinrpc import CoinRPCException
from cryptokit import target_from_diff
from cryptokit.bitcoin import data as bitcoin_data
from cryptokit.util import pack
from hashlib import sha256
from gevent import sleep, with_timeout, spawn
from gevent.event import Event
from gevent.queue import Queue
from gevent.pool import Pool
from hashlib import sha1
from os import urandom
from pprint import pformat

from .server import GenericServer, GenericClient
from .agent_server import AgentServer
from .utils import recursive_update


class StratumManager(object):
    """ Manages the stratum servers and keeps lookup tables for addresses. """
    def _set_config(self, **config):
        self.config = dict(aliases=[], vardiff=dict(spm_target=20,
                                                    interval=30,
                                                    tiers=[8, 16, 32, 64, 96, 128, 192, 256, 512]),
                           push_job_interval=30,
                           agent=dict(enabled=False))
        self.config.update(config)
        self.logger = logging.getLogger('stratum_manager')

        # setup the pow function
        """
        if config['pow_func'] == 'ltc_scrypt':
            from cryptokit.block import scrypt_int
            config['pow_func'] = scrypt_int
        elif config['pow_func'] == 'vert_scrypt':
            from cryptokit.block import vert_scrypt_int
            config['pow_func'] = vert_scrypt_int
        elif config['pow_func'] == 'darkcoin':
            from cryptokit.block import drk_hash_int
            config['pow_func'] = drk_hash_int
        elif config['pow_func'] == 'sha256':
            from cryptokit.block import sha256_int
            config['pow_func'] = sha256_int
        else:
            self.logger.error("pow_func option not valid!")
            exit()
        """

    def __init__(self, server, **config):
        self.server = server
        self._set_config(**config)

        # A dictionary of all connected clients indexed by id
        self.clients = {}
        # A dictionary of lists of connected clients indexed by address
        self.address_lut = {}
        # A dictionary of lists of connected clients indexed by address and
        # worker tuple
        self.address_worker_lut = {}
        self.stratum_servers = []
        self.agent_servers = []

        self.server = server
        self.config = dict(interfaces=[],
                           vardiff={'enabled': False,
                                    'interval': 400,
                                    'spm_target': 2.5,
                                    'tiers': [8, 16, 32, 64, 96, 128, 192, 256, 512]},
                           agent=dict(enabled=False,
                                      port_diff=1111,
                                      timeout=120,
                                      accepted_types=['temp', 'status', 'hashrate', 'thresholds']),
                           aliases={})
        recursive_update(self.config, config)

        # create a single default stratum server if none are defined
        if not self.config['interfaces']:
            self.config['interfaces'].append({})

        # Start up and bind our servers!
        for cfg in self.config['interfaces']:
            # Start a corresponding agent server
            if self.config['agent']['enabled']:
                serv = AgentServer(server, stratum_config=cfg, **self.config['agent'])
                self.agent_servers.append(serv)
                serv.start()

            serv = StratumServer(server, **cfg)
            self.stratum_servers.append(serv)
            serv.start()

    def set_conn(self, client):
        """ Called when a new connection is recieved by stratum """
        self.clients[client.id] = client

    def set_user(self, client):
        # Add the client (or create) appropriate worker and address trackers
        user_worker = (client.address, client.worker)
        self.addr_worker_lut.setdefault(user_worker, [])
        self.addr_worker_lut[user_worker].append(client)

        self.address_lut.setdefault(user_worker[0], [])
        self.address_lut[user_worker[0]].append(client)

    def remove_client(self, id):
        """ Manages removing the StratumClient from the luts """
        obj = self[id]
        del self.clients[id]
        address, worker = obj.address, obj.worker

        # it won't appear in the luts if these values were never set
        if address is None and worker is None:
            return

        # wipe the client from the address tracker
        if address in self.address_lut:
            # remove from lut for address
            self.address_lut[address].clients.remove(obj)
            # if it's the last client in the object, delete the entry
            if not len(self.address_lut[address].clients):
                self.address_lut[address].report()
                del self.address_lut[address]

        # wipe the client from the address/worker tracker
        key = (address, worker)
        if key in self.addr_worker_lut:
            self.addr_worker_lut[key].clients.remove(obj)
            # if it's the last client in the object, delete the entry
            if not len(self.addr_worker_lut[key].clients):
                del self.addr_worker_lut[key]


class ArgumentParserError(Exception):
    pass


class ThrowingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParserError(message)


password_arg_parser = ThrowingArgumentParser()
password_arg_parser.add_argument('-d', '--diff', type=int)


class StratumServer(GenericServer):

    def _set_config(self, **config):
        self.config = dict(address="127.0.0.1", start_difficulty=128,
                           vardiff=True, port=3333)
        self.config.update(config)

    def __init__(self, server, **config):
        self._set_config(**config)
        listener = (self.config['address'], self.config['port'])
        super(GenericServer, self).__init__(listener, spawn=Pool())
        self.server = server
        self.id_count = 0
        self.logger = logging.getLogger('stratum_server')

    def start(self, *args, **kwargs):
        self.logger.info("Stratum server starting up on {address}:{port}"
                         .format(**self.config))
        GenericServer.start(self, *args, **kwargs)

    def stop(self, *args, **kwargs):
        self.logger.info("Stratum server {address}:{port} stopping"
                         .format(**self.config))
        GenericServer.stop(self, *args, **kwargs)

    def handle(self, sock, address):
        self.id_count += 1
        self.server.stratum_connects.incr()
        StratumClient(sock, address, self.id_count, self.server, start_difficulty=self.start_difficulty)


class StratumClient(GenericClient):
    logger = logging.getLogger('stratum_server')

    errors = {20: 'Other/Unknown',
              21: 'Job not found (=stale)',
              22: 'Duplicate share',
              23: 'Low difficulty share',
              24: 'Unauthorized worker',
              25: 'Not subscribed'}
    STALE_SHARE_ERR = 21
    LOW_DIFF_ERR = 23
    DUP_SHARE_ERR = 22
    STALE_SHARE_ERR = 21

    # constansts for share submission outcomes. returned by the share checker
    BLOCK_FOUND = 0
    VALID_SHARE = 0
    DUP_SHARE = 1
    LOW_DIFF = 2
    STALE_FOUND = 3

    def __init__(self, sock, address, id, server):
        self.logger.info("Recieving stratum connection from addr {} on sock {}"
                         .format(address, sock))

        # Seconds before sending keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 120)
        # Interval in seconds between keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 1)
        # Failed keepalive probles before declaring other end dead
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 5)

        # global items
        self.server = server
        self.config = server.config
        self.jobmanager = server.jobmanager
        self.client_manager = server.client_manager
        self.reporter = server.reporter

        # register client into the client dictionary
        self.sock = sock

        # flags for current connection state
        self._disconnected = False
        self.authenticated = False
        self.subscribed = False
        self.address = None
        self.worker = None
        # the worker id. this is also extranonce 1
        self.id = hexlify(struct.pack('Q', id))
        self.client_manager[self.id] = self
        # subscription id for difficulty on stratum
        self.subscr_difficulty = None
        # subscription id for work notif on stratum
        self.subscr_notify = None

        # all shares keyed by timestamp. will get flushed after a period
        # specified in config
        self.valid_shares = {}
        self.dup_shares = {}
        self.stale_shares = {}
        self.low_diff_shares = {}
        # running total for vardiff
        self.accepted_shares = 0
        # debugging entry
        self.transmitted_shares = 0
        # an index of jobs and their difficulty
        self.job_mapper = {}
        # last time we sent graphing data to the server
        self.time_seed = random.uniform(0, 10)  # a random value to jitter timings by
        self.last_graph_transmit = time() - self.time_seed
        self.last_diff_adj = time() - self.time_seed
        self.difficulty = self.config['start_difficulty']
        # the next diff to be used by push job
        self.next_diff = self.config['start_difficulty']
        self.connection_time = int(time())
        self.msg_id = None

        # trigger to send a new block notice to a user
        self.new_block_event = None
        self.new_block_event = Event()
        self.new_block_event.rawlink(self.new_block_call)
        self.new_work_event = None
        self.new_work_event = Event()
        self.new_work_event.rawlink(self.new_work_call)

        # where we put all the messages that need to go out
        self.write_queue = Queue()
        write_greenlet = None
        self.fp = None

        try:
            self.peer_name = sock.getpeername()
            self.fp = sock.makefile()
            write_greenlet = spawn(self.write_loop)
            self.read_loop()
        except socket.error:
            self.logger.debug("Socket error closing connection", exc_info=True)
        except Exception:
            self.logger.error("Unhandled exception!", exc_info=True)
        finally:
            if write_greenlet:
                write_greenlet.kill()

            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except socket.error:
                pass
            try:
                if self.fp:
                    self.fp.close()
                self.sock.close()
            except (socket.error, AttributeError):
                pass
            self.server.stratum_disconnects.incr()
            del self.client_manager[self.id]

            self.logger.info("Closing connection for client {}".format(self.id))

    @property
    def summary(self):
        return dict(kilo_hr=self.kilo_hashrate,
                    worker=self.worker,
                    address=self.address,
                    transmit_over_accept=(self.transmitted_shares > self.accepted_shares))

    @property
    def kilo_hashrate(self):
        return (sum(self.valid_shares.itervalues()) * (2 ** 16)) / 1000 / 600.0

    @property
    def details(self):
        return dict(dup_shares=sum(self.dup_shares.itervalues()),
                    stale_shares=sum(self.stale_shares.itervalues()),
                    low_diff_shares=sum(self.low_diff_shares.itervalues()),
                    valid_shares=sum(self.valid_shares.itervalues()),
                    alltime_accepted_shares=self.accepted_shares,
                    difficulty=self.difficulty,
                    worker=self.worker,
                    address=self.address,
                    peer_name=self.peer_name[0],
                    connection_time=self.connection_time_dt)

    # watch for new work announcements and push accordingly
    def new_work_call(self, event):
        """ An event triggered by the network monitor when it learns of a
        new work on an aux chain. """
        self.logger.info("Signaling new work for client {}.{}"
                         .format(self.address, self.worker))
        # only push jobs to authed workers...
        if self.authenticated is True:
            self.push_job()

    # watch for new block announcements and push accordingly
    def new_block_call(self, event):
        """ An event triggered by the network monitor when it learns of a
        new block on the network. All old work is now useless so must be
        flushed. """
        self.logger.info("Signaling new block for client {}.{}"
                         .format(self.address, self.worker))
        # only push jobs to authed workers...
        if self.authenticated is True:
            self.push_job(flush=True)

    def send_error(self, num=20, id_val=1):
        """ Utility for transmitting an error to the client """
        err = {'id': id_val,
               'result': None,
               'error': (num, self.errors[num], None)}
        self.logger.warn("Error number {} on ip {}".format(num, self.peer_name[0]))
        #self.logger.debug("error response: {}".format(pformat(err)))
        self.write_queue.put(json.dumps(err, separators=(',', ':')) + "\n")

    def send_success(self, id_val=1):
        """ Utility for transmitting success to the client """
        succ = {'id': id_val, 'result': True, 'error': None}
        self.logger.debug("success response: {}".format(pformat(succ)))
        self.write_queue.put(json.dumps(succ, separators=(',', ':')) + "\n")

    def push_difficulty(self):
        """ Pushes the current difficulty to the client. Currently this
        only happens uppon initial connect, but would be used for vardiff
        """
        send = {'params': [self.difficulty],
                'id': None,
                'method': 'mining.set_difficulty'}
        self.write_queue.put(json.dumps(send, separators=(',', ':')) + "\n")

    def push_job(self, flush=False):
        """ Pushes the latest job down to the client. Flush is whether
        or not he should dump his previous jobs or not. Dump will occur
        when a new block is found since work on the old block is
        invalid."""
        job = None
        while True:
            jobid = self.jobmanager.latest_job
            try:
                job = self.jobmanager.jobs[jobid]
                break
            except KeyError:
                self.logger.warn("No jobs available for worker!")
                sleep(0.5)

        # we push the next difficulty here instead of in the vardiff block to
        # prevent a potential mismatch between client and server
        if self.next_diff != self.difficulty:
            self.logger.info("Pushing diff updae {} -> {} before job for {}.{}"
                             .format(self.difficulty, self.next_diff, self.address, self.worker))
            self.difficulty = self.next_diff
            self.push_difficulty()

        new_job_id = sha1(urandom(4)).hexdigest()
        self.job_mapper[new_job_id] = (self.difficulty, jobid)

        send_params = job.stratum_params() + [flush]
        send_params[0] = new_job_id
        # 0: job_id 1: prevhash 2: coinbase1 3: coinbase2 4: merkle_branch
        # 5: version 6: nbits 7: ntime 8: clean_jobs
        self.logger.info("Sending job id {} to worker {}.{}"
                         .format(jobid, self.address, self.worker))
        self.logger.debug(
            "Worker job details\n\tjob_id: {0}\n\tprevhash: {1}"
            "\n\tcoinbase1: {2}\n\tcoinbase2: {3}\n\tmerkle_branch: {4}"
            "\n\tversion: {5}\n\tnbits: {6} ({bt:064x})\n\tntime: {7}"
            "\n\tclean_jobs: {8}\n"
            .format(*send_params, bt=job.bits_target))
        send = {'params': send_params,
                'id': None,
                'method': 'mining.notify'}
        self.write_queue.put(json.dumps(send, separators=(',', ':')) + "\n")

    def submit_job(self, data):
        """ Handles recieving work submission and checking that it is valid
        , if it meets network diff, etc. Sends reply to stratum client. """
        params = data['params']
        # [worker_name, job_id, extranonce2, ntime, nonce]
        # ["slush.miner1", "bf", "00000001", "504e86ed", "b2957c02"]
        self.logger.debug(
            "Recieved work submit:\n\tworker_name: {0}\n\t"
            "job_id: {1}\n\textranonce2: {2}\n\t"
            "ntime: {3}\n\tnonce: {4} ({int_nonce})"
            .format(
                *params,
                int_nonce=struct.unpack(str("<L"), unhexlify(params[4]))))

        try:
            difficulty, jobid = self.job_mapper[data['params'][1]]
        except KeyError:
            # since we can't identify the diff we just have to assume it's
            # current diff
            self.send_error(self.STALE_SHARE_ERR)
            self.server.reject_stale.incr(self.difficulty)
            return self.STALE_SHARE, self.difficulty

        # lookup the job in the global job dictionary. If it's gone from here
        # then a new block was announced which wiped it
        try:
            job = self.jobmanager.jobs[jobid]
        except KeyError:
            self.send_error(self.STALE_SHARE_ERR)
            self.server.reject_stale.incr(difficulty)
            return self.STALE_SHARE, difficulty

        # assemble a complete block header bytestring
        header = job.block_header(
            nonce=params[4],
            extra1=self.id,
            extra2=params[2],
            ntime=params[3])

        # Check a submitted share against previous shares to eliminate
        # duplicates
        share = (self.id, params[2], params[4], params[3])
        if share in job.acc_shares:
            self.logger.info("Duplicate share rejected from worker {}.{}!"
                             .format(self.address, self.worker))
            self.send_error(self.DUP_SHARE_ERR)
            self.server.reject_dup.incr(difficulty)
            return self.DUP_SHARE, difficulty

        job_target = target_from_diff(difficulty, self.config['diff1'])
        hash_int = self.config['pow_func'](header)
        if hash_int >= job_target:
            self.logger.info("Low diff share rejected from worker {}.{}!"
                             .format(self.address, self.worker))
            self.send_error(self.LOW_DIFF_ERR)
            self.server.reject_low.incr(difficulty)
            return self.LOW_DIFF, difficulty

        # we want to send an ack ASAP, so do it here
        self.send_success(self.msg_id)
        self.logger.debug("Valid share accepted from worker {}.{}!"
                          .format(self.address, self.worker))
        # Add the share to the accepted set to check for dups
        job.acc_shares.add(share)
        self.server.shares.incr(difficulty)

        header_hash = sha256(sha256(header).digest()).digest()[::-1]
        header_hash_int = bitcoin_data.hash256(header)
        hash_hex = hexlify(header_hash)

        def check_merged_block(mm_later):
            aux_work, index, hashes = mm_later
            if hash_int <= aux_work['target']:
                monitor = aux_work['monitor']
                self.server.aux_state[monitor.name]['solves'] += 1
                self.logger.log(36, "New {} Aux Block identified!".format(monitor.name))
                aux_block = (
                    pack.IntType(256, 'big').pack(aux_work['hash']).encode('hex'),
                    bitcoin_data.aux_pow_type.pack(dict(
                        merkle_tx=dict(
                            tx=bitcoin_data.tx_type.unpack(job.coinbase.raw),
                            block_hash=header_hash_int,
                            merkle_link=job.merkle_link,
                        ),
                        merkle_link=bitcoin_data.calculate_merkle_link(hashes, index),
                        parent_block_header=bitcoin_data.block_header_type.unpack(header),
                    )).encode('hex'),
                )

                retries = 0
                while retries < 5:
                    retries += 1
                    new_height = self.server.aux_state[monitor.name]['height'] + 1
                    try:
                        res = aux_work['merged_proxy'].getauxblock(*aux_block)
                    except (CoinRPCException, socket.error, ValueError) as e:
                        self.logger.error("{} Aux block failed to submit to the server {}!"
                                          .format(monitor.name), exc_info=True)
                        self.logger.error(getattr(e, 'error'))

                    if res is True:
                        self.logger.info("NEW {} Aux BLOCK ACCEPTED!!!".format(monitor.name))
                        self.server.aux_state[monitor.name]['block_solve'] = int(time())
                        self.server.aux_state[monitor.name]['accepts'] += 1
                        self.server.aux_state[monitor.name]['recent_blocks'].append(
                            dict(height=new_height, timestamp=int(time())))
                        if monitor.send:
                            self.logger.info("Submitting {} new block to reporter"
                                             .format(monitor.name))
                            try:
                                hsh = aux_work['merged_proxy'].getblockhash(new_height)
                            except Exception:
                                self.logger.info("", exc_info=True)
                                hsh = ''
                            try:
                                block = aux_work['merged_proxy'].getblock(hsh)
                            except Exception:
                                self.logger.info("", exc_info=True)
                            try:
                                trans = aux_work['merged_proxy'].gettransaction(block['tx'][0])
                                amount = trans['details'][0]['amount']
                            except Exception:
                                self.logger.info("", exc_info=True)
                                amount = -1
                            self.reporter.add_block(
                                self.address,
                                new_height,
                                int(amount * 100000000),
                                -1,
                                "%0.6X" % bitcoin_data.FloatingInteger.from_target_upper_bound(aux_work['target']).bits,
                                hsh,
                                merged=monitor.celery_id)
                        break  # break retry loop if success
                    else:
                        self.logger.error(
                            "{} Aux Block failed to submit to the server, "
                            "server returned {}!".format(monitor.name, res),
                            exc_info=True)
                    sleep(1)
                else:
                    self.server.aux_state[monitor.name]['rejects'] += 1

        for mm in job.mm_later:
            spawn(check_merged_block, mm)

        # valid network hash?
        if hash_int > job.bits_target:
            return self.VALID_SHARE, difficulty

        self.jobmanager.block_stats['solves'] += 1
        block = hexlify(job.submit_serial(header))

        def submit_block(conn):

            retries = 0
            while retries < 5:
                retries += 1
                res = "failed"
                try:
                    res = conn.getblocktemplate({'mode': 'submit',
                                                 'data': block})
                except (CoinRPCException, socket.error, ValueError) as e:
                    self.logger.info("Block failed to submit to the server {} with submitblock!"
                                     .format(conn.name))
                    if getattr(e, 'error', {}).get('code', 0) != -8:
                        self.logger.error(getattr(e, 'error'), exc_info=True)
                    try:
                        res = conn.submitblock(block)
                    except (CoinRPCException, socket.error, ValueError) as e:
                        self.logger.error("Block failed to submit to the server {}!"
                                          .format(conn.name), exc_info=True)
                        self.logger.error(getattr(e, 'error'))

                if res is None:
                    self.jobmanager.block_stats['accepts'] += 1
                    self.jobmanager.recent_blocks.append(
                        dict(height=job.block_height, timestamp=int(time())))
                    self.reporter.add_block(
                        self.address,
                        job.block_height,
                        job.total_value,
                        job.fee_total,
                        hexlify(job.bits),
                        hash_hex)
                    self.logger.info("NEW BLOCK ACCEPTED by {}!!!"
                                     .format(conn.name))
                    self.jobmanager.block_solve = int(time())
                    break  # break retry loop if success
                else:
                    self.logger.error(
                        "Block failed to submit to the server {}, "
                        "server returned {}!".format(conn.name, res),
                        exc_info=True)
                sleep(1)
                self.logger.info("Retry {} for connection {}".format(retries, conn.name))
            else:
                self.jobmanager.block_stats['rejects'] += 1

        tries = 0
        while tries < 200:
            if not self.jobmanager.live_connections:
                tries += 1
                self.logger.error("No live connections to submit new block to!"
                                  " Retry {} / 200.".format(tries))
                sleep(0.1)
                continue

            for conn in self.jobmanager.live_connections:
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

        return self.BLOCK_FOUND, difficulty

    def authenticate(self, data):
        try:
            password = data.get('params', [None])[1]
        except IndexError:
            password = ""

        # allow the user to use the password field as an argument field
        try:
            args = password_arg_parser.parse_args(password.split())
        except ArgumentParserError:
            pass
        else:
            if args.diff and args.diff in self.config['vardiff']['tiers']:
                self.difficulty = args.diff
                self.next_diff = args.diff

        username = data.get('params', [None])[0]
        self.logger.info("Authentication request from {} for username {}"
                         .format(self.peer_name[0], username))
        user_worker = self.convert_username(username)
        # unpack into state dictionary
        self.address, self.worker = user_worker
        self.client_manager.set_user(self)
        self.authenticated = True
        self.send_success(self.msg_id)
        self.push_difficulty()
        self.push_job()

    def recalc_vardiff(self):
        # ideal difficulty is the n1 shares they solved divided by target
        # shares per minute
        spm_tar = self.config['vardiff']['spm_target']
        ideal_diff = self.client_manager.address_lut[self.address].spm / spm_tar
        self.logger.info("VARDIFF: Calculated client {} ideal diff {}"
                         .format(self.id, ideal_diff))
        # find the closest tier for them
        new_diff = min(self.config['vardiff']['tiers'], key=lambda x: abs(x - ideal_diff))

        if new_diff != self.difficulty:
            self.logger.info(
                "VARDIFF: Moving to D{} from D{}".format(new_diff, self.difficulty))
            self.next_diff = new_diff
        else:
            self.logger.debug("VARDIFF: Not adjusting difficulty, already "
                              "close enough")

        self.last_diff_adj = time()

    def subscribe(self, data):
        self.subscr_notify = sha1(urandom(4)).hexdigest()
        self.subscr_difficulty = sha1(urandom(4)).hexdigest()
        ret = {'result':
               ((("mining.set_difficulty",
                  self.subscr_difficulty),
                 ("mining.notify",
                  self.subscr_notify)),
                self.id,
                self.config['extranonce_size']),
               'error': None,
               'id': self.msg_id}
        self.subscribed = True
        self.logger.debug("Sending subscribe response: {}".format(pformat(ret)))
        self.write_queue.put(json.dumps(ret) + "\n")

    def read_loop(self):
        while True:
            if self._disconnected:
                self.logger.debug("Read loop encountered flag from write, exiting")
                break

            line = with_timeout(self.config['push_job_interval'] - self.time_seed,
                                self.fp.readline,
                                timeout_value='timeout')

            # push a new job every timeout seconds if requested
            if line == 'timeout':
                if self.authenticated is True:
                    if self.config['vardiff']['enabled']:
                        self.recalc_vardiff()
                    self.logger.info("Pushing new job to client {}.{} after timeout"
                                     .format(self.address, self.worker))
                    self.push_job()
                continue

            line = line.strip()

            # if there's data to read, parse it as json
            if len(line):
                try:
                    data = json.loads(line)
                except ValueError:
                    self.logger.warn("Data {} not JSON".format(line))
                    self.send_error()
                    continue
            else:
                break

            # set the msgid
            self.msg_id = data.get('id', 1)
            self.logger.debug("Data {} recieved on client {}".format(data, self.id))

            if 'method' in data:
                meth = data['method'].lower()
                if meth == 'mining.subscribe':
                    if self.subscribed is True:
                        self.send_error()
                        continue

                    self.subscribe(data)
                elif meth == "mining.authorize":
                    if self.subscribed is False:
                        self.send_error(25)
                        continue
                    if self.authenticated is True:
                        self.send_error()
                        continue

                    self.authenticate(data)
                elif meth == "mining.submit":
                    if self.authenticated is False:
                        self.send_error(24)
                        continue

                    outcome, diff = self.submit_job(data)
                    if self.VALID_SHARE == outcome or self.BLOCK_FOUND == outcome:
                        self.accepted_shares += diff

                    # log the share results for aggregation and transmission
                    self.client_manager.log_share(self.address, self.worker,
                                                  diff, outcome)

                    # don't recalc their diff more often than interval
                    if (self.config['vardiff']['enabled'] and
                            (time.time() - self.last_diff_adj) > self.config['vardiff']['interval']):
                        self.recalc_vardiff()
                else:
                    self.logger.warn("Unkown action for command {}"
                                     .format(self.peer_name[0]))
                    self.send_error()
            else:
                self.logger.warn("Unkown action for command {}"
                                 .format(self.peer_name[0]))
                self.send_error()
