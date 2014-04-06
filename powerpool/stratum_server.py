import json
import socket
import logging

from time import time
from binascii import hexlify, unhexlify
from struct import pack, unpack
from bitcoinrpc import CoinRPCException
from cryptokit import target_from_diff
from hashlib import sha256
from gevent import sleep, with_timeout, spawn
from gevent.event import Event
from gevent.queue import Queue
from hashlib import sha1
from os import urandom
from pprint import pformat

from .server import GenericServer, GenericClient


class StratumServer(GenericServer):

    def __init__(self, listener, stratum_clients, config, net_state,
                 server_state, celery, **kwargs):
        super(GenericServer, self).__init__(listener, **kwargs)
        self.stratum_clients = stratum_clients
        self.config = config
        self.net_state = net_state
        self.server_state = server_state
        self.celery = celery
        self.id_count = 0

    def handle(self, sock, address):
        # Warning: Not thread safe in the slightest, should be good for gevent
        self.id_count += 1
        self.server_state['stratum_connects'].incr()
        StratumClient(sock, address, self.id_count, self.stratum_clients,
                      self.config, self.net_state, self.server_state, self.celery)


class StratumClient(GenericClient):
    logger = logging.getLogger('stratum_server')

    errors = {20: 'Other/Unknown',
              21: 'Job not found (=stale)',
              22: 'Duplicate share',
              23: 'Low difficulty share',
              24: 'Unauthorized worker',
              25: 'Not subscribed'}

    # constansts for share submission outcomes. returned by the share checker
    STALE_SHARE = 21
    LOW_DIFF = 23
    DUP_SHARE = 22
    BLOCK_FOUND = 2
    VALID_SHARE = 1

    # a list of different share types to be used when collecting share
    # statistics (faster than remaking the list repeatedly)
    share_types = ['valid_shares', 'dup_shares', 'low_diff_shares',
                   'stale_shares']

    # a simple mapping of outcomes to indexes for how they're stored in local
    # memory until they get shipped of for graphing
    outcome_to_idx = {
        STALE_SHARE: 'stale_shares',
        LOW_DIFF: 'low_diff_shares',
        DUP_SHARE: 'dup_shares',
        BLOCK_FOUND: 'valid_shares',
        VALID_SHARE: 'valid_shares'
    }

    def __init__(self, sock, address, id, stratum_clients, config, net_state,
                 server_state, celery):
        self.logger.info("Recieving stratum connection from addr {} on sock {}"
                         .format(address, sock))

        # Seconds before sending keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 120)
        # Interval in seconds between keepalive probes
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 1)
        # Failed keepalive probles before declaring other end dead
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 5)

        # global items
        self.config = config
        self.net_state = net_state
        self.stratum_clients = stratum_clients
        self.server_state = server_state
        self.celery = celery

        # register client into the client dictionary
        self.sock = sock

        # flags for current connection state
        self._disconnected = False
        self.peer_name = sock.getpeername()
        self.authenticated = False
        self.subscribed = False
        self.address = None
        self.worker = ''
        # the worker id. this is also extranonce 1
        self.id = hexlify(pack('Q', id))
        self.stratum_clients[self.id] = self
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
        # an index of jobs and their difficulty
        self.job_mapper = {}
        # last time the difficulty was changed
        self.diff_change = None
        # last time we sent graphing data to the server
        self.last_graph_transmit = time()
        self.last_diff_adj = time()
        self.difficulty = self.config['start_difficulty']
        self.connection_time = int(time())
        self.msg_id = None
        self.fp = sock.makefile()

        # trigger to send a new block notice to a user
        self.new_block_event = None
        self.new_block_event = Event()
        self.new_block_event.rawlink(self.new_block_call)

        # where we put all the messages that need to go out
        self.write_queue = Queue()

        read_greenlet = spawn(self.write_loop)
        try:
            self.read_loop()
        except socket.error:
            pass
        except Exception:
            self.logger.error("Unhandled exception!", exc_info=True)
        finally:
            read_greenlet.kill()
            self.report_shares(flush=True)
            self.server_state['stratum_disconnects'].incr()
            del self.stratum_clients[self.id]
            addr_worker = (self.address, self.worker)
            # clear the worker from the luts
            try:
                del self.stratum_clients['addr_worker_lut'][addr_worker]
            except KeyError:
                pass
            try:
                # remove from lut for address
                self.stratum_clients['address_lut'][self.address].remove(self)
                # delete the list if its empty
                if not len(self.stratum_clients['address_lut'][self.address]):
                    del self.stratum_clients['address_lut'][self.address]
            except (ValueError, KeyError):
                pass

        self.logger.info("Closing connection for client {}".format(self.id))

    @property
    def summary(self):
        return dict(hr=self.hashrate, worker=self.worker, address=self.address)

    @property
    def hashrate(self):
        return (sum(self.valid_shares) * (2 ** 16)) / 1000000 / 600.0

    @property
    def details(self):
        return dict(dup_shares=self.dup_shares,
                    stale_shares=self.stale_shares,
                    low_diff_shares=self.low_diff_shares,
                    valid_shares=self.valid_shares,
                    worker=self.worker,
                    address=self.address,
                    connection_time=self.connection_time_dt)

    # watch for new block announcements and push accordingly
    def new_block_call(self, event):
        """ An event triggered by the network monitor when it learns of a
        new block on the network. All old work is now useless so must be
        flushed. """
        self.push_job(flush=True)

    def send_error(self, num=20, id_val=1):
        """ Utility for transmitting an error to the client """
        err = {'id': id_val,
               'result': None,
               'error': (num, self.errors[num], None)}
        self.logger.debug("error response: {}".format(pformat(err)))
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
        self.diff_change = int(time())
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
            jobid = self.net_state['latest_job']
            try:
                job = self.net_state['jobs'][jobid]
                break
            except KeyError:
                self.logger.warn("No jobs available for worker!")
                sleep(0.5)

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
                int_nonce=unpack(str("<L"), unhexlify(params[4]))))

        try:
            difficulty, jobid = self.job_mapper[data['params'][1]]
            job = self.net_state['jobs'][jobid]
        except KeyError:
            # stale job
            self.send_error(self.STALE_SHARE)
            # TODO: should really try to use the correct diff
            self.server_state['reject_stale'].incr(self.difficulty)
            return self.STALE_SHARE

        header = job.block_header(
            nonce=params[4],
            extra1=self.id,
            extra2=params[2],
            ntime=params[3])

        # Check a submitted share against previous shares to eliminate
        # duplicates
        share = (self.id, params[2], params[4])
        if share in job.acc_shares:
            self.logger.info("Duplicate share rejected from worker {}.{}!"
                             .format(self.address, self.worker))
            self.send_error(self.DUP_SHARE)
            self.server_state['reject_dup'].incr(difficulty)
            return self.DUP_SHARE

        job_target = target_from_diff(difficulty, self.config['diff1'])
        hash_int = self.config['pow_func'](header)
        if hash_int >= job_target:
            self.logger.info("Low diff share rejected from worker {}.{}!"
                             .format(self.address, self.worker))
            self.send_error(self.LOW_DIFF)
            self.server_state['reject_low'].incr(difficulty)
            return self.LOW_DIFF

        # we want to send an ack ASAP, so do it here
        self.send_success(self.msg_id)
        self.logger.info("Valid share accepted from worker {}.{}!"
                         .format(self.address, self.worker))
        # Add the share to the accepted set to check for dups
        job.acc_shares.add(share)
        self.server_state['shares'].incr(difficulty)
        self.celery.send_task_pp('add_share', self.address, difficulty)

        # valid network hash?
        if hash_int >= job.bits_target:
            return self.VALID_SHARE

        try:
            self.logger.log(35, "Valid network block identified!")
            self.logger.info("New block at height %i" % self.net_state['current_height'])
            self.logger.info("Block coinbase hash %s" % job.coinbase.lehexhash)
            block = hexlify(job.submit_serial(header))
            self.logger.log(35, "New block hex dump:\n{}".format(block))
            self.logger.log(35, "Coinbase: {}".format(str(job.coinbase.to_dict())))
            for trans in job.transactions:
                self.logger.log(35, str(trans.to_dict()))
        except Exception:
            # because I'm paranoid...
            self.logger.error("Unexcpected exception in block logging!", exc_info=True)

        def submit_block(conn):
            retries = 0
            while retries < 5:
                try:
                    res = conn.submitblock(block)
                except (CoinRPCException, socket.error, ValueError) as e:
                    self.logger.error("Block failed to submit to the server {}!"
                                      .format(conn.name), exc_info=True)
                    self.logger.error(getattr(e, 'error'))
                else:
                    if res is None:
                        hash_hex = hexlify(
                            sha256(sha256(header).digest()).digest()[::-1])
                        self.celery.send_task_pp(
                            'add_block',
                            self.address,
                            self.net_state['current_height'] + 1,
                            job.total_value,
                            job.fee_total,
                            hexlify(job.bits),
                            hash_hex)
                        self.logger.info("NEW BLOCK ACCEPTED by {}!!!"
                                         .format(conn.name))
                        self.server_state['block_solve'] = int(time())
                        break  # break retry loop if success
                    else:
                        self.logger.error(
                            "Block failed to submit to the server {}, "
                            "server returned {}!".format(conn.name, res),
                            exc_info=True)
                retries += 1
                sleep(1)
                self.logger.info("Retry {} for connection {}".format(retries, conn.name))
        for conn in self.net_state['live_connections']:
            # spawn a new greenlet for each submission to do them all async.
            # lower orphan chance
            spawn(submit_block, conn)

        return self.BLOCK_FOUND

    def report_shares(self, flush=False):
        """ Goes through the list of recorded shares and aggregates them
        into one minute chunck for graph. Sends a task to record the minute
        chunks when one it found greater than zero. Also records a share
        into internal records.

        Flush argument designates whether we're closing the connection and
        should flush all shares we know about. If False only shares up until
        the last complete minute will be reported. """
        now = int(time())
        if (now - self.last_graph_transmit) > 90:
            # bounds for a share to be grouped for transmission in minute
            # chunks. the upper bound is the last minute that has
            # completely passed, while the lower bound is the last time we
            # sent graph data (which should also be an exact round minute,
            # ensuring that we don't submit info for the same minute twice)
            upper = (now // 60) * 60
            # will cause all shares to get reported
            if flush:
                upper += 120
            lower = self.last_graph_transmit
            # share records that are to be discarded
            expire = now - self.config['keep_share']
            # for transmission
            chunks = {}
            # a list of indexes that have expired and are ready for removal
            rem = []
            for i, key in enumerate(self.share_types):
                for stamp, shares in getattr(self, key).iteritems():
                    if stamp >= lower and stamp < upper:
                        minute = (stamp // 60) * 60
                        chunks.setdefault(minute, [0, 0, 0, 0])
                        chunks[minute][i] += shares
                    if stamp < expire:
                        rem.append((key, stamp))

            for key, stamp in rem:
                del getattr(self, key)[stamp]

            for stamp, shares in chunks.iteritems():
                self.celery.send_task_pp(
                    'add_one_minute', self.address, shares[0],
                    stamp, self.worker, *shares[1:])
            self.last_graph_transmit = upper

        # don't recalc their diff more often than interval
        if (self.config['vardiff']['enabled'] and
            now - self.last_diff_adj > self.config['vardiff']['interval']):
            self.recalc_vardiff()

    def log_share(self, outcome):
        now = int(time())
        self.report_shares()
        key = self.outcome_to_idx[outcome]
        getattr(self, key).setdefault(now, 0)
        getattr(self, key)[now] += self.difficulty

    def authenticate(self, data):
        username = data.get('params', [None])[0]
        self.logger.info("Authentication request from {} for username {}"
                         .format(self.peer_name[0], username))
        user_worker = self.convert_username(username)
        # setup lookup table for easier access from other read sources
        self.stratum_clients['addr_worker_lut'][user_worker] = self
        self.stratum_clients['address_lut'].setdefault(user_worker[0], [])
        self.stratum_clients['address_lut'][user_worker[0]].append(self)
        # unpack into state dictionary
        self.address, self.worker = user_worker
        self.authenticated = True
        self.send_success(self.msg_id)
        self.push_difficulty()
        self.push_job()

    def recalc_vardiff(self):
        n1 = 0
        stot = 0
        for stamp, shares in self.valid_shares.iteritems():
            if stamp > self.last_diff_adj:
                n1 += shares
                stot += 1
        # calculate shares per minute and load their current diff tier
        mins = (time() - self.last_diff_adj) / 60
        spm = stot / mins
        spm_tar = self.config['vardiff']['spm_target']
        self.logger.info("VARDIFF: Calculated client {} SPM as {}"
                         .format(self.id, spm))
        if (spm > (spm_tar * self.config['vardiff']['historesis']) or
                spm < (spm_tar / self.config['vardiff']['historesis'])):
            # ideal difficulty is the n1 shares they solved divided by target
            # shares per minute
            ideal_diff = (n1 / mins) / spm_tar
            # find the closest tier for them
            new_diff = min(self.config['vardiff']['tiers'], key=lambda x: abs(x - ideal_diff))

            if new_diff != self.difficulty:
                self.logger.info(
                    "VARDIFF: Moving to D{} from D{}".format(new_diff, self.difficulty))
                self.difficulty = new_diff
                self.push_difficulty()
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
                break

            line = with_timeout(self.config['push_job_interval'],
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
            if line:
                try:
                    data = json.loads(line)
                except ValueError:
                    self.logger.debug("Data {} not JSON".format(line))
                    self.send_error()
                    continue
            else:
                self.send_error()
                sleep(1)
                continue

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

                    res = self.submit_job(data)
                    if res:
                        self.log_share(res)
            else:
                self.logger.warn("Unkown action for command {}".format(data))
                self.send_error()

        self.sock.shutdown(socket.SHUT_WR)
        self.sock.close()
